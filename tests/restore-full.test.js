import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import { describe, expect, it, vi } from "vitest";
import { writeManifest } from "../src/catalog.js";
import { createEncryptor, generateKey } from "../src/crypto.js";
import { IntegrityError, describeBackup, restoreBackup, verifyBackup } from "../src/restore.js";
import { createFilesystemStorage } from "../src/storage.js";
import { runStreamPipeline } from "../src/stream.js";

function makeLogger() {
  const logger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: () => logger,
  };
  return logger;
}

async function encryptBuffer(key, buf) {
  const enc = createEncryptor(key);
  const out = new PassThrough();
  const chunks = [];
  out.on("data", (c) => chunks.push(c));
  await runStreamPipeline([Readable.from([buf]), enc.stream, out]);
  return { data: Buffer.concat(chunks), envelope: enc.envelope() };
}

describe("Restore & Recursive Verification (restore.ts full coverage)", () => {
  it("verifyBackup recursively verifies encrypted .cluster.enc and all child archives/slices", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-verify-sharded-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const logger = makeLogger();
    const encKey = generateKey();
    const config = {
      workDir,
      encryptionKey: encKey,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
    };

    try {
      const cfgKey = "mv/configsvr/b1.archive.gz.enc";
      const s1Key = "mv/shards/rs1/b1.archive.gz.enc";
      const brKey = "mv/shards/rs1/_bridging/b1.oplog.gz.enc";
      const clusterKey = "mv/_cluster/2026-01-01.cluster.enc";

      for (const [k, raw] of [
        [cfgKey, gzipSync(Buffer.from("cfg-archive"))],
        [s1Key, gzipSync(Buffer.from("shard-archive"))],
        [brKey, gzipSync(Buffer.from("bridge-oplog"))],
      ]) {
        const { data, envelope } = await encryptBuffer(encKey, raw);
        await storage.upload(k, data);
        await writeManifest(
          storage,
          {
            version: 1,
            key: k,
            database: "*",
            createdAt: "2026-01-01T00:00:00Z",
            completedAt: "2026-01-01T00:00:01Z",
            durationMs: 10,
            size: data.length,
            sha256: createHash("sha256").update(data).digest("hex"),
            compression: "gzip",
            encryption: envelope,
            collections: {},
          },
          encKey,
        );
      }

      const clusterJson = Buffer.from(
        JSON.stringify({
          clusterTime: "100:1",
          configsvr: { backupKey: cfgKey, status: "success" },
          shards: { rs1: { backupKey: s1Key, bridgingSliceKeys: [brKey], status: "success" } },
        }),
      );
      const encCluster = await encryptBuffer(encKey, clusterJson);
      await storage.upload(clusterKey, encCluster.data);
      await writeManifest(
        storage,
        {
          version: 1,
          type: "sharded-cluster-backup",
          key: clusterKey,
          database: "*",
          createdAt: "2026-01-01T00:00:00Z",
          completedAt: "2026-01-01T00:00:01Z",
          durationMs: 10,
          size: encCluster.data.length,
          sha256: createHash("sha256").update(encCluster.data).digest("hex"),
          compression: "gzip",
          encryption: encCluster.envelope,
          collections: {},
        },
        encKey,
      );

      const res = await verifyBackup({ config, storage, logger }, clusterKey);
      expect(res.encrypted).toBe(true);
      expect(res.key).toBe(clusterKey);

      // Tamper with child shard object -> recursive verify fails with IntegrityError
      await storage.upload(s1Key, Buffer.from("corrupted"));
      await expect(verifyBackup({ config, storage, logger }, clusterKey)).rejects.toThrow(
        IntegrityError,
      );
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("describeBackup and verifyBackup handle missing checksum and checksum/size mismatches", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-verify-err-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const logger = makeLogger();
    const config = {
      workDir,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "app" },
    };

    try {
      const k1 = "mv/app/no-manifest.archive.gz";
      await storage.upload(k1, gzipSync(Buffer.from("data")));
      const desc = await describeBackup({ config, storage, logger }, k1);
      expect(desc.manifest).toBeNull();
      expect(desc.checksum).toBeNull();

      await expect(verifyBackup({ config, storage, logger }, k1)).rejects.toThrow(
        /no manifest or checksum/,
      );

      const gz = gzipSync(Buffer.from("valid-gzip"));
      const k2 = "mv/app/bad-size.archive.gz";
      await storage.upload(k2, gz);
      await writeManifest(storage, {
        version: 1,
        key: k2,
        database: "app",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:00:01Z",
        durationMs: 1,
        size: gz.length + 100, // wrong size
        sha256: createHash("sha256").update(gz).digest("hex"),
        compression: "gzip",
        encryption: null,
        collections: {},
      });

      await expect(verifyBackup({ config, storage, logger }, k2)).rejects.toThrow(/Size mismatch/);
      await expect(
        restoreBackup({ config, storage, logger }, { ref: k2, preVerify: true }),
      ).rejects.toThrow(/Size mismatch/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
