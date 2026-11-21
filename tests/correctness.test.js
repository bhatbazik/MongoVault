import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import pino from "pino";
import { describe, expect, it } from "vitest";
import { writeManifest } from "../src/catalog.js";
import { loadConfig } from "../src/config.js";
import { createDecryptor, createEncryptor, generateKey, parseKey } from "../src/crypto.js";
import { extractDbPathFromCmdOpts, restorePhysicalBackup } from "../src/physical.js";
import { verifyBackup } from "../src/restore.js";
import { deleteBackup } from "../src/retention.js";
import { FilesystemStorageClient, computeS3UploadParams } from "../src/storage.js";
import { createTarPackStream, extractTarStream } from "../src/tar.js";

const silentLogger = pino({ level: "silent" });

describe("correctness & small-instance scale guards", () => {
  it("auto-scales S3 multipart partSize and clamps queueSize for multi-TB uploads on small RAM", () => {
    const MIB = 1024 * 1024;
    const small = computeS3UploadParams(32 * MIB, 4, 100 * MIB);
    expect(small.partSize).toBe(32 * MIB);
    expect(small.queueSize).toBe(4);

    // 1 TiB archive with 32 MiB default partSize would exceed 10,000 parts -> scales up to >= 128 MiB & queueSize 1
    const oneTb = computeS3UploadParams(32 * MIB, 4, 1024 * 1024 * MIB);
    expect(oneTb.partSize).toBeGreaterThanOrEqual(128 * MIB);
    expect(oneTb.queueSize).toBe(1);

    // 5 TiB caps at 512 MiB partSize and queueSize 1
    const fiveTb = computeS3UploadParams(5 * MIB, 8, 5 * 1024 * 1024 * MIB);
    expect(fiveTb.partSize).toBe(512 * MIB);
    expect(fiveTb.queueSize).toBe(1);
  });

  it("rejects trailing ciphertext bytes appended after final AEAD frame", async () => {
    const key = parseKey(generateKey());
    const enc = createEncryptor(key);
    const encChunks = [];
    Readable.from([Buffer.from("top-secret-backup-data")]).pipe(enc.stream);
    for await (const c of enc.stream) encChunks.push(c);
    const tampered = Buffer.concat([...encChunks, Buffer.from([0xde, 0xad, 0xbe, 0xef])]);

    const dec = createDecryptor([key], enc.envelope());
    const out = [];
    await expect(
      (async () => {
        Readable.from([tampered]).pipe(dec);
        for await (const c of dec) out.push(c);
      })(),
    ).rejects.toThrow(/trailing data/i);
  });

  it("blocks path traversal in FilesystemStorageClient", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "mv-fs-safe-"));
    try {
      const storage = new FilesystemStorageClient(root);
      await expect(storage.putJson("../../etc/escape.json", { bad: true })).rejects.toThrow(
        /escapes base directory/i,
      );
      await expect(storage.head("../outside")).rejects.toThrow(/escapes base directory/i);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("safely extracts --dbpath from getCmdLineOpts without returning argv[0]", () => {
    expect(extractDbPathFromCmdOpts({ parsed: { storage: { dbPath: "/var/lib/mongo" } } })).toBe(
      "/var/lib/mongo",
    );
    expect(extractDbPathFromCmdOpts({ argv: ["mongod", "--port", "27017"] })).toBeUndefined();
    expect(
      extractDbPathFromCmdOpts({ argv: ["mongod", "--dbpath=/data/db", "--port=27017"] }),
    ).toBe("/data/db");
    expect(extractDbPathFromCmdOpts({ argv: ["mongod", "--dbpath", "/data/db2"] })).toBe(
      "/data/db2",
    );
  });

  it("roundtrips paths > 255 UTF-8 bytes via PAX extended TAR headers and rejects truncated TAR streams", async () => {
    const srcDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-pax-src-"));
    const dstDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-pax-dst-"));
    try {
      const longRelPath = `${"a".repeat(120)}/${"b".repeat(120)}/${"c".repeat(80)}.wt`;
      const srcFile = path.join(srcDir, "sample.wt");
      await fs.writeFile(srcFile, "PAX_LONG_PATH_OK");

      const tarChunks = [];
      for await (const c of createTarPackStream([
        { name: longRelPath, fullPath: srcFile, size: 16 },
      ])) {
        tarChunks.push(c);
      }
      const tarBuf = Buffer.concat(tarChunks);
      const res = await extractTarStream(Readable.from([tarBuf]), dstDir);
      expect(res.extractedCount).toBe(1);
      expect(await fs.readFile(path.join(dstDir, longRelPath), "utf8")).toBe("PAX_LONG_PATH_OK");

      // Truncated TAR (missing EOF blocks) must throw
      const truncDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-pax-trunc-"));
      try {
        await expect(
          extractTarStream(Readable.from([tarBuf.subarray(0, tarBuf.length - 1024)]), truncDir),
        ).rejects.toThrow(/truncated tar archive/i);
      } finally {
        await fs.rm(truncDir, { recursive: true, force: true });
      }
    } finally {
      await fs.rm(srcDir, { recursive: true, force: true });
      await fs.rm(dstDir, { recursive: true, force: true });
    }
  });

  it("refuses physical restore when targetDbPath has a non-empty mongod.lock (live mongod guard)", async () => {
    const targetDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-live-lock-"));
    try {
      await fs.writeFile(path.join(targetDir, "mongod.lock"), "12345\n");
      const mockStorage = {
        async getJson() {
          return { data: { key: "k", size: 1, sha256: "abc" } };
        },
      };
      await expect(
        restorePhysicalBackup({
          config: { mongo: { database: "*" }, storage: { prefix: "p" }, workDir: targetDir },
          storage: mockStorage,
          logger: silentLogger,
          targetDbPath: targetDir,
          ref: "k",
          skipRecovery: true,
        }),
      ).rejects.toThrow(/running mongod/i);
    } finally {
      await fs.rm(targetDir, { recursive: true, force: true });
    }
  });

  it("cascades deletion of child configsvr, shard archives, and bridging slices for .cluster backups", async () => {
    const removed = [];
    const clusterKey = "prod/_cluster/2026-03-10/20260310T120000Z.cluster";
    const cfgKey = "prod/configsvr/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const shardKey = "prod/shards/s1/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const bridgeKey = "prod/shards/s1/_cluster/_bridging/20260310T120000Z/slice.oplog.bson.gz";

    const mockStorage = {
      async *list() {},
      async getJson(k) {
        if (k === `${clusterKey}.manifest.json`) {
          return {
            data: {
              type: "sharded-cluster-backup",
              key: clusterKey,
              sharding: {
                clusterTime: "100:1",
                configsvr: { backupKey: cfgKey, manifestKey: `${cfgKey}.manifest.json` },
                shards: {
                  s1: {
                    shardId: "s1",
                    host: "s1/h:27018",
                    backupKey: shardKey,
                    manifestKey: `${shardKey}.manifest.json`,
                    bridgingSliceKeys: [bridgeKey],
                    status: "success",
                  },
                },
              },
            },
          };
        }
        return null;
      },
      async removeMany(keys) {
        removed.push(...keys);
      },
    };

    await deleteBackup(
      {
        config: { mongo: { database: "*" }, storage: { prefix: "prod" } },
        storage: mockStorage,
        logger: silentLogger,
      },
      { key: clusterKey, force: true },
    );

    expect(removed).toContain(clusterKey);
    expect(removed).toContain(`${clusterKey}.manifest.json`);
    expect(removed).toContain(cfgKey);
    expect(removed).toContain(`${cfgKey}.manifest.json`);
    expect(removed).toContain(shardKey);
    expect(removed).toContain(`${shardKey}.manifest.json`);
    expect(removed).toContain(bridgeKey);
    expect(removed).toContain(`${bridgeKey}.manifest.json`);
  });

  it("verifies sharded .cluster backups recursively including child archives", async () => {
    const objects = new Map();
    const putBytes = (k, buf) => {
      objects.set(k, buf);
      return createHash("sha256").update(buf).digest("hex");
    };

    const cfgKey = "prod/configsvr/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const s1Key = "prod/shards/s1/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const clusterKey = "prod/_cluster/2026-03-10/20260310T120000Z.cluster";

    const cfgGz = gzipSync(Buffer.from("cfg-archive"));
    const s1Gz = gzipSync(Buffer.from("shard1-archive"));
    const cfgSha = putBytes(cfgKey, cfgGz);
    const s1Sha = putBytes(s1Key, s1Gz);

    const clusterMeta = {
      clusterTime: "100:1",
      configsvr: { backupKey: cfgKey, manifestKey: `${cfgKey}.manifest.json` },
      shards: {
        s1: {
          shardId: "s1",
          host: "s1/h:27018",
          backupKey: s1Key,
          manifestKey: `${s1Key}.manifest.json`,
          status: "success",
          size: s1Gz.length,
        },
      },
    };
    const clusterBuf = Buffer.from(JSON.stringify(clusterMeta));
    const clusterSha = putBytes(clusterKey, clusterBuf);

    const mockStorage = {
      async putJson(k, v) {
        objects.set(k, Buffer.from(JSON.stringify(v)));
      },
      async getJson(k) {
        const b = objects.get(k);
        return b ? { data: JSON.parse(b.toString("utf8")) } : null;
      },
      async head(k) {
        const b = objects.get(k);
        return b ? { size: b.length, lastModified: new Date() } : null;
      },
      async download(k) {
        return Readable.from([objects.get(k)]);
      },
    };

    await writeManifest(mockStorage, {
      version: 2,
      key: cfgKey,
      database: "*",
      createdAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      durationMs: 1,
      size: cfgGz.length,
      sha256: cfgSha,
      compression: "gzip",
      encryption: null,
      oplog: true,
      excludedCollections: [],
      collections: {},
    });
    await writeManifest(mockStorage, {
      version: 2,
      key: s1Key,
      database: "*",
      createdAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      durationMs: 1,
      size: s1Gz.length,
      sha256: s1Sha,
      compression: "gzip",
      encryption: null,
      oplog: true,
      excludedCollections: [],
      collections: {},
    });
    await writeManifest(mockStorage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      durationMs: 1,
      size: cfgGz.length + s1Gz.length,
      sha256: clusterSha,
      compression: "none",
      encryption: null,
      oplog: true,
      excludedCollections: [],
      collections: {},
      sharding: clusterMeta,
    });

    const res = await verifyBackup(
      {
        config: { mongo: { database: "*" }, storage: { prefix: "prod" }, encryptionKeyRing: [] },
        storage: mockStorage,
        logger: silentLogger,
      },
      clusterKey,
    );
    expect(res.sha256).toBe(clusterSha);
  });

  it("reads AWS_REGION from passed env object in loadConfig instead of process.env", () => {
    const cfg = loadConfig({
      MONGODB_URI: "mongodb://localhost:27017",
      STORAGE_TYPE: "s3",
      S3_BUCKET: "b",
      S3_ACCESS_KEY_ID: "ak",
      S3_SECRET_ACCESS_KEY: "sk",
      AWS_REGION: "eu-central-1",
    });
    expect(cfg.storage.region).toBe("eu-central-1");
  });
});
