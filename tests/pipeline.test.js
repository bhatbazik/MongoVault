import { Buffer } from "node:buffer";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import pino from "pino";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { runBackup } from "../src/backup.js";
import { generateKey, parseKey } from "../src/crypto.js";
import { restoreBackup, verifyBackup } from "../src/restore.js";

describe("backup and restore pipeline", () => {
  let tmpDir;
  let logger;
  let objects;
  let mockStorage;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "pipeline-test-"));
    logger = pino({ level: "silent" });
    objects = new Map();

    mockStorage = {
      bucket: "test-bucket",
      async upload(key, body) {
        const chunks = [];
        for await (const chunk of body) chunks.push(chunk);
        const data = Buffer.concat(chunks);
        objects.set(key, { data, size: data.length, metadata: {} });
      },
      async head(key) {
        const obj = objects.get(key);
        if (!obj) return null;
        return { size: obj.size, lastModified: new Date(), metadata: obj.metadata };
      },
      async download(key) {
        const obj = objects.get(key);
        if (!obj) throw new Error("NoSuchKey");
        return Readable.from(obj.data);
      },
      async putJson(key, value) {
        objects.set(key, {
          data: Buffer.from(JSON.stringify(value)),
          size: Buffer.byteLength(JSON.stringify(value)),
          json: value,
        });
      },
      async getJson(key) {
        const obj = objects.get(key);
        if (!obj?.json) return null;
        return obj.json;
      },
      async remove(key) {
        objects.delete(key);
      },
      async *list(prefix) {
        for (const [key, obj] of objects.entries()) {
          if (key.startsWith(prefix)) {
            yield { key, size: obj.size, lastModified: new Date() };
          }
        }
      },
    };
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  function createMockTools(archiveBytes) {
    let capturedRestoreBytes = [];

    const mockDump = async () => {
      const stream = Readable.from(archiveBytes);
      return {
        stdout: stream,
        done: Promise.resolve({ collections: { users: 10, orders: 5 } }),
      };
    };

    const mockRestore = async () => {
      const stdin = new PassThrough();
      stdin.on("data", (chunk) => capturedRestoreBytes.push(chunk));
      return {
        stdin,
        done: Promise.resolve({ restored: 15, failed: 0 }),
      };
    };

    return {
      tools: { mongodump: mockDump, mongorestore: mockRestore },
      getCapturedRestore: () => Buffer.concat(capturedRestoreBytes),
    };
  }

  it("runs unencrypted backup, verifies, and restores successfully with zero disk", async () => {
    const rawGzip = gzipSync(Buffer.from("dummy-mongodb-archive-payload"));
    const { tools, getCapturedRestore } = createMockTools(rawGzip);

    const config = {
      mongo: { uri: "mongodb://localhost", database: "orders", excludeCollections: [] },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    const manifest = await runBackup({ config, storage: mockStorage, logger, tools });
    expect(manifest.size).toBe(rawGzip.length);
    expect(manifest.collections.users).toBe(10);

    const verified = await verifyBackup({ config, storage: mockStorage, logger }, manifest.key);
    expect(verified.size).toBe(rawGzip.length);
    expect(verified.sha256).toBe(manifest.sha256);

    const restored = await restoreBackup(
      { config, storage: mockStorage, logger, tools },
      { ref: manifest.key, uri: "mongodb://localhost", database: "orders" },
    );
    expect(restored.restored).toBe(15);
    expect(getCapturedRestore()).toEqual(rawGzip);

    // Verify zero temporary files left on disk
    const files = await fs.readdir(tmpDir);
    expect(files).toHaveLength(0);
  });

  it("runs client-side encrypted backup and verifies/restores with key", async () => {
    const rawGzip = gzipSync(Buffer.from("encrypted-secret-archive-payload"));
    const { tools, getCapturedRestore } = createMockTools(rawGzip);
    const key = parseKey(generateKey());

    const config = {
      mongo: { uri: "mongodb://localhost", database: "patients", excludeCollections: [] },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: key,
      schedule: { timezone: "UTC" },
    };

    const manifest = await runBackup({ config, storage: mockStorage, logger, tools });
    expect(manifest.encryption).not.toBeNull();
    expect(manifest.key).toMatch(/\.archive\.gz\.enc$/);

    const verified = await verifyBackup({ config, storage: mockStorage, logger }, manifest.key);
    expect(verified.encrypted).toBe(true);

    const restored = await restoreBackup(
      { config, storage: mockStorage, logger, tools },
      { ref: manifest.key, uri: "mongodb://localhost", database: "patients" },
    );
    expect(restored.restored).toBe(15);
    expect(getCapturedRestore()).toEqual(rawGzip);

    // Verify zero temporary files left on disk
    const files = await fs.readdir(tmpDir);
    expect(files).toHaveLength(0);
  });

  it("fails verification if object data is corrupted in storage", async () => {
    const rawGzip = gzipSync(Buffer.from("some-payload"));
    const { tools } = createMockTools(rawGzip);

    const config = {
      mongo: { uri: "mongodb://localhost", database: "test", excludeCollections: [] },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    const manifest = await runBackup({ config, storage: mockStorage, logger, tools });

    const obj = objects.get(manifest.key);
    obj.data[0] ^= 0xff;

    await expect(
      verifyBackup({ config, storage: mockStorage, logger }, manifest.key),
    ).rejects.toThrow();
  });

  it("records oplog enabled status in manifest", async () => {
    const rawGzip = gzipSync(Buffer.from("oplog-backup-payload"));
    const { tools } = createMockTools(rawGzip);

    const config = {
      mongo: { uri: "mongodb://localhost", database: "*", excludeCollections: [], oplog: true },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    const manifest = await runBackup({ config, storage: mockStorage, logger, tools });
    expect(manifest.oplog).toBe(true);
  });

  it("rejects tampered encrypted manifest via HMAC authentication", async () => {
    const rawGzip = gzipSync(Buffer.from("authenticated-manifest-payload"));
    const { tools } = createMockTools(rawGzip);
    const key = parseKey(generateKey());

    const config = {
      mongo: { uri: "mongodb://localhost", database: "patients", excludeCollections: [] },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: key,
      schedule: { timezone: "UTC" },
    };

    const manifest = await runBackup({ config, storage: mockStorage, logger, tools });
    expect(manifest.encryption.manifestHmac).toBeTypeOf("string");

    // Tamper with collection counts in stored manifest
    const manifestObj = objects.get(`${manifest.key}.manifest.json`);
    manifestObj.json.collections.users = 0;

    await expect(
      verifyBackup({ config, storage: mockStorage, logger }, manifest.key),
    ).rejects.toThrow(/Manifest HMAC verification failed/);
  });

  it("supports cluster-wide backup when database is *", async () => {
    const rawGzip = gzipSync(Buffer.from("cluster-payload"));
    const mockDump = async () => ({
      stdout: Readable.from(rawGzip),
      done: Promise.resolve({
        collections: { "db1.users": 100, "db2.orders": 200 },
      }),
    });

    const config = {
      mongo: { uri: "mongodb://localhost", database: "*", excludeCollections: [] },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    const manifest = await runBackup({
      config,
      storage: mockStorage,
      logger,
      tools: { mongodump: mockDump },
    });

    expect(manifest.key).toContain("backups/_cluster/");
    expect(manifest.collections["db1.users"]).toBe(100);
    expect(manifest.collections["db2.orders"]).toBe(200);
  });
});
