import os from "node:os";
import { Readable } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import { runBackup } from "../src/backup.js";
import { generateKey } from "../src/crypto.js";

function makeMemoryStorage() {
  const blobs = new Map();
  const jsons = new Map();
  return {
    blobs,
    jsons,
    bucket: "mem",
    async upload(key, body, opts) {
      const chunks = [];
      for await (const c of body) chunks.push(Buffer.from(c));
      const buf = Buffer.concat(chunks);
      blobs.set(key, buf);
      opts?.onProgress?.({ loaded: buf.length });
    },
    async head(key) {
      const b = blobs.get(key);
      return b ? { size: b.length, lastModified: new Date() } : null;
    },
    async putJson(key, val) {
      jsons.set(key, val);
    },
    async getJson(key) {
      return jsons.has(key) ? { data: jsons.get(key), etag: "1" } : null;
    },
    async remove(key) {
      blobs.delete(key);
    },
  };
}

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

describe("Logical Backup Runner (backup.ts)", () => {
  const baseConfig = {
    workDir: os.tmpdir(),
    timeoutMs: 30_000,
    storage: { prefix: "mv" },
    mongo: {
      uri: "mongodb://localhost:27017",
      database: "*",
      oplog: true,
      excludeCollections: [],
      parallelCollections: 2,
      readPreference: "secondaryPreferred",
    },
    pitr: { enabled: false },
  };

  it("runs encrypted backup with ArchiveOplogTap bounds and writes manifest", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const encKey = generateKey();

    const manifest = await runBackup({
      config: { ...baseConfig, encryptionKey: encKey },
      storage,
      logger,
      tools: {
        getOplogBounds: vi.fn(async () => ({
          oldestTs: { t: 100, i: 1 },
          newestTs: { t: 200, i: 1 },
        })),
        mongodump: vi.fn(async () => ({
          stdout: Readable.from([Buffer.from("fake-archive-bytes")]),
          done: Promise.resolve({
            collections: { "app.users": 10 },
            oplogStartTs: { t: 201, i: 1 },
            oplogEndTs: { t: 205, i: 2 },
            oplogEntries: 5,
          }),
        })),
      },
    });

    expect(manifest.encryption).not.toBeNull();
    expect(manifest.oplogWindow).toEqual({ startTs: "201:1", endTs: "205:2", entries: 5 });
    expect(storage.jsons.size).toBe(1);
  });

  it("handles quiet dump (oplogEntries === 0) and mocked fallback without tap bounds", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();

    const m1 = await runBackup({
      config: baseConfig,
      storage,
      logger,
      tools: {
        getOplogBounds: vi.fn(async () => ({
          oldestTs: { t: 100, i: 1 },
          newestTs: { t: 150, i: 1 },
        })),
        mongodump: vi.fn(async () => ({
          stdout: Readable.from([Buffer.from("quiet-archive")]),
          done: Promise.resolve({
            collections: {},
            oplogStartTs: null,
            oplogEndTs: null,
            oplogEntries: 0,
          }),
        })),
      },
    });
    expect(m1.oplogWindow).toEqual({ startTs: "150:1", endTs: "150:1", entries: 0 });
    expect(logger.warn).toHaveBeenCalled();

    // Without tap bounds -> triggers post-backup getOplogBounds
    const getOplogBounds = vi
      .fn()
      .mockResolvedValueOnce({ oldestTs: { t: 10, i: 1 }, newestTs: { t: 100, i: 1 } })
      .mockResolvedValueOnce({ oldestTs: { t: 10, i: 1 }, newestTs: { t: 120, i: 3 } });
    const m2 = await runBackup({
      config: baseConfig,
      storage,
      logger,
      tools: {
        getOplogBounds,
        mongodump: vi.fn(async () => ({
          stdout: Readable.from([Buffer.from("mock-archive")]),
          done: Promise.resolve({ collections: { "app.items": 2 } }),
        })),
      },
    });
    expect(m2.oplogWindow).toEqual({ startTs: "100:1", endTs: "120:3" });
    expect(getOplogBounds).toHaveBeenCalledTimes(2);
  });

  it("enforces PITR oplog bounds and cleans up partial uploads on failure", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();

    // Pre-oplog failure under PITR
    await expect(
      runBackup({
        config: { ...baseConfig, pitr: { enabled: true } },
        storage,
        logger,
        tools: {
          getOplogBounds: vi.fn().mockRejectedValueOnce(new Error("no oplog")),
          mongodump: vi.fn(),
        },
      }),
    ).rejects.toThrow(/pre-backup oplog boundary/);

    // Post-oplog failure under PITR
    await expect(
      runBackup({
        config: { ...baseConfig, pitr: { enabled: true } },
        storage,
        logger,
        tools: {
          getOplogBounds: vi
            .fn()
            .mockResolvedValueOnce({ newestTs: { t: 1, i: 1 } })
            .mockRejectedValueOnce(new Error("post oplog err")),
          mongodump: vi.fn(async () => ({
            stdout: Readable.from([Buffer.from("data")]),
            done: Promise.resolve({ collections: { a: 1 } }),
          })),
        },
      }),
    ).rejects.toThrow(/post-backup oplog boundary/);

    // Empty oplogWindow under PITR
    await expect(
      runBackup({
        config: { ...baseConfig, pitr: { enabled: true } },
        storage,
        logger,
        tools: {
          getOplogBounds: vi.fn(async () => ({ oldestTs: null, newestTs: null })),
          mongodump: vi.fn(async () => ({
            stdout: Readable.from([Buffer.from("data")]),
            done: Promise.resolve({ collections: { a: 1 } }),
          })),
        },
      }),
    ).rejects.toThrow(/failed to establish oplogWindow/);

    // Size verification mismatch cleans up partial upload
    const badStorage = makeMemoryStorage();
    badStorage.head = vi.fn(async () => ({ size: 1 }));
    badStorage.remove = vi.fn(async () => {});
    await expect(
      runBackup({
        config: { ...baseConfig, mongo: { ...baseConfig.mongo, oplog: false } },
        storage: badStorage,
        logger,
        tools: {
          mongodump: vi.fn(async () => ({
            stdout: Readable.from([Buffer.from("1234567890")]),
            done: Promise.resolve({ collections: { a: 1 } }),
          })),
        },
      }),
    ).rejects.toThrow(/Upload verification failed/);
    expect(badStorage.remove).toHaveBeenCalled();
  });
});
