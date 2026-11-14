import { Buffer } from "node:buffer";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import { BSON } from "mongodb";
import pino from "pino";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { runBackup } from "../src/backup.js";
import { archiveKey, manifestKey, scopeOf } from "../src/catalog.js";
import { generateKey, parseKey } from "../src/crypto.js";
import {
  captureOplogSlice,
  compareTimestamps,
  downloadAndExtractOplogSlice,
  formatOplogTimestamp,
  fromBsonTimestamp,
  listOplogSlices,
  oplogSliceKey,
  parseOplogTimestamp,
  parsePitrTarget,
  parseSliceKeyTimestamps,
  pitrStateKey,
  resolvePitrPlan,
  toBsonTimestamp,
  uploadOplogSlice,
} from "../src/oplog.js";
import { restoreBackup } from "../src/restore.js";
import { prune } from "../src/retention.js";

describe("Continuous Oplog Archiver and PITR Restore Coordinator", () => {
  let tmpDir;
  let logger;
  let objects;
  let mockStorage;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "pitr-test-"));
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
            yield { key, size: obj.size, lastModified: obj.lastModified || new Date() };
          }
        }
      },
    };
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  function createConfig(encryptionKey = null) {
    return {
      mongo: {
        uri: "mongodb://localhost:27017/?replicaSet=rs0",
        database: "*",
        oplog: true,
        gzip: true,
        parallelCollections: 2,
      },
      storage: { prefix: "mongo-backups" },
      schedule: { timezone: "UTC" },
      pitr: {
        enabled: true,
        flushIntervalSeconds: 30,
        maxSliceMb: 64,
      },
      encryptionKey,
      encryptionKeyRing: encryptionKey ? [encryptionKey] : [],
      retention: { keepLast: 1, keepDaily: 7, keepWeekly: 4, keepMonthly: 3 },
      workDir: tmpDir,
      lockTtlMs: 60_000,
    };
  }

  function createFakeMongoClientFactory(oplogDocs) {
    return () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd && cmd.replSetGetStatus) {
                const newest = oplogDocs[oplogDocs.length - 1];
                return {
                  optimes: {
                    lastCommittedOpTime: {
                      ts: newest ? newest.ts : toBsonTimestamp({ t: 9999999, i: 1 }),
                    },
                  },
                };
              }
              return {};
            },
          };
        }
        if (name === "config") {
          return {
            collection(colName) {
              if (colName === "transactions") {
                return {
                  find() {
                    return {
                      sort() {
                        return this;
                      },
                      limit() {
                        return this;
                      },
                      async next() {
                        return null;
                      },
                    };
                  },
                };
              }
              return {};
            },
          };
        }
        return {
          collection() {
            return {
              find(filter = {}) {
                let filtered = [...oplogDocs];
                if (filter.ts?.$gt) {
                  const gt = fromBsonTimestamp(filter.ts.$gt);
                  filtered = filtered.filter(
                    (doc) => compareTimestamps(fromBsonTimestamp(doc.ts), gt) > 0,
                  );
                }
                if (filter.ts?.$lte) {
                  const lte = fromBsonTimestamp(filter.ts.$lte);
                  filtered = filtered.filter(
                    (doc) => compareTimestamps(fromBsonTimestamp(doc.ts), lte) <= 0,
                  );
                }
                const cursor = {
                  sort(spec) {
                    const dir = spec.$natural ?? spec.ts ?? 1;
                    filtered.sort((a, b) => {
                      const cmp = compareTimestamps(
                        fromBsonTimestamp(a.ts),
                        fromBsonTimestamp(b.ts),
                      );
                      return dir === -1 ? -cmp : cmp;
                    });
                    return cursor;
                  },
                  limit(n) {
                    filtered = filtered.slice(0, n);
                    return cursor;
                  },
                  async next() {
                    return filtered[0] || null;
                  },
                  async *[Symbol.asyncIterator]() {
                    for (const doc of filtered) {
                      yield doc;
                    }
                  },
                };
                return cursor;
              },
            };
          },
        };
      },
    });
  }

  it("parses and compares oplog timestamps and ISO-8601 PITR targets", () => {
    const ts1 = parseOplogTimestamp("1774092000:1");
    const ts2 = parseOplogTimestamp("1774092000:2");
    const ts3 = parseOplogTimestamp("1774092005:1");

    expect(ts1).toEqual({ t: 1774092000, i: 1 });
    expect(compareTimestamps(ts1, ts2)).toBe(-1);
    expect(compareTimestamps(ts2, ts1)).toBe(1);
    expect(compareTimestamps(ts1, { t: 1774092000, i: 1 })).toBe(0);
    expect(compareTimestamps(ts2, ts3)).toBe(-1);
    expect(formatOplogTimestamp(ts2)).toBe("1774092000:2");

    // ISO-8601 target resolves to end of that second (i = 0xffffffff)
    const isoTarget = parsePitrTarget("2026-03-21T11:20:00Z");
    expect(isoTarget.t).toBe(Math.floor(Date.parse("2026-03-21T11:20:00Z") / 1000));
    expect(isoTarget.i).toBe(0xffffffff);

    // Explicit <sec>:<inc> target preserves exact increment
    expect(parsePitrTarget("1774092000:42")).toEqual({ t: 1774092000, i: 42 });

    // Key formatting and parsing roundtrip
    const key = oplogSliceKey({ prefix: "mongo-backups", database: "_cluster" }, ts1, ts3, {
      encrypted: true,
    });
    expect(key).toBe(
      "mongo-backups/_cluster/_pitr/2026-03-21/1774092000_00000001__1774092005_00000001.oplog.bson.gz.enc",
    );
    expect(parseSliceKeyTimestamps(key)).toEqual({
      startTs: ts1,
      endTs: ts3,
      encrypted: true,
    });
  });

  it("encrypts, signs, downloads, verifies, and extracts BSON oplog slices", async () => {
    const encKey = parseKey(generateKey());
    const config = createConfig(encKey);

    const startTs = { t: 1774092000, i: 1 };
    const endTs = { t: 1774092010, i: 2 };
    const doc1 = BSON.serialize({
      ts: toBsonTimestamp(startTs),
      t: 1n,
      h: 100n,
      v: 2,
      op: "i",
      ns: "app.users",
      o: { _id: "u1", name: "Alice" },
    });
    const doc2 = BSON.serialize({
      ts: toBsonTimestamp(endTs),
      t: 1n,
      h: 101n,
      v: 2,
      op: "u",
      ns: "app.users",
      o2: { _id: "u1" },
      o: { $v: 2, diff: { u: { name: "Alice Smith" } } },
    });

    const rawDocs = [Buffer.from(doc1), Buffer.from(doc2)];
    const expectedBson = Buffer.concat(rawDocs);

    const manifest = await uploadOplogSlice({
      config,
      storage: mockStorage,
      logger,
      rawDocs,
      startTs,
      endTs,
    });

    expect(manifest.type).toBe("oplog-slice");
    expect(manifest.encryption.manifestHmac).toBeDefined();
    expect(manifest.oplogWindow.entries).toBe(2);

    const destFile = path.join(tmpDir, "extracted.oplog.bson");
    await downloadAndExtractOplogSlice({ config, storage: mockStorage }, manifest.key, destFile);

    const diskBytes = await fs.readFile(destFile);
    expect(diskBytes.equals(expectedBson)).toBe(true);
    await fs.rm(destFile, { force: true });

    // Tamper with the slice manifest's oplogWindow and verify HMAC catches it
    const mKey = manifestKey(manifest.key);
    const storedManifest = await mockStorage.getJson(mKey);
    storedManifest.oplogWindow.endTs = "1774099999:1";
    await mockStorage.putJson(mKey, storedManifest);

    await expect(
      downloadAndExtractOplogSlice({ config, storage: mockStorage }, manifest.key, destFile),
    ).rejects.toThrow(/Manifest HMAC verification failed/);
  });

  it("captures incremental oplog slices and detects oplog rollover gaps", async () => {
    const encKey = parseKey(generateKey());
    const config = createConfig(encKey);

    const batch1 = [
      { ts: toBsonTimestamp({ t: 1000, i: 1 }), op: "n", ns: "", o: { msg: "periodic noop" } },
      { ts: toBsonTimestamp({ t: 1005, i: 1 }), op: "i", ns: "app.orders", o: { _id: 1 } },
      { ts: toBsonTimestamp({ t: 1010, i: 1 }), op: "i", ns: "app.orders", o: { _id: 2 } },
    ];

    const manifest1 = await captureOplogSlice({
      config,
      storage: mockStorage,
      logger,
      createMongoClient: createFakeMongoClientFactory(batch1),
    });

    expect(manifest1).not.toBeNull();
    expect(manifest1.oplogWindow.gapDetected).toBe(false);
    expect(manifest1.oplogWindow.startTs).toBe("1000:1");
    expect(manifest1.oplogWindow.endTs).toBe("1010:1");

    // Second run with no new entries returns null
    const manifestNoop = await captureOplogSlice({
      config,
      storage: mockStorage,
      logger,
      createMongoClient: createFakeMongoClientFactory(batch1),
    });
    expect(manifestNoop).toBeNull();

    // Third run where local.oplog.rs rolled over (oldest entry is now 2000:1 > lastTs 1010:1)
    const rolledOverBatch = [
      { ts: toBsonTimestamp({ t: 2000, i: 1 }), op: "i", ns: "app.orders", o: { _id: 99 } },
      { ts: toBsonTimestamp({ t: 2005, i: 1 }), op: "i", ns: "app.orders", o: { _id: 100 } },
    ];
    const manifestGap = await captureOplogSlice({
      config,
      storage: mockStorage,
      logger,
      createMongoClient: createFakeMongoClientFactory(rolledOverBatch),
    });
    expect(manifestGap).not.toBeNull();
    expect(manifestGap.oplogWindow.gapDetected).toBe(true);

    const slices = await listOplogSlices(mockStorage, scopeOf(config));
    expect(slices).toHaveLength(2);
  });

  it("records gapDetected in state.json when CappedPositionLost occurs mid-stream", async () => {
    const encKey = parseKey(generateKey());
    const config = createConfig(encKey);

    const failingClientFactory = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.replSetGetStatus) {
                return {
                  optimes: {
                    lastCommittedOpTime: { ts: toBsonTimestamp({ t: 5000, i: 1 }) },
                  },
                };
              }
              return { ok: 1 };
            },
          };
        }
        return {
          collection(_cName) {
            return {
              find() {
                return {
                  sort() {
                    return this;
                  },
                  limit() {
                    return this;
                  },
                  async next() {
                    return { ts: toBsonTimestamp({ t: 1000, i: 1 }) };
                  },
                  async *[Symbol.asyncIterator]() {
                    const err = new Error("collection overflow: CappedPositionLost");
                    err.code = 136;
                    throw err;
                  },
                };
              },
            };
          },
        };
      },
    });

    await expect(
      captureOplogSlice({
        config,
        storage: mockStorage,
        logger,
        createMongoClient: failingClientFactory,
      }),
    ).rejects.toThrow("CappedPositionLost");

    const state = await mockStorage.getJson(pitrStateKey(scopeOf(config)));
    const gap = state?.gapDetected ?? state?.data?.gapDetected;
    expect(gap).toBe(true);
  });

  it("executes full two-stage PITR restore (base snapshot + contiguous oplog slices) and rejects gaps", async () => {
    const encKey = parseKey(generateKey());
    const config = createConfig(encKey);

    const baseArchiveBytes = gzipSync(Buffer.from("BASE-ARCHIVE-BSON-BYTES"));
    const replayedFiles = [];

    const mockTools = {
      async getOplogBounds() {
        return {
          oldestTs: { t: 1000, i: 1 },
          newestTs: { t: 1010, i: 1 },
        };
      },
      async mongodump() {
        return {
          stdout: Readable.from(baseArchiveBytes),
          done: Promise.resolve({ collections: { "app.users": 2 } }),
        };
      },
      async mongorestore() {
        const stdin = new PassThrough();
        stdin.resume();
        return {
          stdin,
          done: Promise.resolve({ restored: 2, failed: 0 }),
        };
      },
      async replayOplogFile(opts) {
        const bytes = await fs.readFile(opts.oplogFile);
        replayedFiles.push({
          oplogLimit: opts.oplogLimit,
          byteLength: bytes.length,
        });
        return { replayed: 2 };
      },
    };

    // 1. Create a base backup whose oplogWindow covers up to 1010:1
    const baseManifest = await runBackup({
      config,
      storage: mockStorage,
      logger,
      tools: mockTools,
    });
    expect(baseManifest.oplogWindow.endTs).toBe("1010:1");

    // 2. Capture Slice 1 covering (1010:1 -> 1020:1]
    const baseOplogDocs = [
      { ts: toBsonTimestamp({ t: 1000, i: 1 }), op: "i", ns: "app.users", o: { _id: 1 } },
      { ts: toBsonTimestamp({ t: 1010, i: 1 }), op: "i", ns: "app.users", o: { _id: 2 } },
    ];
    const slice1Docs = [
      ...baseOplogDocs,
      { ts: toBsonTimestamp({ t: 1015, i: 1 }), op: "i", ns: "app.users", o: { _id: 3 } },
      { ts: toBsonTimestamp({ t: 1020, i: 1 }), op: "u", ns: "app.users", o: { _id: 3, v: 2 } },
    ];
    await captureOplogSlice({
      config,
      storage: mockStorage,
      logger,
      createMongoClient: createFakeMongoClientFactory(slice1Docs),
    });

    // 3. Capture Slice 2 covering (1020:1 -> 1030:1]
    const slice2Docs = [
      ...slice1Docs,
      { ts: toBsonTimestamp({ t: 1025, i: 1 }), op: "i", ns: "app.users", o: { _id: 4 } },
      { ts: toBsonTimestamp({ t: 1030, i: 1 }), op: "d", ns: "app.users", o: { _id: 1 } },
    ];
    await captureOplogSlice({
      config,
      storage: mockStorage,
      logger,
      createMongoClient: createFakeMongoClientFactory(slice2Docs),
    });

    // 4. Restore to exact point in time 1025:1 (before the delete at 1030:1)
    const result = await restoreBackup(
      { config, storage: mockStorage, logger, tools: mockTools },
      {
        pitr: "1025:1",
        drop: true,
      },
    );

    expect(result.key).toBe(baseManifest.key);
    expect(result.pitrTarget).toBe("1025:1");
    expect(result.replayedSlices).toBe(2);
    expect(result.replayedEntries).toBe(4);
    expect(replayedFiles).toHaveLength(2);
    expect(replayedFiles[0].oplogLimit).toBe("1025:1");
    expect(replayedFiles[1].oplogLimit).toBe("1025:1");

    // Verify no temporary files or directories remain in workDir
    const leftover = await fs.readdir(tmpDir);
    expect(leftover).toEqual([]);

    // 5. Requesting a PITR target beyond archived slices (e.g. 1099:1) fails cleanly
    await expect(resolvePitrPlan(mockStorage, scopeOf(config), "1099:1")).rejects.toThrow(
      /PITR oplog coverage ends at 1030:1/,
    );
  });

  it("prunes expired PITR oplog slices older than the oldest kept base backup", async () => {
    const config = createConfig(null);

    // Create an old slice [500:1 -> 600:1] and a recent slice [1010:1 -> 1020:1]
    const dummyBson = Buffer.from(
      BSON.serialize({ ts: toBsonTimestamp({ t: 500, i: 1 }), op: "n" }),
    );
    const oldSlice = await uploadOplogSlice({
      config,
      storage: mockStorage,
      logger,
      rawDocs: [dummyBson],
      startTs: { t: 500, i: 1 },
      endTs: { t: 600, i: 1 },
    });
    const recentSlice = await uploadOplogSlice({
      config,
      storage: mockStorage,
      logger,
      rawDocs: [dummyBson],
      startTs: { t: 1010, i: 1 },
      endTs: { t: 1020, i: 1 },
    });

    // Create a base backup at 1970-01-01T00:16:40.000Z (epoch = 1000s)
    const baseKey = archiveKey(scopeOf(config), new Date(1000 * 1000), { encrypted: false });
    await mockStorage.upload(baseKey, Readable.from(Buffer.from("archive")));
    await mockStorage.putJson(manifestKey(baseKey), {
      version: 2,
      key: baseKey,
      database: "*",
      createdAt: new Date(1000 * 1000).toISOString(),
      oplogWindow: {
        startTs: "1000:1",
        endTs: "1010:1",
      },
      size: 7,
      sha256: "abc",
    });

    const pruneResult = await prune({ config, storage: mockStorage, logger });

    const removedKeys = pruneResult.remove.map((r) => r.key);
    expect(removedKeys).toContain(oldSlice.key);
    expect(removedKeys).not.toContain(recentSlice.key);

    const remainingSlices = await listOplogSlices(mockStorage, scopeOf(config));
    expect(remainingSlices.map((s) => s.key)).toEqual([recentSlice.key]);
  });

  it("fails closed when replSetGetStatus fails or lacks majority optimes", async () => {
    const encKey = parseKey(generateKey());
    const config = createConfig(encKey);

    const clientFactory = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command() {
              throw new Error("command replSetGetStatus requires authentication");
            },
          };
        }
        return {
          collection() {
            return {
              find() {
                return {
                  sort() {
                    return this;
                  },
                  limit() {
                    return this;
                  },
                  async next() {
                    return { ts: toBsonTimestamp({ t: 1000, i: 1 }) };
                  },
                };
              },
            };
          },
        };
      },
    });

    await expect(
      captureOplogSlice({
        config,
        storage: mockStorage,
        logger,
        createMongoClient: clientFactory,
      }),
    ).rejects.toThrow(/Unable to determine majority-committed oplog timestamp/i);
  });

  it("caps oplog slice upper boundary at transaction startOpTime when in-progress transactions exist", async () => {
    const encKey = parseKey(generateKey());
    const config = createConfig(encKey);

    const oplogDocs = [
      { ts: toBsonTimestamp({ t: 1000, i: 1 }), op: "i", ns: "app.users", o: { _id: 1 } },
      { ts: toBsonTimestamp({ t: 1010, i: 1 }), op: "i", ns: "app.users", o: { _id: 2 } },
      { ts: toBsonTimestamp({ t: 1020, i: 1 }), op: "i", ns: "app.users", o: { _id: 3 } },
    ];

    // Majority point is 1020:1, but there is an active transaction that began at 1010:1
    const clientFactory = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd?.replSetGetStatus) {
                return {
                  optimes: {
                    lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1020, i: 1 }) },
                  },
                };
              }
              return {};
            },
          };
        }
        if (name === "config") {
          return {
            collection(colName) {
              if (colName === "transactions") {
                return {
                  find() {
                    return {
                      sort() {
                        return this;
                      },
                      limit() {
                        return this;
                      },
                      async next() {
                        return {
                          state: "inProgress",
                          startOpTime: { ts: toBsonTimestamp({ t: 1010, i: 1 }) },
                        };
                      },
                    };
                  },
                };
              }
              return {};
            },
          };
        }
        return {
          collection() {
            return {
              find(filter = {}) {
                let filtered = [...oplogDocs];
                if (filter.ts?.$gt) {
                  const gt = fromBsonTimestamp(filter.ts.$gt);
                  filtered = filtered.filter(
                    (doc) => compareTimestamps(fromBsonTimestamp(doc.ts), gt) > 0,
                  );
                }
                if (filter.ts?.$lte) {
                  const lte = fromBsonTimestamp(filter.ts.$lte);
                  filtered = filtered.filter(
                    (doc) => compareTimestamps(fromBsonTimestamp(doc.ts), lte) <= 0,
                  );
                }
                return {
                  sort() {
                    return this;
                  },
                  limit(n) {
                    filtered = filtered.slice(0, n);
                    return this;
                  },
                  async next() {
                    return filtered[0] || null;
                  },
                  async *[Symbol.asyncIterator]() {
                    for (const doc of filtered) {
                      yield doc;
                    }
                  },
                };
              },
            };
          },
        };
      },
    });

    const manifest = await captureOplogSlice({
      config,
      storage: mockStorage,
      logger,
      createMongoClient: clientFactory,
    });

    expect(manifest).not.toBeNull();
    // The slice should only capture up to 1010:1 (the start of the transaction), NOT 1020:1
    expect(manifest.oplogWindow.endTs).toBe("1010:1");
    expect(manifest.oplogWindow.entries).toBe(2);
  });

  it("fails closed if config.transactions query throws an unexpected error", async () => {
    const encKey = parseKey(generateKey());
    const config = createConfig(encKey);

    const clientFactory = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd?.replSetGetStatus) {
                return {
                  optimes: {
                    lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1020, i: 1 }) },
                  },
                };
              }
              return {};
            },
          };
        }
        if (name === "config") {
          return {
            collection(colName) {
              if (colName === "transactions") {
                return {
                  find() {
                    throw new Error("Disk read failure on config database");
                  },
                };
              }
              return {};
            },
          };
        }
        return {
          collection() {
            return {
              find() {
                return {
                  sort() {
                    return this;
                  },
                  limit() {
                    return this;
                  },
                  async next() {
                    return { ts: toBsonTimestamp({ t: 1000, i: 1 }) };
                  },
                };
              },
            };
          },
        };
      },
    });

    await expect(
      captureOplogSlice({
        config,
        storage: mockStorage,
        logger,
        createMongoClient: clientFactory,
      }),
    ).rejects.toThrow(/Failed to verify transaction boundaries in config.transactions/i);
  });

  it("does not split chained multi-document transactions (partialTxn: true) across slices", async () => {
    const config = createConfig(null);
    // Set very small maxSliceMb so byte threshold is exceeded after first doc
    config.pitr.maxSliceMb = 0.00001; // ~10 bytes

    const oplogDocs = [
      {
        ts: toBsonTimestamp({ t: 1000, i: 1 }),
        op: "c",
        ns: "admin.$cmd",
        o: { applyOps: [{ op: "i", ns: "app.items", o: { _id: 1 } }], partialTxn: true },
      },
      {
        ts: toBsonTimestamp({ t: 1000, i: 2 }),
        op: "c",
        ns: "admin.$cmd",
        o: { applyOps: [{ op: "i", ns: "app.items", o: { _id: 2 } }] },
      },
      {
        ts: toBsonTimestamp({ t: 1005, i: 1 }),
        op: "i",
        ns: "app.items",
        o: { _id: 3 },
      },
    ];

    const manifest = await captureOplogSlice({
      config,
      storage: mockStorage,
      logger,
      createMongoClient: createFakeMongoClientFactory(oplogDocs),
    });

    expect(manifest).not.toBeNull();
    // Even though doc 1 exceeded maxBytes, it had partialTxn: true so doc 2 (the final commit) must be included in the same slice
    expect(manifest.oplogWindow.entries).toBe(2);
    expect(manifest.oplogWindow.endTs).toBe("1000:2");
  });
});
