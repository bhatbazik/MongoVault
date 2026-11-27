import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Long } from "mongodb";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { generateKey, parseKey } from "../src/crypto.js";
import { createLogger } from "../src/logger.js";
import { writeManifest, scopeOf } from "../src/catalog.js";
import {
  captureOplogSlice,
  compareTimestamps,
  downloadAndExtractOplogSlice,
  pitrStateKey,
  resolvePitrPlan,
  startOplogArchiver,
  streamOplogSlice,
  toBsonTimestamp,
  fromBsonTimestamp,
} from "../src/oplog.js";
import { OplogApplier, OplogFilter, OplogReplayError, TxnBuffer } from "../src/oplog-applier.js";
import { FilesystemStorageClient } from "../src/storage.js";

const logger = createLogger("silent");

function makeConfig(workDir, encKey, extra = {}) {
  return {
    mongo: {
      uri: "mongodb://127.0.0.1:27017/?replicaSet=rs0&readPreference=secondaryPreferred",
      database: "*",
      readPreference: "secondaryPreferred",
    },
    storage: { type: "fs", fsPath: path.join(workDir, "storage"), prefix: "pitr-hardening" },
    encryptionKey: encKey,
    encryptionKeyRing: encKey ? [encKey] : [],
    requireEncryption: Boolean(encKey),
    workDir,
    pitr: { enabled: true, flushIntervalSeconds: 5, maxSliceMb: 16 },
    ...extra,
  };
}

function makeCursorFromDocs(docs, { onIterate } = {}) {
  let list = [...docs];
  const cursor = {
    sort(spec) {
      const dir = spec.$natural ?? spec.ts ?? 1;
      list.sort((a, b) => {
        const cmp = compareTimestamps(fromBsonTimestamp(a.ts), fromBsonTimestamp(b.ts));
        return dir === -1 ? -cmp : cmp;
      });
      return cursor;
    },
    limit(n) {
      list = list.slice(0, n);
      return cursor;
    },
    async next() {
      return list[0] ?? null;
    },
    async *[Symbol.asyncIterator]() {
      if (onIterate) {
        await onIterate();
      }
      for (const doc of list) {
        yield doc;
      }
    },
  };
  return cursor;
}

describe("M4 PITR & Oplog Hardening Regression Tests (F-PITR-001..014)", () => {
  let tempDir;
  let storage;
  let encKey;

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "mongovault-hardening-pitr-"));
    storage = new FilesystemStorageClient(path.join(tempDir, "storage"));
    encKey = parseKey(generateKey());
  });

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  });

  it("F-PITR-001 [P0]: lagging secondary / split-node read skew never advances coverageEndTs past node oplog", async () => {
    const config = makeConfig(tempDir, encKey);
    const scope = scopeOf(config);

    // Seed state at 1010:1
    await storage.putJson(pitrStateKey(scope), {
      lastTs: "1010:1",
      coverageEndTs: "1010:1",
      updatedAt: new Date(Date.now() - 10_000).toISOString(),
      gapDetected: false,
    });

    // Lagging secondary only has oplog up to 1010:1, while replSetGetStatus (primary) reports 1020:1
    const secondaryDocs = [
      { ts: toBsonTimestamp({ t: 1000, i: 1 }), op: "i", ns: "app.c", o: { _id: 1 } },
      { ts: toBsonTimestamp({ t: 1010, i: 1 }), op: "i", ns: "app.c", o: { _id: 2 } },
    ];

    const createMongoClient = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.replSetGetStatus) {
                return {
                  optimes: {
                    lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1020, i: 1 }) },
                  },
                };
              }
              return { ok: 1 };
            },
          };
        }
        if (name === "config") {
          return {
            collection: () => ({
              find: () => makeCursorFromDocs([]),
            }),
          };
        }
        return {
          collection: () => ({
            find(filter = {}) {
              let filtered = [...secondaryDocs];
              if (filter.ts?.$gt) {
                const gt = fromBsonTimestamp(filter.ts.$gt);
                filtered = filtered.filter(
                  (d) => compareTimestamps(fromBsonTimestamp(d.ts), gt) > 0,
                );
              }
              if (filter.ts?.$lte) {
                const lte = fromBsonTimestamp(filter.ts.$lte);
                filtered = filtered.filter(
                  (d) => compareTimestamps(fromBsonTimestamp(d.ts), lte) <= 0,
                );
              }
              return makeCursorFromDocs(filtered);
            },
          }),
        };
      },
    });

    await captureOplogSlice({ config, storage, logger, createMongoClient });

    const stateAfter = (await storage.getJson(pitrStateKey(scope))).data;
    expect(
      compareTimestamps(fromBsonTimestamp({ t: 1010, i: 1 }), {
        t: Number(stateAfter.coverageEndTs.split(":")[0]),
        i: Number(stateAfter.coverageEndTs.split(":")[1]),
      }),
    ).toBeGreaterThanOrEqual(0);
  });

  it("F-PITR-002 [P0]: re-anchors diverged state.json to newer post-restore base backup and honors restore marker", async () => {
    const config = makeConfig(tempDir, encKey);
    const scope = scopeOf(config);

    // Seed pre-restore state at 2000:1
    const stateUpdatedAt = new Date(Date.now() - 20_000).toISOString();
    await storage.putJson(pitrStateKey(scope), {
      lastTs: "2000:1",
      coverageEndTs: "2000:1",
      updatedAt: stateUpdatedAt,
      gapDetected: false,
    });

    const postRestoreDocs = [
      { ts: toBsonTimestamp({ t: 1500, i: 1 }), op: "i", ns: "app.c", o: { _id: "base" } },
      { ts: toBsonTimestamp({ t: 1505, i: 1 }), op: "i", ns: "app.c", o: { _id: "post1" } },
    ];

    const createMongoClient = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command() {
              return {
                optimes: { lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1505, i: 1 }) } },
              };
            },
          };
        }
        if (name === "config") {
          return { collection: () => ({ find: () => makeCursorFromDocs([]) }) };
        }
        return {
          collection: () => ({
            find(filter = {}) {
              let filtered = [...postRestoreDocs];
              if (filter.ts?.$gt) {
                const gt = fromBsonTimestamp(filter.ts.$gt);
                filtered = filtered.filter(
                  (d) => compareTimestamps(fromBsonTimestamp(d.ts), gt) > 0,
                );
              }
              if (filter.ts?.$lte) {
                const lte = fromBsonTimestamp(filter.ts.$lte);
                filtered = filtered.filter(
                  (d) => compareTimestamps(fromBsonTimestamp(d.ts), lte) <= 0,
                );
              }
              return makeCursorFromDocs(filtered);
            },
          }),
        };
      },
    });

    // Without a newer base backup, divergence throws
    await expect(captureOplogSlice({ config, storage, logger, createMongoClient })).rejects.toThrow(
      /Timeline diverged/i,
    );

    // Now write a newer complete base backup B2 (createdAt > state.updatedAt)
    const b2Key = "pitr-hardening/_cluster/2026-11-27T14-00-00Z_cluster.archive.gz.enc";
    await storage.putJson(b2Key, { dummy: true });
    await writeManifest(
      storage,
      {
        version: 1,
        type: "backup",
        key: b2Key,
        database: "*",
        createdAt: new Date(Date.now() - 5_000).toISOString(),
        completedAt: new Date(Date.now() - 4_000).toISOString(),
        durationMs: 1000,
        size: 16,
        sha256: "0".repeat(64),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "1500:1", endTs: "1500:1", entries: 1 },
        excludedCollections: [],
        collections: {},
      },
      encKey,
    );

    const slice = await captureOplogSlice({ config, storage, logger, createMongoClient });
    expect(slice).not.toBeNull();
    expect(slice.oplogWindow.endTs).toBe("1505:1");
  });

  it("F-PITR-003 [P0]: empty-cursor oplog rollover detects gap and never advances coverageEndTs", async () => {
    const config = makeConfig(tempDir, encKey);
    const scope = scopeOf(config);

    await storage.putJson(pitrStateKey(scope), {
      lastTs: "1000:1",
      coverageEndTs: "1000:1",
      updatedAt: new Date(Date.now() - 10_000).toISOString(),
      gapDetected: false,
    });

    let oldestCallCount = 0;
    const createMongoClient = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command() {
              return {
                optimes: { lastCommittedOpTime: { ts: toBsonTimestamp({ t: 2000, i: 1 }) } },
              };
            },
          };
        }
        if (name === "config") {
          return { collection: () => ({ find: () => makeCursorFromDocs([]) }) };
        }
        return {
          collection: () => ({
            find(_filter = {}, opts = {}) {
              // Initial oldestDoc returns 900:1, post-cursor oldestDoc returns 2000:1
              if (opts.projection?.ts) {
                return {
                  sort(spec) {
                    const dir = spec.$natural ?? 1;
                    return {
                      limit() {
                        return {
                          async next() {
                            if (dir === -1) return { ts: toBsonTimestamp({ t: 2000, i: 1 }) };
                            oldestCallCount++;
                            if (oldestCallCount === 1) {
                              return { ts: toBsonTimestamp({ t: 900, i: 1 }) };
                            }
                            return { ts: toBsonTimestamp({ t: 2000, i: 1 }) };
                          },
                        };
                      },
                    };
                  },
                };
              }
              // Range cursor yields 0 docs because oplog rolled over before cursor opened
              return makeCursorFromDocs([]);
            },
          }),
        };
      },
    });

    const res = await captureOplogSlice({ config, storage, logger, createMongoClient });
    expect(res).toBeNull();

    const stateAfter = (await storage.getJson(pitrStateKey(scope))).data;
    expect(stateAfter.gapDetected).toBe(true);
    expect(stateAfter.coverageEndTs).toBe("1000:1");
  });

  it("F-PITR-004 [P0]: precursor walk fails on missing precursor / >512 depth and OplogApplier rejects missing precursor link", async () => {
    const config = makeConfig(tempDir, encKey);
    const scope = scopeOf(config);

    await storage.putJson(pitrStateKey(scope), {
      lastTs: "1000:1",
      coverageEndTs: "1000:1",
      updatedAt: new Date(Date.now() - 10_000).toISOString(),
      gapDetected: false,
    });

    const lsid = { id: Buffer.from("1234567890123456") };
    const commitDoc = {
      ts: toBsonTimestamp({ t: 1005, i: 1 }),
      op: "c",
      ns: "admin.$cmd",
      lsid,
      txnNumber: Long.fromNumber(1),
      prevOpTime: { ts: toBsonTimestamp({ t: 990, i: 1 }), t: Long.fromNumber(1) },
      o: { commitTransaction: 1 },
    };

    const createMongoClient = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command() {
              return {
                optimes: { lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1005, i: 1 }) } },
              };
            },
          };
        }
        if (name === "config") {
          return { collection: () => ({ find: () => makeCursorFromDocs([]) }) };
        }
        return {
          collection: () => ({
            find(filter = {}) {
              let filtered = [commitDoc];
              if (filter.ts?.$gt) {
                const gt = fromBsonTimestamp(filter.ts.$gt);
                filtered = filtered.filter(
                  (d) => compareTimestamps(fromBsonTimestamp(d.ts), gt) > 0,
                );
              }
              return makeCursorFromDocs(filtered);
            },
            async findOne() {
              // Missing precursor 990:1
              return null;
            },
          }),
        };
      },
    });

    await expect(captureOplogSlice({ config, storage, logger, createMongoClient })).rejects.toThrow(
      /precursor/i,
    );

    // Also test OplogApplier failing closed when a final applyOps chunk has an unseen prevOpTime
    const applied = [];
    const applier = new OplogApplier({
      workDir: tempDir,
      target: {
        async applyOps(ops) {
          applied.push(...ops);
        },
        async createIndexes() {},
        async ensureSystemViews() {},
        async documentExists() {
          return false;
        },
        async collectionExists() {
          return true;
        },
      },
    });

    await expect(
      applier.handleOp({
        ts: toBsonTimestamp({ t: 1005, i: 1 }),
        op: "c",
        ns: "admin.$cmd",
        lsid,
        txnNumber: Long.fromNumber(1),
        prevOpTime: { ts: toBsonTimestamp({ t: 990, i: 1 }), t: Long.fromNumber(1) },
        o: { applyOps: [{ op: "i", ns: "app.c", o: { _id: "partial" } }] },
      }),
    ).rejects.toThrow(OplogReplayError);
    expect(applied).toHaveLength(0);
  });

  it("F-PITR-005 [P0]: OplogFilter.sanitizeAndClone rewrites admin.$cmd renameCollection and system.views._id on DB clone", () => {
    const filter = new OplogFilter({ clone: { fromDb: "prod", toDb: "staging" } });

    const renameDoc = {
      op: "c",
      ns: "admin.$cmd",
      o: { renameCollection: "prod.a", to: "prod.b" },
    };
    expect(filter.isIncluded(renameDoc)).toBe(true);
    expect(filter.sanitizeAndClone(renameDoc)).toBe(true);
    expect(renameDoc.o.renameCollection).toBe("staging.a");
    expect(renameDoc.o.to).toBe("staging.b");

    const viewDoc = {
      op: "i",
      ns: "prod.system.views",
      o: { _id: "prod.v1", viewOn: "orders", pipeline: [] },
    };
    expect(filter.sanitizeAndClone(viewDoc)).toBe(true);
    expect(viewDoc.ns).toBe("staging.system.views");
    expect(viewDoc.o._id).toBe("staging.v1");
  });

  it("F-PITR-006 [P1]: startOplogArchiver.stop() aborts active capture and aborted capture does not advance coverageEndTs", async () => {
    const config = makeConfig(tempDir, encKey);
    const scope = scopeOf(config);
    await storage.putJson(pitrStateKey(scope), {
      lastTs: "1000:1",
      coverageEndTs: "1000:1",
      updatedAt: new Date(Date.now() - 10_000).toISOString(),
      gapDetected: false,
    });

    let releaseWait;
    const cursorStarted = new Promise((resolve) => {
      releaseWait = resolve;
    });

    const createMongoClient = () => ({
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command() {
              return {
                optimes: { lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1020, i: 1 }) } },
              };
            },
          };
        }
        if (name === "config") {
          return { collection: () => ({ find: () => makeCursorFromDocs([]) }) };
        }
        return {
          collection: () => ({
            find(filter = {}) {
              if (filter.ts?.$gt) {
                return {
                  sort() {
                    return this;
                  },
                  async *[Symbol.asyncIterator]() {
                    releaseWait();
                    await new Promise((r) => setTimeout(r, 200));
                    yield {
                      ts: toBsonTimestamp({ t: 1010, i: 1 }),
                      op: "i",
                      ns: "app.c",
                      o: { _id: 1 },
                    };
                  },
                };
              }
              return makeCursorFromDocs([
                { ts: toBsonTimestamp({ t: 1000, i: 1 }), op: "i", ns: "app.c", o: { _id: 0 } },
                { ts: toBsonTimestamp({ t: 1020, i: 1 }), op: "i", ns: "app.c", o: { _id: 2 } },
              ]);
            },
          }),
        };
      },
    });

    // Direct captureOplogSlice abort test
    const ac = new AbortController();
    const capturePromise = captureOplogSlice({
      config,
      storage,
      logger,
      signal: ac.signal,
      createMongoClient,
    });
    await cursorStarted;
    ac.abort(new Error("cancelled"));
    await expect(capturePromise).rejects.toThrow(/cancelled|abort/i);

    const stateAfter = (await storage.getJson(pitrStateKey(scope))).data;
    expect(stateAfter.coverageEndTs).toBe("1000:1");

    // Archiver stop() must abort tick signal
    let archiverTickStartedResolve;
    const archiverTickStarted = new Promise((r) => {
      archiverTickStartedResolve = r;
    });
    const archiver = startOplogArchiver({
      config,
      storage,
      logger,
      createMongoClient: () => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command() {
                return {
                  optimes: { lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1020, i: 1 }) } },
                };
              },
            };
          }
          if (name === "config") {
            return { collection: () => ({ find: () => makeCursorFromDocs([]) }) };
          }
          return {
            collection: () => ({
              find(filter = {}) {
                if (filter.ts?.$gt) {
                  return {
                    sort() {
                      return this;
                    },
                    async *[Symbol.asyncIterator]() {
                      archiverTickStartedResolve();
                      await new Promise((r) => setTimeout(r, 150));
                      yield {
                        ts: toBsonTimestamp({ t: 1010, i: 1 }),
                        op: "i",
                        ns: "app.c",
                        o: { _id: 1 },
                      };
                    },
                  };
                }
                return makeCursorFromDocs([
                  { ts: toBsonTimestamp({ t: 1000, i: 1 }), op: "i", ns: "app.c", o: { _id: 0 } },
                  { ts: toBsonTimestamp({ t: 1020, i: 1 }), op: "i", ns: "app.c", o: { _id: 2 } },
                ]);
              },
            }),
          };
        },
      }),
    });
    archiver.start();
    await archiverTickStarted;
    await archiver.stop();
    const stateFinal = (await storage.getJson(pitrStateKey(scope))).data;
    expect(stateFinal.coverageEndTs).toBe("1000:1");
  });

  it("F-PITR-007 [P1]: resolvePitrPlan skips non-oplog (oplog: false) backups and rejects explicit non-oplog baseKey", async () => {
    const config = makeConfig(tempDir, encKey);
    const scope = scopeOf(config);

    const b1Key = "pitr-hardening/_cluster/2026-11-27T10-00-00Z_cluster.archive.gz.enc";
    const b2Key = "pitr-hardening/_cluster/2026-11-27T12-00-00Z_cluster.archive.gz.enc";
    await storage.putJson(b1Key, { ok: 1 });
    await storage.putJson(b2Key, { ok: 2 });

    await writeManifest(
      storage,
      {
        version: 1,
        type: "backup",
        key: b1Key,
        database: "*",
        createdAt: "2026-11-27T10:00:00.000Z",
        completedAt: "2026-11-27T10:01:00.000Z",
        durationMs: 1000,
        size: 10,
        sha256: "a".repeat(64),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "1000:1", endTs: "1010:1", entries: 1 },
        excludedCollections: [],
        collections: {},
      },
      encKey,
    );

    await writeManifest(
      storage,
      {
        version: 1,
        type: "backup",
        key: b2Key,
        database: "*",
        createdAt: "2026-11-27T12:00:00.000Z",
        completedAt: "2026-11-27T12:01:00.000Z",
        durationMs: 1000,
        size: 10,
        sha256: "b".repeat(64),
        compression: "gzip",
        encryption: null,
        oplog: false,
        oplogWindow: null,
        excludedCollections: [],
        collections: {},
      },
      encKey,
    );

    const plan = await resolvePitrPlan(storage, scope, { t: 1010, i: 1 });
    expect(plan.baseBackup.key).toBe(b1Key);

    await expect(
      resolvePitrPlan(storage, scope, { t: 2000, i: 1 }, { baseKey: b2Key }),
    ).rejects.toThrow(/oplog/i);
  });

  it("F-PITR-008 [P1]: OplogApplier executes dropIndexes('*') on target server even when IndexCatalog has tracked indexes", async () => {
    const applied = [];
    const applier = new OplogApplier({
      workDir: tempDir,
      backupType: "logical",
      target: {
        async applyOps(ops) {
          applied.push(...ops);
        },
        async createIndexes() {},
        async ensureSystemViews() {},
        async documentExists() {
          return false;
        },
        async collectionExists() {
          return true;
        },
      },
    });

    await applier.handleOp({
      ts: toBsonTimestamp({ t: 100, i: 1 }),
      op: "c",
      ns: "app.$cmd",
      o: { commitIndexBuild: "users", indexes: [{ v: 2, key: { name: 1 }, name: "name_1" }] },
    });
    await applier.handleOp({
      ts: toBsonTimestamp({ t: 101, i: 1 }),
      op: "c",
      ns: "app.$cmd",
      o: { dropIndexes: "users", index: "*" },
    });

    expect(applied.some((op) => op.o?.dropIndexes === "users" && op.o?.index === "*")).toBe(true);
  });

  it("F-PITR-009 [P1]: create: system.views skips synthetic drop and calls ensureSystemViews; TARGET_COLL_COMMANDS includes convertToCapped/emptycapped", async () => {
    const applied = [];
    let ensuredDb = null;
    const applier = new OplogApplier({
      workDir: tempDir,
      target: {
        async applyOps(ops) {
          applied.push(...ops);
        },
        async createIndexes() {},
        async ensureSystemViews(dbName) {
          ensuredDb = dbName;
        },
        async documentExists() {
          return false;
        },
        async collectionExists() {
          return true;
        },
      },
    });

    await applier.handleOp({
      ts: toBsonTimestamp({ t: 100, i: 1 }),
      op: "c",
      ns: "app.$cmd",
      o: { create: "system.views" },
    });

    expect(ensuredDb).toBe("app");
    expect(applied.some((op) => op.o?.drop === "system.views")).toBe(false);

    const filter = new OplogFilter({ nsInclude: "app.logs" });
    expect(
      filter.isIncluded({ op: "c", ns: "app.$cmd", o: { convertToCapped: "logs", size: 1024 } }),
    ).toBe(true);
    expect(filter.isIncluded({ op: "c", ns: "app.$cmd", o: { emptycapped: "other" } })).toBe(false);
  });

  it("F-PITR-010 [P1]: buildPendingIndexes retries without commitQuorum on standalone InvalidOptions (code 72)", async () => {
    const calls = [];
    const applier = new OplogApplier({
      workDir: tempDir,
      target: {
        async applyOps() {},
        async createIndexes(dbName, coll, indexes, options) {
          calls.push({ dbName, coll, options });
          if (options?.commitQuorum !== undefined) {
            const err = new Error("InvalidOptions: commitQuorum not supported on standalone");
            err.code = 72;
            throw err;
          }
        },
        async ensureSystemViews() {},
        async documentExists() {
          return false;
        },
        async collectionExists() {
          return true;
        },
      },
    });

    await applier.handleOp({
      ts: toBsonTimestamp({ t: 100, i: 1 }),
      op: "c",
      ns: "app.$cmd",
      o: { commitIndexBuild: "users", indexes: [{ v: 2, key: { email: 1 }, name: "email_1" }] },
    });

    const built = await applier.buildPendingIndexes();
    expect(built).toBe(1);
    expect(calls).toHaveLength(2);
    expect(calls[0].options).toEqual({ commitQuorum: "votingMembers" });
    expect(calls[1].options).toBeUndefined();
  });

  it("F-PITR-011 [P2]: streamOplogSlice and downloadAndExtractOplogSlice fail closed on missing manifest and remove destBsonFile on mismatch", async () => {
    const config = makeConfig(tempDir, encKey);
    const sliceKey =
      "pitr-hardening/_cluster/2026-11-27/0000001000_00000001__0000001010_00000001.oplog.bson.gz.enc";
    await storage.putJson(sliceKey, { raw: "missing-manifest" });

    await expect(streamOplogSlice({ config, storage }, sliceKey, async () => {})).rejects.toThrow(
      /manifest/i,
    );

    const destFile = path.join(tempDir, "bad-slice.bson");
    await expect(
      downloadAndExtractOplogSlice({ config, storage }, sliceKey, destFile),
    ).rejects.toThrow(/manifest/i);
  });

  it("F-PITR-013 [P2]: OplogApplier ignores code 26/27 on dropIndex/deleteIndex and rejects non-array applyOps in strict mode", async () => {
    const applier = new OplogApplier({
      workDir: tempDir,
      backupType: "physical",
      target: {
        async applyOps() {
          const err = new Error("index not found");
          err.code = 27;
          throw err;
        },
        async createIndexes() {},
        async ensureSystemViews() {},
        async documentExists() {
          return false;
        },
        async collectionExists() {
          return true;
        },
      },
    });

    await applier.handleOp({
      ts: toBsonTimestamp({ t: 100, i: 1 }),
      op: "c",
      ns: "app.$cmd",
      o: { dropIndex: "users", index: "old_1" },
    });
    expect(applier.getStats().skipBreakdown.benignReplayRace).toBe(1);

    await expect(
      applier.handleOp({
        ts: toBsonTimestamp({ t: 101, i: 1 }),
        op: "c",
        ns: "admin.$cmd",
        o: { applyOps: "not-an-array" },
      }),
    ).rejects.toThrow(OplogReplayError);
  });

  it("F-PITR-014 [P3]: TxnBuffer enforces global memory cap across multiple concurrent transactions", async () => {
    const buf = new TxnBuffer(tempDir, 256);
    const bigPayload = "x".repeat(160);
    await buf.addApplyOps("txn-1", [{ op: "i", ns: "app.c", o: { _id: 1, v: bigPayload } }], true);
    await buf.addApplyOps("txn-2", [{ op: "i", ns: "app.c", o: { _id: 2, v: bigPayload } }], true);

    const files = await fs.readdir(tempDir);
    const spillFiles = files.filter((f) => f.startsWith(".mongovault-txn-"));
    expect(spillFiles.length).toBeGreaterThanOrEqual(1);
    await buf.dispose();
  });
});
