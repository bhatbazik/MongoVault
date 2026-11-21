import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { BSON, Binary, Long, Timestamp } from "mongodb";
import { describe, expect, it } from "vitest";
import { OplogApplier, OplogFilter, TxnBuffer } from "../src/oplog-applier.js";

function bsonStream(docs) {
  return Readable.from(docs.map((d) => Buffer.from(BSON.serialize(d))));
}

function createFakeTarget() {
  const opsApplied = [];
  const indexesCreated = [];
  return {
    opsApplied,
    indexesCreated,
    async applyOps(ops) {
      opsApplied.push(...ops);
    },
    async createIndexes(db, collection, indexes, options) {
      indexesCreated.push({ db, collection, indexes, options });
    },
    async ensureSystemViews() {},
    async documentExists() {
      return false;
    },
    async collectionExists() {
      return true;
    },
  };
}

describe("OplogFilter & OplogApplier", () => {
  it("filters config.* allowlist, balancer/automerge, config.system.sessions UUID, and clones renamed DBs", () => {
    const sessionsUuid = new Binary(Buffer.from("00112233445566778899aabbccddeeff", "hex"), 4);
    const otherUuid = new Binary(Buffer.from("ffeeddccbbaa99887766554433221100", "hex"), 4);

    const cfgFilter = new OplogFilter({
      sessionsUuidHex: "00112233445566778899aabbccddeeff",
    });

    // Allowed config collections pass; non-allowlisted config.* are dropped
    expect(cfgFilter.isIncluded({ op: "i", ns: "config.databases", o: { _id: "app" } })).toBe(true);
    expect(cfgFilter.isIncluded({ op: "i", ns: "config.lockpings", o: { _id: "p1" } })).toBe(false);
    expect(cfgFilter.isIncluded({ op: "i", ns: "config.cache.chunks.app", o: { _id: 1 } })).toBe(
      false,
    );

    // Balancer & automerge settings excluded
    expect(
      cfgFilter.isIncluded({ op: "u", ns: "config.settings", o2: { _id: "balancer" }, o: {} }),
    ).toBe(false);
    expect(cfgFilter.isIncluded({ op: "i", ns: "config.settings", o: { _id: "automerge" } })).toBe(
      false,
    );
    expect(
      cfgFilter.isIncluded({ op: "i", ns: "config.settings", o: { _id: "chunksize", value: 64 } }),
    ).toBe(true);

    // config.system.sessions chunks filtered by backup UUID
    expect(
      cfgFilter.isIncluded({ op: "i", ns: "config.chunks", o: { _id: "c1", uuid: sessionsUuid } }),
    ).toBe(false);
    expect(
      cfgFilter.isIncluded({ op: "i", ns: "config.chunks", o: { _id: "c2", uuid: otherUuid } }),
    ).toBe(true);

    // DB rename clone rule rewrites ns and strips ui
    const cloneFilter = new OplogFilter({ clone: { fromDb: "prod", toDb: "staging" } });
    const op = {
      op: "i",
      ns: "prod.orders",
      ui: otherUuid,
      o: { _id: 42 },
    };
    expect(cloneFilter.isIncluded(op)).toBe(true);
    cloneFilter.sanitizeAndClone(op);
    expect(op.ns).toBe("staging.orders");
    expect(op.ui).toBeUndefined();
    expect(cloneFilter.isIncluded({ op: "i", ns: "other.orders", o: { _id: 1 } })).toBe(false);
  });

  it("buffers multi-slice partialTxn operations across streams and commits atomically on commitTransaction", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-applier-txn-"));
    const target = createFakeTarget();
    const applier = new OplogApplier({
      target,
      workDir,
      startExclusiveTs: { t: 100, i: 1 },
      endTs: { t: 200, i: 1 },
    });

    const lsid = { id: new Binary(Buffer.from("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "hex"), 4) };
    const txnNumber = Long.fromNumber(1);

    try {
      // Slice 1: entry <= startExclusiveTs (skipped) + partialTxn=true (buffered, NOT applied yet)
      await applier.applyStream(
        bsonStream([
          { ts: new Timestamp({ t: 100, i: 1 }), op: "i", ns: "app.items", o: { _id: "old" } },
          {
            ts: new Timestamp({ t: 110, i: 1 }),
            op: "c",
            ns: "admin.$cmd",
            lsid,
            txnNumber,
            o: {
              applyOps: [{ op: "i", ns: "app.items", o: { _id: "tx-1" } }],
              partialTxn: true,
            },
          },
        ]),
      );

      expect(target.opsApplied).toHaveLength(0);
      expect(applier.getTxnLeftovers().uncommittedIds).toHaveLength(1);

      // Slice 2: final chunk + commitTransaction -> flushes both ops!
      await applier.applyStream(
        bsonStream([
          {
            ts: new Timestamp({ t: 120, i: 1 }),
            op: "c",
            ns: "admin.$cmd",
            lsid,
            txnNumber,
            o: {
              applyOps: [{ op: "i", ns: "app.items", o: { _id: "tx-2" } }],
            },
          },
          {
            ts: new Timestamp({ t: 125, i: 1 }),
            op: "c",
            ns: "admin.$cmd",
            lsid,
            txnNumber,
            o: { commitTransaction: 1, commitTimestamp: new Timestamp({ t: 125, i: 1 }) },
          },
          // Entry beyond endTs (must NOT be applied)
          { ts: new Timestamp({ t: 205, i: 1 }), op: "i", ns: "app.items", o: { _id: "future" } },
        ]),
      );

      expect(target.opsApplied.map((o) => o.o._id)).toEqual(["tx-1", "tx-2"]);
      expect(applier.getTxnLeftovers().uncommittedIds).toHaveLength(0);
    } finally {
      await applier.dispose();
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("reconciles cross-shard distributed prepared transactions via handleUncommittedTxn", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-applier-dist-"));
    const targetShard1 = createFakeTarget();
    const targetShard2 = createFakeTarget();

    const applier1 = new OplogApplier({ target: targetShard1, workDir, endTs: { t: 500, i: 1 } });
    const applier2 = new OplogApplier({ target: targetShard2, workDir, endTs: { t: 500, i: 1 } });

    const lsidCommitted = {
      id: new Binary(Buffer.from("11111111111111111111111111111111", "hex"), 4),
    };
    const lsidAborted = {
      id: new Binary(Buffer.from("22222222222222222222222222222222", "hex"), 4),
    };

    try {
      // Shard 1 prepared txn A and txn B (`prepare: true`), neither committed locally on Shard 1 before cutoff
      await applier1.applyStream(
        bsonStream([
          {
            ts: new Timestamp({ t: 400, i: 1 }),
            op: "c",
            ns: "admin.$cmd",
            lsid: lsidCommitted,
            txnNumber: Long.fromNumber(7),
            o: {
              applyOps: [{ op: "i", ns: "app.ledger", o: { _id: "dist-commit" } }],
              prepare: true,
            },
          },
          {
            ts: new Timestamp({ t: 405, i: 1 }),
            op: "c",
            ns: "admin.$cmd",
            lsid: lsidAborted,
            txnNumber: Long.fromNumber(9),
            o: {
              applyOps: [{ op: "i", ns: "app.ledger", o: { _id: "dist-uncommitted" } }],
              prepare: true,
            },
          },
        ]),
      );

      // Shard 2 (coordinator) recorded commitTransaction for txn A
      await applier2.applyStream(
        bsonStream([
          {
            ts: new Timestamp({ t: 410, i: 1 }),
            op: "c",
            ns: "admin.$cmd",
            lsid: lsidCommitted,
            txnNumber: Long.fromNumber(7),
            o: { commitTransaction: 1, commitTimestamp: new Timestamp({ t: 410, i: 1 }) },
          },
        ]),
      );

      expect(targetShard1.opsApplied).toHaveLength(0);
      const commits = new Map();
      for (const c of applier2.getTxnLeftovers().committedDistTxns) {
        commits.set(c.id, c.commitTs);
      }

      await applier1.handleUncommittedTxn(commits);
      expect(targetShard1.opsApplied.map((o) => o.o._id)).toEqual(["dist-commit"]);
    } finally {
      await applier1.dispose();
      await applier2.dispose();
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("tracks index DDL in IndexCatalog and builds only surviving indexes in buildPendingIndexes", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-applier-idx-"));
    const target = createFakeTarget();
    const applier = new OplogApplier({ target, workDir });

    try {
      await applier.applyStream(
        bsonStream([
          // startIndexBuild must be ignored
          {
            ts: new Timestamp({ t: 10, i: 1 }),
            op: "c",
            ns: "app.$cmd",
            o: { startIndexBuild: "users", indexes: [{ name: "email_1", key: { email: 1 } }] },
          },
          // commitIndexBuild registers indexes
          {
            ts: new Timestamp({ t: 11, i: 1 }),
            op: "c",
            ns: "app.$cmd",
            o: {
              commitIndexBuild: "users",
              indexes: [
                { name: "email_1", key: { email: 1 }, unique: true },
                { name: "temp_1", key: { temp: 1 } },
              ],
            },
          },
          // dropIndexes removes temp_1 from catalog
          {
            ts: new Timestamp({ t: 12, i: 1 }),
            op: "c",
            ns: "app.$cmd",
            o: { dropIndexes: "users", index: "temp_1" },
          },
        ]),
      );

      await applier.buildPendingIndexes();
      const created = target.indexesCreated.find((c) => c.db === "app" && c.collection === "users");
      expect(created).toBeDefined();
      expect(created.indexes).toHaveLength(1);
      expect(created.indexes[0].name).toBe("email_1");
    } finally {
      await applier.dispose();
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("spills large transactions to disk in TxnBuffer and cleans up spill files on consume/dispose", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-txn-spill-"));
    // Tiny 256-byte threshold to force disk spilling
    const buf = new TxnBuffer(workDir, 256);
    try {
      await buf.addApplyOps(
        "txn-big",
        [
          { op: "i", ns: "app.big", o: { _id: 1, pad: "x".repeat(300) } },
          { op: "i", ns: "app.big", o: { _id: 2, pad: "y".repeat(300) } },
        ],
        false,
      );

      const filesBefore = await fs.readdir(workDir);
      expect(filesBefore.some((f) => f.includes(".mongovault-txn-"))).toBe(true);

      const replayed = [];
      await buf.consumeOps("txn-big", async (op) => {
        replayed.push(op.o._id.valueOf());
      });
      expect(replayed).toEqual([1, 2]);

      const filesAfter = await fs.readdir(workDir);
      expect(filesAfter).toHaveLength(0);
    } finally {
      await buf.dispose();
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
