import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { BSON, Binary, Timestamp } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { IndexCatalog, OplogApplier, TxnBuffer } from "../src/oplog-applier.js";

function encodeOplogStream(docs) {
  const chunks = docs.map((d) => Buffer.from(BSON.serialize(d)));
  return Readable.from([Buffer.concat(chunks)]);
}

describe("OplogApplier, IndexCatalog & TxnBuffer (full edge-case coverage)", () => {
  it("IndexCatalog supports addIndex, collMod, deleteIndexes, renameCollection, dropCollection, dropDatabase", () => {
    const cat = new IndexCatalog();
    cat.addIndex("app", "users", { name: "_id_", key: { _id: 1 } }); // skipped
    cat.addIndex("app", "users", { name: "email_1", key: { email: 1 }, v: 2, ns: "app.users" });
    cat.addIndex("app", "users", { name: "age_1", key: { age: 1 }, expireAfterSeconds: 10 });

    expect(cat.collMod("app", "missing", { name: "email_1" })).toBe(false);
    expect(
      cat.collMod("app", "users", { name: "age_1", expireAfterSeconds: 60, hidden: true }),
    ).toBe(true);
    expect(cat.collMod("app", "users", { keyPattern: { email: 1 }, hidden: false })).toBe(true);
    expect(cat.collMod("app", "users", { name: "nope" })).toBe(false);

    expect(cat.deleteIndexes("app", "missing", "*")).toBe(false);
    expect(cat.deleteIndexes("app", "users", { email: 1 })).toBe(true);
    expect(cat.deleteIndexes("app", "users", "age_1")).toBe(true);

    cat.addIndex("app", "orders", { name: "ts_1", key: { ts: 1 } });
    cat.renameCollection("app", "orders", "app", "orders_v2");
    expect(cat.entries().map((e) => e.collection)).toEqual(["orders_v2"]);
    cat.renameCollection("app", "orders_v2", "app", "orders_v2"); // same key no-op
    cat.renameCollection("app", "empty", "app", "orders_v2"); // removes target

    cat.addIndex("app", "t1", { name: "k_1", key: { k: 1 } });
    cat.dropCollection("app", "t1");
    cat.addIndex("app", "t2", { name: "k_1", key: { k: 1 } });
    cat.dropDatabase("app");
    expect(cat.entries()).toEqual([]);
  });

  it("TxnBuffer spills large transactions to disk and consumes/discards cleanly", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-txnbuf-"));
    try {
      // Tiny 64-byte threshold to force spill to disk
      const buf = new TxnBuffer(workDir, 64);
      await buf.addApplyOps(
        "txn-1",
        [
          { op: "i", ns: "app.c", o: { _id: 1, pad: "x".repeat(80) } },
          { op: "i", ns: "app.c", o: { _id: 2, pad: "y".repeat(20) } },
        ],
        false,
      );
      expect(buf.has("txn-1")).toBe(true);
      expect(buf.isAllOps("txn-1")).toBe(true);

      const consumed = [];
      await buf.consumeOps("txn-1", async (op) => {
        consumed.push(Number(op.o._id));
      });
      expect(consumed).toEqual([1, 2]);
      expect(buf.has("txn-1")).toBe(false);

      // Discard spilled txn on dispose
      await buf.addApplyOps(
        "txn-2",
        [{ op: "i", ns: "app.c", o: { _id: 3, pad: "z".repeat(80) } }],
        true,
      );
      expect(buf.isAllOps("txn-2")).toBe(false);
      await buf.dispose();
      expect(buf.getLeftovers().uncommittedIds).toEqual([]);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("OplogApplier handles DDL, index catalog, system.views, TTL bucket race, DuplicateKey in unsafe mode, and drop errors", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-applier-"));
    try {
      const applyOps = vi.fn(async (ops) => {
        const op = ops[0];
        if (op.ns === "app.system.buckets.metrics") {
          const err = new Error("missing bucket");
          err.code = 28;
          throw err;
        }
        if (op.ns === "config.chunks") {
          const err = new Error("dup key");
          err.code = 11000;
          throw err;
        }
        if (op.op === "c" && op.o?.drop === "absent") {
          const err = new Error("ns not found");
          err.code = 26;
          throw err;
        }
      });
      const createIndexes = vi.fn(async () => {});
      const ensureSystemViews = vi.fn(async () => {});

      const applier = new OplogApplier({
        target: {
          applyOps,
          createIndexes,
          ensureSystemViews,
          documentExists: vi.fn(async () => false),
          collectionExists: vi.fn(async () => true),
        },
        workDir,
        backupType: "logical",
        unsafe: true,
        logger: { info: vi.fn(), warn: vi.fn(), debug: vi.fn(), error: vi.fn() },
      });

      const stream = encodeOplogStream([
        {
          op: "c",
          ns: "app.$cmd",
          o: { startIndexBuild: "items" },
          ts: new Timestamp({ t: 10, i: 1 }),
        },
        { op: "c", ns: "app.$cmd", o: { unknownCmd: 1 }, ts: new Timestamp({ t: 10, i: 2 }) },
        {
          op: "c",
          ns: "app.$cmd",
          o: { commitIndexBuild: "items", indexes: [{ name: "sku_1", key: { sku: 1 } }] },
          ts: new Timestamp({ t: 10, i: 3 }),
        },
        {
          op: "c",
          ns: "app.$cmd",
          o: { createIndexes: "items", name: "tag_1", key: { tag: 1 } },
          ts: new Timestamp({ t: 10, i: 4 }),
        },
        {
          op: "c",
          ns: "app.$cmd",
          o: { collMod: "items", index: { name: "tag_1", hidden: true } },
          ts: new Timestamp({ t: 10, i: 5 }),
        },
        {
          op: "c",
          ns: "app.$cmd",
          o: { dropIndexes: "items", index: "tag_1" },
          ts: new Timestamp({ t: 10, i: 6 }),
        },
        {
          op: "c",
          ns: "app.$cmd",
          o: { create: "views_src" },
          ts: new Timestamp({ t: 10, i: 7 }),
        },
        {
          op: "i",
          ns: "app.system.views",
          o: { _id: "app.v1", viewOn: "views_src", pipeline: [] },
          ts: new Timestamp({ t: 10, i: 8 }),
        },
        {
          op: "u",
          ns: "app.system.buckets.metrics",
          o2: { _id: 1 },
          o: { $v: 2, diff: { u: { x: 1 } } },
          ts: new Timestamp({ t: 10, i: 9 }),
        },
        {
          op: "i",
          ns: "config.chunks",
          o: { _id: "chunk1" },
          ts: new Timestamp({ t: 10, i: 10 }),
        },
        {
          op: "c",
          ns: "app.$cmd",
          o: { drop: "absent" },
          ts: new Timestamp({ t: 10, i: 11 }),
        },
        {
          op: "c",
          ns: "admin.$cmd",
          o: { renameCollection: "app.items", to: "app.items_new" },
          ts: new Timestamp({ t: 10, i: 12 }),
        },
      ]);

      await applier.applyStream(stream);
      const built = await applier.buildPendingIndexes({ standalone: true });
      expect(built).toBe(1);
      expect(ensureSystemViews).toHaveBeenCalledWith("app");
      expect(createIndexes.mock.calls[0][0]).toBe("app");
      expect(createIndexes.mock.calls[0][1]).toBe("items_new");
      expect(createIndexes.mock.calls[0][2][0].name).toBe("sku_1");
      expect(applier.getStats().skippedEntries).toBeGreaterThanOrEqual(5);
      await applier.dispose();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("OplogApplier handles multi-slice prepared/aborted/distributed txns and handleUncommittedTxn", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-applier-txn-"));
    try {
      const appliedIds = [];
      const applier = new OplogApplier({
        target: {
          applyOps: vi.fn(async (ops) => {
            for (const op of ops) {
              if (op.o?._id) appliedIds.push(op.o._id);
            }
          }),
          createIndexes: vi.fn(async () => {}),
          ensureSystemViews: vi.fn(async () => {}),
          documentExists: vi.fn(async () => false),
          collectionExists: vi.fn(async () => true),
        },
        workDir,
        logger: { info: vi.fn(), warn: vi.fn(), debug: vi.fn(), error: vi.fn() },
      });

      const lsid1 = { id: new Binary(Buffer.from("00112233445566778899aabbccddeeff", "hex"), 4) };
      const lsid2 = { id: new Binary(Buffer.from("11112233445566778899aabbccddeeff", "hex"), 4) };
      const lsid3 = { id: new Binary(Buffer.from("22112233445566778899aabbccddeeff", "hex"), 4) };

      const stream = encodeOplogStream([
        // Aborted txn
        {
          op: "c",
          ns: "admin.$cmd",
          lsid: lsid1,
          txnNumber: 1,
          o: { applyOps: [{ op: "i", ns: "app.c", o: { _id: "aborted" } }], partialTxn: true },
          ts: new Timestamp({ t: 10, i: 1 }),
        },
        {
          op: "c",
          ns: "admin.$cmd",
          lsid: lsid1,
          txnNumber: 1,
          o: { abortTransaction: 1 },
          ts: new Timestamp({ t: 10, i: 2 }),
        },
        // Prepared distributed txn (full ops, waiting for cluster commit)
        {
          op: "c",
          ns: "admin.$cmd",
          lsid: lsid2,
          txnNumber: 2,
          o: { applyOps: [{ op: "i", ns: "app.c", o: { _id: "dist-ok" } }], prepare: true },
          ts: new Timestamp({ t: 10, i: 3 }),
        },
        // Partial prepared txn (cannot safely replay)
        {
          op: "c",
          ns: "admin.$cmd",
          lsid: lsid3,
          txnNumber: 3,
          o: {
            applyOps: [{ op: "i", ns: "app.c", o: { _id: "dist-partial" } }],
            partialTxn: true,
            prepare: true,
          },
          ts: new Timestamp({ t: 10, i: 4 }),
        },
      ]);

      await applier.applyStream(stream);
      const leftovers = applier.getTxnLeftovers();
      expect(leftovers.uncommittedIds.length).toBe(2);

      // Pretend coordinator saw commits for both lsid2 and lsid3
      const clusterCommits = new Map(leftovers.uncommittedIds.map((id) => [id, { t: 11, i: 1 }]));
      const recon = await applier.handleUncommittedTxn(clusterCommits);
      expect(recon.applied).toBe(1);
      expect(recon.partial).toBe(1);
      expect(appliedIds).toEqual(["dist-ok"]);
      await applier.dispose();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
