import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import { BSON, Binary, Long, Timestamp } from "mongodb";
import pino from "pino";
import { describe, expect, it, vi } from "vitest";
import { runBackup } from "../src/backup.js";
import { writeManifest } from "../src/catalog.js";
import { acquireHybridLock, acquireLock } from "../src/lock.js";
import { OplogApplier, OplogReplayError, TxnBuffer } from "../src/oplog-applier.js";
import {
  captureOplogSlice,
  pitrStateKey,
  resolvePitrPlan,
  startOplogArchiver,
  toBsonTimestamp,
  uploadOplogSlice,
} from "../src/oplog.js";
import { PhysicalRecoveryCoordinator, runPhysicalBackup } from "../src/physical.js";
import { IntegrityError, restoreBackup } from "../src/restore.js";
import { resolveShardedPitrPlan, runShardedClusterBackup } from "../src/sharding.js";
import { FilesystemStorageClient, S3StorageClient } from "../src/storage.js";
import { generateTarStream } from "../src/tar.js";

const logger = pino({ level: "silent" });

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

describe("Chaos, Fault-Injection & Fail-Closed Recovery Correctness", () => {
  it("fails closed with OplogReplayError on unrecognized oplog commands and incomplete explicit commits", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-applier-"));
    try {
      const target1 = createFakeTarget();
      const applier1 = new OplogApplier({ target: target1, workDir, logger });
      await expect(
        applier1.applyStream(
          bsonStream([
            {
              ts: new Timestamp({ t: 100, i: 1 }),
              op: "c",
              ns: "app.$cmd",
              o: { futureMutatingDdl: "orders" },
            },
          ]),
        ),
      ).rejects.toThrow(OplogReplayError);
      expect(applier1.getStats().unknownCommands).toEqual(["futureMutatingDdl"]);
      await applier1.dispose();

      // Explicit commitTransaction arriving while buffered ops are still marked partialTxn: true MUST fail closed
      const target2 = createFakeTarget();
      const applier2 = new OplogApplier({ target: target2, workDir, logger });
      const lsid = { id: new Binary(Buffer.from("deadbeefdeadbeefdeadbeefdeadbeef", "hex"), 4) };
      await expect(
        applier2.applyStream(
          bsonStream([
            {
              ts: new Timestamp({ t: 101, i: 1 }),
              op: "c",
              ns: "admin.$cmd",
              lsid,
              txnNumber: Long.fromNumber(1),
              o: {
                applyOps: [{ op: "i", ns: "app.orders", o: { _id: "half" } }],
                partialTxn: true,
              },
            },
            {
              ts: new Timestamp({ t: 101, i: 2 }),
              op: "c",
              ns: "admin.$cmd",
              lsid,
              txnNumber: Long.fromNumber(1),
              o: { commitTransaction: 1, commitTimestamp: new Timestamp({ t: 101, i: 2 }) },
            },
          ]),
        ),
      ).rejects.toThrow(OplogReplayError);
      expect(target2.opsApplied).toHaveLength(0);
      expect(applier2.getStats().partialTxns).toBe(1);
      await applier2.dispose();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("reconciles >150 distributed transactions without eviction loss and deduplicates overlapping slice boundaries", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-150txn-"));
    try {
      const targetShard1 = createFakeTarget();
      const targetCoord = createFakeTarget();
      const applier1 = new OplogApplier({ target: targetShard1, workDir, logger });
      const coordApplier = new OplogApplier({ target: targetCoord, workDir, logger });

      const shardDocs = [];
      const coordDocs = [];
      const count = 150;
      for (let i = 1; i <= count; i++) {
        const hex = i.toString(16).padStart(32, "0");
        const lsid = { id: new Binary(Buffer.from(hex, "hex"), 4) };
        shardDocs.push({
          ts: new Timestamp({ t: 1000 + i, i: 1 }),
          op: "c",
          ns: "admin.$cmd",
          lsid,
          txnNumber: Long.fromNumber(1),
          o: {
            applyOps: [{ op: "i", ns: "app.txns", o: { _id: `tx-${i}` } }],
            prepare: true,
          },
        });
        coordDocs.push({
          ts: new Timestamp({ t: 2000 + i, i: 1 }),
          op: "c",
          ns: "admin.$cmd",
          lsid,
          txnNumber: Long.fromNumber(1),
          o: { commitTransaction: 1, commitTimestamp: new Timestamp({ t: 2000 + i, i: 1 }) },
        });
      }

      // Replay shardDocs TWICE (simulating overlapping slices) -> duplicates must be skipped cleanly
      await applier1.applyStream(bsonStream(shardDocs));
      await applier1.applyStream(bsonStream(shardDocs.slice(-5)));
      await coordApplier.applyStream(bsonStream(coordDocs));

      const commits = new Map(
        coordApplier.getTxnLeftovers().committedDistTxns.map((c) => [c.id, c.commitTs]),
      );
      expect(commits.size).toBe(count);

      const recon = await applier1.handleUncommittedTxn(commits);
      expect(recon.applied).toBe(count);
      expect(recon.partial).toBe(0);
      expect(targetShard1.opsApplied).toHaveLength(count);

      const stats = applier1.getStats();
      expect(stats.skipBreakdown.beforeWindow).toBe(5);
      const accountedSkips = Object.values(stats.skipBreakdown).reduce((a, b) => a + b, 0);
      expect(accountedSkips).toBe(stats.skippedEntries);

      await applier1.dispose();
      await coordApplier.dispose();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("cleans up partial archive and writes no manifest when backup is aborted mid-stream", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-abort-"));
    const storage = new FilesystemStorageClient(path.join(workDir, "store"));
    const ac = new AbortController();

    const slowStream = new Readable({
      read() {
        this.push(Buffer.alloc(4096, 0x41));
        setTimeout(() => ac.abort(new Error("SIGKILL simulated")), 20);
      },
    });

    const config = {
      mongo: { uri: "mongodb://localhost:27017", database: "app", excludeCollections: [] },
      storage: { prefix: "chaos" },
      workDir: path.join(workDir, "tmp"),
      timeoutMs: 10_000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    try {
      await expect(
        runBackup({
          config,
          storage,
          logger,
          signal: ac.signal,
          tools: {
            mongodump: async () => ({
              stdout: slowStream,
              done: new Promise((_res, rej) => {
                ac.signal.addEventListener("abort", () => rej(ac.signal.reason), { once: true });
              }),
            }),
          },
        }),
      ).rejects.toThrow();

      const remaining = [];
      for await (const obj of storage.list("chaos/")) {
        remaining.push(obj.key);
      }
      expect(remaining).toEqual([]);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("preserves confirmed state.lastTs on mid-cursor CappedPositionLost and enforces ISO-second coverage", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-rollover-"));
    const storage = new FilesystemStorageClient(path.join(workDir, "store"));
    const config = {
      mongo: { uri: "mongodb://localhost:27017/?replicaSet=rs0", database: "*", oplog: true },
      storage: { prefix: "prod" },
      pitr: { enabled: true, flushIntervalSeconds: 60, maxSliceMb: 16 },
      workDir,
      encryptionKey: null,
      encryptionKeyRing: [],
      schedule: { timezone: "UTC" },
    };
    const scope = { prefix: "prod", database: "_cluster" };

    try {
      // Seed state.json at confirmed 1000:5
      await storage.putJson(pitrStateKey(scope), {
        lastTs: "1000:5",
        coverageEndTs: "1000:5",
        gapDetected: false,
      });

      // Simulate cursor reading 1 doc (1010:1) then throwing CappedPositionLost (code 136)
      const rolloverClientFactory = () => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command(cmd) {
                if (cmd.replSetGetStatus) {
                  return {
                    optimes: { lastCommittedOpTime: { ts: toBsonTimestamp({ t: 1050, i: 1 }) } },
                  };
                }
                return { ok: 1 };
              },
            };
          }
          if (name === "config") {
            return {
              collection: () => ({
                find: () => ({
                  sort: () => ({ limit: () => ({ next: async () => null }) }),
                }),
              }),
            };
          }
          return {
            collection: () => ({
              find: () => ({
                sort: () => ({
                  limit: () => ({
                    next: async () => ({ ts: toBsonTimestamp({ t: 1000, i: 1 }) }),
                  }),
                  async *[Symbol.asyncIterator]() {
                    yield {
                      ts: toBsonTimestamp({ t: 1010, i: 1 }),
                      op: "i",
                      ns: "app.items",
                      o: { _id: 1 },
                    };
                    const err = new Error("CappedPositionLost");
                    err.code = 136;
                    throw err;
                  },
                }),
              }),
            }),
          };
        },
      });

      await expect(
        captureOplogSlice({ config, storage, logger, createMongoClient: rolloverClientFactory }),
      ).rejects.toThrow("CappedPositionLost");

      const state = (await storage.getJson(pitrStateKey(scope))).data;
      expect(state.gapDetected).toBe(true);
      // MUST NOT advance lastTs to un-uploaded 1010:1!
      expect(state.lastTs).toBe("1000:5");

      // ISO-second target coverage check: coverage ending at 1774092000:1 must NOT satisfy ISO 2026-03-21T11:20:00Z (1774092000:4294967295)
      const isoSec = 1774092000;
      const baseKey = "prod/_cluster/2026-03-21/20260321T110000Z.archive.gz";
      await storage.upload(baseKey, Buffer.from("base"));
      await writeManifest(storage, {
        version: 2,
        engine: "logical",
        key: baseKey,
        database: "*",
        createdAt: "2026-03-21T11:00:00.000Z",
        completedAt: "2026-03-21T11:01:00.000Z",
        durationMs: 1000,
        size: 4,
        sha256: "abc",
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: `${isoSec - 10}:1`, endTs: `${isoSec - 5}:1` },
        excludedCollections: [],
        collections: {},
      });
      await uploadOplogSlice({
        config,
        storage,
        logger,
        rawDocs: [
          Buffer.from(
            BSON.serialize({
              ts: toBsonTimestamp({ t: isoSec, i: 1 }),
              op: "i",
              ns: "app.items",
              o: { _id: 1 },
            }),
          ),
        ],
        startTs: { t: isoSec - 5, i: 2 },
        endTs: { t: isoSec, i: 1 },
        previousEndTs: { t: isoSec - 5, i: 1 },
      });

      const isoStr = new Date(isoSec * 1000).toISOString();
      await expect(resolvePitrPlan(storage, scope, isoStr)).rejects.toThrow(
        /PITR oplog coverage ends at 1774092000:1/,
      );
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("resumes S3 downloads across mid-stream TCP resets using Range+IfMatch and fails closed on ETag change", async () => {
    const fullPayload = Buffer.from(
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    );
    const s3 = new S3StorageClient({
      region: "us-east-1",
      bucket: "test-bucket",
      accessKeyId: "ak",
      secretAccessKey: "sk",
    });

    let callCount = 0;
    const sendSpy = vi.spyOn(s3.client, "send").mockImplementation(async (cmd) => {
      callCount++;
      const range = cmd.input?.Range;
      if (callCount === 1) {
        // First response drops connection after 20 bytes
        const broken = new PassThrough();
        broken.write(fullPayload.subarray(0, 20));
        setTimeout(() => broken.destroy(new Error("ECONNRESET")), 5);
        return { Body: broken, ETag: '"etag-v1"', ContentLength: fullPayload.length };
      }
      expect(range).toBe("bytes=20-");
      expect(cmd.input?.IfMatch).toBe('"etag-v1"');
      return {
        Body: Readable.from([fullPayload.subarray(20)]),
        ETag: '"etag-v1"',
        ContentLength: fullPayload.length - 20,
      };
    });

    const dlStream = await s3.download("backups/archive.gz");
    const chunks = [];
    for await (const c of dlStream) chunks.push(c);
    expect(Buffer.concat(chunks).equals(fullPayload)).toBe(true);
    expect(callCount).toBe(2);

    // Now simulate ETag mutation (412 PreconditionFailed) -> MUST fail closed!
    callCount = 0;
    sendSpy.mockImplementation(async () => {
      callCount++;
      if (callCount === 1) {
        const broken = new PassThrough();
        broken.write(fullPayload.subarray(0, 10));
        setTimeout(() => broken.destroy(new Error("ECONNRESET")), 5);
        return { Body: broken, ETag: '"etag-v1"', ContentLength: fullPayload.length };
      }
      const err = new Error("PreconditionFailed");
      err.name = "PreconditionFailed";
      err.$metadata = { httpStatusCode: 412 };
      throw err;
    });

    const mutatedStream = await s3.download("backups/archive.gz");
    await expect(
      (async () => {
        for await (const _c of mutatedStream) {
          // drain
        }
      })(),
    ).rejects.toThrow(/PreconditionFailed/);
  });

  it("fails closed when TxnBuffer disk spill fails, when TAR source file vanishes, and when physical fsync fails", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-io-"));
    try {
      // 1. TxnBuffer spill to a file path (ENOTDIR / unwritable dir) must throw cleanly
      const blockingFile = path.join(workDir, "not-a-dir");
      await fs.writeFile(blockingFile, "block");
      const buf = new TxnBuffer(path.join(blockingFile, "sub"), 16);
      await expect(
        buf.addApplyOps(
          "txn-1",
          [{ op: "i", ns: "app.c", o: { _id: 1, pad: "x".repeat(64) } }],
          false,
        ),
      ).rejects.toThrow();
      await buf.dispose();

      // 2. Missing physical file during TAR streaming MUST throw instead of silently skipping
      const missingPath = path.join(workDir, "vanished.wt");
      await expect(
        (async () => {
          for await (const _c of generateTarStream([
            { name: "vanished.wt", fullPath: missingPath, size: 128 },
          ])) {
            // drain
          }
        })(),
      ).rejects.toThrow(/missing or unreadable/i);

      // 3. PhysicalRecoveryCoordinator MUST fail closed when fsync returns ok: 0
      const coord = new PhysicalRecoveryCoordinator();
      vi.spyOn(coord, "runWithEphemeralMongod").mockImplementation(async (_w, _d, _a, fn) => {
        await fn({
          db: () => ({
            command: async (cmd) => (cmd.fsync ? { ok: 0, errmsg: "disk full" } : { ok: 1 }),
            listCollections: () => ({ toArray: async () => [] }),
            collection: () => ({
              deleteMany: async () => ({}),
              insertOne: async () => ({}),
              drop: async () => ({}),
            }),
          }),
        });
      });

      await expect(
        coord.run({
          config: {
            workDir,
            storage: { prefix: "p" },
            mongo: { uri: "mongodb://127.0.0.1:27017", database: "*" },
          },
          storage: {},
          logger,
          targetDbPath: workDir,
          manifest: { physical: { cursorType: "fsyncLock" } },
          pitrPlan: null,
        }),
      ).rejects.toThrow(/WiredTiger fsync checkpoint failed/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("fails closed in resolveShardedPitrPlan when configsvr (oplog: false) lacks coverage up to targetTs", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-sharded-"));
    const storage = new FilesystemStorageClient(path.join(workDir, "store"));
    try {
      const clusterKey = "prod/_cluster/2026-03-10/20260310T120000Z.cluster";
      const cfgKey = "prod/configsvr/_cluster/2026-03-10/20260310T120000Z.archive.gz";
      const s1Key = "prod/shards/shard01/_cluster/2026-03-10/20260310T120000Z.archive.gz";

      await storage.putJson(clusterKey, { clusterTime: "1000:5" });
      await writeManifest(storage, {
        version: 2,
        type: "sharded-cluster-backup",
        engine: "logical",
        key: clusterKey,
        database: "*",
        createdAt: "2026-03-10T12:00:00.000Z",
        completedAt: "2026-03-10T12:01:00.000Z",
        durationMs: 1000,
        size: 10,
        sha256: "abc",
        compression: "none",
        encryption: null,
        oplog: true,
        excludedCollections: [],
        collections: {},
        sharding: {
          clusterTime: "1000:5",
          configsvr: {
            backupKey: cfgKey,
            manifestKey: `${cfgKey}.manifest.json`,
            host: "cfg/c1:27019",
          },
          shards: {
            shard01: {
              shardId: "shard01",
              host: "shard01/s1:27018",
              backupKey: s1Key,
              manifestKey: `${s1Key}.manifest.json`,
              status: "success",
            },
          },
        },
      });
      await storage.upload(cfgKey, Buffer.from("cfg"));
      await writeManifest(storage, {
        version: 2,
        key: cfgKey,
        database: "config",
        createdAt: "2026-03-10T12:00:00.000Z",
        completedAt: "2026-03-10T12:01:00.000Z",
        durationMs: 1000,
        size: 3,
        sha256: "cfg",
        compression: "gzip",
        encryption: null,
        oplog: false, // safe CSRS dump mode!
        excludedCollections: [],
        collections: {},
      });
      await storage.upload(s1Key, Buffer.from("s1"));
      await writeManifest(storage, {
        version: 2,
        key: s1Key,
        database: "*",
        createdAt: "2026-03-10T12:00:00.000Z",
        completedAt: "2026-03-10T12:01:00.000Z",
        durationMs: 1000,
        size: 2,
        sha256: "s1",
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "1000:1", endTs: "1000:5" },
        excludedCollections: [],
        collections: {},
      });

      // Shard has slice up to 1020:1, but configsvr has NO slices -> MUST throw!
      await uploadOplogSlice({
        config: {
          mongo: { uri: "mongodb://s1:27018", database: "*" },
          storage: { prefix: "prod/shards/shard01" },
        },
        storage,
        logger,
        rawDocs: [
          Buffer.from(
            BSON.serialize({
              ts: toBsonTimestamp({ t: 1015, i: 1 }),
              op: "i",
              ns: "app.u",
              o: { _id: 1 },
            }),
          ),
        ],
        startTs: { t: 1000, i: 6 },
        endTs: { t: 1020, i: 1 },
        previousEndTs: { t: 1000, i: 5 },
      });

      await expect(
        resolveShardedPitrPlan(storage, { prefix: "prod", database: "*" }, "1015:1"),
      ).rejects.toThrow(/no configsvr oplog slices are available/i);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("recovers from stale dead-PID locks and triggers onLost when a distributed lease is stolen", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-lock-"));
    const storage = new FilesystemStorageClient(path.join(workDir, "store"));
    const lockFile = path.join(workDir, ".backup.lock");
    try {
      // Plant a stale lock from a dead PID
      await fs.writeFile(
        lockFile,
        JSON.stringify({
          pid: 999_999,
          token: "dead-owner",
          acquiredAt: new Date(Date.now() - 60_000).toISOString(),
          lastHeartbeatAt: new Date(Date.now() - 60_000).toISOString(),
        }),
      );

      const rel = await acquireLock(lockFile, { staleAfterMs: 60_000, logger });
      expect(rel).not.toBeNull();
      await rel();

      // Test distributed lease loss callback when another process overwrites the distributed lock
      let lostErr = null;
      const distKey = "prod/.locks/backup.lock.json";
      const hybridRel = await acquireHybridLock({
        file: lockFile,
        storage,
        lockKey: distKey,
        staleAfterMs: 30_000,
        distributedTtlMs: 30_000,
        heartbeatIntervalMs: 30,
        logger,
        onLost: (err) => {
          lostErr = err;
        },
      });
      expect(hybridRel).not.toBeNull();

      // Another process steals the distributed lock file
      await storage.putJson(distKey, {
        token: "attacker-or-peer",
        holder: "other-host",
        acquiredAt: new Date().toISOString(),
        expiresAt: Date.now() + 60_000,
      });

      await new Promise((r) => setTimeout(r, 120));
      expect(lostErr).toBeInstanceOf(Error);
      expect(lostErr.message).toMatch(/Distributed lock lease lost/);
      await hybridRel();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("pre-verifies all planned PITR oplog slices BEFORE executing destructive --drop on base restore", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-preverify-"));
    const storage = new FilesystemStorageClient(path.join(workDir, "store"));
    try {
      const config = {
        workDir,
        mongo: { uri: "mongodb://127.0.0.1:27017", database: "*", oplog: true },
        storage: { prefix: "prod" },
        pitr: { enabled: true, flushIntervalSeconds: 60, maxSliceMb: 16 },
        encryptionKey: null,
        encryptionKeyRing: [],
      };

      const baseKey = "prod/_cluster/2026-03-10/20260310T120000Z.archive.gz";
      const baseGz = gzipSync(Buffer.from("valid-base-archive"));
      await storage.upload(baseKey, baseGz);
      await writeManifest(storage, {
        version: 2,
        key: baseKey,
        database: "*",
        createdAt: "2026-03-10T12:00:00.000Z",
        completedAt: "2026-03-10T12:00:05.000Z",
        durationMs: 5000,
        size: baseGz.length,
        sha256: createHash("sha256").update(baseGz).digest("hex"),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "1000:1", endTs: "1000:5" },
        excludedCollections: [],
        collections: {},
      });

      const sliceManifest = await uploadOplogSlice({
        config,
        storage,
        logger,
        rawDocs: [
          Buffer.from(
            BSON.serialize({
              ts: toBsonTimestamp({ t: 1010, i: 1 }),
              op: "i",
              ns: "app.users",
              o: { _id: "u1" },
            }),
          ),
        ],
        startTs: { t: 1000, i: 6 },
        endTs: { t: 1010, i: 1 },
        previousEndTs: { t: 1000, i: 5 },
      });
      expect(sliceManifest).not.toBeNull();

      // Tamper with the PITR slice payload after upload
      await storage.upload(sliceManifest.key, gzipSync(Buffer.from("tampered-oplog-slice")));

      const mongorestoreSpy = vi.fn();
      await expect(
        restoreBackup(
          { config, storage, logger, tools: { mongorestore: mongorestoreSpy } },
          { uri: "mongodb://127.0.0.1:27017", pitr: "1010:1", drop: true },
        ),
      ).rejects.toThrow(IntegrityError);

      // CRITICAL: mongorestore (--drop) must NEVER have been invoked!
      expect(mongorestoreSpy).not.toHaveBeenCalled();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("dispatches immediate email failure alert when oplog archiver detects a rollover gap slice and enforces REQUIRE_ENCRYPTION", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-chaos-gap-notify-"));
    const storage = new FilesystemStorageClient(path.join(workDir, "store"));
    try {
      const config = {
        workDir,
        mongo: { uri: "mongodb://127.0.0.1:27017", database: "*", oplog: true },
        storage: { prefix: "prod" },
        pitr: { enabled: true, flushIntervalSeconds: 60, maxSliceMb: 16 },
        encryptionKey: null,
        encryptionKeyRing: [],
      };

      // Seed prior state at 1000:1, while MongoDB oplog oldest is now 2000:1 -> gapDetected = true!
      await storage.putJson(pitrStateKey({ prefix: "prod", database: "*" }), {
        lastTs: "1000:1",
        updatedAt: new Date().toISOString(),
      });

      const fakeDoc = Buffer.from(
        BSON.serialize({
          ts: toBsonTimestamp({ t: 2005, i: 1 }),
          op: "i",
          ns: "app.items",
          o: { _id: "x1" },
        }),
      );
      const fakeMongoClient = () => ({
        connect: async () => {},
        close: async () => {},
        db: (name) => ({
          command: async (cmd) => {
            if (cmd.replSetGetStatus) {
              return {
                optimes: { lastCommittedOpTime: { ts: toBsonTimestamp({ t: 2005, i: 1 }) } },
              };
            }
            return { ok: 1 };
          },
          collection: () => {
            if (name === "config") {
              return {
                find: () => ({
                  sort: () => ({
                    limit: () => ({ next: async () => null }),
                  }),
                }),
              };
            }
            return {
              find: (query) => {
                if (query && query.ts) {
                  return {
                    sort: () => ({
                      async *[Symbol.asyncIterator]() {
                        yield fakeDoc;
                      },
                    }),
                  };
                }
                return {
                  sort: () => ({
                    limit: () => ({
                      next: async () => ({ ts: toBsonTimestamp({ t: 2000, i: 1 }) }),
                    }),
                  }),
                };
              },
            };
          },
        }),
      });

      const notifyFailure = vi.fn(async () => {});
      const archiver = startOplogArchiver({
        config,
        storage,
        logger,
        notifier: { notifySuccess: vi.fn(async () => {}), notifyFailure },
        createMongoClient: fakeMongoClient,
      });
      archiver.start();
      await new Promise((r) => setTimeout(r, 150));
      await archiver.stop();

      expect(notifyFailure).toHaveBeenCalledWith(
        "pitr-oplog-gap",
        expect.objectContaining({ message: expect.stringMatching(/PITR timeline gap detected/i) }),
      );

      // REQUIRE_ENCRYPTION fail-closed checks across all backup engines & oplog slices
      const strictCfg = { ...config, requireEncryption: true, encryptionKey: null };
      await expect(runBackup({ config: strictCfg, storage, logger })).rejects.toThrow(
        /REQUIRE_ENCRYPTION/,
      );
      await expect(
        uploadOplogSlice({
          config: strictCfg,
          storage,
          logger,
          rawDocs: [fakeDoc],
          startTs: { t: 2005, i: 1 },
          endTs: { t: 2005, i: 1 },
        }),
      ).rejects.toThrow(/REQUIRE_ENCRYPTION/);
      await expect(runPhysicalBackup({ config: strictCfg, storage, logger })).rejects.toThrow(
        /REQUIRE_ENCRYPTION/,
      );
      await expect(runShardedClusterBackup({ config: strictCfg, storage, logger })).rejects.toThrow(
        /REQUIRE_ENCRYPTION/,
      );
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
