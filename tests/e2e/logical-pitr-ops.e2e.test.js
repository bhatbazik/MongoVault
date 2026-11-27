import { createHash, randomBytes } from "node:crypto";
import { watch as fsWatch } from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { MongoClient } from "mongodb";
import pino from "pino";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { runBackup } from "../../src/backup.js";
import { manifestKey, scopeOf, writeManifest } from "../../src/catalog.js";
import { generateKey, parseKey } from "../../src/crypto.js";
import { DrillError, runDrill } from "../../src/drill.js";
import { createMetricsRegistry } from "../../src/metrics.js";
import {
  captureOplogSlice,
  compareTimestamps,
  formatOplogTimestamp,
  fromBsonTimestamp,
  getOplogBounds,
  parseOplogTimestamp,
  pitrStateKey,
  resolvePitrPlan,
  startOplogArchiver,
} from "../../src/oplog.js";
import { IntegrityError, restoreBackup } from "../../src/restore.js";
import { pruneOplogSlices } from "../../src/retention.js";
import { FilesystemStorageClient, S3StorageClient } from "../../src/storage.js";
import { mongodump } from "../../src/tools.js";
import { cleanupAllChildren, runCliCommand, startDaemon } from "./cli-runner.js";
import {
  createTcpChaosProxy,
  getLatestOplogTs,
  startMultiNodeReplicaSet,
  startReplicaSet,
  waitForMajorityCatchup,
  waitForPrimary,
} from "./cluster-harness.js";
import { startMockS3Server } from "./s3-mock-server.js";

const logger = pino({ level: "silent" });

function makeFsConfig({
  uri,
  storageDir,
  workDir,
  encKey = null,
  prefix = "e2e",
  database = "*",
  oplog = true,
  pitrEnabled = true,
  maxSliceMb = 16,
  flushIntervalSeconds = 60,
  readPreference = "primaryPreferred",
  drillUri = null,
  requireEncryption = false,
}) {
  return {
    engine: "logical",
    mongo: {
      uri,
      database,
      excludeCollections: [],
      oplog,
      parallelCollections: 2,
      readPreference,
    },
    pitr: {
      enabled: pitrEnabled,
      flushIntervalSeconds,
      maxSliceMb,
    },
    storage: {
      type: "filesystem",
      path: storageDir,
      bucket: storageDir,
      region: "us-east-1",
      forcePathStyle: false,
      accessKeyId: "",
      secretAccessKey: "",
      prefix,
      partSizeMb: 32,
      queueSize: 2,
    },
    schedule: { cron: "0 2 * * *", timezone: "UTC", runOnStart: false },
    timeoutMs: 120_000,
    workDir,
    encryptionKey: encKey,
    encryptionKeyRing: encKey ? [encKey] : [],
    requireEncryption,
    retention: null,
    drill: drillUri ? { uri: drillUri, cron: null } : null,
    physical: null,
    sharding: null,
    notifications: { onSuccess: false, email: null },
    http: { port: 0 },
    logLevel: "silent",
  };
}

function makeS3Config({
  uri,
  s3Endpoint,
  bucket = "e2e-bucket",
  workDir,
  encKey = null,
  prefix = "e2e",
  partSizeMb = 5,
  queueSize = 1,
  database = "*",
  oplog = true,
  pitrEnabled = true,
  maxSliceMb = 16,
  flushIntervalSeconds = 60,
  readPreference = "primaryPreferred",
  timeoutMs = 120_000,
}) {
  return {
    engine: "logical",
    mongo: {
      uri,
      database,
      excludeCollections: [],
      oplog,
      parallelCollections: 2,
      readPreference,
    },
    pitr: {
      enabled: pitrEnabled,
      flushIntervalSeconds,
      maxSliceMb,
    },
    storage: {
      type: "s3",
      bucket,
      endpoint: s3Endpoint,
      region: "us-east-1",
      forcePathStyle: true,
      accessKeyId: "test-access",
      secretAccessKey: "test-secret",
      prefix,
      partSizeMb,
      queueSize,
    },
    schedule: { cron: "0 2 * * *", timezone: "UTC", runOnStart: false },
    timeoutMs,
    workDir,
    encryptionKey: encKey,
    encryptionKeyRing: encKey ? [encKey] : [],
    requireEncryption: false,
    retention: null,
    drill: null,
    physical: null,
    sharding: null,
    notifications: { onSuccess: false, email: null },
    http: { port: 0 },
    logLevel: "silent",
  };
}

function makeS3Storage(s3Endpoint, { bucket = "e2e-bucket", partSizeMb = 5, queueSize = 1 } = {}) {
  return new S3StorageClient({
    type: "s3",
    endpoint: s3Endpoint,
    region: "us-east-1",
    bucket,
    accessKeyId: "test-access",
    secretAccessKey: "test-secret",
    forcePathStyle: true,
    partSizeMb,
    queueSize,
  });
}

async function waitForCondition(predicate, timeoutMs = 10_000, intervalMs = 50) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return false;
}

async function listFilesRecursive(dir) {
  const results = [];
  const entries = await fs.readdir(dir, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...(await listFilesRecursive(full)));
    } else {
      results.push(full);
    }
  }
  return results;
}

async function countYamlFiles(dir) {
  const files = await listFilesRecursive(dir);
  return files.filter((f) => f.endsWith(".yaml") || f.endsWith(".yml")).length;
}

async function findUserDatabases(client) {
  const { databases } = await client.db("admin").admin().listDatabases();
  return databases.map((d) => d.name).filter((n) => !["admin", "local", "config"].includes(n));
}

async function dropAllUserDatabases(client) {
  const dbs = await findUserDatabases(client);
  for (const name of dbs) {
    await client.db(name).dropDatabase();
  }
}

describe("E2E-M2: Logical Backup/Restore, PITR Slicer/Replay & Ops/Daemon (F5–F6, F9–F10, F13–F15)", () => {
  let rootDir;
  let s3Server;

  beforeAll(async () => {
    rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-e2e-m2-"));
    s3Server = await startMockS3Server({ defaultBucket: "e2e-bucket" });
  });

  afterAll(async () => {
    await cleanupAllChildren();
    await s3Server?.stop().catch(() => {});
    if (rootDir) {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  beforeEach(() => {
    s3Server.reset();
  });

  afterEach(async () => {
    await cleanupAllChildren();
    s3Server.reset();
  });

  describe("F5 — Logical Backup (3-Node RS Lag, Step-Down, NetworkCut, Bounds, Quiet)", () => {
    let rsF5;
    let f5Dir;
    let workDir;

    beforeAll(async () => {
      f5Dir = path.join(rootDir, "f5");
      workDir = path.join(f5Dir, "work");
      await fs.mkdir(workDir, { recursive: true });
      rsF5 = await startMultiNodeReplicaSet({
        rootDir: f5Dir,
        name: "rsF5",
        enableTestCommands: true,
      });
    });

    afterAll(async () => {
      await rsF5?.restoreMajority().catch(() => {});
      await rsF5?.stop().catch(() => {});
    });

    afterEach(async () => {
      await rsF5?.restoreMajority().catch(() => {});
      for (const node of rsF5?.nodes ?? []) {
        if (node.isRunning()) {
          await rsF5.clearFailPoint(node, "rsSyncApplyStop").catch(() => {});
          await rsF5.clearFailPoint(node, "failCommand").catch(() => {});
        }
      }
    });

    it("5.1 [M3-Gated F-LOG-002] — 3-node RS logical backup with secondary lag sets oplogWindow.endTs <= T0", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF5.uri);
      await client.connect();

      const secondaries = await rsF5.getSecondaryNodes();
      expect(secondaries.length).toBe(2);
      const [sec0, sec1] = secondaries;

      try {
        await dropAllUserDatabases(client);
        await client
          .db("f5lag")
          .collection("items")
          .insertOne({ _id: "d0", step: 0 }, { writeConcern: { w: "majority" } });
        const latestBeforeLag = await getLatestOplogTs(rsF5.uri);
        await waitForMajorityCatchup(rsF5.uri, latestBeforeLag);
        const t0 = await getLatestOplogTs(sec0.directUri);

        // Freeze replication apply on sec0, then commit d1 to primary + sec1
        await rsF5.setFailPoint(sec0, "rsSyncApplyStop", { mode: "alwaysOn" });
        await client
          .db("f5lag")
          .collection("items")
          .insertOne({ _id: "d1", step: 1 }, { writeConcern: { w: "majority" } });
        const t1 = await getLatestOplogTs(rsF5.uri);
        expect(compareTimestamps(t1, t0)).toBeGreaterThan(0);

        // Stop sec1 so only lagged sec0 serves secondary reads
        await sec1.stop();

        const cfg = makeS3Config({
          uri: rsF5.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f5-lag",
          readPreference: "secondary",
          oplog: true,
          pitrEnabled: true,
        });

        const manifest = await runBackup({ config: cfg, storage, logger });
        expect(manifest.oplog).toBe(true);
        expect(manifest.oplogWindow?.endTs).toBeTruthy();

        const recordedEndTs = parseOplogTimestamp(manifest.oplogWindow.endTs);
        expect(compareTimestamps(recordedEndTs, t0)).toBeLessThanOrEqual(0);
      } finally {
        await rsF5.clearFailPoint(sec0, "rsSyncApplyStop").catch(() => {});
        if (!sec1.isRunning()) {
          await sec1.restart().catch(() => {});
        }
        await waitForPrimary(
          rsF5.nodes.map((n) => n.directUri),
          45_000,
        ).catch(() => {});
        const latest = await getLatestOplogTs(rsF5.uri).catch(() => null);
        if (latest) await waitForMajorityCatchup(rsF5.uri, latest).catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("5.2 — primary replSetStepDown mid-backup aborts & cleans up, then retry after election succeeds", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF5.uri);
      await client.connect();

      const oldPrimary = await rsF5.getPrimaryNode();
      const proxy = await createTcpChaosProxy({ targetPort: oldPrimary.port });
      try {
        await dropAllUserDatabases(client);
        // Seed ~24 MiB so mongodump streams continuously across multiple getMore batches
        const chunk = randomBytes(6 * 1024).toString("hex"); // 12 KiB
        for (let b = 0; b < 4; b++) {
          const docs = Array.from({ length: 500 }, (_, i) => ({
            _id: `sd-${b}-${i}`,
            payload: chunk,
          }));
          await client
            .db("f5step")
            .collection("items")
            .insertMany(docs, {
              writeConcern: { w: "majority" },
            });
        }
        await waitForMajorityCatchup(rsF5.uri, await getLatestOplogTs(rsF5.uri));

        // Pace mongodump stream via TCP proxy connected directly to oldPrimary, then step down mid-dump
        proxy.setDelayMs(25);
        const cfg = makeS3Config({
          uri: `mongodb://127.0.0.1:${proxy.listenPort}/?directConnection=true`,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f5-stepdown",
          readPreference: "primary",
          oplog: true,
          pitrEnabled: true,
        });

        const backupPromise = runBackup({ config: cfg, storage, logger });
        const streaming = await waitForCondition(
          () => proxy.getStats().bytesOut >= 500_000,
          15_000,
          20,
        );
        expect(streaming).toBe(true);

        const newPrimary = await rsF5.stepDownPrimary(30, 5);
        expect(newPrimary.id).not.toBe(oldPrimary.id);

        await expect(backupPromise).rejects.toThrow();
        proxy.setDelayMs(0);

        // Storage & workDir must have zero manifests, zero active multipart uploads, zero .yaml files
        const remainingKeys = [];
        for await (const obj of storage.list("f5-stepdown/")) {
          remainingKeys.push(obj.key);
        }
        expect(remainingKeys).toEqual([]);
        expect(s3Server.stats.activeUploads).toBe(0);
        expect(await countYamlFiles(workDir)).toBe(0);

        await waitForMajorityCatchup(rsF5.uri, await getLatestOplogTs(rsF5.uri));

        // Retry backup against newly elected primary and verify restore succeeds
        const retryCfg = makeS3Config({
          uri: rsF5.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f5-stepdown",
          readPreference: "primary",
          oplog: true,
          pitrEnabled: true,
        });
        const retryManifest = await runBackup({ config: retryCfg, storage, logger });
        expect(retryManifest.collections["f5step.items"]).toBe(2000);

        await client.db("f5step").dropDatabase();
        await restoreBackup(
          { config: retryCfg, storage, logger },
          { ref: retryManifest.key, uri: rsF5.uri, drop: true, oplogReplay: true },
        );
        expect(await client.db("f5step").collection("items").countDocuments()).toBe(2000);
      } finally {
        await proxy.close().catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("5.3 — TCP proxy NetworkCut mid-mongodump aborts & cleans up storage/uploads/workDir", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint, { partSizeMb: 5, queueSize: 1 });
      const primary = await rsF5.getPrimaryNode();
      const proxy = await createTcpChaosProxy({ targetPort: primary.port });
      const client = new MongoClient(rsF5.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        // ~6 MiB incompressible random hex so mongodump streams continuously and starts S3 upload
        const batchDocs = Array.from({ length: 600 }, (_, i) => ({
          _id: `nc-${i}`,
          blob: randomBytes(5 * 1024).toString("hex"),
        }));
        await client
          .db("f5cut")
          .collection("blobs")
          .insertMany(batchDocs, {
            writeConcern: { w: "majority" },
          });
        await waitForMajorityCatchup(rsF5.uri, await getLatestOplogTs(rsF5.uri));

        // Slow downstream relay slightly so we reliably cut mid-mongodump
        proxy.setDelayMs(5);

        const cfg = makeS3Config({
          uri: `mongodb://127.0.0.1:${proxy.listenPort}/?directConnection=true`,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f5-netcut",
          partSizeMb: 5,
          readPreference: "primary",
          oplog: true,
          pitrEnabled: true,
          timeoutMs: 20_000,
        });

        const backupPromise = runBackup({ config: cfg, storage, logger });

        const sawTraffic = await waitForCondition(
          () =>
            (proxy.getStats().bytesOut > 200_000 || s3Server.stats.activeUploads > 0) &&
            proxy.getStats().activeConnections > 0,
          15_000,
          25,
        );
        expect(sawTraffic).toBe(true);

        const preCutStats = proxy.getStats();
        expect(preCutStats.activeConnections).toBeGreaterThan(0);

        proxy.blackhole(true);
        proxy.resetAll();

        let backupErr = null;
        try {
          await backupPromise;
        } catch (err) {
          backupErr = err;
        }
        expect(backupErr).toBeInstanceOf(Error);
        expect(backupErr.message).not.toMatch(/corrupt or truncated archive stream/i);
        expect(backupErr.message).toMatch(
          /mongodump|ECONNRESET|socket|network|EOF|exit code|abort/i,
        );
        expect(s3Server.stats.activeUploads).toBe(0);

        const remainingKeys = [];
        for await (const obj of storage.list("f5-netcut/")) {
          remainingKeys.push(obj.key);
        }
        expect(remainingKeys).toEqual([]);
        expect(await countYamlFiles(workDir)).toBe(0);
      } finally {
        await proxy.close().catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("5.4 [M3-Gated F-LOG-002/ArchiveOplogTap] — BackupBoundsCheck (WriteTime <= endTs) under concurrent w:majority writes + ArchiveOplogTap", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF5.uri);
      await client.connect();
      const primary = await rsF5.getPrimaryNode();
      const proxy = await createTcpChaosProxy({ targetPort: primary.port });

      try {
        await dropAllUserDatabases(client);
        const col = client.db("f5bounds").collection("items");
        // Seed ~5 MiB initially + mild TCP relay delay so mongodump overlaps concurrent w:majority writes
        const pad = randomBytes(3 * 1024).toString("hex");
        await col.insertMany(
          Array.from({ length: 800 }, (_, i) => ({ _id: `init-${i}`, seq: -1, pad })),
          { writeConcern: { w: "majority" } },
        );

        proxy.setDelayMs(8);
        const cfg = makeS3Config({
          uri: `mongodb://127.0.0.1:${proxy.listenPort}/?directConnection=true`,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f5-bounds",
          readPreference: "primary",
          oplog: true,
          pitrEnabled: true,
        });

        let running = true;
        let seq = 0;
        const writerPromise = (async () => {
          while (running) {
            const cur = seq++;
            await col.insertOne(
              { _id: `w-${cur}`, seq: cur, pad: "concurrent-write" },
              { writeConcern: { w: "majority" } },
            );
            await new Promise((r) => setTimeout(r, 5));
          }
        })();

        let manifest;
        try {
          manifest = await runBackup({ config: cfg, storage, logger });
        } finally {
          running = false;
          await writerPromise.catch(() => {});
          proxy.setDelayMs(0);
        }

        expect(manifest.oplog).toBe(true);
        expect(manifest.oplogWindow?.endTs).toBeTruthy();
        expect(manifest.oplogWindow?.entries ?? 0).toBeGreaterThan(0);

        const endTs = parseOplogTimestamp(manifest.oplogWindow.endTs);

        // Map each inserted _id to its exact oplog ts before dropping
        const writeTsById = new Map();
        const oplogDocs = await client
          .db("local")
          .collection("oplog.rs")
          .find({ op: "i", ns: "f5bounds.items" })
          .toArray();
        for (const entry of oplogDocs) {
          const id = entry.o?._id;
          const ts = fromBsonTimestamp(entry.ts);
          if (id && ts) writeTsById.set(id, ts);
        }

        await client.db("f5bounds").dropDatabase();
        const restoreCfg = {
          ...cfg,
          mongo: { ...cfg.mongo, uri: rsF5.uri },
        };
        await restoreBackup(
          { config: restoreCfg, storage, logger },
          { ref: manifest.key, uri: rsF5.uri, drop: true, oplogReplay: true },
        );

        const restoredIds = new Set(
          (await col.find({}, { projection: { _id: 1 } }).toArray()).map((d) => d._id),
        );
        expect(restoredIds.has("init-0")).toBe(true);

        for (const id of restoredIds) {
          const ts = writeTsById.get(id);
          expect(ts).toBeTruthy();
          expect(compareTimestamps(ts, endTs)).toBeLessThanOrEqual(0);
        }

        // Verify post-endTs writes were excluded
        for (const [id, ts] of writeTsById.entries()) {
          if (compareTimestamps(ts, endTs) > 0) {
            expect(restoredIds.has(id)).toBe(false);
          }
        }
      } finally {
        await proxy.close().catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("5.5 — quiet cluster logical backup records oplogEntries === 0 and oplogWindow.endTs === preOplog.newestTs", async () => {
      const quietDir = path.join(f5Dir, "quiet-rs");
      const quietWorkDir = path.join(quietDir, "work");
      await fs.mkdir(quietWorkDir, { recursive: true });

      const quietRs = await startReplicaSet({
        rootDir: quietDir,
        name: "rsF5Quiet",
        extraArgs: [
          "--setParameter=writePeriodicNoops=false",
          "--setParameter=disableLogicalSessionCacheRefresh=true",
        ],
      });
      const client = new MongoClient(quietRs.uri);

      try {
        await client.connect();
        const encKey = parseKey(generateKey());
        const storage = makeS3Storage(s3Server.endpoint);

        await client
          .db("f5quiet")
          .collection("items")
          .insertOne({ _id: "q1", v: 1 }, { writeConcern: { w: "majority" } });
        await waitForMajorityCatchup(quietRs.uri, await getLatestOplogTs(quietRs.uri));
        await new Promise((r) => setTimeout(r, 300));

        let observedPreOplog = null;
        const cfg = makeS3Config({
          uri: quietRs.uri,
          s3Endpoint: s3Server.endpoint,
          workDir: quietWorkDir,
          encKey,
          prefix: "f5-quiet",
          readPreference: "primary",
          oplog: true,
          pitrEnabled: true,
        });

        const manifest = await runBackup({
          config: cfg,
          storage,
          logger,
          tools: {
            mongodump,
            getOplogBounds: async (uri, opts) => {
              const bounds = await getOplogBounds(uri, opts);
              if (opts?.phase === "pre") observedPreOplog = bounds;
              return bounds;
            },
          },
        });

        expect(manifest.oplog).toBe(true);
        expect(observedPreOplog?.newestTs).toBeTruthy();
        expect(manifest.oplogWindow?.entries).toBe(0);
        expect(manifest.oplogWindow?.endTs).toBe(formatOplogTimestamp(observedPreOplog.newestTs));
      } finally {
        await client.close().catch(() => {});
        await quietRs.stop().catch(() => {});
      }
    });
  });

  describe("F6 — Logical Restore (Full, Selective, --no-drop/--drop, preVerify Corrupt Abort)", () => {
    let f6Dir;
    let storageDir;
    let workDir;
    let rsF6;

    beforeAll(async () => {
      f6Dir = path.join(rootDir, "f6");
      storageDir = path.join(f6Dir, "storage");
      workDir = path.join(f6Dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });
      rsF6 = await startReplicaSet({ rootDir: f6Dir, name: "rsF6" });
    });

    afterAll(async () => {
      await rsF6?.stop().catch(() => {});
    });

    it("6.1a — full & selective (nsInclude/nsExclude/DB rename) logical restore + nsInclude+oplogReplay rejection", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF6.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("shop").collection("users").insertOne({ _id: "u1", name: "Alice" });
        await client.db("shop").collection("orders").insertOne({ _id: "o1", item: "book" });
        await client.db("shop").collection("audit_log").insertOne({ _id: "a1", ev: "login" });
        await client.db("analytics").collection("events").insertOne({ _id: "e1", count: 1 });

        const cfgAll = makeFsConfig({
          uri: rsF6.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f6-sel",
          database: "*",
          oplog: true,
          pitrEnabled: false,
        });
        const cfgShop = makeFsConfig({
          uri: rsF6.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f6-shop",
          database: "shop",
          oplog: false,
          pitrEnabled: false,
        });

        const bAll = await runBackup({ config: cfgAll, storage, logger });
        const bShop = await runBackup({ config: cfgShop, storage, logger });

        // Reject nsInclude + oplogReplay: true
        await expect(
          restoreBackup(
            { config: cfgAll, storage, logger },
            { ref: bAll.key, uri: rsF6.uri, nsInclude: "shop.users", oplogReplay: true },
          ),
        ).rejects.toThrow(/Cannot use --oplog-replay/);

        // (a) nsInclude: "shop.users" leaves shop.orders & analytics.events sentinels intact
        await client.db("shop").collection("users").deleteMany({});
        await client.db("shop").collection("orders").insertOne({ _id: "o_sentinel", keep: true });
        await client
          .db("analytics")
          .collection("events")
          .insertOne({ _id: "e_sentinel", keep: true });

        await restoreBackup(
          { config: cfgAll, storage, logger },
          { ref: bAll.key, uri: rsF6.uri, nsInclude: "shop.users", drop: true },
        );
        expect(await client.db("shop").collection("users").countDocuments()).toBe(1);
        expect(
          await client.db("shop").collection("orders").findOne({ _id: "o_sentinel" }),
        ).toBeTruthy();
        expect(
          await client.db("analytics").collection("events").findOne({ _id: "e_sentinel" }),
        ).toBeTruthy();

        // (b) nsExclude: "shop.audit_log" restores shop.orders while preserving shop.audit_log sentinel
        await client
          .db("shop")
          .collection("audit_log")
          .insertOne({ _id: "a_sentinel", keep: true });
        await restoreBackup(
          { config: cfgAll, storage, logger },
          { ref: bAll.key, uri: rsF6.uri, nsExclude: "shop.audit_log", drop: true },
        );
        expect(await client.db("shop").collection("orders").countDocuments()).toBe(1);
        expect(
          await client.db("shop").collection("orders").findOne({ _id: "o_sentinel" }),
        ).toBeNull();
        expect(
          await client.db("shop").collection("audit_log").findOne({ _id: "a_sentinel" }),
        ).toBeTruthy();

        // (c) DB rename from single-DB backup: shop -> shop_clone leaves original shop intact
        await restoreBackup(
          { config: cfgShop, storage, logger },
          { ref: bShop.key, uri: rsF6.uri, database: "shop_clone", drop: true },
        );
        expect(
          await client.db("shop_clone").collection("users").findOne({ _id: "u1" }),
        ).toBeTruthy();
        expect(
          await client.db("shop_clone").collection("orders").findOne({ _id: "o1" }),
        ).toBeTruthy();
        expect(
          await client.db("shop").collection("audit_log").findOne({ _id: "a_sentinel" }),
        ).toBeTruthy();
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("6.1b [M3-Gated F-LOG-001] — nsExclude restore preserves pre-existing admin.system.version / config.system.sessions", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF6.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("f6sys").collection("docs").insertOne({ _id: "s1" });
        await client.db("f6sys").collection("skipme").insertOne({ _id: "k1" });

        const cfg = makeFsConfig({
          uri: rsF6.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f6-sys",
          database: "*",
          oplog: false,
          pitrEnabled: false,
        });

        const backup = await runBackup({ config: cfg, storage, logger });

        const collInfoBefore = await client
          .db("config")
          .listCollections({ name: "system.sessions" })
          .toArray();
        const uuidBefore = collInfoBefore[0]?.info?.uuid?.toString();

        await client
          .db("admin")
          .collection("system.version")
          .updateOne({ _id: "f6-sentinel" }, { $set: { marker: "keep" } }, { upsert: true });

        await client.db("f6sys").collection("docs").deleteMany({});
        await client.db("f6sys").collection("skipme").insertOne({ _id: "skip-sentinel" });

        await restoreBackup(
          { config: cfg, storage, logger },
          { ref: backup.key, uri: rsF6.uri, nsExclude: "f6sys.skipme", drop: true },
        );

        const docsCountAfter = await client.db("f6sys").collection("docs").countDocuments();
        expect(docsCountAfter).toBeGreaterThanOrEqual(1);
        expect(await client.db("f6sys").collection("docs").findOne({ _id: "s1" })).toBeTruthy();

        const skipmeCountAfter = await client.db("f6sys").collection("skipme").countDocuments();
        expect(skipmeCountAfter).toBe(2);
        expect(
          await client.db("f6sys").collection("skipme").findOne({ _id: "skip-sentinel" }),
        ).toBeTruthy();

        const collInfoAfter = await client
          .db("config")
          .listCollections({ name: "system.sessions" })
          .toArray();
        const uuidAfter = collInfoAfter[0]?.info?.uuid?.toString();
        expect(uuidAfter).toBe(uuidBefore);

        const sentinelAfter = await client
          .db("admin")
          .collection("system.version")
          .findOne({ _id: "f6-sentinel" });
        expect(sentinelAfter?.marker).toBe("keep");
      } finally {
        await client
          .db("admin")
          .collection("system.version")
          .deleteOne({ _id: "f6-sentinel" })
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("6.2 — --no-drop restores missing docs while preserving live-only docs, whereas --drop reverts cleanly", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF6.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        const col = client.db("f6drop").collection("items");
        await col.insertMany([
          { _id: "b1", val: "original-1" },
          { _id: "b2", val: "original-2" },
        ]);

        const cfg = makeFsConfig({
          uri: rsF6.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f6-drop",
          database: "*",
          oplog: false,
          pitrEnabled: false,
        });
        const manifest = await runBackup({ config: cfg, storage, logger });

        // Delete b2 (so --no-drop can re-insert b2 cleanly without E11000) and insert live-only
        await col.deleteOne({ _id: "b2" });
        await col.dropIndex("_id_").catch(() => {}); // no-op, _id_ cannot be dropped
        await col.deleteOne({ _id: "b1" });
        await col.insertOne({ _id: "live-only", val: "extra" });

        await restoreBackup(
          { config: cfg, storage, logger },
          { ref: manifest.key, uri: rsF6.uri, drop: false },
        );
        expect((await col.findOne({ _id: "b1" }))?.val).toBe("original-1");
        expect((await col.findOne({ _id: "b2" }))?.val).toBe("original-2");
        expect((await col.findOne({ _id: "live-only" }))?.val).toBe("extra");

        await col.updateOne({ _id: "b1" }, { $set: { val: "modified" } });
        await restoreBackup(
          { config: cfg, storage, logger },
          { ref: manifest.key, uri: rsF6.uri, drop: true },
        );
        expect((await col.findOne({ _id: "b1" }))?.val).toBe("original-1");
        expect(await col.findOne({ _id: "live-only" })).toBeNull();
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("6.3 — preVerify aborts before --drop on bit-flipped archive AND corrupt PITR slice, preserving live RS intact", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF6.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        const survivors = client.db("shop").collection("survivors");
        await survivors.insertOne({ _id: "s1", status: "backed-up" });

        const cfg = makeFsConfig({
          uri: rsF6.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f6-corrupt",
          database: "*",
          oplog: true,
          pitrEnabled: true,
        });

        const baseManifest = await runBackup({ config: cfg, storage, logger });

        // Capture S1 and S2 PITR slices
        await survivors.insertOne({ _id: "s2", status: "slice-1" });
        await waitForMajorityCatchup(rsF6.uri, await getLatestOplogTs(rsF6.uri));
        const s1 = await captureOplogSlice({ config: cfg, storage, logger });
        expect(s1).toBeTruthy();

        await survivors.insertOne({ _id: "s3", status: "slice-2" });
        await waitForMajorityCatchup(rsF6.uri, await getLatestOplogTs(rsF6.uri));
        const s2 = await captureOplogSlice({ config: cfg, storage, logger });
        expect(s2).toBeTruthy();

        // Add live sentinel that MUST survive failed --drop restores
        await survivors.insertOne({ _id: "live-sentinel", status: "intact" });

        // (a) Corrupt PITR slice S2 on disk before PITR restore with drop: true
        const s2Path = path.join(storageDir, s2.key);
        const s2Bytes = await fs.readFile(s2Path);
        const s2Flipped = Buffer.from(s2Bytes);
        s2Flipped[Math.floor(s2Flipped.length / 2)] ^= 0xff;
        await fs.writeFile(s2Path, s2Flipped);

        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF6.uri, pitr: s2.oplogWindow.endTs, drop: true },
          ),
        ).rejects.toThrow(IntegrityError);
        expect((await survivors.findOne({ _id: "live-sentinel" }))?.status).toBe("intact");
        expect(await survivors.countDocuments()).toBe(4);

        // (b) Corrupt base archive on disk before base restore with drop: true
        const basePath = path.join(storageDir, baseManifest.key);
        const baseBytes = await fs.readFile(basePath);
        const baseFlipped = Buffer.from(baseBytes);
        baseFlipped[Math.floor(baseFlipped.length / 2)] ^= 0xff;
        await fs.writeFile(basePath, baseFlipped);

        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { ref: baseManifest.key, uri: rsF6.uri, drop: true },
          ),
        ).rejects.toThrow(IntegrityError);
        expect((await survivors.findOne({ _id: "live-sentinel" }))?.status).toBe("intact");
        expect(await survivors.countDocuments()).toBe(4);
      } finally {
        await client.close().catch(() => {});
      }
    });
  });

  describe("F9 — PITR Oplog Slicer (Majority Stall, Rollover, CappedPositionLost, Active Txn, Split, Archiver)", () => {
    it("9.1 — 3-node RS majority commit stall freezes PITR archiving of uncommitted w:1 writes until majority restores", async () => {
      const dir = path.join(rootDir, "f9-majority");
      const storageDir = path.join(dir, "storage");
      const workDir = path.join(dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });

      const rs = await startMultiNodeReplicaSet({
        rootDir: dir,
        name: "rsF9Maj",
        enableTestCommands: true,
      });
      const client = new MongoClient(rs.uri);

      try {
        await client.connect();
        const encKey = parseKey(generateKey());
        const storage = new FilesystemStorageClient(storageDir);
        const cfg = makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f9-maj",
          oplog: true,
          pitrEnabled: true,
        });

        await client
          .db("f9maj")
          .collection("docs")
          .insertOne({ _id: "c1", v: 1 }, { writeConcern: { w: "majority" } });
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));

        const base = await runBackup({ config: cfg, storage, logger });

        await rs.induceMajorityStall({ mode: "freezeCommitPoint" });

        // Insert uncommitted w:1 document while majority commit point is frozen
        await client
          .db("f9maj")
          .collection("docs")
          .insertOne({ _id: "uncommitted-w1", v: 2 }, { writeConcern: { w: 1 } });

        const oplogEntry = await client
          .db("local")
          .collection("oplog.rs")
          .findOne({ "o._id": "uncommitted-w1" });
        expect(oplogEntry).toBeTruthy();

        const stalledSlice = await captureOplogSlice({ config: cfg, storage, logger });
        if (stalledSlice) {
          const sliceEnd = parseOplogTimestamp(stalledSlice.oplogWindow.endTs);
          const uncommTs = fromBsonTimestamp(oplogEntry.ts);
          expect(compareTimestamps(sliceEnd, uncommTs)).toBeLessThan(0);
        } else {
          expect(stalledSlice).toBeNull();
        }

        // Restore majority -> uncommitted-w1 becomes majority-committed and can be archived + PITR restored
        await rs.restoreMajority();
        const recoveredSlice = await captureOplogSlice({ config: cfg, storage, logger });
        expect(recoveredSlice).toBeTruthy();
        expect(recoveredSlice.oplogWindow?.entries).toBeGreaterThanOrEqual(1);

        await client.db("f9maj").dropDatabase();
        await restoreBackup(
          { config: cfg, storage, logger },
          { uri: rs.uri, pitr: recoveredSlice.oplogWindow.endTs, drop: true },
        );
        expect(await client.db("f9maj").collection("docs").findOne({ _id: "c1" })).toBeTruthy();
        expect(
          await client.db("f9maj").collection("docs").findOne({ _id: "uncommitted-w1" }),
        ).toBeTruthy();
        expect(base.oplog).toBe(true);
      } finally {
        await rs.restoreMajority().catch(() => {});
        await client.close().catch(() => {});
        await rs.stop().catch(() => {});
      }
    });

    it("9.2a — real --oplogSize=1 rollover detects gapDetected: true and blocks --drop PITR restore preserving live sentinel", async () => {
      const dir = path.join(rootDir, "f9-rollover");
      const storageDir = path.join(dir, "storage");
      const workDir = path.join(dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });

      const rs = await startReplicaSet({
        rootDir: dir,
        name: "rsF9Roll",
        oplogSizeMb: 1,
        enableTestCommands: true,
        extraArgs: ["--setParameter=syncdelay=1"],
      });
      const client = new MongoClient(rs.uri);

      try {
        await client.connect();
        const encKey = parseKey(generateKey());
        const storage = new FilesystemStorageClient(storageDir);
        const cfg = makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f9-roll",
          oplog: true,
          pitrEnabled: true,
        });

        await client.db("f9roll").collection("docs").insertOne({ _id: "b1", v: 1 });
        await runBackup({ config: cfg, storage, logger });

        await client.db("f9roll").collection("docs").insertOne({ _id: "w1", v: 2 });
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));
        const s1 = await captureOplogSlice({ config: cfg, storage, logger });
        expect(s1).toBeTruthy();
        const s1EndTs = parseOplogTimestamp(s1.oplogWindow.endTs);

        // Roll over 1 MiB oplog deterministically past s1.endTs (checkpoint via syncdelay=1 + fsync:1)
        const chunk = randomBytes(8 * 1024).toString("hex"); // 16 KiB
        let rolledOver = false;
        for (let round = 0; round < 10; round++) {
          const fillerDocs = Array.from({ length: 100 }, (_, i) => ({
            _id: `roll-${round}-${i}`,
            pad: chunk,
          }));
          await client.db("f9roll").collection("filler").insertMany(fillerDocs);
          await client.db("admin").command({ fsync: 1 });
          const oldest = await client
            .db("local")
            .collection("oplog.rs")
            .find({}, { projection: { ts: 1 } })
            .sort({ $natural: 1 })
            .limit(1)
            .next();
          const oldestTs = fromBsonTimestamp(oldest?.ts);
          if (oldestTs && compareTimestamps(oldestTs, s1EndTs) > 0) {
            rolledOver = true;
            break;
          }
        }
        expect(rolledOver).toBe(true);
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));

        const gapSlice = await captureOplogSlice({ config: cfg, storage, logger });
        expect(gapSlice).toBeTruthy();
        expect(gapSlice.oplogWindow?.gapDetected).toBe(true);

        const stateObj = await storage.getJson(pitrStateKey(scopeOf(cfg)));
        expect(stateObj?.data?.gapDetected).toBe(true);

        // Live sentinel must survive blocked --drop PITR restore
        await client
          .db("f9roll")
          .collection("docs")
          .insertOne({ _id: "live-sentinel", keep: true });
        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rs.uri, pitr: gapSlice.oplogWindow.endTs, drop: true },
          ),
        ).rejects.toThrow(/gap/i);

        expect(
          (await client.db("f9roll").collection("docs").findOne({ _id: "live-sentinel" }))?.keep,
        ).toBe(true);
      } finally {
        await client.close().catch(() => {});
        await rs.stop().catch(() => {});
      }
    });

    it("9.2b [M4-Gated F-PITR-003] — mid-cursor CappedPositionLost (Int32(136)) with startOplogArchiver preserves state.lastTs, sets gap metric=1, and alerts", async () => {
      const dir = path.join(rootDir, "f9-cpl");
      const storageDir = path.join(dir, "storage");
      const workDir = path.join(dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });

      const rs = await startReplicaSet({
        rootDir: dir,
        name: "rsF9Cpl",
        oplogSizeMb: 1,
        enableTestCommands: true,
        extraArgs: ["--setParameter=syncdelay=1"],
      });
      const client = new MongoClient(rs.uri);

      try {
        await client.connect();
        const encKey = parseKey(generateKey());
        const storage = new FilesystemStorageClient(storageDir);
        const cfg = makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f9-cpl",
          oplog: true,
          pitrEnabled: true,
          flushIntervalSeconds: 60,
        });

        await client.db("f9cpl").collection("docs").insertOne({ _id: "base", v: 1 });
        await runBackup({ config: cfg, storage, logger });

        await client.db("f9cpl").collection("docs").insertOne({ _id: "s1", v: 2 });
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));
        const s1 = await captureOplogSlice({ config: cfg, storage, logger });
        expect(s1).toBeTruthy();
        const confirmedLastTs = s1.oplogWindow.endTs;

        // Insert 130 separate docs (>101 firstBatch size) so oplog.rs cursor must issue getMore
        for (let i = 0; i < 130; i++) {
          await client
            .db("f9cpl")
            .collection("small")
            .insertOne({ _id: `sm-${i}`, n: i });
        }
        const lastSmallTs = await getLatestOplogTs(rs.uri);
        await waitForMajorityCatchup(rs.uri, lastSmallTs);

        // Block getMore long enough to roll over the 1 MiB oplog past cursor position
        await rs.setFailPoint(rs.nodes[0], "failCommand", {
          mode: { times: 1 },
          data: {
            failCommands: ["getMore"],
            blockConnection: true,
            blockTimeMS: 5_000,
          },
        });

        const metrics = createMetricsRegistry();
        const failureAlerts = [];
        const notifier = {
          notifySuccess: async () => {},
          notifyFailure: async (event, error) => {
            failureAlerts.push({ event, message: error?.message ?? String(error) });
          },
        };

        const archiver = startOplogArchiver({ config: cfg, storage, logger, notifier, metrics });
        archiver.start();

        // Wait briefly for archiver tick to open cursor and enter blocked getMore, then roll over oplog
        await new Promise((r) => setTimeout(r, 300));
        const chunk = randomBytes(8 * 1024).toString("hex");
        for (let b = 0; b < 12; b++) {
          const docs = Array.from({ length: 60 }, (_, i) => ({
            _id: `over-${b}-${i}`,
            pad: chunk,
          }));
          await client.db("f9cpl").collection("overflow").insertMany(docs);
          await client.db("admin").command({ fsync: 1 });
          const oldest = await client
            .db("local")
            .collection("oplog.rs")
            .find({}, { projection: { ts: 1 } })
            .sort({ $natural: 1 })
            .limit(1)
            .next();
          const oldestTs = fromBsonTimestamp(oldest?.ts);
          if (oldestTs && compareTimestamps(oldestTs, lastSmallTs) > 0) {
            break;
          }
        }

        const alerted = await waitForCondition(() => failureAlerts.length > 0, 15_000, 50);
        await archiver.stop();
        expect(alerted).toBe(true);

        const stateAfter = await storage.getJson(pitrStateKey(scopeOf(cfg)));
        expect(stateAfter?.data?.gapDetected).toBe(true);
        expect(stateAfter?.data?.lastTs).toBe(confirmedLastTs);

        const metricsText = await metrics.getMetrics();
        expect(metricsText).toMatch(/mongodb_backup_pitr_gap_detected\s+1(\.0+)?(\s|$)/);
      } finally {
        await rs.clearFailPoint(rs.nodes[0], "failCommand").catch(() => {});
        await client.close().catch(() => {});
        await rs.stop().catch(() => {});
      }
    });

    it("9.3 — active config.transactions clamping (onCappedByActiveTxn) & prevOpTime precursor chain recovery on commit", async () => {
      const dir = path.join(rootDir, "f9-activetxn");
      const storageDir = path.join(dir, "storage");
      const workDir = path.join(dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });

      const rs = await startReplicaSet({
        rootDir: dir,
        name: "rsF9Txn",
        oplogSizeMb: 64,
        enableTestCommands: true,
      });
      const client = new MongoClient(rs.uri);

      try {
        await client.connect();
        const encKey = parseKey(generateKey());
        const storage = new FilesystemStorageClient(storageDir);
        const cfg = makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f9-txn",
          oplog: true,
          pitrEnabled: true,
        });

        await client.db("f9txn").collection("docs").insertOne({ _id: "base1", v: 1 });
        await client.db("f9txn").collection("prep").insertOne({ _id: "init", v: 0 });
        await runBackup({ config: cfg, storage, logger });

        // Committed write before open transaction
        await client.db("f9txn").collection("docs").insertOne({ _id: "c_before", v: 2 });

        // Start & prepare a multi-doc transaction so config.transactions has state: "prepared" + startOpTime
        const session = client.startSession();
        session.startTransaction({ writeConcern: { w: "majority" } });
        await client.db("f9txn").collection("prep").insertOne({ _id: "p1", v: 42 }, { session });
        const prepRes = await client.db("admin").command({ prepareTransaction: 1 }, { session });
        expect(prepRes.prepareTimestamp).toBeTruthy();

        const activeTxDoc = await client
          .db("config")
          .collection("transactions")
          .findOne({ state: { $in: ["inProgress", "prepared"] } });
        expect(activeTxDoc?.startOpTime?.ts).toBeTruthy();
        const activeStartTs = fromBsonTimestamp(activeTxDoc.startOpTime.ts);

        let cappedAtTs = null;
        const cappedSlice = await captureOplogSlice({
          config: cfg,
          storage,
          logger,
          onCappedByActiveTxn: (ts) => {
            cappedAtTs = ts;
          },
        });
        expect(cappedAtTs).toBeTruthy();
        expect(compareTimestamps(cappedAtTs, activeStartTs)).toBe(0);
        if (cappedSlice) {
          expect(
            compareTimestamps(parseOplogTimestamp(cappedSlice.oplogWindow.endTs), activeStartTs),
          ).toBeLessThan(0);
        }

        // Now test prevOpTime precursor chain when base backup completes while transaction is prepared
        const midCfg = makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f9-txn-precursor",
          oplog: true,
          pitrEnabled: true,
        });
        const midBase = await runBackup({ config: midCfg, storage, logger });
        expect(midBase.oplogWindow?.endTs).toBeTruthy();

        await client
          .db("admin")
          .command(
            { commitTransaction: 1, commitTimestamp: prepRes.prepareTimestamp },
            { session },
          );
        await session.endSession();
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));

        const commitSlice = await captureOplogSlice({ config: midCfg, storage, logger });
        expect(commitSlice).toBeTruthy();

        // Restoring midBase + commitSlice must recover prepared p1 atomically via prevOpTime precursor chain
        await client.db("f9txn").dropDatabase();
        await restoreBackup(
          { config: midCfg, storage, logger },
          { uri: rs.uri, pitr: commitSlice.oplogWindow.endTs, drop: true },
        );

        expect(
          await client.db("f9txn").collection("docs").findOne({ _id: "c_before" }),
        ).toBeTruthy();
        expect((await client.db("f9txn").collection("prep").findOne({ _id: "p1" }))?.v).toBe(42);
      } finally {
        await client.close().catch(() => {});
        await rs.stop().catch(() => {});
      }
    });

    it("9.4 / 9.5 / 9.6 — quiet window coverageEndTs, PITR_MAX_SLICE_MB=1 split, and continuous startOplogArchiver race", async () => {
      const dir = path.join(rootDir, "f9-split");
      const storageDir = path.join(dir, "storage");
      const workDir = path.join(dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });

      const rs = await startReplicaSet({ rootDir: dir, name: "rsF9Split", oplogSizeMb: 64 });
      const client = new MongoClient(rs.uri);

      try {
        await client.connect();
        const encKey = parseKey(generateKey());
        const storage = new FilesystemStorageClient(storageDir);
        const cfg = makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f9-split",
          oplog: true,
          pitrEnabled: true,
          maxSliceMb: 1,
          flushIntervalSeconds: 1,
        });

        await client.db("f9s").collection("items").insertOne({ _id: "init", v: 0 });
        await runBackup({ config: cfg, storage, logger });

        // 9.4 Quiet window returns null, records coverageEndTs with gapDetected: false, and resolves plan
        await client.db("f9s").collection("items").insertOne({ _id: "s1", v: 1 });
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));
        const s1 = await captureOplogSlice({ config: cfg, storage, logger });
        expect(s1).toBeTruthy();

        const quiet = await captureOplogSlice({ config: cfg, storage, logger });
        expect(quiet).toBeNull();
        const quietState = await storage.getJson(pitrStateKey(scopeOf(cfg)));
        expect(quietState?.data?.gapDetected).toBe(false);
        expect(quietState?.data?.coverageEndTs).toBeTruthy();
        const quietPlan = await resolvePitrPlan(
          storage,
          scopeOf(cfg),
          quietState.data.coverageEndTs,
        );
        expect(quietPlan.slices.length).toBeGreaterThanOrEqual(1);

        // 9.5 PITR_MAX_SLICE_MB=1 split into >= 2 contiguous slices
        const bigStr = randomBytes(16 * 1024).toString("hex"); // 32 KiB
        const splitDocs = Array.from({ length: 85 }, (_, i) => ({
          _id: `sp-${i}`,
          blob: bigStr,
        }));
        await client.db("f9s").collection("split").insertMany(splitDocs);
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));

        const splitSlices = [];
        while (true) {
          const sl = await captureOplogSlice({ config: cfg, storage, logger });
          if (!sl) break;
          splitSlices.push(sl);
        }
        expect(splitSlices.length).toBeGreaterThanOrEqual(2);
        for (let i = 1; i < splitSlices.length; i++) {
          expect(splitSlices[i].oplogWindow?.previousEndTs).toBe(
            splitSlices[i - 1].oplogWindow?.endTs,
          );
          expect(splitSlices[i].oplogWindow?.gapDetected).toBe(false);
        }
        const splitPlan = await resolvePitrPlan(
          storage,
          scopeOf(cfg),
          splitSlices[splitSlices.length - 1].oplogWindow.endTs,
        );
        expect(splitPlan.slices.length).toBeGreaterThanOrEqual(splitSlices.length + 1);

        // 9.6 Continuous startOplogArchiver racing runBackup, pruneOplogSlices, and concurrent writes
        const preRaceTs = formatOplogTimestamp(await getLatestOplogTs(rs.uri));
        const archiver = startOplogArchiver({ config: cfg, storage, logger });
        archiver.start();

        let writing = true;
        let writeCount = 0;
        const writerTask = (async () => {
          while (writing) {
            const idx = writeCount++;
            await client
              .db("f9s")
              .collection("race")
              .insertOne({ _id: `r-${idx}`, idx });
            await new Promise((r) => setTimeout(r, 30));
          }
        })();

        try {
          await new Promise((r) => setTimeout(r, 400));
          await runBackup({ config: cfg, storage, logger });
          await pruneOplogSlices({ config: cfg, storage, logger }, { olderThan: preRaceTs });
          await new Promise((r) => setTimeout(r, 1_200));
        } finally {
          writing = false;
          await writerTask.catch(() => {});
          await archiver.stop().catch(() => {});
        }

        // Flush any remaining committed ops post-archiver stop
        const lastWriteTs = await getLatestOplogTs(rs.uri);
        await waitForMajorityCatchup(rs.uri, lastWriteTs);
        let finalSlice = null;
        while (true) {
          const sl = await captureOplogSlice({ config: cfg, storage, logger });
          if (!sl) break;
          finalSlice = sl;
        }

        const pitrState = await storage.getJson(pitrStateKey(scopeOf(cfg)));
        const targetTs =
          finalSlice?.oplogWindow?.endTs ??
          pitrState?.data?.coverageEndTs ??
          pitrState?.data?.lastTs;
        expect(targetTs).toBeTruthy();
        expect(
          compareTimestamps(parseOplogTimestamp(targetTs), lastWriteTs),
        ).toBeGreaterThanOrEqual(0);

        const expectedRaceCount = writeCount;
        expect(expectedRaceCount).toBeGreaterThan(5);

        await client.db("f9s").dropDatabase();
        await restoreBackup(
          { config: cfg, storage, logger },
          { uri: rs.uri, pitr: targetTs, drop: true },
        );

        expect(await client.db("f9s").collection("race").countDocuments()).toBe(expectedRaceCount);
      } finally {
        await client.close().catch(() => {});
        await rs.stop().catch(() => {});
      }
    });
  });

  describe("F10 — PITR Replay & OplogApplier (Txns, 18 MiB Spill, Stage-2 Abort, DDL, Rename, Timeseries, Gap)", () => {
    let f10Dir;
    let storageDir;
    let workDir;
    let rsF10;

    beforeAll(async () => {
      f10Dir = path.join(rootDir, "f10");
      storageDir = path.join(f10Dir, "storage");
      workDir = path.join(f10Dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });
      rsF10 = await startReplicaSet({ rootDir: f10Dir, name: "rsF10", oplogSizeMb: 128 });
    });

    afterAll(async () => {
      await rsF10?.stop().catch(() => {});
    });

    it("10.1 — multi-doc ACID txn committed and aborted txn excluded in PITR", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF10.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client
          .db("f10tx")
          .collection("acct")
          .insertMany([
            { _id: "a", bal: 100 },
            { _id: "b", bal: 50 },
          ]);

        const cfg = makeFsConfig({
          uri: rsF10.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f10-tx",
          oplog: true,
          pitrEnabled: true,
          maxSliceMb: 1,
        });
        await runBackup({ config: cfg, storage, logger });

        // Committed ACID txn
        const sCommit = client.startSession();
        await sCommit.withTransaction(async () => {
          await client
            .db("f10tx")
            .collection("acct")
            .updateOne({ _id: "a" }, { $inc: { bal: -30 } }, { session: sCommit });
          await client
            .db("f10tx")
            .collection("acct")
            .updateOne({ _id: "b" }, { $inc: { bal: 30 } }, { session: sCommit });
          await client
            .db("f10tx")
            .collection("ledger")
            .insertOne({ _id: "l1", amt: 30 }, { session: sCommit });
        });
        await sCommit.endSession();

        // Aborted ACID txn
        const sAbort = client.startSession();
        sAbort.startTransaction();
        await client
          .db("f10tx")
          .collection("acct")
          .updateOne({ _id: "a" }, { $set: { bal: -999 } }, { session: sAbort });
        await client
          .db("f10tx")
          .collection("ledger")
          .insertOne({ _id: "l_aborted", amt: 999 }, { session: sAbort });
        await sAbort.abortTransaction();
        await sAbort.endSession();

        await waitForMajorityCatchup(rsF10.uri, await getLatestOplogTs(rsF10.uri));
        let lastSlice = null;
        while (true) {
          const sl = await captureOplogSlice({ config: cfg, storage, logger });
          if (!sl) break;
          lastSlice = sl;
        }
        expect(lastSlice).toBeTruthy();

        await client.db("f10tx").dropDatabase();
        await restoreBackup(
          { config: cfg, storage, logger },
          { uri: rsF10.uri, pitr: lastSlice.oplogWindow.endTs, drop: true },
        );

        expect((await client.db("f10tx").collection("acct").findOne({ _id: "a" }))?.bal).toBe(70);
        expect((await client.db("f10tx").collection("acct").findOne({ _id: "b" }))?.bal).toBe(80);
        expect(await client.db("f10tx").collection("ledger").findOne({ _id: "l1" })).toBeTruthy();
        expect(
          await client.db("f10tx").collection("ledger").findOne({ _id: "l_aborted" }),
        ).toBeNull();
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("10.2 & 10.3 — real 18 MiB txn triggers TxnBuffer disk spill + Stage-2 mid-OplogApplier abort cleanup + clean re-restore", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const spillWorkDir = path.join(f10Dir, "work-spill");
      await fs.mkdir(spillWorkDir, { recursive: true });
      const client = new MongoClient(rsF10.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("f10spill").collection("docs").insertOne({ _id: "seed", v: 0 });

        const cfg = makeFsConfig({
          uri: rsF10.uri,
          storageDir,
          workDir: spillWorkDir,
          encKey,
          prefix: "f10-spill",
          oplog: true,
          pitrEnabled: true,
          maxSliceMb: 16,
        });

        await runBackup({ config: cfg, storage, logger });

        // Execute 18 MiB incompressible ACID transaction (36 docs x 512 KiB > 8 MiB TXN_SPILL_THRESHOLD_BYTES)
        const docBody = randomBytes(384 * 1024).toString("base64");
        const expectedSha = createHash("sha256").update(docBody).digest("hex");
        const session = client.startSession();
        await session.withTransaction(async () => {
          const col = client.db("f10spill").collection("big");
          for (let i = 0; i < 36; i++) {
            await col.insertOne({ _id: `sp-${i}`, i, body: docBody }, { session });
          }
        });
        await session.endSession();

        await waitForMajorityCatchup(rsF10.uri, await getLatestOplogTs(rsF10.uri));
        let lastSlice = null;
        while (true) {
          const sl = await captureOplogSlice({ config: cfg, storage, logger });
          if (!sl) break;
          lastSlice = sl;
        }
        expect(lastSlice).toBeTruthy();
        const targetTs = lastSlice.oplogWindow.endTs;

        // 10.2: Watch spillWorkDir for .mongovault-txn-*.bson during full PITR restore
        await client.db("f10spill").dropDatabase();
        let observedSpillFile = null;
        const watcher1 = fsWatch(spillWorkDir, (_ev, filename) => {
          if (filename && /^\.mongovault-txn-.*\.bson$/.test(filename)) {
            observedSpillFile = filename;
          }
        });

        try {
          await restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF10.uri, pitr: targetTs, drop: true },
          );
        } finally {
          watcher1.close();
        }

        expect(observedSpillFile).toBeTruthy();
        const postRestoreFiles = await fs.readdir(spillWorkDir);
        expect(postRestoreFiles.filter((f) => f.startsWith(".mongovault-txn-"))).toEqual([]);
        expect(await client.db("f10spill").collection("big").countDocuments()).toBe(36);
        const sampleDoc = await client.db("f10spill").collection("big").findOne({ _id: "sp-35" });
        expect(createHash("sha256").update(sampleDoc.body).digest("hex")).toBe(expectedSha);

        // 10.3: True Stage-2 mid-OplogApplier abort while spill file exists, verify cleanup, then clean re-restore
        const abortController = new AbortController();
        let abortedMidStage2 = false;
        let sliceDownloadPass = 0;
        const wrappedStorage = Object.create(storage);
        wrappedStorage.download = async (key, opts) => {
          const inner = await storage.download(key, opts);
          if (!key.includes(".oplog.bson")) {
            return inner;
          }
          sliceDownloadPass++;
          // Pass 1 is preVerify; intercept Pass 2 (Stage 2 OplogApplier replay)
          if (sliceDownloadPass < 2) {
            return inner;
          }
          const pass = new PassThrough();
          const watcher2 = fsWatch(spillWorkDir, (_ev, filename) => {
            if (!abortedMidStage2 && filename && /^\.mongovault-txn-.*\.bson$/.test(filename)) {
              abortedMidStage2 = true;
              watcher2.close();
              abortController.abort(new Error("mid-OplogApplier abort"));
              inner.destroy();
              pass.destroy(new Error("mid-OplogApplier abort"));
            }
          });
          // Feed chunks gradually so OplogApplier spills at >8 MiB before commitTransaction arrives
          (async () => {
            try {
              for await (const chunk of inner) {
                if (abortedMidStage2 || pass.destroyed) break;
                const ok = pass.write(chunk);
                if (!ok) {
                  await new Promise((r) => pass.once("drain", r));
                }
                await new Promise((r) => setTimeout(r, 20));
                const entries = await fs.readdir(spillWorkDir).catch(() => []);
                if (
                  !abortedMidStage2 &&
                  entries.some((f) => /^\.mongovault-txn-.*\.bson$/.test(f))
                ) {
                  abortedMidStage2 = true;
                  watcher2.close();
                  abortController.abort(new Error("mid-OplogApplier abort"));
                  pass.destroy(new Error("mid-OplogApplier abort"));
                  break;
                }
              }
              if (!abortedMidStage2 && !pass.destroyed) pass.end();
            } catch (err) {
              if (!pass.destroyed) pass.destroy(err);
            } finally {
              watcher2.close();
            }
          })();
          return pass;
        };

        await expect(
          restoreBackup(
            { config: cfg, storage: wrappedStorage, logger, signal: abortController.signal },
            { uri: rsF10.uri, pitr: targetTs, drop: true },
          ),
        ).rejects.toThrow();

        expect(abortedMidStage2).toBe(true);
        const postAbortFiles = await fs.readdir(spillWorkDir);
        expect(postAbortFiles.filter((f) => f.startsWith(".mongovault-txn-"))).toEqual([]);

        // Clean re-restore without abort succeeds
        await restoreBackup(
          { config: cfg, storage, logger },
          { uri: rsF10.uri, pitr: targetTs, drop: true },
        );
        expect(await client.db("f10spill").collection("big").countDocuments()).toBe(36);
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("10.4 — concurrent DDL + failed unique abortIndexBuild + dropDatabase purging deferred indexes + valid index rebuilt", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF10.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("f10ddl").collection("orders").insertOne({ _id: "o1", sku: "S1" });

        const cfg = makeFsConfig({
          uri: rsF10.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f10-ddl",
          oplog: true,
          pitrEnabled: true,
        });
        await runBackup({ config: cfg, storage, logger });

        // Duplicate docs -> unique index creation fails and emits abortIndexBuild in oplog
        await client
          .db("f10ddl")
          .collection("dups")
          .insertMany([
            { _id: "d1", code: "DUP" },
            { _id: "d2", code: "DUP" },
          ]);
        await expect(
          client.db("f10ddl").collection("dups").createIndex({ code: 1 }, { unique: true }),
        ).rejects.toThrow();

        // Create temporary DB with index, then dropDatabase (must purge deferred index)
        await client.db("temp_drop").collection("widgets").insertOne({ _id: "w1", sku: "W1" });
        await client
          .db("temp_drop")
          .collection("widgets")
          .createIndex({ sku: 1 }, { name: "temp_sku_idx" });
        await client.db("temp_drop").dropDatabase();

        // Create valid index on f10ddl.orders
        await client
          .db("f10ddl")
          .collection("orders")
          .createIndex({ sku: 1 }, { unique: true, name: "orders_sku_unique" });

        await waitForMajorityCatchup(rsF10.uri, await getLatestOplogTs(rsF10.uri));
        const slice = await captureOplogSlice({ config: cfg, storage, logger });
        expect(slice).toBeTruthy();

        await dropAllUserDatabases(client);
        const res = await restoreBackup(
          { config: cfg, storage, logger },
          { uri: rsF10.uri, pitr: slice.oplogWindow.endTs, drop: true },
        );
        expect(res.oplogStats?.indexesBuilt).toBeGreaterThanOrEqual(1);

        const orderIndexes = await client.db("f10ddl").collection("orders").indexes();
        expect(orderIndexes.some((idx) => idx.name === "orders_sku_unique")).toBe(true);
        const dbsAfter = await findUserDatabases(client);
        expect(dbsAfter).not.toContain("temp_drop");
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("10.5 — DB rename PITR (shop -> shop_pitr_clone) preserves original shop intact", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF10.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("shop").collection("items").insertOne({ _id: "i1", price: 10 });

        const cfgShopBase = makeFsConfig({
          uri: rsF10.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f10-rename",
          database: "shop",
          oplog: false,
          pitrEnabled: false,
        });
        const base = await runBackup({ config: cfgShopBase, storage, logger });

        // Wait >1s so post-backup oplog entries land strictly after completedAt second boundary
        await new Promise((r) => setTimeout(r, 1_100));
        await client.db("shop").collection("items").insertOne({ _id: "i2", price: 20 });
        await waitForMajorityCatchup(rsF10.uri, await getLatestOplogTs(rsF10.uri));

        const cfgShopPitr = makeFsConfig({
          uri: rsF10.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f10-rename",
          database: "shop",
          oplog: true,
          pitrEnabled: true,
        });
        const baseAnchor = formatOplogTimestamp({
          t: Math.floor(new Date(base.completedAt).getTime() / 1000) >>> 0,
          i: 0,
        });
        const slice = await captureOplogSlice({
          config: cfgShopPitr,
          storage,
          logger,
          sinceTs: baseAnchor,
        });
        expect(slice).toBeTruthy();

        // Add live-only doc to shop after slice target to verify original shop stays untouched
        await client.db("shop").collection("items").insertOne({ _id: "live-orig", price: 99 });

        await restoreBackup(
          { config: cfgShopPitr, storage, logger },
          {
            uri: rsF10.uri,
            database: "shop_pitr_clone",
            pitr: slice.oplogWindow.endTs,
            drop: true,
          },
        );

        expect(await client.db("shop_pitr_clone").collection("items").countDocuments()).toBe(2);
        expect(
          await client.db("shop_pitr_clone").collection("items").findOne({ _id: "i2" }),
        ).toBeTruthy();
        expect(
          await client.db("shop").collection("items").findOne({ _id: "live-orig" }),
        ).toBeTruthy();
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("10.6 — timeseries system.buckets.* PITR E2E restores measurements & timeseries collection options", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF10.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("tsdb").createCollection("metrics", {
          timeseries: {
            timeField: "ts",
            metaField: "meta",
            granularity: "seconds",
          },
        });
        const tBase = new Date("2026-11-26T10:00:00.000Z");
        await client
          .db("tsdb")
          .collection("metrics")
          .insertMany([
            { ts: tBase, meta: { host: "h1" }, cpu: 0.25 },
            { ts: new Date(tBase.getTime() + 1000), meta: { host: "h1" }, cpu: 0.35 },
          ]);
        // Advance oplog past system.buckets.metrics initial insert so mongodump --oplog does not replay it against a newly generated UUID
        await client.db("tsdb").collection("marker").insertOne({ _id: "m1" });

        const cfg = makeFsConfig({
          uri: rsF10.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f10-ts",
          oplog: true,
          pitrEnabled: true,
        });
        await runBackup({ config: cfg, storage, logger });

        // Post-backup measurements: update existing bucket (h1) + create new bucket (h2)
        await client
          .db("tsdb")
          .collection("metrics")
          .insertMany([
            { ts: new Date(tBase.getTime() + 2000), meta: { host: "h1" }, cpu: 0.55 },
            { ts: new Date(tBase.getTime() + 3000), meta: { host: "h2" }, cpu: 0.85 },
          ]);
        await waitForMajorityCatchup(rsF10.uri, await getLatestOplogTs(rsF10.uri));
        const slice = await captureOplogSlice({ config: cfg, storage, logger });
        expect(slice).toBeTruthy();

        await client.db("tsdb").dropDatabase();
        await restoreBackup(
          { config: cfg, storage, logger },
          { uri: rsF10.uri, pitr: slice.oplogWindow.endTs, drop: true },
        );

        const colInfos = await client.db("tsdb").listCollections({ name: "metrics" }).toArray();
        expect(colInfos[0]?.options?.timeseries?.timeField).toBe("ts");
        expect(colInfos[0]?.options?.timeseries?.metaField).toBe("meta");

        const docs = await client
          .db("tsdb")
          .collection("metrics")
          .find({})
          .sort({ ts: 1 })
          .toArray();
        expect(docs.length).toBe(4);
        expect(docs.map((d) => d.cpu)).toEqual([0.25, 0.35, 0.55, 0.85]);
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("10.7 — resolvePitrPlan timeline gap rejection (missing middle slice & target after lastCoveredTs) before --drop", async () => {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF10.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("f10gap").collection("docs").insertOne({ _id: "g0" });

        const cfg = makeFsConfig({
          uri: rsF10.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f10-gap",
          oplog: true,
          pitrEnabled: true,
        });
        await runBackup({ config: cfg, storage, logger });

        const slices = [];
        for (let i = 1; i <= 3; i++) {
          await client
            .db("f10gap")
            .collection("docs")
            .insertOne({ _id: `g${i}` });
          await waitForMajorityCatchup(rsF10.uri, await getLatestOplogTs(rsF10.uri));
          const sl = await captureOplogSlice({ config: cfg, storage, logger });
          expect(sl).toBeTruthy();
          slices.push(sl);
        }

        const lastEnd = parseOplogTimestamp(slices[2].oplogWindow.endTs);
        const futureTs = formatOplogTimestamp({ t: lastEnd.t + 60, i: 1 });

        // (a) Target after lastCoveredTs rejects before --drop
        await expect(resolvePitrPlan(storage, scopeOf(cfg), futureTs)).rejects.toThrow();
        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF10.uri, pitr: futureTs, drop: true },
          ),
        ).rejects.toThrow();
        expect(await client.db("f10gap").collection("docs").countDocuments()).toBe(4);

        // (b) Delete middle slice S2 -> timeline discontinuity rejects before --drop
        await storage.remove(slices[1].key);
        await storage.remove(manifestKey(slices[1].key));

        await expect(
          resolvePitrPlan(storage, scopeOf(cfg), slices[2].oplogWindow.endTs),
        ).rejects.toThrow();
        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF10.uri, pitr: slices[2].oplogWindow.endTs, drop: true },
          ),
        ).rejects.toThrow();
        expect(await client.db("f10gap").collection("docs").countDocuments()).toBe(4);
      } finally {
        await client.close().catch(() => {});
      }
    });
  });

  describe("F13 — Restore Drills (Base & --pitr latest, Isolation Guard, Tamper Detection, Live DRILL_CRON)", () => {
    let f13Dir;
    let storageDir;
    let workDir;
    let rsF13;
    let drillRs;

    beforeAll(async () => {
      f13Dir = path.join(rootDir, "f13");
      storageDir = path.join(f13Dir, "storage");
      workDir = path.join(f13Dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });
      [rsF13, drillRs] = await Promise.all([
        startMultiNodeReplicaSet({ rootDir: f13Dir, name: "rsF13" }),
        startReplicaSet({ rootDir: f13Dir, name: "drillRs" }),
      ]);
    });

    afterAll(async () => {
      await Promise.all([rsF13?.stop().catch(() => {}), drillRs?.stop().catch(() => {})]);
    });

    it("13.1 — programmatic base drill & CLI drill --pitr latest verify counts, indexes, validators, views, R/W probe & clean up temp DBs", async () => {
      const encKeyStr = generateKey();
      const encKey = parseKey(encKeyStr);
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF13.uri);
      const drillClient = new MongoClient(drillRs.uri);
      await Promise.all([client.connect(), drillClient.connect()]);

      try {
        await dropAllUserDatabases(client);
        await dropAllUserDatabases(drillClient);

        await client.db("drillshop").createCollection("users", {
          validator: {
            $jsonSchema: {
              bsonType: "object",
              required: ["email"],
              properties: { email: { bsonType: "string" } },
            },
          },
        });
        await client
          .db("drillshop")
          .collection("users")
          .createIndex({ email: 1 }, { unique: true, name: "email_unique" });
        await client
          .db("drillshop")
          .collection("users")
          .insertOne({ _id: "u1", email: "a@example.com" }, { writeConcern: { w: "majority" } });
        await client.db("drillshop").createCollection("active_users", {
          viewOn: "users",
          pipeline: [{ $match: {} }],
        });
        // Advance oplog past view creation so mongodump --oplog does not replay view creation under --preserveUUID
        await client
          .db("drillshop")
          .collection("orders")
          .insertOne({ _id: "o1", userId: "u1" }, { writeConcern: { w: "majority" } });

        const cfg = makeFsConfig({
          uri: rsF13.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f13-main",
          database: "*",
          oplog: true,
          pitrEnabled: true,
          drillUri: drillRs.uri,
        });

        const base = await runBackup({ config: cfg, storage, logger });

        const baseDrill = await runDrill({ config: cfg, storage, logger });
        expect(baseDrill.key).toBe(base.key);
        expect(baseDrill.collections).toBeGreaterThanOrEqual(2);
        expect(baseDrill.documents).toBeGreaterThanOrEqual(2);
        expect(baseDrill.indexesVerified).toBeGreaterThanOrEqual(2);
        expect(baseDrill.validatorsVerified).toBe(1);
        expect(baseDrill.viewsVerified).toBe(1);
        expect(baseDrill.readWriteVerified).toBe(true);
        expect(await findUserDatabases(drillClient)).toEqual([]);

        // Post-backup write + PITR slice -> CLI `drill --pitr latest --json`
        await client
          .db("drillshop")
          .collection("users")
          .insertOne({ _id: "u2", email: "b@example.com" }, { writeConcern: { w: "majority" } });
        await waitForMajorityCatchup(rsF13.uri, await getLatestOplogTs(rsF13.uri));
        const slice = await captureOplogSlice({ config: cfg, storage, logger });
        expect(slice).toBeTruthy();

        const cliDrill = await runCliCommand(["drill", "--pitr", "latest", "--json"], {
          workDir,
          parseJson: true,
          env: {
            MONGODB_URI: rsF13.uri,
            MONGODB_DATABASE: "*",
            MONGODB_OPLOG: "true",
            PITR_ENABLED: "true",
            STORAGE_TYPE: "filesystem",
            STORAGE_PATH: storageDir,
            S3_PREFIX: "f13-main",
            ENCRYPTION_KEY: encKeyStr,
            DRILL_MONGODB_URI: drillRs.uri,
            LOG_LEVEL: "fatal",
          },
        });
        expect(cliDrill.exitCode).toBe(0);
        expect(cliDrill.json.readWriteVerified).toBe(true);
        expect(cliDrill.json.replayedSlices).toBeGreaterThanOrEqual(1);
        expect(cliDrill.json.pitrTarget).toBeTruthy();
        expect(await findUserDatabases(drillClient)).toEqual([]);
      } finally {
        await Promise.all([client.close().catch(() => {}), drillClient.close().catch(() => {})]);
      }
    });

    it("13.2 — DRILL_MONGODB_URI same-cluster isolation guard rejects primary vs secondary of same 3-node RS", async () => {
      const storage = new FilesystemStorageClient(storageDir);
      const primary = await rsF13.getPrimaryNode();
      const [secondary] = await rsF13.getSecondaryNodes();
      const client = new MongoClient(rsF13.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client
          .db("isoapp")
          .collection("items")
          .insertOne({ _id: "keep1", v: 42 }, { writeConcern: { w: "majority" } });

        const cfg = makeFsConfig({
          uri: primary.directUri,
          storageDir,
          workDir,
          encKey: null,
          prefix: "f13-iso",
          database: "isoapp",
          oplog: false,
          pitrEnabled: false,
          drillUri: secondary.directUri,
        });
        await runBackup({ config: cfg, storage, logger });

        await expect(runDrill({ config: cfg, storage, logger })).rejects.toThrow(
          /Safety violation:/,
        );
        expect((await client.db("isoapp").collection("items").findOne({ _id: "keep1" }))?.v).toBe(
          42,
        );
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("13.3 — manifest count & schema tamper detection throws DrillError and cleans up drillapp_drill_* DBs", async () => {
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF13.uri);
      const drillClient = new MongoClient(drillRs.uri);
      await Promise.all([client.connect(), drillClient.connect()]);

      try {
        await dropAllUserDatabases(client);
        await dropAllUserDatabases(drillClient);

        await client
          .db("drillapp")
          .collection("items")
          .createIndex({ sku: 1 }, { unique: true, name: "sku_unique" });
        await client
          .db("drillapp")
          .collection("items")
          .insertOne({ _id: "i1", sku: "S1" }, { writeConcern: { w: "majority" } });

        const cfg = makeFsConfig({
          uri: rsF13.uri,
          storageDir,
          workDir,
          encKey: null,
          prefix: "f13-tamper",
          database: "drillapp",
          oplog: false,
          pitrEnabled: false,
          drillUri: drillRs.uri,
        });

        const manifest = await runBackup({ config: cfg, storage, logger });

        // (a) Tamper collection count -> DrillError with mismatches
        const countTampered = {
          ...manifest,
          collections: { ...manifest.collections, items: 9999 },
        };
        await writeManifest(storage, countTampered, null);

        let err1 = null;
        try {
          await runDrill({ config: cfg, storage, logger });
        } catch (err) {
          err1 = err;
        }
        expect(err1).toBeInstanceOf(DrillError);
        expect(err1.mismatches.length).toBeGreaterThan(0);
        expect(
          (await findUserDatabases(drillClient)).filter((d) => d.startsWith("drillapp_drill_")),
        ).toEqual([]);

        // (b) Tamper schema index unique flag -> DrillError with schemaMismatches
        const origSchema = manifest.schema;
        const tamperedColSchema = {
          ...origSchema.collections.items,
          indexes: origSchema.collections.items.indexes.map((idx) =>
            idx.name === "sku_unique" ? { ...idx, unique: false } : idx,
          ),
        };
        const schemaTampered = {
          ...manifest,
          schema: {
            ...origSchema,
            collections: { ...origSchema.collections, items: tamperedColSchema },
          },
        };
        await writeManifest(storage, schemaTampered, null);

        let err2 = null;
        try {
          await runDrill({ config: cfg, storage, logger });
        } catch (err) {
          err2 = err;
        }
        expect(err2).toBeInstanceOf(DrillError);
        expect(err2.schemaMismatches.length).toBeGreaterThan(0);
        expect(
          (await findUserDatabases(drillClient)).filter((d) => d.startsWith("drillapp_drill_")),
        ).toEqual([]);
      } finally {
        await Promise.all([client.close().catch(() => {}), drillClient.close().catch(() => {})]);
      }
    });

    it("13.4 — live daemon running DRILL_CRON exposes mongodb_backup_drill_runs_total{status='success'} >= 1 & lastDrillAt", async () => {
      const encKeyStr = generateKey();
      const encKey = parseKey(encKeyStr);
      const storage = new FilesystemStorageClient(storageDir);
      const client = new MongoClient(rsF13.uri);
      const drillClient = new MongoClient(drillRs.uri);
      await Promise.all([client.connect(), drillClient.connect()]);

      let daemon = null;
      try {
        await dropAllUserDatabases(client);
        await dropAllUserDatabases(drillClient);
        await client
          .db("crondrill")
          .collection("items")
          .insertOne({ _id: "cd1", ok: true }, { writeConcern: { w: "majority" } });

        const cfg = makeFsConfig({
          uri: rsF13.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f13-cron",
          database: "crondrill",
          oplog: false,
          pitrEnabled: false,
          drillUri: drillRs.uri,
        });
        await runBackup({ config: cfg, storage, logger });

        const daemonWorkDir = path.join(f13Dir, "daemon-work");
        await fs.mkdir(daemonWorkDir, { recursive: true });
        daemon = await startDaemon({
          workDir: daemonWorkDir,
          env: {
            MONGODB_URI: rsF13.uri,
            MONGODB_DATABASE: "crondrill",
            MONGODB_OPLOG: "false",
            PITR_ENABLED: "false",
            STORAGE_TYPE: "filesystem",
            STORAGE_PATH: storageDir,
            S3_PREFIX: "f13-cron",
            ENCRYPTION_KEY: encKeyStr,
            DRILL_MONGODB_URI: drillRs.uri,
            DRILL_CRON: "*/2 * * * * *",
            BACKUP_ON_START: "false",
            BACKUP_CRON: "0 0 1 1 *",
            LOG_LEVEL: "fatal",
          },
        });

        let metricsText = "";
        let healthBody = null;
        const ran = await waitForCondition(
          async () => {
            const [m, h] = await Promise.all([daemon.getMetrics(), daemon.getHealth()]);
            metricsText = m.text;
            healthBody = h.body;
            return (
              /mongodb_backup_drill_runs_total\{status="success"\}\s+[1-9]/.test(metricsText) &&
              healthBody?.lastDrillAt !== null
            );
          },
          20_000,
          250,
        );

        expect(ran).toBe(true);
        expect(healthBody.lastDrillAt).toBeTruthy();
        expect(metricsText).toMatch(/mongodb_backup_drill_runs_total\{status="success"\}\s+[1-9]/);
      } finally {
        await daemon?.stop().catch(() => {});
        await Promise.all([client.close().catch(() => {}), drillClient.close().catch(() => {})]);
      }
    });
  });

  describe("F14 — Preflight (check), Status & Sanitized Diagnostic", () => {
    it("14.1 & 14.2 — check --json (happy, 80% oplog failure, sharding mismatch) and status --json (PITR gapWarning after rollover)", async () => {
      const dir = path.join(rootDir, "f14-check-status");
      const storageDir = path.join(dir, "storage");
      const workDir = path.join(dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });

      const rs = await startReplicaSet({
        rootDir: dir,
        name: "rsF14",
        oplogSizeMb: 1,
        extraArgs: ["--setParameter=syncdelay=1"],
      });
      const client = new MongoClient(rs.uri);

      try {
        await client.connect();
        await client.db("f14app").collection("docs").insertOne({ _id: "d1", v: 1 });

        const encKeyStr = generateKey();
        const baseEnv = {
          MONGODB_URI: rs.uri,
          MONGODB_DATABASE: "*",
          MONGODB_OPLOG: "true",
          PITR_ENABLED: "true",
          STORAGE_TYPE: "filesystem",
          STORAGE_PATH: storageDir,
          S3_PREFIX: "f14-cs",
          ENCRYPTION_KEY: encKeyStr,
        };

        // 14.1a Happy check --json
        const happyCheck = await runCliCommand(["check", "--json"], {
          workDir,
          env: baseEnv,
          parseJson: true,
        });
        expect(happyCheck.exitCode).toBe(0);
        expect(happyCheck.json.allPassed).toBe(true);

        // 14.1c Sharded flag mismatch on RS
        const shardedCheck = await runCliCommand(["check", "--json"], {
          workDir,
          env: { ...baseEnv, SHARDING_ENABLED: "true" },
          parseJson: true,
        });
        expect(shardedCheck.exitCode).toBe(1);
        expect(shardedCheck.json.allPassed).toBe(false);
        const shardedItem = shardedCheck.json.checks.find((c) =>
          c.name.includes("Sharded Cluster"),
        );
        expect(shardedItem?.passed).toBe(false);

        // Take base backup + 1st PITR slice before rolling over the 1 MiB oplog
        const encKey = parseKey(encKeyStr);
        const storage = new FilesystemStorageClient(storageDir);
        const cfg = makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "f14-cs",
          oplog: true,
          pitrEnabled: true,
        });
        await runBackup({ config: cfg, storage, logger });
        await client.db("f14app").collection("docs").insertOne({ _id: "d2", v: 2 });
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));
        const slice1 = await captureOplogSlice({ config: cfg, storage, logger });
        expect(slice1).toBeTruthy();
        const slice1EndTs = parseOplogTimestamp(slice1.oplogWindow.endTs);

        // Fill >= 80% and roll over 1 MiB oplog deterministically (within < 3600s)
        const pad = randomBytes(8 * 1024).toString("hex");
        let rolled = false;
        for (let round = 0; round < 10; round++) {
          const filler = Array.from({ length: 100 }, (_, i) => ({
            _id: `f-${round}-${i}`,
            pad,
          }));
          await client.db("f14app").collection("filler").insertMany(filler);
          await client.db("admin").command({ fsync: 1 });
          const oldest = await client
            .db("local")
            .collection("oplog.rs")
            .find({}, { projection: { ts: 1 } })
            .sort({ $natural: 1 })
            .limit(1)
            .next();
          const oldestTs = fromBsonTimestamp(oldest?.ts);
          if (oldestTs && compareTimestamps(oldestTs, slice1EndTs) > 0) {
            rolled = true;
            break;
          }
        }
        expect(rolled).toBe(true);
        await waitForMajorityCatchup(rs.uri, await getLatestOplogTs(rs.uri));

        // 14.1b 80% oplog window failure
        const windowFailCheck = await runCliCommand(["check", "--json"], {
          workDir,
          env: baseEnv,
          parseJson: true,
        });
        expect(windowFailCheck.exitCode).toBe(1);
        expect(windowFailCheck.json.allPassed).toBe(false);
        const windowItem = windowFailCheck.json.checks.find(
          (c) => c.name === "MongoDB Oplog Window",
        );
        expect(windowItem?.passed).toBe(false);

        // Capture post-rollover slice so gapDetected: true is recorded, then verify status --json
        const gapSlice = await captureOplogSlice({ config: cfg, storage, logger });
        expect(gapSlice?.oplogWindow?.gapDetected).toBe(true);

        const statusRes = await runCliCommand(["status", "--json"], {
          workDir,
          env: baseEnv,
          parseJson: true,
        });
        expect(statusRes.exitCode).toBe(0);
        expect(statusRes.json.cluster.connected).toBe(true);
        expect(statusRes.json.pitr.enabled).toBe(true);
        expect(statusRes.json.pitr.slicesCount).toBeGreaterThanOrEqual(1);
        expect(statusRes.json.pitr.timelineContinuous).toBe(false);
        expect(statusRes.json.pitr.gapWarning).toBeTruthy();
      } finally {
        await client.close().catch(() => {});
        await rs.stop().catch(() => {});
      }
    });

    it("14.3 — diagnostic --output against live --auth --keyFile RS redacts secretpass & populates live cluster metadata", async () => {
      const dir = path.join(rootDir, "f14-diag-auth");
      const storageDir = path.join(dir, "storage");
      const workDir = path.join(dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });

      const keyFilePath = path.join(dir, "rs.keyfile");
      await fs.writeFile(keyFilePath, randomBytes(64).toString("base64"), { mode: 0o600 });
      await fs.chmod(keyFilePath, 0o600);

      const rs = await startReplicaSet({
        rootDir: dir,
        name: "rsF14Auth",
        extraArgs: ["--auth", `--keyFile=${keyFilePath}`],
      });

      const bootstrapClient = new MongoClient(rs.directUri, { directConnection: true });
      try {
        await bootstrapClient.connect();
        await bootstrapClient.db("admin").command({
          createUser: "backupuser",
          pwd: "secretpass",
          roles: [{ role: "root", db: "admin" }],
        });
        await bootstrapClient.close();

        const authUri = `mongodb://backupuser:secretpass@127.0.0.1:${rs.port}/?authSource=admin&replicaSet=rsF14Auth`;
        const authClient = new MongoClient(authUri);
        try {
          await authClient.connect();
          await authClient.db("authapp").collection("items").insertOne({ _id: "a1", v: 1 });
        } finally {
          await authClient.close().catch(() => {});
        }

        const encKeyStr = generateKey();
        const env = {
          MONGODB_URI: authUri,
          MONGODB_DATABASE: "*",
          MONGODB_OPLOG: "true",
          PITR_ENABLED: "false",
          STORAGE_TYPE: "filesystem",
          STORAGE_PATH: storageDir,
          S3_PREFIX: "f14-auth",
          ENCRYPTION_KEY: encKeyStr,
          LOG_LEVEL: "fatal",
        };

        const bRes = await runCliCommand(["backup", "--oplog", "--json"], {
          workDir,
          env,
          parseJson: true,
        });
        expect(bRes.exitCode).toBe(0);

        const outPath = path.join(dir, "diag-bundle.json");
        const diagRes = await runCliCommand(["diagnostic", "--output", outPath], {
          workDir,
          env,
        });
        expect(diagRes.exitCode).toBe(0);

        const rawBundle = await fs.readFile(outPath, "utf8");
        expect(rawBundle).not.toContain("secretpass");

        const bundle = JSON.parse(rawBundle);
        expect(bundle.database.connected).toBe(true);
        expect(bundle.database.maskedUri).toContain("***:***");
        expect(bundle.database.hello).toBeTruthy();
        expect(bundle.database.buildInfo).toBeTruthy();
        expect(bundle.tools?.mongodump?.available).toBe(true);
        expect(bundle.storage.totalBackups).toBeGreaterThanOrEqual(1);
        expect(bundle.recentManifests.length).toBeGreaterThanOrEqual(1);
      } finally {
        await bootstrapClient.close().catch(() => {});
        await rs.stop().catch(() => {});
      }
    });
  });

  describe("F15 — Live CLI Lifecycle, Daemon Probes/SIGTERM/SIGKILL & ClockSkew", () => {
    let f15Dir;
    let storageDir;
    let workDir;
    let rsF15;

    beforeAll(async () => {
      f15Dir = path.join(rootDir, "f15");
      storageDir = path.join(f15Dir, "storage");
      workDir = path.join(f15Dir, "work");
      await fs.mkdir(storageDir, { recursive: true });
      await fs.mkdir(workDir, { recursive: true });
      rsF15 = await startReplicaSet({ rootDir: f15Dir, name: "rsF15" });
    });

    afterAll(async () => {
      await rsF15?.stop().catch(() => {});
    });

    it("15.1a — live node dist/index.js CLI lifecycle (key:generate, backup, list, describe, verify, oplog:slice, oplog:list, restore, delete, prune)", async () => {
      const client = new MongoClient(rsF15.uri);
      await client.connect();

      try {
        await dropAllUserDatabases(client);
        await client.db("cliapp").collection("items").insertOne({ _id: "c1", v: 1 });

        const keyGen = await runCliCommand(["key:generate"], { workDir });
        expect(keyGen.exitCode).toBe(0);
        const generatedKey = keyGen.stdout.trim();
        expect(Buffer.from(generatedKey, "base64").length).toBe(32);

        const cliEnv = {
          MONGODB_URI: rsF15.uri,
          MONGODB_DATABASE: "*",
          MONGODB_OPLOG: "true",
          PITR_ENABLED: "true",
          STORAGE_TYPE: "filesystem",
          STORAGE_PATH: storageDir,
          S3_PREFIX: "f15-cli",
          ENCRYPTION_KEY: generatedKey,
          RETENTION_KEEP_LAST: "1",
          LOG_LEVEL: "fatal",
        };

        const b1 = await runCliCommand(["backup", "--oplog", "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(b1.exitCode).toBe(0);
        expect(b1.json.key).toBeTruthy();

        const list1 = await runCliCommand(["list", "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(list1.exitCode).toBe(0);
        expect(list1.json.some((b) => b.key === b1.json.key)).toBe(true);

        const desc1 = await runCliCommand(["describe", "--key", b1.json.key, "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(desc1.exitCode).toBe(0);
        expect(desc1.json.key).toBe(b1.json.key);

        const ver1 = await runCliCommand(["verify", "--key", b1.json.key, "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(ver1.exitCode).toBe(0);
        expect(ver1.json.sha256).toBe(b1.json.sha256);

        // Post-backup write -> oplog:slice -> oplog:list
        await client.db("cliapp").collection("items").insertOne({ _id: "c2", v: 2 });
        await waitForMajorityCatchup(rsF15.uri, await getLatestOplogTs(rsF15.uri));

        const slice1 = await runCliCommand(["oplog:slice", "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(slice1.exitCode).toBe(0);
        expect(slice1.json.key).toBeTruthy();

        const olist = await runCliCommand(["oplog:list", "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(olist.exitCode).toBe(0);
        expect(olist.json.length).toBeGreaterThanOrEqual(1);

        // Drop & restore base backup with --drop --oplog-replay
        await client.db("cliapp").dropDatabase();
        const rest = await runCliCommand(
          ["restore", "--key", b1.json.key, "--drop", "--oplog-replay", "--json"],
          { workDir, env: cliEnv, parseJson: true },
        );
        expect(rest.exitCode).toBe(0);
        expect(await client.db("cliapp").collection("items").findOne({ _id: "c1" })).toBeTruthy();

        // Take a second backup b2, prune to keepLast=1, and delete b2
        const b2 = await runCliCommand(["backup", "--oplog", "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(b2.exitCode).toBe(0);

        const pruneRes = await runCliCommand(["prune", "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(pruneRes.exitCode).toBe(0);
        expect(pruneRes.json.keep.length).toBe(1);

        // Delete remaining backup (--force since PITR slices exist)
        const remainingKey = pruneRes.json.keep[0].key;
        const delRes = await runCliCommand(["delete", "--key", remainingKey, "--force", "--json"], {
          workDir,
          env: cliEnv,
          parseJson: true,
        });
        expect(delRes.exitCode).toBe(0);
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("15.1b [M5-Gated F-CLI-011] — backup --json emits pure parseable JSON on stdout at default LOG_LEVEL", async () => {
      const client = new MongoClient(rsF15.uri);
      await client.connect();
      try {
        await dropAllUserDatabases(client);
        await client.db("clijson").collection("docs").insertOne({ _id: "j1" });

        const res = await runCliCommand(["backup", "--oplog", "--json"], {
          workDir,
          parseJson: true,
          env: {
            LOG_LEVEL: undefined,
            MONGODB_URI: rsF15.uri,
            MONGODB_DATABASE: "*",
            MONGODB_OPLOG: "true",
            PITR_ENABLED: "false",
            STORAGE_TYPE: "filesystem",
            STORAGE_PATH: storageDir,
            S3_PREFIX: "f15-json-purity",
            ENCRYPTION_KEY: generateKey(),
          },
        });
        expect(res.exitCode).toBe(0);
        expect(res.json.key).toBeTruthy();
      } finally {
        await client.close().catch(() => {});
      }
    });

    it("15.2a — live daemon BACKUP_ON_START exposes /healthz (200), /ready (200), and /metrics (mongodb_backup_runs_total)", async () => {
      const client = new MongoClient(rsF15.uri);
      await client.connect();
      let daemon = null;
      try {
        await dropAllUserDatabases(client);
        await client.db("f15d1").collection("docs").insertOne({ _id: "d1", v: 1 });

        const dWork = path.join(f15Dir, "daemon-happy");
        await fs.mkdir(dWork, { recursive: true });
        daemon = await startDaemon({
          workDir: dWork,
          env: {
            MONGODB_URI: rsF15.uri,
            MONGODB_DATABASE: "*",
            MONGODB_OPLOG: "true",
            PITR_ENABLED: "true",
            PITR_FLUSH_INTERVAL_SECONDS: "5",
            STORAGE_TYPE: "s3",
            S3_BUCKET: "e2e-bucket",
            S3_ENDPOINT: s3Server.endpoint,
            S3_REGION: "us-east-1",
            S3_FORCE_PATH_STYLE: "true",
            S3_ACCESS_KEY_ID: "test-access",
            S3_SECRET_ACCESS_KEY: "test-secret",
            S3_PREFIX: "f15-d1",
            ENCRYPTION_KEY: generateKey(),
            BACKUP_ON_START: "true",
            BACKUP_CRON: "0 0 1 1 *",
            LOG_LEVEL: "fatal",
          },
        });

        const h = await daemon.getHealth();
        expect(h.status).toBe(200);
        const r = await daemon.getReady();
        expect(r.status).toBe(200);

        let metricsText = "";
        const completed = await waitForCondition(
          async () => {
            const m = await daemon.getMetrics();
            metricsText = m.text;
            return /mongodb_backup_runs_total\{status="success",database="\*"\}\s+[1-9]/.test(
              metricsText,
            );
          },
          20_000,
          150,
        );
        expect(completed).toBe(true);

        daemon.kill("SIGTERM");
        const exit = await daemon.waitForExit(15_000);
        expect(exit.exitCode).toBe(0);
      } finally {
        await daemon?.stop().catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("15.2b [M5-Gated F-CLI-004] — daemon SIGTERM during S3-delayed backup drains (/ready -> 503), releases locks, exits 0", async () => {
      const client = new MongoClient(rsF15.uri);
      await client.connect();
      let daemon = null;

      try {
        await dropAllUserDatabases(client);
        await client.db("f15drain").collection("docs").insertOne({ _id: "dr1", v: 1 });

        // Delay S3 PutObject upload so SIGTERM hits while backup is actively running
        s3Server.addFault({
          op: "PutObject",
          keyPattern: /\.gz\.enc$/,
          type: "delay",
          delayMs: 2_500,
          once: true,
        });

        const dWork = path.join(f15Dir, "daemon-drain");
        await fs.mkdir(dWork, { recursive: true });
        daemon = await startDaemon({
          workDir: dWork,
          env: {
            MONGODB_URI: rsF15.uri,
            MONGODB_DATABASE: "*",
            MONGODB_OPLOG: "true",
            PITR_ENABLED: "false",
            STORAGE_TYPE: "s3",
            S3_BUCKET: "e2e-bucket",
            S3_ENDPOINT: s3Server.endpoint,
            S3_REGION: "us-east-1",
            S3_FORCE_PATH_STYLE: "true",
            S3_ACCESS_KEY_ID: "test-access",
            S3_SECRET_ACCESS_KEY: "test-secret",
            S3_PREFIX: "f15-drain",
            ENCRYPTION_KEY: generateKey(),
            BACKUP_ON_START: "true",
            BACKUP_CRON: "0 0 1 1 *",
            LOG_LEVEL: "fatal",
          },
        });

        const runningObserved = await waitForCondition(
          async () => {
            const h = await daemon.getHealth();
            return h.body?.status === "running";
          },
          15_000,
          30,
        );
        expect(runningObserved).toBe(true);

        let ready503 = null;
        const poll503Promise = waitForCondition(
          async () => {
            try {
              const r = await daemon.getReady();
              if (r.status === 503 && r.body?.isShuttingDown === true) {
                ready503 = r;
                return true;
              }
            } catch {
              // HTTP server closed
            }
            return false;
          },
          5_000,
          5,
        );
        daemon.kill("SIGTERM");

        const saw503 = await poll503Promise;
        expect(saw503).toBe(true);
        expect(ready503.status).toBe(503);

        const exit = await daemon.waitForExit(15_000);
        expect(exit.exitCode).toBe(0);

        // Locks must be released cleanly after graceful drain
        await expect(fs.stat(path.join(dWork, ".backup.lock"))).rejects.toThrow();
        const s3Storage = makeS3Storage(s3Server.endpoint);
        const lockObj = await s3Storage.getJson("f15-drain/_cluster/.locks/backup.lock.json");
        expect(lockObj).toBeNull();
      } finally {
        await daemon?.stop().catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("15.3 — SIGKILL crash with -87s clock skew leaves stale lock until 3s TTL expires, then unskewed backup reclaims & recovers", async () => {
      const client = new MongoClient(rsF15.uri);
      await client.connect();
      const primary = await rsF15.getPrimaryNode();
      const proxy = await createTcpChaosProxy({ targetPort: primary.port });
      let daemon = null;

      try {
        await dropAllUserDatabases(client);
        // Seed data and throttle via TCP proxy so backup holds both locks long enough to SIGKILL mid-flight
        const docs = Array.from({ length: 600 }, (_, i) => ({
          _id: `sk-${i}`,
          blob: randomBytes(4 * 1024).toString("hex"),
        }));
        await client.db("f15sigkill").collection("blobs").insertMany(docs);

        const killWorkDir = path.join(f15Dir, "sigkill-work");
        await fs.mkdir(killWorkDir, { recursive: true });
        const prefix = "f15-sigkill";
        const distLockPath = path.join(storageDir, prefix, "_cluster/.locks/backup.lock.json");
        const localLockPath = path.join(killWorkDir, ".backup.lock");

        const sharedEnv = {
          MONGODB_URI: rsF15.uri,
          MONGODB_DATABASE: "*",
          MONGODB_OPLOG: "true",
          PITR_ENABLED: "false",
          STORAGE_TYPE: "filesystem",
          STORAGE_PATH: storageDir,
          S3_PREFIX: prefix,
          ENCRYPTION_KEY: generateKey(),
          BACKUP_ON_START: "true",
          BACKUP_CRON: "0 0 1 1 *",
          LOG_LEVEL: "fatal",
        };

        proxy.setDelayMs(10);
        // Skewed -87s clock -> 90s distributed TTL expires in ~3 real seconds
        daemon = await startDaemon({
          workDir: killWorkDir,
          clockSkewMs: -87_000,
          env: {
            ...sharedEnv,
            MONGODB_URI: `mongodb://127.0.0.1:${proxy.listenPort}/?directConnection=true`,
          },
        });

        const locksHeld = await waitForCondition(
          async () => {
            const [loc, dist] = await Promise.all([
              fs
                .stat(localLockPath)
                .then(() => true)
                .catch(() => false),
              fs
                .stat(distLockPath)
                .then(() => true)
                .catch(() => false),
            ]);
            return loc && dist;
          },
          15_000,
          20,
        );
        expect(locksHeld).toBe(true);

        daemon.kill("SIGKILL");
        const killExit = await daemon.waitForExit(5_000);
        expect(killExit.signal).toBe("SIGKILL");

        // Immediately after SIGKILL, both stale lock files remain and unskewed backup is blocked until TTL expires
        await expect(fs.stat(localLockPath)).resolves.toBeTruthy();
        await expect(fs.stat(distLockPath)).resolves.toBeTruthy();

        const busyRes = await runCliCommand(["backup", "--oplog", "--json"], {
          workDir: killWorkDir,
          env: { ...sharedEnv, BACKUP_ON_START: "false" },
        });
        expect(busyRes.exitCode).toBe(1);

        // Wait >3.5s for -87s-skewed 90s TTL to expire, then unskewed backup reclaims stale locks & succeeds
        await new Promise((r) => setTimeout(r, 3_600));

        const recoverRes = await runCliCommand(["backup", "--oplog", "--json"], {
          workDir: killWorkDir,
          env: { ...sharedEnv, BACKUP_ON_START: "false" },
          parseJson: true,
        });
        expect(recoverRes.exitCode).toBe(0);
        expect(recoverRes.json.collections["f15sigkill.blobs"]).toBe(600);

        await expect(fs.stat(localLockPath)).rejects.toThrow();
        await expect(fs.stat(distLockPath)).rejects.toThrow();
      } finally {
        await daemon?.stop().catch(() => {});
        await proxy.close().catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("15.4 [M3-Gated F-LOG-002/ArchiveOplogTap] — ClockSkew (+2h preload) BackupBoundsCheck (WriteTime <= endTs) under concurrent writes", async () => {
      const client = new MongoClient(rsF15.uri);
      await client.connect();
      const primary = await rsF15.getPrimaryNode();
      const proxy = await createTcpChaosProxy({ targetPort: primary.port });

      try {
        await dropAllUserDatabases(client);
        const col = client.db("f15skew").collection("events");
        const pad = randomBytes(3 * 1024).toString("hex");
        await col.insertMany(
          Array.from({ length: 800 }, (_, i) => ({ _id: `init-${i}`, seq: -1, pad })),
          { writeConcern: { w: "majority" } },
        );

        const skewWorkDir = path.join(f15Dir, "skew-work");
        await fs.mkdir(skewWorkDir, { recursive: true });
        const encKeyStr = generateKey();
        const prefix = "f15-skew";
        const skewEnv = {
          MONGODB_URI: rsF15.uri,
          MONGODB_DATABASE: "*",
          MONGODB_OPLOG: "true",
          PITR_ENABLED: "true",
          STORAGE_TYPE: "filesystem",
          STORAGE_PATH: storageDir,
          S3_PREFIX: prefix,
          ENCRYPTION_KEY: encKeyStr,
          LOG_LEVEL: "fatal",
        };

        let running = true;
        let seq = 0;
        const writer = (async () => {
          while (running) {
            const cur = seq++;
            await col.insertOne(
              { _id: `sk-${cur}`, seq: cur },
              { writeConcern: { w: "majority" } },
            );
            await new Promise((r) => setTimeout(r, 5));
          }
        })();

        proxy.setDelayMs(8);
        let backupRes;
        try {
          backupRes = await runCliCommand(["backup", "--oplog", "--json"], {
            workDir: skewWorkDir,
            env: {
              ...skewEnv,
              MONGODB_URI: `mongodb://127.0.0.1:${proxy.listenPort}/?directConnection=true`,
            },
            clockSkewMs: +7_200_000,
            parseJson: true,
          });
        } finally {
          running = false;
          await writer.catch(() => {});
          proxy.setDelayMs(0);
        }

        expect(backupRes.exitCode).toBe(0);
        expect(backupRes.json.oplogWindow?.endTs).toBeTruthy();
        expect(backupRes.json.oplogWindow?.entries ?? 0).toBeGreaterThan(0);
        const endTs = parseOplogTimestamp(backupRes.json.oplogWindow.endTs);

        const writeTsById = new Map();
        const oplogDocs = await client
          .db("local")
          .collection("oplog.rs")
          .find({ op: "i", ns: "f15skew.events" })
          .toArray();
        for (const entry of oplogDocs) {
          const id = entry.o?._id;
          const ts = fromBsonTimestamp(entry.ts);
          if (id && ts) writeTsById.set(id, ts);
        }

        await client.db("f15skew").dropDatabase();
        const restoreRes = await runCliCommand(
          ["restore", "--key", backupRes.json.key, "--drop", "--oplog-replay", "--json"],
          { workDir: skewWorkDir, env: skewEnv, parseJson: true },
        );
        expect(restoreRes.exitCode).toBe(0);

        const restoredIds = new Set(
          (await col.find({}, { projection: { _id: 1 } }).toArray()).map((d) => d._id),
        );
        expect(restoredIds.has("init-0")).toBe(true);
        for (const id of restoredIds) {
          const ts = writeTsById.get(id);
          expect(ts).toBeTruthy();
          expect(compareTimestamps(ts, endTs)).toBeLessThanOrEqual(0);
        }
        for (const [id, ts] of writeTsById.entries()) {
          if (compareTimestamps(ts, endTs) > 0) {
            expect(restoredIds.has(id)).toBe(false);
          }
        }
      } finally {
        await proxy.close().catch(() => {});
        await client.close().catch(() => {});
      }
    });
  });
});
