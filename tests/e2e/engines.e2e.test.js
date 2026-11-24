import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { MongoClient } from "mongodb";
import pino from "pino";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { runBackup } from "../../src/backup.js";
import { generateKey, parseKey } from "../../src/crypto.js";
import { runDrill } from "../../src/drill.js";
import { captureOplogSlice } from "../../src/oplog.js";
import { restorePhysicalBackup, runPhysicalBackup } from "../../src/physical.js";
import { restoreBackup, verifyBackup } from "../../src/restore.js";
import {
  captureShardedOplogSlices,
  restoreShardedCluster,
  runShardedClusterBackup,
} from "../../src/sharding.js";
import { FilesystemStorageClient } from "../../src/storage.js";
import {
  formatOplogTimestamp,
  getClusterTime,
  getLatestOplogTs,
  startReplicaSet,
  startShardedCluster,
  startStandalone,
  waitForMajorityCatchup,
} from "./cluster-harness.js";

const logger = pino({ level: "silent" });

function makeBaseConfig({
  uri,
  storageDir,
  workDir,
  encKey,
  prefix = "e2e",
  engine = "logical",
  sharding = null,
  drillUri = null,
  dbPath = null,
}) {
  return {
    engine,
    mongo: {
      uri,
      database: "*",
      excludeCollections: [],
      oplog: true,
      parallelCollections: 2,
      readPreference: "primaryPreferred",
    },
    pitr: {
      enabled: true,
      flushIntervalSeconds: 60,
      maxSliceMb: 16,
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
    retention: null,
    drill: drillUri ? { uri: drillUri, cron: null } : null,
    physical: dbPath ? { dbPath } : null,
    sharding,
    notifications: {
      onSuccess: false,
      email: null,
    },
    http: { port: 0 },
    logLevel: "silent",
  };
}

describe("Real MongoDB End-to-End Verification (All 3 Engines + PITR)", () => {
  let rootDir;

  beforeAll(async () => {
    rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "mongovault-e2e-"));
  });

  afterAll(async () => {
    if (rootDir) {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("Logical Engine: encrypted streaming backup, ArchiveOplogTap, zero-disk PITR with multi-doc ACID txn & index DDL, DB-rename PITR, and restore drill", async () => {
    const suiteDir = path.join(rootDir, "logical");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rsLogical" });
    const drillRs = await startReplicaSet({ rootDir: suiteDir, name: "rsDrill" });
    const client = new MongoClient(rs.uri);

    try {
      await client.connect();
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeBaseConfig({
        uri: rs.uri,
        storageDir,
        workDir,
        encKey,
        prefix: "logical-e2e",
        drillUri: drillRs.uri,
      });

      // 1. Seed base collections + JSON schema validator + unique & TTL indexes + view
      await client.db("shop").createCollection("users", {
        validator: {
          $jsonSchema: {
            bsonType: "object",
            required: ["name", "balance"],
            properties: {
              name: { bsonType: "string" },
              balance: { bsonType: "number" },
            },
          },
        },
        validationLevel: "strict",
        validationAction: "error",
      });
      await client
        .db("shop")
        .collection("users")
        .createIndex({ name: 1 }, { unique: true, name: "name_unique" });
      await client
        .db("shop")
        .collection("users")
        .insertMany([
          { _id: "u1", name: "Alice", balance: 100 },
          { _id: "u2", name: "Bob", balance: 50 },
        ]);
      await client.db("shop").createCollection("active_users", {
        viewOn: "users",
        pipeline: [{ $match: { balance: { $gt: 0 } } }],
      });
      await client
        .db("shop")
        .collection("orders")
        .insertMany([{ _id: "o1", userId: "u1", sku: "SKU-1", amount: 25 }]);
      await client
        .db("analytics")
        .collection("events")
        .createIndex({ createdAt: 1 }, { expireAfterSeconds: 86400, name: "events_ttl" });
      await client
        .db("analytics")
        .collection("events")
        .insertOne({ _id: "e1", type: "signup", createdAt: new Date() });

      // 2. Run encrypted full-cluster logical backup with --oplog
      const manifest = await runBackup({ config, storage, logger });
      expect(manifest.oplog).toBe(true);
      expect(manifest.oplogWindow?.endTs).toBeTruthy();
      expect(manifest.encryption?.algorithm).toBe("aes-256-gcm-chunked");
      expect(manifest.collections["shop.users"]).toBe(2);
      expect(manifest.schema?.collections["shop.users"]?.options?.validator).toBeDefined();
      expect(manifest.schema?.views.some((v) => v.name === "shop.active_users")).toBe(true);

      // Verify encrypted archive integrity end-to-end
      const verified = await verifyBackup({ config, storage, logger }, manifest.key);
      expect(verified.sha256).toBe(manifest.sha256);

      // Run automated restore drill against isolated drillRs (verifies counts, indexes, validators, views & read/write)
      const drillRes = await runDrill({ config, storage, logger });
      expect(drillRes.key).toBe(manifest.key);
      expect(drillRes.collections).toBeGreaterThanOrEqual(3);
      expect(drillRes.indexesVerified).toBeGreaterThanOrEqual(5);
      expect(drillRes.validatorsVerified).toBe(1);
      expect(drillRes.viewsVerified).toBe(1);
      expect(drillRes.readWriteVerified).toBe(true);

      // 3. Post-backup writes: rich DDL (createIndex, dropIndex, renameCollection, collMod TTL,
      //    new post-backup view), committed multi-doc ACID txn, AND aborted multi-doc txn
      await client
        .db("shop")
        .collection("orders")
        .createIndex({ sku: 1 }, { unique: true, name: "sku_unique" });
      await client
        .db("shop")
        .collection("orders")
        .createIndex({ amount: 1 }, { name: "temp_amount_idx" });
      await client.db("shop").collection("orders").dropIndex("temp_amount_idx");
      await client
        .db("shop")
        .collection("orders")
        .insertOne({ _id: "o2", userId: "u2", sku: "SKU-2", amount: 30 });
      await client
        .db("shop")
        .collection("temp_ledger")
        .insertOne({ _id: "l1", note: "renamed-in-pitr" });
      await client.db("shop").collection("temp_ledger").rename("ledger");
      await client.db("analytics").command({
        collMod: "events",
        index: { name: "events_ttl", expireAfterSeconds: 43200 },
      });
      await client.db("shop").createCollection("high_value_orders", {
        viewOn: "orders",
        pipeline: [{ $match: { amount: { $gte: 30 } } }],
      });

      const session = client.startSession();
      try {
        await session.withTransaction(async () => {
          await client
            .db("shop")
            .collection("users")
            .updateOne({ _id: "u1" }, { $inc: { balance: -40 } }, { session });
          await client
            .db("shop")
            .collection("orders")
            .insertOne({ _id: "o3-txn", userId: "u1", sku: "SKU-TXN", amount: 40 }, { session });
        });
      } finally {
        await session.endSession();
      }

      // Execute and explicitly ABORT a multi-document transaction -> must NEVER appear after PITR
      const abortSession = client.startSession();
      try {
        abortSession.startTransaction();
        await client
          .db("shop")
          .collection("users")
          .updateOne({ _id: "u2" }, { $inc: { balance: 999 } }, { session: abortSession });
        await client
          .db("shop")
          .collection("orders")
          .insertOne(
            { _id: "o-aborted", userId: "u2", sku: "SKU-ABORT", amount: 999 },
            { session: abortSession },
          );
        await abortSession.abortTransaction();
      } finally {
        await abortSession.endSession();
      }

      const targetTs = await getLatestOplogTs(rs.uri);
      await waitForMajorityCatchup(rs.uri, targetTs);

      // Capture Slice 1 covering all valid writes up to targetTs
      const slice1 = await captureOplogSlice({ config, storage, logger });
      expect(slice1).not.toBeNull();
      expect(slice1.oplogWindow.entries).toBeGreaterThan(0);

      // 4. Catastrophic accidental writes AFTER targetTs
      await new Promise((r) => setTimeout(r, 1100)); // ensure distinct oplog second
      await client.db("shop").collection("orders").deleteMany({});
      await client
        .db("shop")
        .collection("users")
        .insertOne({ _id: "u-bad", name: "PostDisaster", balance: 0 });
      await client.db("analytics").collection("events").drop();

      const afterDisasterTs = await getLatestOplogTs(rs.uri);
      await waitForMajorityCatchup(rs.uri, afterDisasterTs);
      const slice2 = await captureOplogSlice({ config, storage, logger });
      expect(slice2).not.toBeNull();

      // Confirm disaster state before restore
      expect(await client.db("shop").collection("orders").countDocuments()).toBe(0);

      // 5a. Simulate an interrupted Stage 2 PITR restore (abort mid-slice) -> must fail closed,
      //     and prove subsequent clean re-restore recovers the exact target state!
      const pitrStr = formatOplogTimestamp(targetTs);
      const interruptController = new AbortController();
      const interruptingStorage = Object.create(storage);
      let sliceDownloadCount = 0;
      interruptingStorage.download = async (key, opts) => {
        if (key.includes(".oplog.bson")) {
          sliceDownloadCount++;
          // Abort on replay pass (after pre-verification pass)
          if (sliceDownloadCount > 1) {
            interruptController.abort(new Error("Simulated mid-replay interruption"));
          }
        }
        return storage.download(key, opts);
      };
      await expect(
        restoreBackup(
          { config, storage: interruptingStorage, logger, signal: interruptController.signal },
          { uri: rs.uri, pitr: pitrStr, drop: true },
        ),
      ).rejects.toThrow();

      // 5b. Full-cluster streaming PITR restore to exact targetTs (clean recovery after interruption!)
      const restoreRes = await restoreBackup(
        { config, storage, logger },
        { uri: rs.uri, pitr: pitrStr, drop: true },
      );
      expect(restoreRes.pitrTarget).toBe(pitrStr);
      expect(restoreRes.replayedEntries).toBeGreaterThan(0);
      expect(restoreRes.oplogStats?.partialTxns).toBe(0);
      expect(restoreRes.oplogStats?.unknownCommands).toEqual([]);

      // Also run an automated PITR restore drill against isolated drillRs!
      const pitrDrillRes = await runDrill({ config, storage, logger }, { pitr: pitrStr });
      expect(pitrDrillRes.pitrTarget).toBe(pitrStr);
      expect(pitrDrillRes.validatorsVerified).toBe(1);
      expect(pitrDrillRes.viewsVerified).toBe(1);
      expect(pitrDrillRes.readWriteVerified).toBe(true);
      expect(pitrDrillRes.oplogStats?.partialTxns).toBe(0);

      // Verify exact PITR state (committed txn applied, aborted txn absent, post-disaster undone)
      const u1 = await client.db("shop").collection("users").findOne({ _id: "u1" });
      const u2 = await client.db("shop").collection("users").findOne({ _id: "u2" });
      expect(u1.balance).toBe(60); // Multi-doc ACID txn applied!
      expect(u2.balance).toBe(50); // Aborted txn did NOT mutate Bob!
      expect(await client.db("shop").collection("users").findOne({ _id: "u-bad" })).toBeNull();

      const orders = await client
        .db("shop")
        .collection("orders")
        .find({})
        .sort({ _id: 1 })
        .toArray();
      expect(orders.map((o) => o._id)).toEqual(["o1", "o2", "o3-txn"]);
      expect(await client.db("shop").collection("orders").findOne({ _id: "o-aborted" })).toBeNull();

      // Verify post-backup DDL: createIndex, dropIndex, renameCollection, collMod TTL, and new view
      const indexes = await client.db("shop").collection("orders").indexes();
      expect(indexes.some((idx) => idx.name === "sku_unique" && idx.unique === true)).toBe(true);
      expect(indexes.some((idx) => idx.name === "temp_amount_idx")).toBe(false);
      expect(await client.db("shop").collection("ledger").findOne({ _id: "l1" })).toEqual({
        _id: "l1",
        note: "renamed-in-pitr",
      });
      const eventsIndexes = await client.db("analytics").collection("events").indexes();
      expect(eventsIndexes.find((idx) => idx.name === "events_ttl")?.expireAfterSeconds).toBe(
        43200,
      );
      const highValueOrders = await client
        .db("shop")
        .collection("high_value_orders")
        .find({})
        .toArray();
      expect(highValueOrders.length).toBe(2);

      // Verify analytics.events drop after targetTs was undone & restored constraints are enforced
      expect(await client.db("analytics").collection("events").countDocuments()).toBe(1);
      await expect(
        client
          .db("shop")
          .collection("users")
          .insertOne({ _id: "u-invalid", name: "MissingBalance" }),
      ).rejects.toThrow(/validation/i);
      await expect(
        client
          .db("shop")
          .collection("users")
          .insertOne({ _id: "u-dup", name: "Alice", balance: 1 }),
      ).rejects.toThrow(/E11000|duplicate key/i);
      const activeViewDocs = await client.db("shop").collection("active_users").find({}).toArray();
      expect(activeViewDocs.length).toBe(2);

      // 6. Verify selective namespace-filtered PITR restore into an isolated target database on drillRs
      const selectiveRes = await restoreBackup(
        { config, storage, logger },
        { uri: drillRs.uri, database: "shop", pitr: pitrStr, drop: true },
      );
      expect(selectiveRes.pitrTarget).toBe(pitrStr);
      const drillClient = new MongoClient(drillRs.uri);
      try {
        await drillClient.connect();
        expect(await drillClient.db("shop").collection("orders").countDocuments()).toBe(3);
        const drillU1 = await drillClient.db("shop").collection("users").findOne({ _id: "u1" });
        expect(drillU1.balance).toBe(60);
      } finally {
        await drillClient.close().catch(() => {});
      }
    } finally {
      await client.close().catch(() => {});
      await Promise.all([rs.stop(), drillRs.stop()]);
    }
  });

  it("Physical Engine: encrypted hot backup, real PhysicalRecoveryCoordinator, streaming physical PITR, clean mongod boot, and live mongod.lock guard", async () => {
    const suiteDir = path.join(rootDir, "physical");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    const restoreDbPath = path.join(suiteDir, "restored-db");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rsPhysical" });
    const client = new MongoClient(rs.uri);
    let restoredMongod = null;
    let restoredClient = null;

    try {
      await client.connect();
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeBaseConfig({
        uri: rs.uri,
        storageDir,
        workDir,
        encKey,
        prefix: "phys-e2e",
        engine: "physical",
        dbPath: rs.dbPath,
      });

      // 1. Seed base data
      await client
        .db("vault")
        .collection("records")
        .insertMany([
          { _id: "r1", status: "initial" },
          { _id: "r2", status: "initial" },
        ]);
      // Ensure WiredTiger flushes checkpoint to disk
      await client.db("admin").command({ fsync: 1 });

      // 2. Run encrypted physical backup (real fsyncLock + WiredTiger files + oplogWindow + mongoVersion)
      const physBackup = await runPhysicalBackup({
        config,
        storage,
        logger,
        dbPath: rs.dbPath,
      });
      expect(physBackup.manifest.engine).toBe("physical");
      expect(physBackup.manifest.physical.mongoVersion).toMatch(/^(7|8|9)\.\d+\./);
      expect(physBackup.manifest.oplogWindow?.endTs).toBeTruthy();

      // Verify live mongod.lock guard refuses to overwrite running source dbPath
      await expect(
        restorePhysicalBackup({
          config,
          storage,
          logger,
          targetDbPath: rs.dbPath,
          ref: physBackup.manifest.key,
        }),
      ).rejects.toThrow(/running mongod/i);

      // 3. Post-backup writes + index creation -> capture PITR slice
      await client
        .db("vault")
        .collection("records")
        .updateOne({ _id: "r1" }, { $set: { status: "updated-in-pitr" } });
      await client
        .db("vault")
        .collection("records")
        .insertOne({ _id: "r3", status: "inserted-in-pitr" });
      await client
        .db("vault")
        .collection("records")
        .createIndex({ status: 1 }, { name: "status_idx" });

      const targetTs = await getLatestOplogTs(rs.uri);
      await waitForMajorityCatchup(rs.uri, targetTs);
      const slice1 = await captureOplogSlice({ config, storage, logger });
      expect(slice1).not.toBeNull();

      // 4. Catastrophic delete after targetTs
      await new Promise((r) => setTimeout(r, 1100));
      await client.db("vault").collection("records").deleteMany({});
      const afterTs = await getLatestOplogTs(rs.uri);
      await waitForMajorityCatchup(rs.uri, afterTs);
      await captureOplogSlice({ config, storage, logger });

      // 5. Real physical restore + REAL PhysicalRecoveryCoordinator + streaming PITR replay!
      const pitrStr = formatOplogTimestamp(targetTs);
      const physRestore = await restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: restoreDbPath,
        pitr: pitrStr,
      });
      expect(physRestore.restoredFiles).toBeGreaterThan(0);
      expect(physRestore.pitrTarget).toBe(pitrStr);
      expect(physRestore.replayedEntries).toBeGreaterThan(0);

      // 6. Boot a fresh standalone mongod directly on restoreDbPath and verify integrity!
      restoredMongod = await startStandalone({
        rootDir: suiteDir,
        dbPath: restoreDbPath,
        name: "phys-verify",
      });
      restoredClient = new MongoClient(restoredMongod.uri, { directConnection: true });
      await restoredClient.connect();

      const docs = await restoredClient
        .db("vault")
        .collection("records")
        .find({})
        .sort({ _id: 1 })
        .toArray();
      expect(docs).toEqual([
        { _id: "r1", status: "updated-in-pitr" },
        { _id: "r2", status: "initial" },
        { _id: "r3", status: "inserted-in-pitr" },
      ]);

      // Verify index built during physical standalone recovery (`commitQuorum` omitted cleanly!)
      const indexes = await restoredClient.db("vault").collection("records").indexes();
      expect(indexes.some((i) => i.name === "status_idx")).toBe(true);

      // Verify stale local.system.replset was cleaned by PhysicalRecoveryCoordinator
      const replsetDocs = await restoredClient
        .db("local")
        .collection("system.replset")
        .countDocuments();
      expect(replsetDocs).toBe(0);
    } finally {
      await restoredClient?.close().catch(() => {});
      await restoredMongod?.stop().catch(() => {});
      await client.close().catch(() => {});
      await rs.stop();
    }
  });

  it("Sharded Cluster Engine: real CSRS + 2 shards + mongos, encrypted .cluster.enc, recursive verifyBackup, CsrsRestorer, sharded PITR, and cross-shard distributed ACID txn", async () => {
    const suiteDir = path.join(rootDir, "sharded");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster(suiteDir);
    const mongosClient = new MongoClient(cluster.mongosUri);

    try {
      await mongosClient.connect();
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeBaseConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "sharded-e2e",
        sharding: { enabled: true },
      });

      // 1. Enable sharding on globaldb and shard globaldb.accounts on hashed _id across 2 shards
      await mongosClient.db("admin").command({ enableSharding: "globaldb" });
      await mongosClient.db("admin").command({
        shardCollection: "globaldb.accounts",
        key: { _id: "hashed" },
      });

      const seedDocs = Array.from({ length: 40 }, (_, i) => ({
        _id: `acct-${String(i).padStart(2, "0")}`,
        balance: 1000,
        phase: "base",
      }));
      await mongosClient.db("globaldb").collection("accounts").insertMany(seedDocs);

      // Verify documents are genuinely distributed across BOTH shard01 and shard02
      const s1Client = new MongoClient(cluster.shard01.uri);
      const s2Client = new MongoClient(cluster.shard02.uri);
      let s1Count = 0;
      let s2Count = 0;
      let s1DocId;
      let s2DocId;
      try {
        await Promise.all([s1Client.connect(), s2Client.connect()]);
        const s1Docs = await s1Client.db("globaldb").collection("accounts").find({}).toArray();
        const s2Docs = await s2Client.db("globaldb").collection("accounts").find({}).toArray();
        s1Count = s1Docs.length;
        s2Count = s2Docs.length;
        s1DocId = s1Docs[0]?._id;
        s2DocId = s2Docs[0]?._id;
      } finally {
        await Promise.all([s1Client.close().catch(() => {}), s2Client.close().catch(() => {})]);
      }
      expect(s1Count).toBeGreaterThan(0);
      expect(s2Count).toBeGreaterThan(0);
      expect(s1Count + s2Count).toBe(40);

      // 2. Run encrypted coordinated sharded cluster backup
      const clusterBackup = await runShardedClusterBackup({ config, storage, logger });
      expect(clusterBackup.manifest.type).toBe("sharded-cluster-backup");
      expect(clusterBackup.manifest.key).toMatch(/\.cluster\.enc$/);
      expect(clusterBackup.manifest.sha256).toMatch(/^[a-f0-9]{64}$/);
      expect(Object.keys(clusterBackup.clusterMeta.shards).sort()).toEqual(["shard01", "shard02"]);

      // Recursive verifyBackup checks .cluster.enc + configsvr + shard01 + shard02 + bridging slices
      const verified = await verifyBackup({ config, storage, logger }, clusterBackup.manifest.key);
      expect(verified.sha256).toBe(clusterBackup.manifest.sha256);

      // 3. Post-backup writes: new insert, capture pre-commit timestamp, then REAL cross-shard distributed ACID transaction!
      await mongosClient.db("globaldb").collection("accounts").insertOne({
        _id: "acct-pitr-new",
        balance: 500,
        phase: "pitr",
      });

      const [preCfgTs, preS1Ts, preS2Ts] = await Promise.all([
        getLatestOplogTs(cluster.cfgRs.uri),
        getLatestOplogTs(cluster.shard01.uri),
        getLatestOplogTs(cluster.shard02.uri),
      ]);
      await Promise.all([
        waitForMajorityCatchup(cluster.cfgRs.uri, preCfgTs),
        waitForMajorityCatchup(cluster.shard01.uri, preS1Ts),
        waitForMajorityCatchup(cluster.shard02.uri, preS2Ts),
      ]);
      const preCommitClusterTs = await getClusterTime(cluster.mongosUri);
      await new Promise((r) => setTimeout(r, 1100)); // distinct oplog second before distributed commit

      const session = mongosClient.startSession();
      try {
        await session.withTransaction(async () => {
          // Debit account on shard01 and credit account on shard02 atomically across shards!
          await mongosClient
            .db("globaldb")
            .collection("accounts")
            .updateOne(
              { _id: s1DocId },
              { $inc: { balance: -250 }, $set: { phase: "cross-shard-txn" } },
              { session },
            );
          await mongosClient
            .db("globaldb")
            .collection("accounts")
            .updateOne(
              { _id: s2DocId },
              { $inc: { balance: 250 }, $set: { phase: "cross-shard-txn" } },
              { session },
            );
        });
      } finally {
        await session.endSession();
      }

      // Ensure majority catchup on configsvr + both shards, then capture sharded PITR slices
      const [cfgTs, s1Ts, s2Ts] = await Promise.all([
        getLatestOplogTs(cluster.cfgRs.uri),
        getLatestOplogTs(cluster.shard01.uri),
        getLatestOplogTs(cluster.shard02.uri),
      ]);
      await Promise.all([
        waitForMajorityCatchup(cluster.cfgRs.uri, cfgTs),
        waitForMajorityCatchup(cluster.shard01.uri, s1Ts),
        waitForMajorityCatchup(cluster.shard02.uri, s2Ts),
      ]);
      const targetClusterTs = await getClusterTime(cluster.mongosUri);

      const slices1 = await captureShardedOplogSlices({ config, storage, logger });
      expect(slices1).not.toBeNull();

      // 4. Catastrophic post-target disaster: wipe globaldb.accounts via mongos!
      await new Promise((r) => setTimeout(r, 1100));
      await mongosClient.db("globaldb").collection("accounts").deleteMany({});
      expect(await mongosClient.db("globaldb").collection("accounts").countDocuments()).toBe(0);

      const [cfgTs2, s1Ts2, s2Ts2] = await Promise.all([
        getLatestOplogTs(cluster.cfgRs.uri),
        getLatestOplogTs(cluster.shard01.uri),
        getLatestOplogTs(cluster.shard02.uri),
      ]);
      await Promise.all([
        waitForMajorityCatchup(cluster.cfgRs.uri, cfgTs2),
        waitForMajorityCatchup(cluster.shard01.uri, s1Ts2),
        waitForMajorityCatchup(cluster.shard02.uri, s2Ts2),
      ]);
      await captureShardedOplogSlices({ config, storage, logger });

      // 5a. First prove sharded PITR recovery immediately BEFORE the distributed commit!
      const prePitrStr = formatOplogTimestamp(preCommitClusterTs);
      const preRestoreRes = await restoreShardedCluster({
        config,
        storage,
        logger,
        pitr: prePitrStr,
        drop: true,
      });
      expect(preRestoreRes.oplogStats?.partialTxns).toBe(0);
      expect(await mongosClient.db("globaldb").collection("accounts").countDocuments()).toBe(41);
      const preAcct1 = await mongosClient
        .db("globaldb")
        .collection("accounts")
        .findOne({ _id: s1DocId });
      const preAcct2 = await mongosClient
        .db("globaldb")
        .collection("accounts")
        .findOne({ _id: s2DocId });
      expect(preAcct1.balance).toBe(1000);
      expect(preAcct2.balance).toBe(1000);

      // 5b. Now prove sharded PITR recovery immediately AFTER the distributed commit!
      const pitrStr = formatOplogTimestamp(targetClusterTs);
      const restoreRes = await restoreShardedCluster({
        config,
        storage,
        logger,
        pitr: pitrStr,
        drop: true,
      });
      expect(restoreRes.configsvrRestored).toBe(true);
      expect(restoreRes.restoredShards.sort()).toEqual(["shard01", "shard02"]);
      expect(restoreRes.pitrTarget).toBe(pitrStr);
      expect(restoreRes.oplogStats?.partialTxns).toBe(0);
      expect(restoreRes.oplogStats?.unknownCommands).toEqual([]);

      // 6. Verify exact cluster state via mongos!
      const totalRestored = await mongosClient
        .db("globaldb")
        .collection("accounts")
        .countDocuments();
      expect(totalRestored).toBe(41);

      const acct1 = await mongosClient
        .db("globaldb")
        .collection("accounts")
        .findOne({ _id: s1DocId });
      const acct2 = await mongosClient
        .db("globaldb")
        .collection("accounts")
        .findOne({ _id: s2DocId });
      const acctNew = await mongosClient
        .db("globaldb")
        .collection("accounts")
        .findOne({ _id: "acct-pitr-new" });

      expect(acct1.balance).toBe(750);
      expect(acct1.phase).toBe("cross-shard-txn");
      expect(acct2.balance).toBe(1250);
      expect(acct2.phase).toBe("cross-shard-txn");
      expect(acctNew.balance).toBe(500);

      // Verify CSRS routing metadata & live sessions remain intact on mongos
      const collMeta = await mongosClient
        .db("config")
        .collection("collections")
        .findOne({ _id: "globaldb.accounts" });
      expect(collMeta).not.toBeNull();
      const postSession = mongosClient.startSession();
      try {
        await mongosClient
          .db("globaldb")
          .collection("accounts")
          .findOne({ _id: s1DocId }, { session: postSession });
      } finally {
        await postSession.endSession();
      }
    } finally {
      await mongosClient.close().catch(() => {});
      await cluster.stop();
    }
  });
});
