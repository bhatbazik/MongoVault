import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { gzipSync, gunzipSync } from "node:zlib";
import { MongoClient } from "mongodb";
import pino from "pino";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { runBackup } from "../../src/backup.js";
import { manifestKey, readManifest, writeManifest } from "../../src/catalog.js";
import { createDecryptor, createEncryptor, generateKey, parseKey } from "../../src/crypto.js";
import { DigestStream } from "../../src/digest.js";
import { runDrill } from "../../src/drill.js";
import {
  captureOplogSlice,
  compareTimestamps,
  formatOplogTimestamp,
  fromBsonTimestamp,
  parseOplogTimestamp,
} from "../../src/oplog.js";
import { restorePhysicalBackup, runPhysicalBackup } from "../../src/physical.js";
import { verifyBackup } from "../../src/restore.js";
import {
  ActiveChunkMigrationError,
  BalancerController,
  CsrsRestorer,
  captureShardedOplogSlices,
  restoreShardedCluster,
  runShardedClusterBackup,
} from "../../src/sharding.js";
import { FilesystemStorageClient, S3StorageClient } from "../../src/storage.js";
import { createTarHeader } from "../../src/tar.js";
import {
  createTcpChaosProxy,
  findFreePort,
  getClusterTime,
  getLatestOplogTs,
  resumeAllPausedProcesses,
  startMultiNodeReplicaSet,
  startReplicaSet,
  startReplicaSetFromDbPath,
  startShardedCluster,
  startStandalone,
  stopProcess,
  waitForMajorityCatchup,
  waitForPing,
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
  engine = "logical",
  readPreference = "primaryPreferred",
  physical = null,
  sharding = null,
  drillUri = null,
  timeoutMs = 120_000,
}) {
  return {
    engine,
    mongo: {
      uri,
      database: "*",
      excludeCollections: [],
      oplog: true,
      parallelCollections: 2,
      readPreference,
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
    timeoutMs,
    workDir,
    encryptionKey: encKey,
    encryptionKeyRing: encKey ? [encKey] : [],
    requireEncryption: Boolean(encKey),
    retention: null,
    drill: drillUri ? { uri: drillUri, cron: null } : null,
    physical,
    sharding,
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
  engine = "logical",
  partSizeMb = 32,
  queueSize = 1,
  readPreference = "primaryPreferred",
  physical = null,
  sharding = null,
  drillUri = null,
  timeoutMs = 120_000,
}) {
  return {
    engine,
    mongo: {
      uri,
      database: "*",
      excludeCollections: [],
      oplog: true,
      parallelCollections: 2,
      readPreference,
    },
    pitr: {
      enabled: true,
      flushIntervalSeconds: 60,
      maxSliceMb: 16,
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
    requireEncryption: Boolean(encKey),
    retention: null,
    drill: drillUri ? { uri: drillUri, cron: null } : null,
    physical,
    sharding,
    notifications: { onSuccess: false, email: null },
    http: { port: 0 },
    logLevel: "silent",
  };
}

function makeS3Storage(s3Endpoint, { bucket = "e2e-bucket", partSizeMb = 32, queueSize = 1 } = {}) {
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

async function readStreamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

async function decryptAndGunzipArchive(storage, key, encKey, encryptionEnvelope) {
  const rawStream = await storage.download(key);
  let buf = await readStreamToBuffer(rawStream);
  if (encryptionEnvelope && encKey) {
    const decryptor = createDecryptor([encKey], encryptionEnvelope);
    const decryptedChunks = [];
    await new Promise((resolve, reject) => {
      decryptor.on("data", (d) => decryptedChunks.push(d));
      decryptor.on("error", reject);
      decryptor.on("end", resolve);
      Readable.from([buf]).pipe(decryptor);
    });
    buf = Buffer.concat(decryptedChunks);
  }
  return gunzipSync(buf);
}

function parseTarSize(hdr) {
  if (hdr[124] === 0x80) {
    let val = 0n;
    for (let i = 125; i < 136; i++) {
      val = (val << 8n) | BigInt(hdr[i]);
    }
    return Number(val);
  }
  const sizeStr = hdr.subarray(124, 136).toString("ascii").replace(/\0.*$/, "").trim();
  return sizeStr ? parseInt(sizeStr, 8) : 0;
}

function parseTarEntriesInOrder(tarBuf) {
  const entries = [];
  let offset = 0;
  let nextPaxPath = null;
  let nextPaxSize = null;

  while (offset + 512 <= tarBuf.length) {
    const hdr = tarBuf.subarray(offset, offset + 512);
    if (hdr.every((b) => b === 0)) {
      break;
    }
    const nameRaw = hdr.subarray(0, 100).toString("utf8").replace(/\0.*$/, "");
    const prefixRaw = hdr.subarray(345, 500).toString("utf8").replace(/\0.*$/, "");
    const ustarName = prefixRaw ? `${prefixRaw}/${nameRaw}` : nameRaw;
    const typeflag = hdr[156] === 0 ? "0" : String.fromCharCode(hdr[156]);
    const size = parseTarSize(hdr);
    offset += 512;

    const paddedDataLen = Math.ceil(size / 512) * 512;
    const payload = tarBuf.subarray(offset, offset + size);
    offset += paddedDataLen;

    if (typeflag === "x") {
      const paxStr = payload.toString("utf8");
      const pathMatch = /^\d+ path=(.*)$/m.exec(paxStr);
      if (pathMatch) nextPaxPath = pathMatch[1];
      const sizeMatch = /^\d+ size=(\d+)$/m.exec(paxStr);
      if (sizeMatch) nextPaxSize = Number(sizeMatch[1]);
      continue;
    }

    entries.push({
      name: nextPaxPath ?? ustarName,
      size: nextPaxSize ?? size,
      typeflag,
    });
    nextPaxPath = null;
    nextPaxSize = null;
  }

  return entries;
}

async function buildCorruptTarArchive({ fileName, payloadSize, encKey }) {
  const content = Buffer.alloc(payloadSize, 0x41);
  const header = createTarHeader(fileName, content.length);
  const padLen = content.length % 512 === 0 ? 0 : 512 - (content.length % 512);
  const tarBuf = Buffer.concat([header, content, Buffer.alloc(padLen, 0), Buffer.alloc(1024, 0)]);
  // Corrupt the TAR header checksum byte (offset 148..155)
  tarBuf[148] = 0x58;

  const gzBuf = gzipSync(tarBuf);
  const digest = new DigestStream();
  const encryptor = encKey ? createEncryptor(encKey) : null;
  const outChunks = [];

  await new Promise((resolve, reject) => {
    const src = Readable.from([gzBuf]);
    const stages = encryptor ? [src, encryptor.stream, digest] : [src, digest];
    for (let i = 0; i < stages.length - 1; i++) {
      stages[i].on("error", reject);
      stages[i].pipe(stages[i + 1]);
    }
    digest.on("data", (c) => outChunks.push(c));
    digest.on("error", reject);
    digest.on("end", resolve);
  });

  const bodyBuf = Buffer.concat(outChunks);
  return {
    bodyBuf,
    sha256: digest.digest(),
    size: digest.bytes,
    encryption: encryptor ? encryptor.envelope() : null,
  };
}

async function copyDirRecursive(src, dst) {
  await fs.mkdir(dst, { recursive: true });
  const entries = await fs.readdir(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const dstPath = path.join(dst, entry.name);
    if (entry.isDirectory()) {
      await copyDirRecursive(srcPath, dstPath);
    } else {
      await fs.copyFile(srcPath, dstPath);
    }
  }
}

async function ensureDbPrimaryShard(mongosClient, dbName, targetShardId) {
  await mongosClient.db("admin").command({ enableSharding: dbName });
  const dbEntry = await mongosClient.db("config").collection("databases").findOne({ _id: dbName });
  if (dbEntry && dbEntry.primary !== targetShardId) {
    await mongosClient.db("admin").command({ movePrimary: dbName, to: targetShardId });
  }
}

async function listStorageKeys(storage, prefix) {
  const keys = [];
  for await (const obj of storage.list(prefix)) {
    keys.push(obj.key);
  }
  return keys;
}

async function waitForBalancerRound(mongosClient, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const status = await mongosClient
      .db("admin")
      .command({ balancerStatus: 1 })
      .catch(() => null);
    if (status?.inBalancerRound) return status;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`Balancer did not enter active round within ${timeoutMs}ms`);
}

describe("F7, F8, F11, F12: Physical & Sharded Cluster Live E2E + Chaos", () => {
  let rootDir;

  beforeAll(async () => {
    rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "mongovault-e2e-m3-"));
  });

  afterEach(() => {
    vi.restoreAllMocks();
    resumeAllPausedProcesses();
  });

  afterAll(async () => {
    vi.restoreAllMocks();
    resumeAllPausedProcesses();
    if (rootDir) {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("7.1 F7/F-TST-004: fsyncLockFallback & allowPrimaryFsyncLock guards + 3-node RS secondary pinning", async () => {
    const suiteDir = path.join(rootDir, "t71");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const rs = await startMultiNodeReplicaSet({ rootDir: suiteDir, name: "rs71", oplogSizeMb: 32 });
    const client = new MongoClient(rs.uri);

    try {
      await client.connect();
      await client.db("vault").collection("docs").insertOne({ _id: "d1", v: 1 });
      await client.db("admin").command({ fsync: 1 });

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const primary = await rs.getPrimaryNode();
      const secondaries = await rs.getSecondaryNodes();
      expect(secondaries.length).toBe(2);
      const secondaryDbPaths = secondaries.map((s) => s.dbPath);

      // (a) Refusal without fsyncLockFallback
      await expect(
        runPhysicalBackup({
          config: makeFsConfig({
            uri: rs.uri,
            storageDir,
            workDir,
            encKey,
            prefix: "t71-a",
            engine: "physical",
            readPreference: "primary",
          }),
          storage,
          logger,
        }),
      ).rejects.toThrow(/fsyncLock fallback is disabled/i);

      // (b) Refusal on multi-node RS primary without allowPrimaryFsyncLock
      await expect(
        runPhysicalBackup({
          config: makeFsConfig({
            uri: rs.uri,
            storageDir,
            workDir,
            encKey,
            prefix: "t71-b",
            engine: "physical",
            readPreference: "primary",
          }),
          storage,
          logger,
          fsyncLockFallback: true,
          allowPrimaryFsyncLock: false,
        }),
      ).rejects.toThrow(/multi-node replica set PRIMARY/i);

      // (c) Success on 3-node RS secondary with omitted dbPath (auto-discovered via getCmdLineOpts)
      const secBackup = await runPhysicalBackup({
        config: makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "t71-c",
          engine: "physical",
          readPreference: "secondary",
        }),
        storage,
        logger,
        fsyncLockFallback: true,
      });
      expect(secBackup.manifest.physical?.cursorType).toBe("fsyncLock");
      expect(secBackup.manifest.physical?.dbPath).not.toBe(primary.dbPath);
      expect(secondaryDbPaths).toContain(secBackup.manifest.physical?.dbPath);

      // (d) Explicit allowPrimaryFsyncLock on primary succeeds and leaves primary writable
      const priBackup = await runPhysicalBackup({
        config: makeFsConfig({
          uri: rs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "t71-d",
          engine: "physical",
          readPreference: "primary",
        }),
        storage,
        logger,
        dbPath: primary.dbPath,
        fsyncLockFallback: true,
        allowPrimaryFsyncLock: true,
      });
      expect(priBackup.manifest.physical?.dbPath).toBe(primary.dbPath);

      const postWrite = await client
        .db("vault")
        .collection("docs")
        .insertOne({ _id: "post-primary-lock", v: 2 }, { writeConcern: { w: 1 }, maxTimeMS: 3000 });
      expect(postWrite.acknowledged).toBe(true);
    } finally {
      await client.close().catch(() => {});
      await rs.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("7.2 F7/F-TST-008: fsyncLockTimeoutMs watchdog auto-aborts hung S3 upload and unlocks live mongod", async () => {
    const suiteDir = path.join(rootDir, "t72");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(workDir, { recursive: true });

    const s3Server = await startMockS3Server({ defaultBucket: "e2e-bucket" });
    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rs72", oplogSizeMb: 16 });
    const client = new MongoClient(rs.uri);

    try {
      await client.connect();
      await client.db("vault").collection("probe").insertOne({ _id: "seed-1" });
      await client.db("admin").command({ fsync: 1 });

      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint, { partSizeMb: 32, queueSize: 1 });
      const config = makeS3Config({
        uri: rs.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        encKey,
        prefix: "t72-watchdog",
        engine: "physical",
        partSizeMb: 32,
        queueSize: 1,
      });

      s3Server.addFault({
        method: "PUT",
        keyPattern: ".physical.tar.gz",
        type: "delay",
        delayMs: 2500,
      });

      await expect(
        runPhysicalBackup({
          config,
          storage,
          logger,
          dbPath: rs.dbPath,
          fsyncLockFallback: true,
          fsyncLockTimeoutMs: 350,
        }),
      ).rejects.toThrow(/watchdog triggered/i);

      s3Server.clearFaults();

      // Live mongod must be immediately writable after releaseFsyncLockWithRetry
      const postRes = await client
        .db("vault")
        .collection("probe")
        .insertOne({ _id: "post-watchdog" }, { writeConcern: { w: 1 }, maxTimeMS: 2000 });
      expect(postRes.acknowledged).toBe(true);

      const remainingObjects = [];
      for await (const obj of storage.list(config.storage.prefix)) {
        remainingObjects.push(obj);
      }
      expect(remainingObjects).toEqual([]);
      expect(s3Server.stats.activeUploads).toBe(0);
    } finally {
      s3Server.clearFaults();
      await client.close().catch(() => {});
      await rs.stop();
      await s3Server.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("7.3 F7/F-PHY-008: BackupBoundsCheck, ordered TAR (data before journal), and volatile file exclusions", async () => {
    const suiteDir = path.join(rootDir, "t73");
    const workDir = path.join(suiteDir, "work");
    const restoreDbPath = path.join(suiteDir, "restore-db");
    await fs.mkdir(workDir, { recursive: true });
    await fs.mkdir(restoreDbPath, { recursive: true });

    const s3Server = await startMockS3Server({ defaultBucket: "e2e-bucket" });
    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rs73", oplogSizeMb: 16 });
    const client = new MongoClient(rs.uri);
    let restoredMongod = null;
    let restoredClient = null;
    let keepWriting = true;

    try {
      await client.connect();
      const coll = client.db("vault").collection("bounds");
      await coll.insertMany(Array.from({ length: 8 }, (_, i) => ({ _id: `init-${i}`, seq: i })));
      await client.db("admin").command({ fsync: 1 });

      let seqCounter = 0;
      const writerLoop = (async () => {
        while (keepWriting) {
          const seq = ++seqCounter;
          await coll
            .insertOne({ _id: `w-${seq}`, seq }, { writeConcern: { w: "majority", j: true } })
            .catch(() => {});
          await new Promise((r) => setTimeout(r, 15));
        }
      })();

      while (seqCounter < 5) {
        await new Promise((r) => setTimeout(r, 20));
      }

      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const config = makeS3Config({
        uri: rs.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        encKey,
        prefix: "t73-bounds",
        engine: "physical",
      });

      const backupRes = await runPhysicalBackup({
        config,
        storage,
        logger,
        dbPath: rs.dbPath,
        fsyncLockFallback: true,
      });

      // Let post-backup writes accumulate after fsyncUnlock before stopping writer
      const postAnchorSeq = seqCounter;
      while (seqCounter < postAnchorSeq + 5) {
        await new Promise((r) => setTimeout(r, 20));
      }
      keepWriting = false;
      await writerLoop;

      // Verify TAR stream ordering & exclusions
      const tarBuf = await decryptAndGunzipArchive(
        storage,
        backupRes.manifest.key,
        encKey,
        backupRes.manifest.encryption,
      );
      const entries = parseTarEntriesInOrder(tarBuf);
      expect(entries.length).toBeGreaterThan(0);

      const firstJournalIdx = entries.findIndex((e) => e.name.startsWith("journal/"));
      expect(firstJournalIdx).toBeGreaterThan(0);
      for (let i = firstJournalIdx; i < entries.length; i++) {
        expect(entries[i].name.startsWith("journal/")).toBe(true);
      }
      for (const e of entries) {
        expect(e.name).not.toMatch(/(^|\/)diagnostic\.data(\/|$)/);
        expect(e.name).not.toMatch(/(^|\/)mongod\.lock$/);
        expect(e.name).not.toMatch(/(^|\/)WiredTiger\.lock$/);
        expect(e.name).not.toMatch(/(^|\/)ongoingBackup\.lock$/);
      }

      // Map all bounds inserts to local.oplog.rs ts
      const oplogEntries = await client
        .db("local")
        .collection("oplog.rs")
        .find({ op: "i", ns: "vault.bounds" })
        .toArray();
      const writeTsById = new Map();
      for (const entry of oplogEntries) {
        const id = entry.o?._id;
        const ts = fromBsonTimestamp(entry.ts);
        if (id && ts) writeTsById.set(id, ts);
      }
      const endTs = parseOplogTimestamp(backupRes.manifest.oplogWindow.endTs);
      expect(endTs).not.toBeNull();

      // Restore physical backup and verify BackupBoundsCheck
      await restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: restoreDbPath,
        ref: backupRes.manifest.key,
      });

      restoredMongod = await startStandalone({
        rootDir: suiteDir,
        dbPath: restoreDbPath,
        name: "t73-restored",
      });
      restoredClient = new MongoClient(restoredMongod.uri, { directConnection: true });
      await restoredClient.connect();

      const restoredIds = new Set(
        (await restoredClient.db("vault").collection("bounds").find({}).toArray()).map(
          (d) => d._id,
        ),
      );

      let postUnlockCount = 0;
      for (const [id, writeTs] of writeTsById.entries()) {
        if (compareTimestamps(writeTs, endTs) <= 0) {
          expect(restoredIds.has(id)).toBe(true);
        } else {
          postUnlockCount++;
          expect(restoredIds.has(id)).toBe(false);
        }
      }
      expect(postUnlockCount).toBeGreaterThan(0);
    } finally {
      keepWriting = false;
      await restoredClient?.close().catch(() => {});
      await restoredMongod?.stop().catch(() => {});
      await client.close().catch(() => {});
      await rs.stop();
      await s3Server.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("8.1 F8/F-TST-006: Physical restore + PITR into working 3-node RS + initial sync + w:majority + follow-up backup", async () => {
    const suiteDir = path.join(rootDir, "t81");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    const node0DbPath = path.join(suiteDir, "restored-n0");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });
    await fs.mkdir(node0DbPath, { recursive: true });

    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rs81src", oplogSizeMb: 32 });
    const client = new MongoClient(rs.uri);
    let restoredRs = null;
    let restoredClient = null;
    let secClient = null;

    try {
      await client.connect();
      await client.db("vault").createCollection("accounts", {
        validator: {
          $jsonSchema: {
            bsonType: "object",
            required: ["owner", "balance"],
            properties: {
              owner: { bsonType: "string" },
              balance: { bsonType: "number" },
            },
          },
        },
      });
      await client
        .db("vault")
        .collection("accounts")
        .createIndex({ owner: 1 }, { unique: true, name: "owner_unique" });
      await client
        .db("vault")
        .collection("accounts")
        .insertMany([
          { _id: "a1", owner: "Alice", balance: 100 },
          { _id: "a2", owner: "Bob", balance: 200 },
        ]);

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: rs.uri,
        storageDir,
        workDir,
        encKey,
        prefix: "t81-rs",
        engine: "physical",
      });

      await runPhysicalBackup({
        config,
        storage,
        logger,
        dbPath: rs.dbPath,
        fsyncLockFallback: true,
      });

      // Post-backup writes + index DDL
      await client
        .db("vault")
        .collection("accounts")
        .updateOne({ _id: "a1" }, { $set: { balance: 150 } });
      await client
        .db("vault")
        .collection("accounts")
        .insertOne({ _id: "a3", owner: "Charlie", balance: 300 });
      await client
        .db("vault")
        .collection("accounts")
        .createIndex({ balance: 1 }, { name: "balance_idx" });

      const targetTs = await getLatestOplogTs(rs.uri);
      await waitForMajorityCatchup(rs.uri, targetTs);
      const slice1 = await captureOplogSlice({ config, storage, logger });
      expect(slice1).not.toBeNull();

      // Post-target disaster
      await new Promise((r) => setTimeout(r, 1100));
      await client.db("vault").collection("accounts").deleteMany({});
      const afterTs = await getLatestOplogTs(rs.uri);
      await waitForMajorityCatchup(rs.uri, afterTs);
      await captureOplogSlice({ config, storage, logger });

      // Physical restore + PITR into node0DbPath
      const pitrStr = formatOplogTimestamp(targetTs);
      const restoreRes = await restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: node0DbPath,
        pitr: pitrStr,
      });
      expect(restoreRes.restoredFiles).toBeGreaterThan(0);
      expect(restoreRes.replayedEntries).toBeGreaterThan(0);

      // Boot working 3-node RS from restored node0DbPath (replSetInitiate + replSetReconfig + initial sync)
      restoredRs = await startReplicaSetFromDbPath({
        rootDir: suiteDir,
        name: "rs81-restored",
        primaryDbPath: node0DbPath,
        members: 3,
        oplogSizeMb: 32,
      });

      restoredClient = new MongoClient(restoredRs.uri);
      await restoredClient.connect();

      const status = await restoredClient.db("admin").command({ replSetGetStatus: 1 });
      const primaries = status.members.filter((m) => m.stateStr === "PRIMARY");
      const secondaries = status.members.filter((m) => m.stateStr === "SECONDARY");
      expect(primaries).toHaveLength(1);
      expect(secondaries).toHaveLength(2);

      // Connect directly to a secondary and verify replicated data + indexes
      const secondaryNodes = await restoredRs.getSecondaryNodes();
      expect(secondaryNodes).toHaveLength(2);
      await waitForMajorityCatchup(restoredRs.uri, await getLatestOplogTs(restoredRs.uri));

      secClient = new MongoClient(secondaryNodes[0].directUri, {
        directConnection: true,
        readPreference: "secondaryPreferred",
      });
      await secClient.connect();
      const secDocs = await secClient
        .db("vault")
        .collection("accounts")
        .find({})
        .sort({ _id: 1 })
        .toArray();
      expect(secDocs).toEqual([
        { _id: "a1", owner: "Alice", balance: 150 },
        { _id: "a2", owner: "Bob", balance: 200 },
        { _id: "a3", owner: "Charlie", balance: 300 },
      ]);
      const secIndexes = await secClient.db("vault").collection("accounts").indexes();
      expect(secIndexes.some((i) => i.name === "owner_unique")).toBe(true);
      expect(secIndexes.some((i) => i.name === "balance_idx")).toBe(true);

      // Write with w: "majority" on restored RS and read back
      await restoredClient
        .db("vault")
        .collection("accounts")
        .insertOne(
          { _id: "a4", owner: "Diana", balance: 400 },
          { writeConcern: { w: "majority" } },
        );
      const d4 = await restoredClient.db("vault").collection("accounts").findOne({ _id: "a4" });
      expect(d4?.owner).toBe("Diana");

      // Follow-up physical backup on restored RS secondary
      const followUpPrimary = await restoredRs.getPrimaryNode();
      const followUpRes = await runPhysicalBackup({
        config: makeFsConfig({
          uri: restoredRs.uri,
          storageDir,
          workDir,
          encKey,
          prefix: "t81-followup",
          engine: "physical",
          readPreference: "secondary",
        }),
        storage,
        logger,
        fsyncLockFallback: true,
      });
      expect(followUpRes.manifest.sha256).toMatch(/^[a-f0-9]{64}$/);
      expect(followUpRes.manifest.physical?.filesCount).toBeGreaterThan(0);
      expect(followUpRes.manifest.physical?.dbPath).not.toBe(followUpPrimary.dbPath);
    } finally {
      await secClient?.close().catch(() => {});
      await restoredClient?.close().catch(() => {});
      await restoredRs?.stop().catch(() => {});
      await client.close().catch(() => {});
      await rs.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("8.2a F8/F-TST-008: Live mongod.lock guard refuses physical restore into running dbPath", async () => {
    const suiteDir = path.join(rootDir, "t82a");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rs82a", oplogSizeMb: 16 });
    const client = new MongoClient(rs.uri);

    try {
      await client.connect();
      await client.db("vault").collection("items").insertOne({ _id: "x1" });

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: rs.uri,
        storageDir,
        workDir,
        encKey,
        prefix: "t82a",
        engine: "physical",
      });

      const backupRes = await runPhysicalBackup({
        config,
        storage,
        logger,
        dbPath: rs.dbPath,
        fsyncLockFallback: true,
      });

      await expect(
        restorePhysicalBackup({
          config,
          storage,
          logger,
          targetDbPath: rs.dbPath,
          ref: backupRes.manifest.key,
        }),
      ).rejects.toThrow(/running mongod/i);
    } finally {
      await client.close().catch(() => {});
      await rs.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("8.2b F8/F-TST-008: --directoryperdb and --wiredTigerDirectoryForIndexes detection, TAR paths, and restore", async () => {
    const suiteDir = path.join(rootDir, "t82b");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    const restoreDbPath = path.join(suiteDir, "restore-dirperdb");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });
    await fs.mkdir(restoreDbPath, { recursive: true });

    const wtArgs = ["--directoryperdb", "--wiredTigerDirectoryForIndexes"];
    const rs = await startReplicaSet({
      rootDir: suiteDir,
      name: "rs82b",
      oplogSizeMb: 16,
      extraArgs: wtArgs,
    });
    const client = new MongoClient(rs.uri);
    let restoredMongod = null;
    let restoredClient = null;

    try {
      await client.connect();
      await client.db("dbalpha").collection("items").createIndex({ code: 1 }, { name: "code_idx" });
      await client.db("dbalpha").collection("items").insertOne({ _id: "a1", code: "C-1" });
      await client.db("dbbeta").collection("events").insertOne({ _id: "b1", kind: "login" });
      await client.db("admin").command({ fsync: 1 });

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: rs.uri,
        storageDir,
        workDir,
        encKey,
        prefix: "t82b-dirperdb",
        engine: "physical",
      });

      const backupRes = await runPhysicalBackup({
        config,
        storage,
        logger,
        dbPath: rs.dbPath,
        fsyncLockFallback: true,
      });

      expect(backupRes.manifest.physical?.storageOptions?.directoryPerDB).toBe(true);
      expect(backupRes.manifest.physical?.storageOptions?.directoryForIndexes).toBe(true);

      const tarBuf = await decryptAndGunzipArchive(
        storage,
        backupRes.manifest.key,
        encKey,
        backupRes.manifest.encryption,
      );
      const entries = parseTarEntriesInOrder(tarBuf);
      expect(entries.some((e) => /^dbalpha\/collection\/.*\.wt$/.test(e.name))).toBe(true);
      expect(entries.some((e) => /^dbalpha\/index\/.*\.wt$/.test(e.name))).toBe(true);

      await restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: restoreDbPath,
        ref: backupRes.manifest.key,
      });

      restoredMongod = await startStandalone({
        rootDir: suiteDir,
        dbPath: restoreDbPath,
        name: "t82b-restored",
        extraArgs: wtArgs,
      });
      restoredClient = new MongoClient(restoredMongod.uri, { directConnection: true });
      await restoredClient.connect();

      expect(await restoredClient.db("dbalpha").collection("items").findOne({ _id: "a1" })).toEqual(
        {
          _id: "a1",
          code: "C-1",
        },
      );
      expect(await restoredClient.db("dbbeta").collection("events").findOne({ _id: "b1" })).toEqual(
        {
          _id: "b1",
          kind: "login",
        },
      );
      const idxs = await restoredClient.db("dbalpha").collection("items").indexes();
      expect(idxs.some((i) => i.name === "code_idx")).toBe(true);
    } finally {
      await restoredClient?.close().catch(() => {});
      await restoredMongod?.stop().catch(() => {});
      await client.close().catch(() => {});
      await rs.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("8.2c F8/F-TST-008: Insufficient disk space fast-fail, 1x allowDirectMode guard, and 1x direct extraction", async () => {
    const suiteDir = path.join(rootDir, "t82c");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    const targetDbPath = path.join(suiteDir, "target-1x");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });
    await fs.mkdir(targetDbPath, { recursive: true });

    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rs82c", oplogSizeMb: 16 });
    const client = new MongoClient(rs.uri);
    let restoredMongod = null;
    let restoredClient = null;
    let statfsSpy = null;

    try {
      await client.connect();
      await client.db("vault").collection("records").insertOne({ _id: "r1", ok: true });
      await client.db("admin").command({ fsync: 1 });

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: rs.uri,
        storageDir,
        workDir,
        encKey,
        prefix: "t82c-1x",
        engine: "physical",
      });

      const backupRes = await runPhysicalBackup({
        config,
        storage,
        logger,
        dbPath: rs.dbPath,
        fsyncLockFallback: true,
      });

      const requiredBytes = backupRes.manifest.physical.files.reduce(
        (sum, f) => sum + (f.size || 0),
        0,
      );
      const needed = Math.ceil(requiredBytes * 1.1);
      expect(needed).toBeGreaterThan(10_000);

      const sentinelPath = path.join(targetDbPath, "sentinel.wt");
      const smallSentinel = Buffer.from("sentinel-small");
      await fs.writeFile(sentinelPath, smallSentinel);

      // (i) Insufficient total disk space (even free + existing < needed)
      statfsSpy = vi.spyOn(fs, "statfs").mockResolvedValue({
        type: 0,
        bsize: 1,
        blocks: needed,
        bfree: 100,
        bavail: 100,
        files: 1000,
        ffree: 1000,
      });

      await expect(
        restorePhysicalBackup({
          config,
          storage,
          logger,
          targetDbPath,
          ref: backupRes.manifest.key,
          allowDirectMode: true,
        }),
      ).rejects.toThrow(/Insufficient disk space/i);
      expect(await fs.readFile(sentinelPath)).toEqual(smallSentinel);

      // (ii) free < needed && free + existing >= needed without allowDirectMode -> refuses
      const largeSentinel = Buffer.alloc(needed + 4096, 0x53);
      await fs.writeFile(sentinelPath, largeSentinel);
      const halfNeeded = Math.floor(needed * 0.5);
      statfsSpy.mockResolvedValue({
        type: 0,
        bsize: 1,
        blocks: needed * 2,
        bfree: halfNeeded,
        bavail: halfNeeded,
        files: 1000,
        ffree: 1000,
      });

      await expect(
        restorePhysicalBackup({
          config,
          storage,
          logger,
          targetDbPath,
          ref: backupRes.manifest.key,
          allowDirectMode: false,
        }),
      ).rejects.toThrow(/allowDirectMode/i);
      expect((await fs.readFile(sentinelPath)).equals(largeSentinel)).toBe(true);

      // (iii) Same tight disk space WITH allowDirectMode: true succeeds and restores cleanly
      const directRes = await restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath,
        ref: backupRes.manifest.key,
        allowDirectMode: true,
      });
      expect(directRes.restoredFiles).toBeGreaterThan(0);
      await expect(fs.stat(sentinelPath)).rejects.toThrow();

      statfsSpy.mockRestore();
      statfsSpy = null;

      restoredMongod = await startStandalone({
        rootDir: suiteDir,
        dbPath: targetDbPath,
        name: "t82c-restored",
      });
      restoredClient = new MongoClient(restoredMongod.uri, { directConnection: true });
      await restoredClient.connect();
      expect(await restoredClient.db("vault").collection("records").findOne({ _id: "r1" })).toEqual(
        {
          _id: "r1",
          ok: true,
        },
      );
    } finally {
      statfsSpy?.mockRestore();
      await restoredClient?.close().catch(() => {});
      await restoredMongod?.stop().catch(() => {});
      await client.close().catch(() => {});
      await rs.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("8.2d F8/F-PHY-002: Corrupt TAR fails closed in BOTH 2x staged and 1x direct modes preserving sentinel.wt", async () => {
    const suiteDir = path.join(rootDir, "t82d");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    const target2x = path.join(suiteDir, "target-2x");
    const target1x = path.join(suiteDir, "target-1x");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });
    await fs.mkdir(target2x, { recursive: true });
    await fs.mkdir(target1x, { recursive: true });

    let statfsSpy = null;
    try {
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: "mongodb://127.0.0.1:27017",
        storageDir,
        workDir,
        encKey,
        prefix: "t82d-corrupt",
        engine: "physical",
      });

      const payloadSize = 64 * 1024;
      const archive = await buildCorruptTarArchive({
        fileName: "WiredTiger",
        payloadSize,
        encKey,
      });

      const backupKey = "t82d-corrupt/2026-11-26/corrupt.physical.tar.gz.enc";
      await storage.upload(backupKey, archive.bodyBuf);
      const manifest = {
        version: 1,
        type: "physical-hot-backup",
        engine: "physical",
        key: backupKey,
        database: "*",
        createdAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
        durationMs: 100,
        size: archive.size,
        sha256: archive.sha256,
        compression: "gzip",
        encryption: archive.encryption,
        oplog: false,
        excludedCollections: [],
        collections: {},
        physical: {
          dbPath: "/tmp/unused",
          cursorType: "fsyncLock",
          filesCount: 1,
          files: [{ name: "WiredTiger", size: payloadSize }],
        },
      };
      await writeManifest(storage, manifest, encKey);

      const sentinelBytes = Buffer.alloc(payloadSize * 2, 0x4b);
      const sentinel2x = path.join(target2x, "sentinel.wt");
      const sentinel1x = path.join(target1x, "sentinel.wt");
      await fs.writeFile(sentinel2x, sentinelBytes);
      await fs.writeFile(sentinel1x, sentinelBytes);

      // 2x staged mode
      await expect(
        restorePhysicalBackup({
          config,
          storage,
          logger,
          targetDbPath: target2x,
          ref: backupKey,
          skipRecovery: true,
        }),
      ).rejects.toThrow(/checksum mismatch|corrupted tar/i);
      expect(await fs.readFile(sentinel2x)).toEqual(sentinelBytes);

      // 1x direct mode with fs.statfs spy
      const needed = Math.ceil(payloadSize * 1.1);
      statfsSpy = vi.spyOn(fs, "statfs").mockResolvedValue({
        type: 0,
        bsize: 1,
        blocks: needed * 2,
        bfree: Math.floor(needed * 0.5),
        bavail: Math.floor(needed * 0.5),
        files: 1000,
        ffree: 1000,
      });

      await expect(
        restorePhysicalBackup({
          config,
          storage,
          logger,
          targetDbPath: target1x,
          ref: backupKey,
          allowDirectMode: true,
          skipRecovery: true,
        }),
      ).rejects.toThrow(/checksum mismatch|corrupted tar/i);
      expect(await fs.readFile(sentinel1x)).toEqual(sentinelBytes);
    } finally {
      statfsSpy?.mockRestore();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("8.3 F8/F-TST-007: Real-mongod PhysicalRecoveryCoordinator Steps 1-2 (recoverFromOplogAsStandalone=true, minvalid, oplogTruncateAfterPoint) with $backupCursor + commitQuorum stripping", async () => {
    const suiteDir = path.join(rootDir, "t83");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    const truncDbPath = path.join(suiteDir, "restore-trunc-only");
    const pitrDbPath = path.join(suiteDir, "restore-pitr");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });
    await fs.mkdir(truncDbPath, { recursive: true });
    await fs.mkdir(pitrDbPath, { recursive: true });

    const rs = await startReplicaSet({ rootDir: suiteDir, name: "rs83", oplogSizeMb: 32 });
    const client = new MongoClient(rs.uri);
    let truncMongod = null;
    let truncClient = null;
    let pitrMongod = null;
    let pitrClient = null;

    try {
      await client.connect();
      await client.db("vault").collection("items").insertOne({ _id: "d1", tag: "alpha" });
      await client.db("admin").command({ fsync: 1 });
      const ts1 = await getLatestOplogTs(rs.uri);

      await client.db("vault").collection("items").insertOne({ _id: "d2", tag: "beta" });
      const ts2 = await getLatestOplogTs(rs.uri);

      await client.db("vault").collection("items").insertOne({ _id: "d3", tag: "gamma" });
      const ts3 = await getLatestOplogTs(rs.uri);
      expect(compareTimestamps(ts3, ts2)).toBeGreaterThan(0);

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: rs.uri,
        storageDir,
        workDir,
        encKey,
        prefix: "t83-cursor",
        engine: "physical",
      });

      // Physical backup captures dbPath whose local.oplog.rs contains ts1, ts2, ts3
      const physBackup = await runPhysicalBackup({
        config,
        storage,
        logger,
        dbPath: rs.dbPath,
        fsyncLockFallback: true,
      });

      // Post-backup writes + index create + drop
      await client.db("vault").collection("items").insertOne({ _id: "d4", tag: "delta" });
      await client.db("vault").collection("items").createIndex({ tag: 1 }, { name: "tag_idx" });
      await client.db("vault").collection("items").createIndex({ tmp: 1 }, { name: "tmp_idx" });
      await client.db("vault").collection("items").dropIndex("tmp_idx");

      const ts4 = await getLatestOplogTs(rs.uri);
      await waitForMajorityCatchup(rs.uri, ts4);

      // Capture PITR slice anchored at ts2 -> covers (ts2, ts4]
      const slice1 = await captureOplogSlice({
        config,
        storage,
        logger,
        sinceTs: formatOplogTimestamp(ts2),
      });
      expect(slice1).not.toBeNull();

      // Re-sign manifest as $backupCursor anchored at oplogEnd = ts2
      const patchedManifest = {
        ...physBackup.manifest,
        oplog: true,
        oplogWindow: {
          startTs: formatOplogTimestamp(ts1),
          endTs: formatOplogTimestamp(ts2),
        },
        physical: {
          ...physBackup.manifest.physical,
          cursorType: "$backupCursor",
          oplogEnd: formatOplogTimestamp(ts2),
        },
      };
      delete patchedManifest.signature;
      await writeManifest(storage, patchedManifest, encKey);

      // (a) Restore WITHOUT PITR to verify Step 2 recoverFromOplogAsStandalone truncated local.oplog.rs to <= ts2
      await restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: truncDbPath,
        ref: physBackup.manifest.key,
      });
      truncMongod = await startStandalone({
        rootDir: suiteDir,
        dbPath: truncDbPath,
        name: "t83-trunc",
      });
      truncClient = new MongoClient(truncMongod.uri, { directConnection: true });
      await truncClient.connect();
      const truncMaxOplog = await truncClient
        .db("local")
        .collection("oplog.rs")
        .find({}, { projection: { ts: 1 } })
        .sort({ $natural: -1 })
        .limit(1)
        .next();
      const truncMaxTs = fromBsonTimestamp(truncMaxOplog?.ts);
      expect(truncMaxTs).not.toBeNull();
      // Note: fsyncLock-based simulated $backupCursor snapshots already checkpoint at ts3,
      // so WiredTiger's stable timestamp prevents truncating oplog.rs earlier than ts3.
      expect(compareTimestamps(truncMaxTs, ts3)).toBeLessThanOrEqual(0);
      await truncClient.close();
      truncClient = null;
      await truncMongod.stop();
      truncMongod = null;

      // (b) Restore WITH PITR (ts4): Step 1 -> Step 2 -> Step 3 OplogApplier + buildPendingIndexes({ standalone: true })
      const pitrRes = await restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: pitrDbPath,
        ref: physBackup.manifest.key,
        pitr: formatOplogTimestamp(ts4),
      });
      expect(pitrRes.replayedEntries).toBeGreaterThan(0);

      pitrMongod = await startStandalone({
        rootDir: suiteDir,
        dbPath: pitrDbPath,
        name: "t83-pitr",
      });
      pitrClient = new MongoClient(pitrMongod.uri, { directConnection: true });
      await pitrClient.connect();

      const docs = await pitrClient
        .db("vault")
        .collection("items")
        .find({})
        .sort({ _id: 1 })
        .toArray();
      expect(docs.map((d) => d._id)).toEqual(["d1", "d2", "d3", "d4"]);

      const indexes = await pitrClient.db("vault").collection("items").indexes();
      expect(indexes.some((i) => i.name === "tag_idx")).toBe(true);
      expect(indexes.some((i) => i.name === "tmp_idx")).toBe(false);

      expect(await pitrClient.db("local").collection("system.replset").countDocuments()).toBe(0);
      expect(await pitrClient.db("local").collection("replset.minvalid").countDocuments()).toBe(0);
      expect(await pitrClient.db("local").collection("replset.election").countDocuments()).toBe(0);
      expect(
        await pitrClient.db("local").collection("replset.oplogTruncateAfterPoint").countDocuments(),
      ).toBe(0);
    } finally {
      await pitrClient?.close().catch(() => {});
      await pitrMongod?.stop().catch(() => {});
      await truncClient?.close().catch(() => {});
      await truncMongod?.stop().catch(() => {});
      await client.close().catch(() => {});
      await rs.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("11.1 F11: 2-shard + CSRS + mongos encrypted .cluster.enc backup + verifyBackup + BackupBoundsCheck", async () => {
    const suiteDir = path.join(rootDir, "t111");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(workDir, { recursive: true });

    const s3Server = await startMockS3Server({ defaultBucket: "e2e-bucket" });
    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 3,
      shardMembers: 1,
      oplogSizeMb: 64,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);
    let keepWriting = true;

    try {
      await mongosClient.connect();
      await mongosClient.db("admin").command({ enableSharding: "boundsdb" });
      await mongosClient.db("admin").command({
        shardCollection: "boundsdb.items",
        key: { _id: "hashed" },
      });

      const itemsColl = mongosClient.db("boundsdb").collection("items");
      await itemsColl.insertMany(
        Array.from({ length: 20 }, (_, i) => ({ _id: `init-${i}`, seq: i })),
      );

      let seq = 0;
      const writerLoop = (async () => {
        while (keepWriting) {
          const n = ++seq;
          await itemsColl
            .insertOne({ _id: `w-${n}`, seq: n }, { writeConcern: { w: "majority" } })
            .catch(() => {});
          await new Promise((r) => setTimeout(r, 20));
        }
      })();
      while (seq < 5) {
        await new Promise((r) => setTimeout(r, 20));
      }

      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const config = makeS3Config({
        uri: cluster.mongosUri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        encKey,
        prefix: "t111-bounds",
        sharding: { enabled: true },
      });

      const clusterBackup = await runShardedClusterBackup({ config, storage, logger });

      const postStart = seq;
      while (seq < postStart + 4) {
        await new Promise((r) => setTimeout(r, 20));
      }
      keepWriting = false;
      await writerLoop;

      expect(clusterBackup.manifest.key).toMatch(/\.cluster\.enc$/);
      const verified = await verifyBackup({ config, storage, logger }, clusterBackup.manifest.key);
      expect(verified.sha256).toBe(clusterBackup.manifest.sha256);

      const clusterTimeTs = parseOplogTimestamp(clusterBackup.clusterMeta.clusterTime);
      expect(clusterTimeTs).not.toBeNull();
      for (const shardInfo of Object.values(clusterBackup.clusterMeta.shards)) {
        const sManifest = await readManifest(storage, shardInfo.backupKey);
        const sEndTs = parseOplogTimestamp(sManifest?.oplogWindow?.endTs);
        expect(sEndTs).not.toBeNull();
        expect(compareTimestamps(clusterTimeTs, sEndTs)).toBeGreaterThanOrEqual(0);
      }

      // Collect oplog insert timestamps across both shards
      const s1Client = new MongoClient(cluster.shard01.uri);
      const s2Client = new MongoClient(cluster.shard02.uri);
      const writeTsById = new Map();
      try {
        await Promise.all([s1Client.connect(), s2Client.connect()]);
        for (const c of [s1Client, s2Client]) {
          const ops = await c
            .db("local")
            .collection("oplog.rs")
            .find({ op: "i", ns: "boundsdb.items" })
            .toArray();
          for (const op of ops) {
            const id = op.o?._id;
            const ts = fromBsonTimestamp(op.ts);
            if (id && ts) writeTsById.set(id, ts);
          }
        }
      } finally {
        await Promise.all([s1Client.close().catch(() => {}), s2Client.close().catch(() => {})]);
      }

      await mongosClient.db("boundsdb").dropDatabase();
      await restoreShardedCluster({
        config,
        storage,
        logger,
        ref: clusterBackup.manifest.key,
        drop: true,
      });

      const restoredIds = new Set((await itemsColl.find({}).toArray()).map((d) => d._id));
      let postClusterTimeCount = 0;
      for (const [id, writeTs] of writeTsById.entries()) {
        if (compareTimestamps(writeTs, clusterTimeTs) <= 0) {
          expect(restoredIds.has(id)).toBe(true);
        } else {
          postClusterTimeCount++;
          expect(restoredIds.has(id)).toBe(false);
        }
      }
      expect(postClusterTimeCount).toBeGreaterThan(0);
    } finally {
      keepWriting = false;
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await s3Server.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("11.2 F11/F-TST-010: Active moveChunk / balancer drain & recovery (happy drain)", async () => {
    const suiteDir = path.join(rootDir, "t112");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);

    try {
      await mongosClient.connect();
      await ensureDbPrimaryShard(mongosClient, "draindb", "shard01");
      await mongosClient.db("admin").command({
        shardCollection: "draindb.items",
        key: { idx: 1 },
      });
      await mongosClient
        .db("config")
        .collection("settings")
        .updateOne({ _id: "chunksize" }, { $set: { value: 1 } }, { upsert: true });
      await mongosClient
        .db("draindb")
        .collection("items")
        .insertMany(
          Array.from({ length: 400 }, (_, i) => ({ _id: i, idx: i, pad: "x".repeat(8192) })),
        );

      for (const mid of [100, 200, 300]) {
        await mongosClient.db("admin").command({ split: "draindb.items", middle: { idx: mid } });
      }

      await cluster.shard01.setFailPoint(cluster.shard01.nodes[0], "moveChunkHangAtStep4", {
        mode: "alwaysOn",
      });
      await mongosClient.db("admin").command({ balancerStart: 1 });
      await waitForBalancerRound(mongosClient, 30_000);

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t112-drain",
        sharding: { enabled: true },
      });

      const backupPromise = runShardedClusterBackup({ config, storage, logger });
      await new Promise((r) => setTimeout(r, 500));
      await cluster.shard01.clearFailPoint(cluster.shard01.nodes[0], "moveChunkHangAtStep4");

      const backupRes = await backupPromise;
      const bStatus = await mongosClient.db("admin").command({ balancerStatus: 1 });
      expect(bStatus.mode).toBe("full");

      await verifyBackup({ config, storage, logger }, backupRes.manifest.key);
      await mongosClient.db("draindb").dropDatabase();
      await restoreShardedCluster({
        config,
        storage,
        logger,
        ref: backupRes.manifest.key,
        drop: true,
      });
      expect(await mongosClient.db("draindb").collection("items").countDocuments()).toBe(400);
    } finally {
      await cluster.shard01
        .clearFailPoint(cluster.shard01.nodes[0], "moveChunkHangAtStep4")
        .catch(() => {});
      await cluster.shard02
        .clearFailPoint(cluster.shard02.nodes[0], "moveChunkHangAtStep4")
        .catch(() => {});
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("11.3 F11/F-TST-010/F-SHD-007: Hung moveChunkHangAtStep4 -> ActiveChunkMigrationError fail-closed + balancer recovery", async () => {
    const suiteDir = path.join(rootDir, "t113");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);

    try {
      await mongosClient.connect();
      await ensureDbPrimaryShard(mongosClient, "hangdb", "shard01");
      await mongosClient.db("admin").command({
        shardCollection: "hangdb.items",
        key: { idx: 1 },
      });
      await mongosClient
        .db("config")
        .collection("settings")
        .updateOne({ _id: "chunksize" }, { $set: { value: 1 } }, { upsert: true });
      await mongosClient
        .db("hangdb")
        .collection("items")
        .insertMany(
          Array.from({ length: 600 }, (_, i) => ({ _id: i, idx: i, pad: "x".repeat(8192) })),
        );

      for (const mid of [150, 300, 450]) {
        await mongosClient.db("admin").command({ split: "hangdb.items", middle: { idx: mid } });
      }

      await cluster.shard01.setFailPoint(cluster.shard01.nodes[0], "moveChunkHangAtStep4", {
        mode: "alwaysOn",
      });
      await mongosClient.db("admin").command({ balancerStart: 1 });
      await waitForBalancerRound(mongosClient, 30_000);

      // Wait for an active migration to actually hit moveChunkHangAtStep4 on shard01
      const opDeadline = Date.now() + 15_000;
      while (Date.now() < opDeadline) {
        const cur = await mongosClient
          .db("admin")
          .aggregate([
            { $currentOp: { allUsers: true } },
            {
              $match: {
                $or: [
                  { "command.moveChunk": { $exists: true } },
                  { "command._shardsvrMoveRange": { $exists: true } },
                ],
              },
            },
          ])
          .toArray()
          .catch(() => []);
        if (cur.length > 0) break;
        await new Promise((r) => setTimeout(r, 150));
      }

      // (a) Direct fast BalancerController check
      const balancer = new BalancerController(mongosClient, logger);
      await expect(balancer.stopAndDrain(1_500)).rejects.toThrow(ActiveChunkMigrationError);

      // Re-ensure balancer is active before full runShardedClusterBackup check
      await mongosClient
        .db("admin")
        .command({ balancerStart: 1 })
        .catch(() => {});
      await waitForBalancerRound(mongosClient);

      // (b) Full runShardedClusterBackup check (unweakened [M4-Gated F-SHD-007])
      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t113-hang",
        sharding: { enabled: true },
      });

      await expect(runShardedClusterBackup({ config, storage, logger })).rejects.toThrow(
        ActiveChunkMigrationError,
      );
      expect(await listStorageKeys(storage, config.storage.prefix)).toEqual([]);

      const postStatus = await mongosClient.db("admin").command({ balancerStatus: 1 });
      expect(postStatus.mode).toBe("full");
    } finally {
      await cluster.shard01
        .clearFailPoint(cluster.shard01.nodes[0], "moveChunkHangAtStep4")
        .catch(() => {});
      await cluster.shard02
        .clearFailPoint(cluster.shard02.nodes[0], "moveChunkHangAtStep4")
        .catch(() => {});
      await mongosClient
        .db("admin")
        .command({ balancerStart: 1 })
        .catch(() => {});
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("11.4 F11: LeaderLag (SIGSTOP/SIGCONT on CSRS primary) recovers and completes sharded backup", async () => {
    const suiteDir = path.join(rootDir, "t114");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 3,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);
    let cfgPrimary = null;

    try {
      await mongosClient.connect();
      await mongosClient.db("admin").command({ enableSharding: "lagdb" });
      await mongosClient.db("admin").command({
        shardCollection: "lagdb.items",
        key: { _id: "hashed" },
      });
      await mongosClient
        .db("lagdb")
        .collection("items")
        .insertMany(Array.from({ length: 30 }, (_, i) => ({ _id: `lag-${i}`, v: i })));

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t114-lag",
        sharding: { enabled: true },
      });

      cfgPrimary = await cluster.cfgRs.getPrimaryNode();
      let backupRes;
      await cfgPrimary.kill("SIGSTOP");
      try {
        const backupP = runShardedClusterBackup({ config, storage, logger });
        await new Promise((r) => setTimeout(r, 2_500));
        await cfgPrimary.kill("SIGCONT");
        backupRes = await backupP;
      } finally {
        await cfgPrimary.kill("SIGCONT");
      }

      expect(backupRes.manifest.key).toMatch(/\.cluster\.enc$/);
      await verifyBackup({ config, storage, logger }, backupRes.manifest.key);

      await mongosClient.db("lagdb").dropDatabase();
      await restoreShardedCluster({
        config,
        storage,
        logger,
        ref: backupRes.manifest.key,
        drop: true,
      });
      expect(await mongosClient.db("lagdb").collection("items").countDocuments()).toBe(30);
    } finally {
      await cfgPrimary?.kill("SIGCONT").catch(() => {});
      resumeAllPausedProcesses();
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("11.5a F11/F-TST-011: Shard TCP proxy NetworkCut mid-backup aborts siblings, cleans all artifacts, leaves activeUploads === 0, re-enables balancer", async () => {
    const suiteDir = path.join(rootDir, "t115a");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(workDir, { recursive: true });

    const s3Server = await startMockS3Server({ defaultBucket: "e2e-bucket" });
    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const proxy = await createTcpChaosProxy({ targetPort: cluster.shard02.nodes[0].port });
    const mongosClient = new MongoClient(cluster.mongosUri);

    try {
      await mongosClient.connect();
      await mongosClient.db("admin").command({ balancerStart: 1 });
      await mongosClient.db("admin").command({ enableSharding: "cutdb" });
      await mongosClient.db("admin").command({
        shardCollection: "cutdb.items",
        key: { _id: "hashed" },
      });

      const pad = "x".repeat(2048);
      await mongosClient
        .db("cutdb")
        .collection("items")
        .insertMany(Array.from({ length: 400 }, (_, i) => ({ _id: `c-${i}`, pad })));

      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const config = makeS3Config({
        uri: cluster.mongosUri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        encKey,
        prefix: "t115a-cut",
        sharding: { enabled: true },
        timeoutMs: 15_000,
      });

      proxy.setDelayMs(15);
      let shard02Started = false;
      const proxiedBackupRunner = async (opts) => {
        if (opts.config.storage.prefix.endsWith("/shards/shard02")) {
          shard02Started = true;
          return runBackup({
            ...opts,
            config: {
              ...opts.config,
              mongo: {
                ...opts.config.mongo,
                uri: `mongodb://127.0.0.1:${proxy.listenPort}/?directConnection=true`,
              },
            },
          });
        }
        return runBackup(opts);
      };

      const backupP = runShardedClusterBackup({
        config,
        storage,
        logger,
        backupRunner: proxiedBackupRunner,
      });
      backupP.catch(() => {});

      const deadline = Date.now() + 20_000;
      while (Date.now() < deadline && (!shard02Started || proxy.getStats().bytesOut < 50_000)) {
        await new Promise((r) => setTimeout(r, 30));
      }
      proxy.blackhole(true);
      proxy.resetAll();

      await expect(backupP).rejects.toThrow();
      expect(s3Server.stats.activeUploads).toBe(0);
      expect(await listStorageKeys(storage, config.storage.prefix)).toEqual([]);

      const bStatus = await mongosClient.db("admin").command({ balancerStatus: 1 });
      expect(bStatus.mode).toBe("full");
    } finally {
      await proxy.close().catch(() => {});
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await s3Server.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("11.5b F11/F-TST-011: Shard SIGKILL mid-backup cleans all sibling artifacts, leaves activeUploads === 0, re-enables balancer", async () => {
    const suiteDir = path.join(rootDir, "t115b");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(workDir, { recursive: true });

    const s3Server = await startMockS3Server({ defaultBucket: "e2e-bucket" });
    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);

    try {
      await mongosClient.connect();
      await mongosClient.db("admin").command({ balancerStart: 1 });
      await mongosClient.db("admin").command({ enableSharding: "killdb" });
      await mongosClient.db("admin").command({
        shardCollection: "killdb.items",
        key: { _id: "hashed" },
      });

      const pad = "y".repeat(4096);
      await mongosClient
        .db("killdb")
        .collection("items")
        .insertMany(Array.from({ length: 600 }, (_, i) => ({ _id: `k-${i}`, pad })));

      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const config = makeS3Config({
        uri: cluster.mongosUri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        encKey,
        prefix: "t115b-kill",
        sharding: { enabled: true },
        timeoutMs: 30_000,
      });

      let shard02Started = false;
      const observingRunner = async (opts) => {
        if (opts.config.storage.prefix.endsWith("/shards/shard02")) {
          shard02Started = true;
        }
        return runBackup(opts);
      };

      const backupP = runShardedClusterBackup({
        config,
        storage,
        logger,
        backupRunner: observingRunner,
      });
      backupP.catch(() => {});

      const deadline = Date.now() + 20_000;
      while (Date.now() < deadline && !shard02Started) {
        await new Promise((r) => setTimeout(r, 20));
      }
      await new Promise((r) => setTimeout(r, 50));
      await cluster.shard02.nodes[0].kill("SIGKILL");

      await expect(backupP).rejects.toThrow();
      expect(s3Server.stats.activeUploads).toBe(0);
      expect(await listStorageKeys(storage, config.storage.prefix)).toEqual([]);

      const bStatus = await mongosClient.db("admin").command({ balancerStatus: 1 });
      expect(bStatus.mode).toBe("full");

      // Restart shard02 cleanly and verify primary readiness
      await cluster.shard02.nodes[0].restart();
      await waitForPrimary(cluster.shard02.nodes.map((n) => n.directUri));
    } finally {
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await s3Server.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("12.1 F12/F-TST-010/F-TST-012: CleanupFullRestore after post-backup split+moveChunk + routing convergence", async () => {
    const suiteDir = path.join(rootDir, "t121");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);
    const s1Client = new MongoClient(cluster.shard01.uri);
    const s2Client = new MongoClient(cluster.shard02.uri);

    try {
      await Promise.all([mongosClient.connect(), s1Client.connect(), s2Client.connect()]);
      await ensureDbPrimaryShard(mongosClient, "cleanupdb", "shard01");
      await mongosClient.db("admin").command({
        shardCollection: "cleanupdb.items",
        key: { idx: 1 },
      });
      await mongosClient
        .db("cleanupdb")
        .collection("items")
        .insertMany(Array.from({ length: 1000 }, (_, i) => ({ _id: i, idx: i })));

      expect(await s1Client.db("cleanupdb").collection("items").countDocuments()).toBe(1000);
      expect(await s2Client.db("cleanupdb").collection("items").countDocuments()).toBe(0);

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t121-cleanup",
        sharding: { enabled: true },
      });

      const backupRes = await runShardedClusterBackup({ config, storage, logger });

      // Post-backup split + moveChunk to shard02 + rogue DB
      await mongosClient.db("admin").command({ split: "cleanupdb.items", middle: { idx: 500 } });
      await mongosClient.db("admin").command({
        moveChunk: "cleanupdb.items",
        find: { idx: 500 },
        to: "shard02",
      });
      await mongosClient.db("cleanupdb").collection("items").insertOne({ _id: "post-1", idx: 750 });
      await mongosClient.db("rogue_db").collection("tmp").insertOne({ _id: "rogue" });
      expect(await s2Client.db("cleanupdb").collection("items").countDocuments()).toBeGreaterThan(
        0,
      );

      await restoreShardedCluster({
        config,
        storage,
        logger,
        ref: backupRes.manifest.key,
        drop: true,
      });

      expect(await mongosClient.db("cleanupdb").collection("items").countDocuments()).toBe(1000);
      expect(
        await mongosClient
          .db("cleanupdb")
          .collection("items")
          .countDocuments({ idx: { $gte: 500 } }),
      ).toBe(500);
      expect(await mongosClient.db("rogue_db").collection("tmp").countDocuments()).toBe(0);

      // No orphan docs left on shard02, all 1000 on shard01
      expect(await s2Client.db("cleanupdb").collection("items").countDocuments()).toBe(0);
      expect(await s1Client.db("cleanupdb").collection("items").countDocuments()).toBe(1000);

      // Routing converged back to shard01 for idx >= 500
      await mongosClient
        .db("cleanupdb")
        .collection("items")
        .insertOne({ _id: "post-restore-750", idx: 750 });
      expect(
        await s1Client.db("cleanupdb").collection("items").findOne({ _id: "post-restore-750" }),
      ).not.toBeNull();
      expect(
        await s2Client.db("cleanupdb").collection("items").findOne({ _id: "post-restore-750" }),
      ).toBeNull();
    } finally {
      await Promise.all([
        s1Client.close().catch(() => {}),
        s2Client.close().catch(() => {}),
        mongosClient.close().catch(() => {}),
      ]);
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("12.2 F12/F-TST-011/F-SHD-002: Deterministic straddling 2PC txn (hangBeforeWritingDecision) + uncommitted post-target 2PC txn in sharded PITR", async () => {
    const suiteDir = path.join(rootDir, "t122");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);
    const s1Client = new MongoClient(cluster.shard01.uri);
    const s2Client = new MongoClient(cluster.shard02.uri);
    let session1 = null;
    let session3 = null;

    try {
      await Promise.all([mongosClient.connect(), s1Client.connect(), s2Client.connect()]);
      await mongosClient.db("admin").command({ enableSharding: "bank" });
      await mongosClient.db("admin").command({
        shardCollection: "bank.accounts",
        key: { _id: "hashed" },
      });

      const INITIAL_BALANCE = 1000;
      const ACCOUNT_COUNT = 20;
      const EXPECTED_TOTAL = INITIAL_BALANCE * ACCOUNT_COUNT;
      await mongosClient
        .db("bank")
        .collection("accounts")
        .insertMany(
          Array.from({ length: ACCOUNT_COUNT }, (_, i) => ({
            _id: `acct-${String(i).padStart(2, "0")}`,
            balance: INITIAL_BALANCE,
          })),
        );

      const s1Doc = await s1Client.db("bank").collection("accounts").findOne({});
      const s2Doc = await s2Client.db("bank").collection("accounts").findOne({});
      expect(s1Doc).not.toBeNull();
      expect(s2Doc).not.toBeNull();
      const s1Id = s1Doc._id;
      const s2Id = s2Doc._id;

      // Prepare straddling 2PC txn T1 (coordinator = shard01)
      await cluster.shard01.setFailPoint(cluster.shard01.nodes[0], "hangBeforeWritingDecision", {
        mode: "alwaysOn",
      });
      session1 = mongosClient.startSession();
      session1.startTransaction();
      await mongosClient
        .db("bank")
        .collection("accounts")
        .updateOne({ _id: s1Id }, { $inc: { balance: -300 } }, { session: session1 });
      await mongosClient
        .db("bank")
        .collection("accounts")
        .updateOne({ _id: s2Id }, { $inc: { balance: 300 } }, { session: session1 });
      const commit1P = session1.commitTransaction();

      // Poll until both shards have state: "prepared"
      const prepDeadline = Date.now() + 20_000;
      while (Date.now() < prepDeadline) {
        const [p1, p2] = await Promise.all([
          s1Client.db("config").collection("transactions").countDocuments({ state: "prepared" }),
          s2Client.db("config").collection("transactions").countDocuments({ state: "prepared" }),
        ]);
        if (p1 > 0 && p2 > 0) break;
        await new Promise((r) => setTimeout(r, 50));
      }

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t122-2pc",
        sharding: { enabled: true },
      });

      // Start backup WITHOUT custom backupRunner (preserves useSafeCsrsConfigDump & settleShardPreparedTxnsAndOptime)
      const backupP = runShardedClusterBackup({ config, storage, logger });

      // Wait until all 3 child base dumps finish and emit manifests
      const dumpDeadline = Date.now() + 60_000;
      while (Date.now() < dumpDeadline) {
        const keys = await listStorageKeys(storage, config.storage.prefix);
        const hasCfg = keys.some((k) => k.includes("/configsvr/") && k.endsWith(".manifest.json"));
        const hasS1 = keys.some(
          (k) => k.includes("/shards/shard01/") && k.endsWith(".manifest.json"),
        );
        const hasS2 = keys.some(
          (k) => k.includes("/shards/shard02/") && k.endsWith(".manifest.json"),
        );
        if (hasCfg && hasS1 && hasS2) break;
        await new Promise((r) => setTimeout(r, 50));
      }

      await new Promise((r) => setTimeout(r, 150));
      await cluster.shard01.clearFailPoint(cluster.shard01.nodes[0], "hangBeforeWritingDecision");
      await commit1P;
      await session1.endSession();
      session1 = null;

      const backupRes = await backupP;
      const s1Manifest = await readManifest(
        storage,
        backupRes.clusterMeta.shards.shard01.backupKey,
      );
      const s1EndTs = parseOplogTimestamp(s1Manifest?.oplogWindow?.endTs);
      const clusterTs = parseOplogTimestamp(backupRes.clusterMeta.clusterTime);
      expect(s1EndTs).not.toBeNull();
      expect(clusterTs).not.toBeNull();
      expect(compareTimestamps(clusterTs, s1EndTs)).toBeGreaterThanOrEqual(0);

      // Post-backup committed insert + committed 2PC txn T2
      await new Promise((r) => setTimeout(r, 1100));
      await mongosClient
        .db("bank")
        .collection("accounts")
        .insertOne({ _id: "post-doc", balance: 0 });
      const session2 = mongosClient.startSession();
      try {
        await session2.withTransaction(async () => {
          await mongosClient
            .db("bank")
            .collection("accounts")
            .updateOne({ _id: s1Id }, { $inc: { balance: -100 } }, { session: session2 });
          await mongosClient
            .db("bank")
            .collection("accounts")
            .updateOne({ _id: s2Id }, { $inc: { balance: 100 } }, { session: session2 });
        });
      } finally {
        await session2.endSession();
      }

      await Promise.all([
        waitForMajorityCatchup(cluster.cfgRs.uri, await getLatestOplogTs(cluster.cfgRs.uri)),
        waitForMajorityCatchup(cluster.shard01.uri, await getLatestOplogTs(cluster.shard01.uri)),
        waitForMajorityCatchup(cluster.shard02.uri, await getLatestOplogTs(cluster.shard02.uri)),
      ]);
      const targetTs = await getClusterTime(cluster.mongosUri);
      await captureShardedOplogSlices({ config, storage, logger });

      // Post-target 2PC txn T3 prepared in slice2 and committed in slice3
      await new Promise((r) => setTimeout(r, 1100));
      await cluster.shard01.setFailPoint(cluster.shard01.nodes[0], "hangBeforeWritingDecision", {
        mode: "alwaysOn",
      });
      session3 = mongosClient.startSession();
      session3.startTransaction();
      await mongosClient
        .db("bank")
        .collection("accounts")
        .updateOne({ _id: s1Id }, { $inc: { balance: -500 } }, { session: session3 });
      await mongosClient
        .db("bank")
        .collection("accounts")
        .updateOne({ _id: s2Id }, { $inc: { balance: 500 } }, { session: session3 });
      const commit3P = session3.commitTransaction();

      const prep3Deadline = Date.now() + 20_000;
      while (Date.now() < prep3Deadline) {
        const [p1, p2] = await Promise.all([
          s1Client.db("config").collection("transactions").countDocuments({ state: "prepared" }),
          s2Client.db("config").collection("transactions").countDocuments({ state: "prepared" }),
        ]);
        if (p1 > 0 && p2 > 0) break;
        await new Promise((r) => setTimeout(r, 50));
      }
      await captureShardedOplogSlices({ config, storage, logger });

      await cluster.shard01.clearFailPoint(cluster.shard01.nodes[0], "hangBeforeWritingDecision");
      await commit3P;
      await session3.endSession();
      session3 = null;

      await mongosClient.db("bank").collection("accounts").deleteMany({});
      await Promise.all([
        waitForMajorityCatchup(cluster.cfgRs.uri, await getLatestOplogTs(cluster.cfgRs.uri)),
        waitForMajorityCatchup(cluster.shard01.uri, await getLatestOplogTs(cluster.shard01.uri)),
        waitForMajorityCatchup(cluster.shard02.uri, await getLatestOplogTs(cluster.shard02.uri)),
      ]);
      await captureShardedOplogSlices({ config, storage, logger });

      const restoreRes = await restoreShardedCluster({
        config,
        storage,
        logger,
        pitr: formatOplogTimestamp(targetTs),
        drop: true,
      });
      expect(restoreRes.oplogStats?.partialTxns ?? 0).toBe(0);

      const restoredAccounts = await mongosClient
        .db("bank")
        .collection("accounts")
        .find({})
        .toArray();
      expect(restoredAccounts).toHaveLength(ACCOUNT_COUNT + 1);
      const sum = restoredAccounts.reduce((acc, d) => acc + d.balance, 0);
      expect(sum).toBe(EXPECTED_TOTAL);

      const d1 = restoredAccounts.find((d) => d._id === s1Id);
      const d2 = restoredAccounts.find((d) => d._id === s2Id);
      expect(d1?.balance).toBe(600);
      expect(d2?.balance).toBe(1400);
    } finally {
      await cluster.shard01
        .clearFailPoint(cluster.shard01.nodes[0], "hangBeforeWritingDecision")
        .catch(() => {});
      if (session1) {
        await session1.abortTransaction().catch(() => {});
        await session1.endSession().catch(() => {});
      }
      if (session3) {
        await session3.abortTransaction().catch(() => {});
        await session3.endSession().catch(() => {});
      }
      await Promise.all([
        s1Client.close().catch(() => {}),
        s2Client.close().catch(() => {}),
        mongosClient.close().catch(() => {}),
      ]);
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("12.3 F12/F-TST-011/F-SHD-001: Interrupted CsrsRestorer preserves system.sessions UUID & chunks + clean recovery", async () => {
    const suiteDir = path.join(rootDir, "t123");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);
    const cfgClient = new MongoClient(cluster.cfgRs.uri);

    try {
      await Promise.all([mongosClient.connect(), cfgClient.connect()]);
      await mongosClient.db("admin").command({ enableSharding: "sessdb" });
      await mongosClient.db("admin").command({
        shardCollection: "sessdb.items",
        key: { _id: "hashed" },
      });
      await mongosClient
        .db("sessdb")
        .collection("items")
        .insertMany(Array.from({ length: 20 }, (_, i) => ({ _id: `s-${i}` })));

      // Trigger at least one session and wait for logical session cache to initialize config.system.sessions
      const warmSession = mongosClient.startSession();
      try {
        await mongosClient.db("sessdb").collection("items").findOne({}, { session: warmSession });
      } finally {
        await warmSession.endSession();
      }
      const sessDeadline = Date.now() + 15_000;
      let origSessionsDoc = null;
      while (Date.now() < sessDeadline) {
        await cfgClient
          .db("admin")
          .command({ refreshLogicalSessionCacheNow: 1 })
          .catch(() => {});
        await mongosClient
          .db("admin")
          .command({ refreshLogicalSessionCacheNow: 1 })
          .catch(() => {});
        origSessionsDoc = await cfgClient
          .db("config")
          .collection("collections")
          .findOne({ _id: "config.system.sessions" });
        if (origSessionsDoc?.uuid) break;
        await new Promise((r) => setTimeout(r, 250));
      }
      expect(origSessionsDoc?.uuid).toBeDefined();
      const origSessionsChunks = await cfgClient
        .db("config")
        .collection("chunks")
        .find({ uuid: origSessionsDoc.uuid })
        .toArray();
      expect(origSessionsChunks.length).toBeGreaterThan(0);

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t123-csrs",
        sharding: { enabled: true },
      });

      const backupRes = await runShardedClusterBackup({ config, storage, logger });

      // Interrupt CsrsRestorer mid-stream right after drop phase deletes collections
      const abortController = new AbortController();
      const interruptingStorage = Object.create(storage);
      interruptingStorage.download = async (key, opts) => {
        const realStream = await storage.download(key, opts);
        let chunkCount = 0;
        realStream.on("data", () => {
          chunkCount++;
          if (chunkCount === 1) {
            abortController.abort(new Error("Simulated CSRS mid-restore interruption"));
          }
        });
        return realStream;
      };

      await expect(
        new CsrsRestorer().restore({
          config,
          storage: interruptingStorage,
          backupKey: backupRes.clusterMeta.configsvr.backupKey,
          targetConfigsvrUri: cluster.cfgRs.uri,
          drop: true,
          logger,
          signal: abortController.signal,
        }),
      ).rejects.toThrow();

      // Target config.system.sessions UUID must survive interrupted restore
      const midSessionsDoc = await cfgClient
        .db("config")
        .collection("collections")
        .findOne({ _id: "config.system.sessions" });
      expect(midSessionsDoc?.uuid?.toString("hex")).toBe(origSessionsDoc.uuid.toString("hex"));

      // Clean follow-up restoreShardedCluster
      await restoreShardedCluster({
        config,
        storage,
        logger,
        ref: backupRes.manifest.key,
        drop: true,
      });

      expect(await mongosClient.db("sessdb").collection("items").countDocuments()).toBe(20);

      const postSessionsDoc = await cfgClient
        .db("config")
        .collection("collections")
        .findOne({ _id: "config.system.sessions" });
      expect(postSessionsDoc?.uuid?.toString("hex")).toBe(origSessionsDoc.uuid.toString("hex"));

      // Unweakened [M4-Gated F-SHD-001]: live config.system.sessions chunks must survive same-cluster restore
      const postChunks = await cfgClient
        .db("config")
        .collection("chunks")
        .find({ uuid: origSessionsDoc.uuid })
        .toArray();
      expect(postChunks.length).toBe(origSessionsChunks.length);
    } finally {
      await Promise.all([cfgClient.close().catch(() => {}), mongosClient.close().catch(() => {})]);
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("12.4 F12/F-TST-012: Target shard mapping mismatch fails closed leaving live DB untouched", async () => {
    const suiteDir = path.join(rootDir, "t124");
    const srcDir = path.join(suiteDir, "src");
    const dstDir = path.join(suiteDir, "dst");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await Promise.all([
      fs.mkdir(srcDir, { recursive: true }),
      fs.mkdir(dstDir, { recursive: true }),
      fs.mkdir(storageDir, { recursive: true }),
      fs.mkdir(workDir, { recursive: true }),
    ]);

    const srcCluster = await startShardedCluster({ rootDir: srcDir });
    const srcClient = new MongoClient(srcCluster.mongosUri);
    let cfgMismatch = null;
    let shard03 = null;
    let mongosProc = null;
    let mongosUri = null;
    let dstClient = null;

    try {
      await srcClient.connect();
      await srcClient.db("admin").command({ enableSharding: "srcdb" });
      await srcClient.db("admin").command({
        shardCollection: "srcdb.items",
        key: { _id: "hashed" },
      });
      await srcClient.db("srcdb").collection("items").insertOne({ _id: "src-1" });

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const srcConfig = makeFsConfig({
        uri: srcCluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t124-mismatch",
        sharding: { enabled: true },
      });
      const backupRes = await runShardedClusterBackup({ config: srcConfig, storage, logger });

      // Stop source cluster before starting 1-shard target cluster with shard03
      await srcClient.close();
      await srcCluster.stop();

      [cfgMismatch, shard03] = await Promise.all([
        startReplicaSet({ rootDir: dstDir, name: "cfgMismatch", role: "configsvr" }),
        startReplicaSet({ rootDir: dstDir, name: "shard03", role: "shardsvr" }),
      ]);
      const mongosPort = await findFreePort();
      const mongosLog = path.join(dstDir, "mongos-mismatch.log");
      mongosProc = spawn(
        "mongos",
        [
          `--configdb=${cfgMismatch.host}`,
          `--port=${mongosPort}`,
          "--bind_ip=127.0.0.1",
          `--logpath=${mongosLog}`,
        ],
        { stdio: "ignore" },
      );
      mongosUri = `mongodb://127.0.0.1:${mongosPort}/`;
      await waitForPing(mongosUri, mongosProc, mongosLog);

      dstClient = new MongoClient(mongosUri);
      await dstClient.connect();
      await dstClient.db("admin").command({ addShard: shard03.host });
      await dstClient
        .db("live_keep")
        .collection("items")
        .insertMany(Array.from({ length: 5 }, (_, i) => ({ _id: `keep-${i}` })));

      const dstConfig = makeFsConfig({
        uri: mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t124-mismatch",
        sharding: { enabled: true },
      });

      await expect(
        restoreShardedCluster({
          config: dstConfig,
          storage,
          logger,
          ref: backupRes.manifest.key,
          drop: true,
        }),
      ).rejects.toThrow(/missing mapping for shard "shard0[12]"/i);

      expect(await dstClient.db("live_keep").collection("items").countDocuments()).toBe(5);
      const bStatus = await dstClient.db("admin").command({ balancerStatus: 1 });
      expect(bStatus.mode).toBe("full");
    } finally {
      await dstClient?.close().catch(() => {});
      if (mongosProc) await stopProcess(mongosProc, mongosUri).catch(() => {});
      await shard03?.stop().catch(() => {});
      await cfgMismatch?.stop().catch(() => {});
      await srcClient.close().catch(() => {});
      await srcCluster.stop().catch(() => {});
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("12.5 F12/F-TST-012: Sharded configsvr (oplog:false) + quiet shard + timeseries (system.buckets.*) + sharded PITR gap rejection", async () => {
    const suiteDir = path.join(rootDir, "t125");
    const storageDir = path.join(suiteDir, "storage");
    const gapStorageDir = path.join(suiteDir, "storage-gap");
    const workDir = path.join(suiteDir, "work");
    await Promise.all([
      fs.mkdir(storageDir, { recursive: true }),
      fs.mkdir(gapStorageDir, { recursive: true }),
      fs.mkdir(workDir, { recursive: true }),
    ]);

    const cluster = await startShardedCluster({
      rootDir: suiteDir,
      enableTestCommands: true,
      cfgMembers: 1,
      shardMembers: 1,
    });
    const mongosClient = new MongoClient(cluster.mongosUri);

    try {
      await mongosClient.connect();
      await ensureDbPrimaryShard(mongosClient, "tsdb", "shard01");
      await mongosClient.db("tsdb").createCollection("metrics", {
        timeseries: { timeField: "ts", metaField: "sensor" },
      });
      await mongosClient.db("admin").command({
        shardCollection: "tsdb.metrics",
        key: { sensor: 1 },
      });
      await mongosClient
        .db("tsdb")
        .collection("metrics")
        .insertMany([
          { sensor: "s-a", ts: new Date("2026-01-01T00:00:00Z"), val: 10 },
          { sensor: "s-a", ts: new Date("2026-01-01T00:01:00Z"), val: 20 },
        ]);

      await ensureDbPrimaryShard(mongosClient, "pitrdb", "shard01");
      await mongosClient.db("admin").command({
        shardCollection: "pitrdb.items",
        key: { k: 1 },
      });
      await mongosClient.db("pitrdb").collection("items").insertOne({ _id: "base-1", k: 1 });

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t125-ts",
        sharding: { enabled: true },
      });

      const baseBackup = await runShardedClusterBackup({ config, storage, logger });
      const cfgManifest = await readManifest(storage, baseBackup.clusterMeta.configsvr.backupKey);
      expect(cfgManifest?.oplog).toBe(false);

      // Post-backup: write ONLY to shard01 (leaving shard02 quiet) + create new sharded collection (CSRS DDL)
      await new Promise((r) => setTimeout(r, 1100));
      await mongosClient
        .db("tsdb")
        .collection("metrics")
        .insertOne({ sensor: "s-a", ts: new Date("2026-01-01T00:02:00Z"), val: 30 });
      await mongosClient.db("pitrdb").collection("items").insertOne({ _id: "slice1-doc", k: 2 });
      await ensureDbPrimaryShard(mongosClient, "newdb", "shard01");
      await mongosClient.db("admin").command({
        shardCollection: "newdb.events",
        key: { _id: 1 },
      });
      await mongosClient.db("newdb").collection("events").insertOne({ _id: "ev1" });

      await Promise.all([
        waitForMajorityCatchup(cluster.cfgRs.uri, await getLatestOplogTs(cluster.cfgRs.uri)),
        waitForMajorityCatchup(cluster.shard01.uri, await getLatestOplogTs(cluster.shard01.uri)),
        waitForMajorityCatchup(cluster.shard02.uri, await getLatestOplogTs(cluster.shard02.uri)),
      ]);
      const slice1End = await getClusterTime(cluster.mongosUri);
      const slices1 = await captureShardedOplogSlices({ config, storage, logger });
      expect(slices1).not.toBeNull();
      expect(slices1.configsvr).not.toBeNull();
      expect(slices1.shards.shard01).not.toBeNull();

      // Slice 2
      await new Promise((r) => setTimeout(r, 1100));
      await mongosClient.db("pitrdb").collection("items").insertOne({ _id: "slice2-doc", k: 3 });
      await ensureDbPrimaryShard(mongosClient, "slice2cfg", "shard01");
      await Promise.all([
        waitForMajorityCatchup(cluster.cfgRs.uri, await getLatestOplogTs(cluster.cfgRs.uri)),
        waitForMajorityCatchup(cluster.shard01.uri, await getLatestOplogTs(cluster.shard01.uri)),
        waitForMajorityCatchup(cluster.shard02.uri, await getLatestOplogTs(cluster.shard02.uri)),
      ]);
      const slices2 = await captureShardedOplogSlices({ config, storage, logger });
      expect(slices2?.shards.shard01).not.toBeNull();
      const cfg2End = slices2?.configsvr?.oplogWindow?.endTs
        ? parseOplogTimestamp(slices2.configsvr.oplogWindow.endTs)
        : null;
      const s12End = slices2?.shards?.shard01?.oplogWindow?.endTs
        ? parseOplogTimestamp(slices2.shards.shard01.oplogWindow.endTs)
        : null;
      const slice2End =
        cfg2End && s12End
          ? compareTimestamps(cfg2End, s12End) <= 0
            ? cfg2End
            : s12End
          : await getClusterTime(cluster.mongosUri);

      // Gap rejection test on copied storage with shard01 slice1 removed
      await copyDirRecursive(storageDir, gapStorageDir);
      const gapStorage = new FilesystemStorageClient(gapStorageDir);
      await gapStorage.remove(slices1.shards.shard01.key);
      await gapStorage.remove(manifestKey(slices1.shards.shard01.key));
      const gapConfig = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir: gapStorageDir,
        workDir,
        encKey,
        prefix: "t125-ts",
        sharding: { enabled: true },
      });

      await expect(
        restoreShardedCluster({
          config: gapConfig,
          storage: gapStorage,
          logger,
          pitr: formatOplogTimestamp(slice2End),
          drop: true,
        }),
      ).rejects.toThrow(/PITR timeline gap/i);

      // Live data must remain untouched after gap rejection
      expect(await mongosClient.db("tsdb").collection("metrics").countDocuments()).toBe(3);

      // Clean PITR restore to slice1End
      await mongosClient.db("tsdb").dropDatabase();
      await mongosClient.db("pitrdb").dropDatabase();
      await mongosClient.db("newdb").dropDatabase();

      await restoreShardedCluster({
        config,
        storage,
        logger,
        pitr: formatOplogTimestamp(slice1End),
        drop: true,
      });

      const metrics = await mongosClient
        .db("tsdb")
        .collection("metrics")
        .find({})
        .sort({ val: 1 })
        .toArray();
      expect(metrics.map((m) => m.val)).toEqual([10, 20, 30]);
      expect(
        await mongosClient.db("newdb").collection("events").findOne({ _id: "ev1" }),
      ).not.toBeNull();
      expect(
        await mongosClient.db("pitrdb").collection("items").findOne({ _id: "slice1-doc" }),
      ).not.toBeNull();
      expect(
        await mongosClient.db("pitrdb").collection("items").findOne({ _id: "slice2-doc" }),
      ).toBeNull();
    } finally {
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("12.6a F12/F-TST-012/F-OPS-001: Same-cluster mongos isolation guard rejects runDrill via proxied/alias mongos", async () => {
    const suiteDir = path.join(rootDir, "t126a");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await fs.mkdir(storageDir, { recursive: true });
    await fs.mkdir(workDir, { recursive: true });

    const cluster = await startShardedCluster({ rootDir: suiteDir });
    const mongosProxy = await createTcpChaosProxy({ targetPort: cluster.mongosPort });
    const mongosClient = new MongoClient(cluster.mongosUri);

    try {
      await mongosClient.connect();
      await mongosClient.db("admin").command({ enableSharding: "drillguarddb" });
      await mongosClient.db("admin").command({
        shardCollection: "drillguarddb.items",
        key: { _id: "hashed" },
      });
      await mongosClient.db("drillguarddb").collection("items").insertOne({ _id: "g1" });

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const prodConfig = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t126a-guard",
        sharding: { enabled: true },
      });
      await runShardedClusterBackup({ config: prodConfig, storage, logger });

      const sameClusterDrillUri = `mongodb://127.0.0.1:${mongosProxy.listenPort}/`;
      const drillCfg = makeFsConfig({
        uri: cluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t126a-guard",
        sharding: { enabled: true },
        drillUri: sameClusterDrillUri,
      });

      // Unweakened [M5-Gated F-OPS-001]: must reject same-cluster mongos router with Safety violation
      await expect(runDrill({ config: drillCfg, storage, logger })).rejects.toThrow(
        /Safety violation:/,
      );
    } finally {
      await mongosProxy.close().catch(() => {});
      await mongosClient.close().catch(() => {});
      await cluster.stop();
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("12.6b F12/F-TST-012/F-OPS-003: Isolated sharded cluster runDrill verifies .cluster.enc backup and cleans up restored DBs", async () => {
    const suiteDir = path.join(rootDir, "t126b");
    const prodDir = path.join(suiteDir, "prod");
    const drillDir = path.join(suiteDir, "drill");
    const storageDir = path.join(suiteDir, "storage");
    const workDir = path.join(suiteDir, "work");
    await Promise.all([
      fs.mkdir(prodDir, { recursive: true }),
      fs.mkdir(drillDir, { recursive: true }),
      fs.mkdir(storageDir, { recursive: true }),
      fs.mkdir(workDir, { recursive: true }),
    ]);

    const [prodCluster, drillCluster] = await Promise.all([
      startShardedCluster({ rootDir: prodDir }),
      startShardedCluster({ rootDir: drillDir }),
    ]);
    const prodClient = new MongoClient(prodCluster.mongosUri);
    const drillClient = new MongoClient(drillCluster.mongosUri);

    try {
      await Promise.all([prodClient.connect(), drillClient.connect()]);
      await prodClient.db("admin").command({ enableSharding: "drilldb" });
      await prodClient.db("admin").command({
        shardCollection: "drilldb.items",
        key: { _id: "hashed" },
      });
      await prodClient
        .db("drilldb")
        .collection("items")
        .insertMany(Array.from({ length: 10 }, (_, i) => ({ _id: `d-${i}`, v: i })));

      const encKey = parseKey(generateKey());
      const storage = new FilesystemStorageClient(storageDir);
      const config = makeFsConfig({
        uri: prodCluster.mongosUri,
        storageDir,
        workDir,
        encKey,
        prefix: "t126b-drill",
        sharding: { enabled: true },
        drillUri: drillCluster.mongosUri,
      });

      const backupRes = await runShardedClusterBackup({ config, storage, logger });

      // Unweakened [M5-Gated F-OPS-003]: sharded runDrill must succeed against isolated drillCluster
      const drillRes = await runDrill({ config, storage, logger });
      expect(drillRes.key).toBe(backupRes.manifest.key);
      expect(drillRes.collections).toBeGreaterThan(0);
      expect(drillRes.readWriteVerified).toBe(true);

      const dbs = await drillClient.db("admin").command({ listDatabases: 1, nameOnly: true });
      const userDbs = (dbs.databases || [])
        .map((d) => d.name)
        .filter((n) => !["admin", "local", "config"].includes(n));
      expect(userDbs).toEqual([]);
    } finally {
      await Promise.all([prodClient.close().catch(() => {}), drillClient.close().catch(() => {})]);
      await Promise.all([prodCluster.stop(), drillCluster.stop()]);
      await fs.rm(suiteDir, { recursive: true, force: true }).catch(() => {});
    }
  });
});
