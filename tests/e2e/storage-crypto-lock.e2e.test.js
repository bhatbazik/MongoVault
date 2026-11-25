import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { createGzip } from "node:zlib";
import { MongoClient } from "mongodb";
import pino from "pino";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { runBackup } from "../../src/backup.js";
import {
  MANIFEST_VERSION,
  databasePrefix,
  manifestKey,
  readManifest,
  scopeOf,
  writeManifest,
} from "../../src/catalog.js";
import { ConfigError, loadConfig } from "../../src/config.js";
import {
  createDecryptor,
  createEncryptor,
  generateKey,
  keyId,
  parseKey,
  signManifest,
  verifyManifest,
} from "../../src/crypto.js";
import { DigestStream } from "../../src/digest.js";
import { acquireHybridLock } from "../../src/lock.js";
import { captureOplogSlice, oplogSliceKey } from "../../src/oplog.js";
import { IntegrityError, restoreBackup, verifyBackup } from "../../src/restore.js";
import { deleteBackup, prune } from "../../src/retention.js";
import { runShardedClusterBackup } from "../../src/sharding.js";
import { S3StorageClient } from "../../src/storage.js";
import { cleanupAllChildren, spawnLockWorker } from "./cli-runner.js";
import {
  getLatestOplogTs,
  startReplicaSet,
  startShardedCluster,
  waitForMajorityCatchup,
} from "./cluster-harness.js";
import { startMockS3Server } from "./s3-mock-server.js";

const logger = pino({ level: "silent" });

function makeS3Config({
  uri,
  s3Endpoint,
  bucket = "e2e-bucket",
  workDir,
  encKey = null,
  encKeyRing,
  requireEncryption = false,
  prefix = "e2e",
  partSizeMb = 5,
  queueSize = 1,
  database = "*",
  oplog = true,
  pitrEnabled = true,
  retention = null,
  sharding = null,
  objectLock,
}) {
  return {
    engine: "logical",
    mongo: {
      uri,
      database,
      excludeCollections: [],
      oplog,
      parallelCollections: 2,
      readPreference: "primaryPreferred",
    },
    pitr: {
      enabled: pitrEnabled,
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
      ...(objectLock ? { objectLock } : {}),
    },
    schedule: { cron: "0 2 * * *", timezone: "UTC", runOnStart: false },
    timeoutMs: 120_000,
    workDir,
    encryptionKey: encKey,
    encryptionKeyRing: encKeyRing ?? (encKey ? [encKey] : []),
    requireEncryption,
    retention,
    drill: null,
    physical: null,
    sharding,
    notifications: { onSuccess: false, email: null },
    http: { port: 0 },
    logLevel: "silent",
  };
}

function makeS3Storage(
  s3Endpoint,
  { bucket = "e2e-bucket", partSizeMb = 5, queueSize = 1, objectLock } = {},
) {
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
    objectLock,
  });
}

async function consumeStream(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

async function waitForCondition(predicate, timeoutMs = 5_000, intervalMs = 50) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return false;
}

function countAeadFrames(encryptedBuf) {
  let offset = 0;
  let frames = 0;
  while (offset < encryptedBuf.length) {
    const cipherLen = encryptedBuf.readUInt32BE(offset);
    const total = 5 + 12 + cipherLen + 16;
    offset += total;
    frames++;
  }
  expect(offset).toBe(encryptedBuf.length);
  return frames;
}

describe("E2E-M1: Storage, Encryption, Hybrid Locking & Retention (F1–F4)", () => {
  let rootDir;
  let s3Server;

  beforeAll(async () => {
    rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-e2e-m1-"));
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
  });

  describe("F1 — S3 HTTP Storage & Transport Resilience", () => {
    let rsF1;
    let workDir;

    beforeAll(async () => {
      const f1Dir = path.join(rootDir, "f1");
      workDir = path.join(f1Dir, "work");
      await fs.mkdir(workDir, { recursive: true });
      rsF1 = await startReplicaSet({ rootDir: f1Dir, name: "rsF1" });
    });

    afterAll(async () => {
      await rsF1?.stop().catch(() => {});
    });

    it("1.1 Live S3 single-part (<1 MiB) and multipart (>5 MiB) backup, verify, and restore", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint, { partSizeMb: 5, queueSize: 1 });
      const client = new MongoClient(rsF1.uri);
      await client.connect();

      try {
        // Small (<1 MiB) backup -> single-part PutObject path
        await client.db("smalldb").collection("docs").insertOne({ _id: "s1", hello: "world" });
        const smallCfg = makeS3Config({
          uri: rsF1.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f1-small",
          database: "smalldb",
          oplog: false,
          pitrEnabled: false,
        });
        const smallManifest = await runBackup({ config: smallCfg, storage, logger });
        expect(s3Server.stats.putObjects).toBeGreaterThanOrEqual(1);
        const smallVerified = await verifyBackup(
          { config: smallCfg, storage, logger },
          smallManifest.key,
        );
        expect(smallVerified.sha256).toBe(smallManifest.sha256);

        // Large (>5 MiB incompressible) backup -> multipart upload path
        const docs = Array.from({ length: 110 }, (_, idx) => ({
          _id: `item-${idx}`,
          blob: randomBytes(64 * 1024),
        }));
        await client.db("shop").collection("items").insertMany(docs);
        const expectedDigest = createHash("sha256");
        for (const d of docs) expectedDigest.update(d.blob);
        const expectedAllHex = expectedDigest.digest("hex");

        const multiCfg = makeS3Config({
          uri: rsF1.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f1-multi",
          partSizeMb: 5,
          queueSize: 1,
          database: "*",
          oplog: true,
          pitrEnabled: false,
        });

        const multiManifest = await runBackup({ config: multiCfg, storage, logger });
        expect(multiManifest.size).toBeGreaterThan(5 * 1024 * 1024);
        expect(s3Server.stats.multipartCreates).toBeGreaterThanOrEqual(1);
        expect(s3Server.stats.uploadParts).toBeGreaterThanOrEqual(2);
        expect(s3Server.stats.multipartCompletes).toBeGreaterThanOrEqual(1);
        expect(s3Server.stats.activeUploads).toBe(0);

        const verified = await verifyBackup(
          { config: multiCfg, storage, logger },
          multiManifest.key,
        );
        expect(verified.sha256).toBe(multiManifest.sha256);

        await client.db("shop").collection("items").drop();
        const restored = await restoreBackup(
          { config: multiCfg, storage, logger },
          { uri: rsF1.uri, ref: multiManifest.key, drop: true },
        );
        expect(restored.key).toBe(multiManifest.key);

        const restoredDocs = await client
          .db("shop")
          .collection("items")
          .find({})
          .sort({ _id: 1 })
          .toArray();
        // Sort matching insertion order item-0..item-109
        const byIdx = new Map(restoredDocs.map((d) => [d._id, d.blob]));
        expect(restoredDocs.length).toBe(110);
        const actualDigest = createHash("sha256");
        for (let idx = 0; idx < 110; idx++) {
          const val = byIdx.get(`item-${idx}`);
          const buf = Buffer.isBuffer(val) ? val : Buffer.from(val.buffer);
          actualDigest.update(buf);
        }
        expect(actualDigest.digest("hex")).toBe(expectedAllHex);
      } finally {
        await client
          .db("shop")
          .dropDatabase()
          .catch(() => {});
        await client
          .db("smalldb")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("1.2 ListObjectsV2 pagination (maxKeys=2) & DeleteObjects >1000-key batching", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      s3Server.setListMaxKeys(2);

      const expectedKeys = [];
      for (let i = 0; i < 7; i++) {
        const key = `pag-test/item-${i}.json`;
        expectedKeys.push(key);
        await storage.putJson(key, { idx: i });
      }

      const listed = [];
      for await (const obj of storage.list("pag-test/")) {
        listed.push(obj);
      }
      expect(listed.map((o) => o.key).sort()).toEqual(expectedKeys.sort());
      expect(s3Server.stats.listRequests).toBeGreaterThanOrEqual(4);
      for (const obj of listed) {
        const raw = s3Server.getRaw("e2e-bucket", obj.key);
        expect(obj.size).toBe(raw.body.length);
      }

      // Batch delete >1000 keys (1005 keys -> 2 DeleteObjects HTTP requests)
      const batchKeys = [];
      for (let i = 0; i < 1005; i++) {
        const k = `batch-del/k-${String(i).padStart(4, "0")}.txt`;
        batchKeys.push(k);
        s3Server.putRaw("e2e-bucket", k, `v-${i}`);
      }
      await storage.removeMany(batchKeys);
      expect(s3Server.stats.batchDeleteRequests).toBe(2);
      expect(await storage.head(batchKeys[0])).toBeNull();
      expect(await storage.head(batchKeys[999])).toBeNull();
      expect(await storage.head(batchKeys[1004])).toBeNull();
    });

    it("1.3 TCP reset Range+IfMatch HTTP 206 resume & HTTP 200 non-Range rejection (F-SCL-011)", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      const payload = randomBytes(256 * 1024);
      await storage.upload("range-test/blob.bin", payload);

      // (a) One-shot TCP reset after 64 KiB -> Range + If-Match 206 resume succeeds byte-for-byte
      s3Server.addFault({
        op: "GetObject",
        method: "GET",
        keyPattern: /blob\.bin$/,
        type: "tcp-reset-after-bytes",
        bytes: 65_536,
        once: true,
      });

      const streamA = await storage.download("range-test/blob.bin");
      const reassembled = await consumeStream(streamA);
      expect(reassembled.equals(payload)).toBe(true);

      const resumed206 = s3Server.requestLog.find((entry) => {
        if (
          entry.op !== "GetObject" ||
          entry.key !== "range-test/blob.bin" ||
          !entry.headers["if-match"] ||
          entry.status !== 206
        ) {
          return false;
        }
        const m = /^bytes=(\d+)-$/.exec(String(entry.headers.range ?? ""));
        if (!m) return false;
        const offset = Number(m[1]);
        return (
          offset > 0 && offset <= 65_536 && entry.contentRange === `bytes ${offset}-262143/262144`
        );
      });
      expect(resumed206).toBeDefined();

      // (b) Resumed Range request replying HTTP 200 without Content-Range MUST reject (F-SCL-011)
      s3Server.addFault({
        op: "GetObject",
        method: "GET",
        keyPattern: /blob\.bin$/,
        hasRange: false,
        type: "tcp-reset-after-bytes",
        bytes: 65_536,
        once: true,
      });
      s3Server.addFault({
        op: "GetObject",
        method: "GET",
        keyPattern: /blob\.bin$/,
        hasRange: true,
        type: "http-200-ignore-range",
        once: true,
      });

      await expect(
        (async () => {
          const streamB = await storage.download("range-test/blob.bin");
          return consumeStream(streamB);
        })(),
      ).rejects.toThrow();
    });

    it("1.4 ETag mutation 412 fail-closed BEFORE --drop (F1 x F6)", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF1.uri);
      await client.connect();

      try {
        await client.db("shop").collection("survivors").insertOne({ _id: "keep-1", v: 1 });
        const cfg = makeS3Config({
          uri: rsF1.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f1-412",
          database: "shop",
          oplog: false,
          pitrEnabled: false,
        });
        const manifest = await runBackup({ config: cfg, storage, logger });

        s3Server.addFault({
          op: "GetObject",
          method: "GET",
          keyPattern: /\.archive\.gz\.enc$/,
          type: "mutate-object-after-bytes",
          bytes: 4096,
          newBody: Buffer.from("mutated-version"),
          once: true,
        });

        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF1.uri, ref: manifest.key, drop: true },
          ),
        ).rejects.toThrow();

        const count = await client.db("shop").collection("survivors").countDocuments();
        expect(count).toBe(1);
        const doc = await client.db("shop").collection("survivors").findOne({ _id: "keep-1" });
        expect(doc).toEqual({ _id: "keep-1", v: 1 });
      } finally {
        await client
          .db("shop")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("1.5 Mid-upload AbortMultipartUpload (activeUploads === 0), pre-aborted signal (F-SCL-015), & 501 releaseLock fallback", async () => {
      const storage = makeS3Storage(s3Server.endpoint, { partSizeMb: 5, queueSize: 1 });

      // (a) Mid-upload abort cleans multipart upload on S3
      s3Server.addFault({
        op: "UploadPart",
        keyPattern: /abort-test\/big\.bin$/,
        type: "delay",
        delayMs: 300,
        count: 5,
      });

      const ac = new AbortController();
      let pushed = 0;
      const totalChunks = 14; // 14 * 512 KiB = 7 MiB
      const slowStream = new Readable({
        read() {
          if (pushed >= totalChunks) {
            this.push(null);
            return;
          }
          pushed++;
          setTimeout(() => {
            if (!this.destroyed) {
              this.push(Buffer.alloc(512 * 1024, pushed));
            }
          }, 20);
        },
      });

      const uploadPromise = storage.upload("abort-test/big.bin", slowStream, { signal: ac.signal });
      const sawUpload = await waitForCondition(
        () => s3Server.stats.activeUploads === 1 && s3Server.stats.uploadParts >= 1,
        5_000,
      );
      expect(sawUpload).toBe(true);

      ac.abort(new Error("mid-upload abort"));
      await expect(uploadPromise).rejects.toThrow();

      const abortedCleanly = await waitForCondition(
        () => s3Server.stats.activeUploads === 0 && s3Server.stats.multipartAborts >= 1,
        3_000,
      );
      expect(abortedCleanly).toBe(true);
      expect(s3Server.stats.activeUploads).toBe(0);
      expect(s3Server.stats.multipartAborts).toBeGreaterThanOrEqual(1);

      // (b) Pre-aborted signal MUST fail fast with zero S3 requests (F-SCL-015)
      const reqsBeforePreAbort = s3Server.stats.requests;
      const preAc = new AbortController();
      preAc.abort(new Error("pre-aborted"));
      await expect(
        storage.upload("abort-test/preaborted.bin", Buffer.alloc(1024), {
          signal: preAc.signal,
        }),
      ).rejects.toThrow();
      expect(s3Server.stats.requests).toBe(reqsBeforePreAbort);

      // (c) 501 releaseLock fallback to unconditional DeleteObject
      const lockKey = "e2e/.locks/test-501.lock.json";
      const token = await storage.acquireLock(lockKey, { holder: "t1", ttlMs: 30_000 });
      expect(token).toBeTruthy();

      s3Server.addFault({
        op: "DeleteObject",
        method: "DELETE",
        keyPattern: /test-501\.lock\.json$/,
        hasIfMatch: true,
        type: "http-error",
        status: 501,
        code: "NotImplemented",
        message: "Conditional delete not supported",
        once: true,
      });

      await storage.releaseLock(lockKey, token);
      expect(await storage.getJson(lockKey)).toBeNull();
    });
  });

  describe("F2 — Chunked AEAD, Key Ring Rotation & Manifest Authentication", () => {
    let rsF2;
    let workDir;

    beforeAll(async () => {
      const f2Dir = path.join(rootDir, "f2");
      workDir = path.join(f2Dir, "work");
      await fs.mkdir(workDir, { recursive: true });
      rsF2 = await startReplicaSet({ rootDir: f2Dir, name: "rsF2" });
    });

    afterAll(async () => {
      await rsF2?.stop().catch(() => {});
    });

    it("2.1 & 2.2 Live key-ring rotation (v1->v2->v3) across RS base + PITR slices & missing old key fail-closed BEFORE --drop", async () => {
      const k1 = parseKey(generateKey());
      const k2 = parseKey(generateKey());
      const k3 = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF2.uri);
      await client.connect();

      try {
        await client.db("shop").collection("ledger").insertOne({ _id: "d1", step: 1 });
        await waitForMajorityCatchup(rsF2.uri, await getLatestOplogTs(rsF2.uri));

        // 1. Base backup with k1
        const cfg1 = makeS3Config({
          uri: rsF2.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey: k1,
          encKeyRing: [k1],
          prefix: "f2-rot",
        });
        const baseManifest = await runBackup({ config: cfg1, storage, logger });
        expect(baseManifest.encryption?.keyId).toBe(keyId(k1));

        // 2. Write d2, rotate to k2, capture PITR slice S1
        await client.db("shop").collection("ledger").insertOne({ _id: "d2", step: 2 });
        await waitForMajorityCatchup(rsF2.uri, await getLatestOplogTs(rsF2.uri));
        const cfg2 = makeS3Config({
          uri: rsF2.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey: k2,
          encKeyRing: [k2, k1],
          prefix: "f2-rot",
        });
        const s1 = await captureOplogSlice({ config: cfg2, storage, logger });
        expect(s1).not.toBeNull();
        expect(s1.encryption?.keyId).toBe(keyId(k2));

        // 3. Write d3, rotate to k3, capture PITR slice S2
        await client.db("shop").collection("ledger").insertOne({ _id: "d3", step: 3 });
        const targetTs = await getLatestOplogTs(rsF2.uri);
        await waitForMajorityCatchup(rsF2.uri, targetTs);
        const cfg3 = makeS3Config({
          uri: rsF2.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey: k3,
          encKeyRing: [k3, k2, k1],
          prefix: "f2-rot",
        });
        const s2 = await captureOplogSlice({ config: cfg3, storage, logger });
        expect(s2).not.toBeNull();
        expect(s2.encryption?.keyId).toBe(keyId(k3));

        // 2.2 Missing old key k1 fail-closed BEFORE --drop
        await client.db("shop").collection("ledger").deleteMany({});
        await client
          .db("shop")
          .collection("ledger")
          .insertOne({ _id: "live-sentinel", keep: true });
        const missingOldCfg = makeS3Config({
          uri: rsF2.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey: k3,
          encKeyRing: [k3, k2],
          prefix: "f2-rot",
        });
        await expect(
          restoreBackup(
            { config: missingOldCfg, storage, logger },
            { uri: rsF2.uri, pitr: targetTs, drop: true },
          ),
        ).rejects.toThrow(/key/i);

        const sentinelAfterFail = await client
          .db("shop")
          .collection("ledger")
          .find({ _id: "live-sentinel" })
          .toArray();
        expect(sentinelAfterFail.length).toBe(1);

        // 2.1 Full key ring [k3, k2, k1] verifies and PITR-restores d1, d2, d3
        const verified = await verifyBackup({ config: cfg3, storage, logger }, baseManifest.key);
        expect(verified.sha256).toBe(baseManifest.sha256);

        await client.db("shop").collection("ledger").drop();
        await restoreBackup(
          { config: cfg3, storage, logger },
          { uri: rsF2.uri, pitr: targetTs, drop: true },
        );

        const restoredDocs = await client
          .db("shop")
          .collection("ledger")
          .find({})
          .sort({ _id: 1 })
          .toArray();
        expect(restoredDocs.map((d) => d._id)).toEqual(["d1", "d2", "d3"]);
      } finally {
        await client
          .db("shop")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("2.3 1-byte manifest HMAC tamper, 1-byte schema tamper (F-SCL-008), & 1-byte ciphertext frame tamper BEFORE --drop", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF2.uri);
      await client.connect();

      try {
        await client
          .db("shop")
          .collection("users")
          .createIndex({ name: 1 }, { unique: true, name: "name_unique" });
        await client.db("shop").collection("users").insertOne({ _id: "u1", name: "Alice" });
        await waitForMajorityCatchup(rsF2.uri, await getLatestOplogTs(rsF2.uri));

        const cfg = makeS3Config({
          uri: rsF2.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f2-tamper",
        });
        const manifest = await runBackup({ config: cfg, storage, logger });

        await client.db("shop").collection("users").insertOne({ _id: "u2", name: "Bob" });
        const sliceTargetTs = await getLatestOplogTs(rsF2.uri);
        await waitForMajorityCatchup(rsF2.uri, sliceTargetTs);
        const sliceManifest = await captureOplogSlice({ config: cfg, storage, logger });
        expect(sliceManifest).not.toBeNull();

        // Seed live sentinel
        await client.db("shop").collection("users").deleteMany({});
        await client
          .db("shop")
          .collection("users")
          .insertOne({ _id: "tamper-sentinel", name: "Sentinel" });

        const mKey = manifestKey(manifest.key);
        const origManifestRaw = Buffer.from(s3Server.getRaw("e2e-bucket", mKey).body);
        const origArchiveRaw = Buffer.from(s3Server.getRaw("e2e-bucket", manifest.key).body);
        const origSliceRaw = Buffer.from(s3Server.getRaw("e2e-bucket", sliceManifest.key).body);

        // (a) Corrupt 1 hex char in manifest HMAC -> rejects before --drop
        const hmacTampered = JSON.parse(origManifestRaw.toString("utf8"));
        const flipChar = (hex) => (hex[0] === "a" ? "b" : "a") + hex.slice(1);
        hmacTampered.signature.hmac = flipChar(hmacTampered.signature.hmac);
        hmacTampered.encryption.manifestHmac = hmacTampered.signature.hmac;
        s3Server.putRaw("e2e-bucket", mKey, JSON.stringify(hmacTampered, null, 2), {
          contentType: "application/json",
        });

        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF2.uri, ref: manifest.key, drop: true },
          ),
        ).rejects.toThrow(/HMAC|tamper|IntegrityError/i);
        expect(await client.db("shop").collection("users").countDocuments()).toBe(1);

        // (c) Restore valid manifest, flip 1 byte in archive ciphertext -> rejects before --drop
        s3Server.putRaw("e2e-bucket", mKey, origManifestRaw, {
          contentType: "application/json",
        });
        const badArchive = Buffer.from(origArchiveRaw);
        const archFlipOffset = Math.min(25, badArchive.length - 1);
        badArchive[archFlipOffset] ^= 0xff;
        s3Server.putRaw("e2e-bucket", manifest.key, badArchive);

        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF2.uri, ref: manifest.key, drop: true },
          ),
        ).rejects.toThrow(IntegrityError);
        expect(await client.db("shop").collection("users").countDocuments()).toBe(1);

        // Restore valid archive, flip 1 byte in PITR slice ciphertext -> PITR restore rejects before --drop
        s3Server.putRaw("e2e-bucket", manifest.key, origArchiveRaw);
        const badSlice = Buffer.from(origSliceRaw);
        const sliceFlipOffset = Math.min(25, badSlice.length - 1);
        badSlice[sliceFlipOffset] ^= 0xff;
        s3Server.putRaw("e2e-bucket", sliceManifest.key, badSlice);

        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF2.uri, pitr: sliceTargetTs, drop: true },
          ),
        ).rejects.toThrow(IntegrityError);
        expect(await client.db("shop").collection("users").countDocuments()).toBe(1);

        // (b) Restore valid slice, mutate manifest.schema without re-signing -> verifyBackup MUST reject (F-SCL-008)
        s3Server.putRaw("e2e-bucket", sliceManifest.key, origSliceRaw);
        const schemaTampered = JSON.parse(origManifestRaw.toString("utf8"));
        const idxSpec = schemaTampered.schema.collections["shop.users"].indexes.find(
          (i) => i.name === "name_unique",
        );
        expect(idxSpec?.unique).toBe(true);
        idxSpec.unique = false;
        s3Server.putRaw("e2e-bucket", mKey, JSON.stringify(schemaTampered, null, 2), {
          contentType: "application/json",
        });

        await expect(verifyBackup({ config: cfg, storage, logger }, manifest.key)).rejects.toThrow(
          IntegrityError,
        );
      } finally {
        await client
          .db("shop")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("2.4 Manifest signature-stripping attack rejected when key ring is configured (F-SCL-003)", async () => {
      const k1 = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF2.uri);
      await client.connect();

      try {
        await client.db("shop").collection("secrets").insertOne({ _id: "orig", secret: "real" });
        const cfg = makeS3Config({
          uri: rsF2.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey: k1,
          prefix: "f2-strip",
          database: "shop",
          oplog: false,
          pitrEnabled: false,
        });
        const manifest = await runBackup({ config: cfg, storage, logger });

        // Attacker replaces encrypted archive with a valid k1-encrypted fake gzip stream and strips HMAC fields
        const fakePlain = Buffer.from("forged-archive-payload");
        const gzip = createGzip();
        const enc = createEncryptor(k1);
        const digest = new DigestStream();
        const forgedBuf = await (async () => {
          const pass = Readable.from([fakePlain]).pipe(gzip).pipe(enc.stream).pipe(digest);
          return consumeStream(pass);
        })();
        await storage.upload(manifest.key, forgedBuf);

        const strippedManifest = {
          ...manifest,
          size: digest.bytes,
          sha256: digest.digest(),
          encryption: enc.envelope(),
        };
        delete strippedManifest.signature;
        if (strippedManifest.encryption) {
          delete strippedManifest.encryption.manifestHmac;
        }
        await storage.putJson(manifestKey(manifest.key), strippedManifest, { immutable: true });

        await expect(verifyBackup({ config: cfg, storage, logger }, manifest.key)).rejects.toThrow(
          IntegrityError,
        );
        await expect(
          restoreBackup(
            { config: cfg, storage, logger },
            { uri: rsF2.uri, ref: manifest.key, drop: true },
          ),
        ).rejects.toThrow();

        // Unencrypted signed manifest stripped of signature MUST also fail verifyManifest([k1], ...)
        const unencManifest = {
          version: MANIFEST_VERSION,
          key: "f2-strip/shop/2026-11-25/20261125T100000Z.archive.gz",
          database: "shop",
          createdAt: "2026-11-25T10:00:00.000Z",
          completedAt: "2026-11-25T10:00:01.000Z",
          durationMs: 1000,
          size: 12,
          sha256: "abc",
          compression: "gzip",
          encryption: null,
          excludedCollections: [],
          collections: {},
        };
        unencManifest.signature = {
          algorithm: "HMAC-SHA256",
          keyId: keyId(k1),
          hmac: signManifest(k1, unencManifest),
        };
        const strippedUnenc = { ...unencManifest, sha256: "tampered" };
        delete strippedUnenc.signature;

        expect(() => verifyManifest([k1], strippedUnenc)).toThrow();
      } finally {
        await client
          .db("shop")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("2.5 REQUIRE_ENCRYPTION enforcement, 64 KiB frame boundaries (0, 64K-1, 64K, 64K+1), & empty mongodump", async () => {
      const storage = makeS3Storage(s3Server.endpoint);

      // (a) REQUIRE_ENCRYPTION without key fails in both runBackup and loadConfig
      const noKeyCfg = makeS3Config({
        uri: rsF2.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        encKey: null,
        requireEncryption: true,
        prefix: "f2-req-enc",
        database: "shop",
        oplog: false,
        pitrEnabled: false,
      });
      await expect(runBackup({ config: noKeyCfg, storage, logger })).rejects.toThrow(
        /REQUIRE_ENCRYPTION/,
      );
      expect(() =>
        loadConfig({
          MONGODB_URI: rsF2.uri,
          STORAGE_TYPE: "filesystem",
          STORAGE_PATH: workDir,
          REQUIRE_ENCRYPTION: "true",
        }),
      ).toThrow(ConfigError);

      // (b) 64 KiB frame boundary round-trips
      const masterKey = parseKey(generateKey());
      const sizes = [
        { len: 0, expectedFrames: 1 },
        { len: 65_535, expectedFrames: 1 },
        { len: 65_536, expectedFrames: 2 },
        { len: 65_537, expectedFrames: 2 },
      ];
      for (const { len, expectedFrames } of sizes) {
        const plain = randomBytes(len);
        const enc = createEncryptor(masterKey);
        const encryptedBuf = await consumeStream(Readable.from([plain]).pipe(enc.stream));
        expect(countAeadFrames(encryptedBuf)).toBe(expectedFrames);

        const dec = createDecryptor(masterKey, enc.envelope());
        const decryptedBuf = await consumeStream(Readable.from([encryptedBuf]).pipe(dec));
        expect(decryptedBuf.equals(plain)).toBe(true);
      }

      // (c) Empty collection mongodump (0 docs)
      const client = new MongoClient(rsF2.uri);
      await client.connect();
      try {
        await client.db("empty_db").createCollection("blank");
        const cfg = makeS3Config({
          uri: rsF2.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey: masterKey,
          prefix: "f2-empty",
          database: "empty_db",
          oplog: false,
          pitrEnabled: false,
        });
        const manifest = await runBackup({ config: cfg, storage, logger });
        expect(manifest.size).toBeGreaterThan(0);

        const verified = await verifyBackup({ config: cfg, storage, logger }, manifest.key);
        expect(verified.sha256).toBe(manifest.sha256);

        await client.db("empty_db").dropDatabase();
        await restoreBackup(
          { config: cfg, storage, logger },
          { uri: rsF2.uri, ref: manifest.key, drop: true },
        );
        const colls = await client.db("empty_db").listCollections().toArray();
        expect(colls.some((c) => c.name === "blank")).toBe(true);
        expect(await client.db("empty_db").collection("blank").countDocuments()).toBe(0);
      } finally {
        await client
          .db("empty_db")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });
  });

  describe("F3 — Multi-Process Hybrid Locking, SIGKILL, Lease Theft & Clock Skew", () => {
    let rsF3;
    let workDir;

    beforeAll(async () => {
      const f3Dir = path.join(rootDir, "f3");
      workDir = path.join(f3Dir, "work");
      await fs.mkdir(workDir, { recursive: true });
      rsF3 = await startReplicaSet({ rootDir: f3Dir, name: "rsF3" });
    });

    afterAll(async () => {
      await rsF3?.stop().catch(() => {});
    });

    it("3.1 Multi-process hybrid lock contention via lock-worker.mjs (FS + S3)", async () => {
      const lockFile = path.join(workDir, "f3-1.backup.lock");
      const lockKey = "f3-1/.locks/backup.lock.json";
      const s3 = {
        endpoint: s3Server.endpoint,
        bucket: "e2e-bucket",
        region: "us-east-1",
        accessKeyId: "test-access",
        secretAccessKey: "test-secret",
        forcePathStyle: true,
      };
      const storage = makeS3Storage(s3Server.endpoint);

      const workerA = await spawnLockWorker({
        workDir,
        mode: "hold",
        lockFile,
        lockKey,
        s3,
        staleAfterMs: 30_000,
        distributedTtlMs: 30_000,
        heartbeatIntervalMs: 500,
        holder: "worker-a",
      });
      await workerA.waitForEvent("acquired");

      const workerB = await spawnLockWorker({
        workDir,
        mode: "acquire-once",
        lockFile,
        lockKey,
        s3,
        staleAfterMs: 30_000,
        distributedTtlMs: 30_000,
        heartbeatIntervalMs: 500,
        holder: "worker-b",
      });
      const exitB = await workerB.waitForExit();
      expect(exitB.exitCode).toBe(2);
      expect(workerB.events.some((e) => e.event === "busy" && e.acquired === false)).toBe(true);

      await workerA.kill("SIGTERM");
      const exitA = await workerA.waitForExit();
      expect(exitA.exitCode).toBe(0);

      await expect(fs.stat(lockFile)).rejects.toThrow();
      expect(await storage.getJson(lockKey)).toBeNull();

      const workerC = await spawnLockWorker({
        workDir,
        mode: "acquire-once",
        lockFile,
        lockKey,
        s3,
        staleAfterMs: 30_000,
        distributedTtlMs: 30_000,
        heartbeatIntervalMs: 500,
        holder: "worker-c",
      });
      const exitC = await workerC.waitForExit();
      expect(exitC.exitCode).toBe(0);
      expect(workerC.events.some((e) => e.event === "acquired" && e.acquired === true)).toBe(true);
    });

    it("3.2 SIGKILL recovery + active S3 TTL guard + concurrent IfMatch CAS race", async () => {
      const lockFile = path.join(workDir, "f3-2.backup.lock");
      const lockKey = "f3-2/.locks/backup.lock.json";
      const s3 = {
        endpoint: s3Server.endpoint,
        bucket: "e2e-bucket",
        region: "us-east-1",
        accessKeyId: "test-access",
        secretAccessKey: "test-secret",
        forcePathStyle: true,
      };

      const workerA = await spawnLockWorker({
        workDir,
        mode: "hold",
        lockFile,
        lockKey,
        s3,
        staleAfterMs: 30_000,
        distributedTtlMs: 2_500,
        heartbeatIntervalMs: 300,
        holder: "worker-a",
      });
      await workerA.waitForEvent("acquired");
      await workerA.kill("SIGKILL");

      // (a) Dead local PID, but S3 TTL still active -> Worker B MUST fail with exit 2
      const workerB = await spawnLockWorker({
        workDir,
        mode: "acquire-once",
        lockFile,
        lockKey,
        s3,
        staleAfterMs: 30_000,
        distributedTtlMs: 2_500,
        heartbeatIntervalMs: 300,
        holder: "worker-b",
      });
      const exitB = await workerB.waitForExit();
      expect(exitB.exitCode).toBe(2);

      // (b) Wait past 2.5s TTL, then race Workers C & D concurrently
      await new Promise((r) => setTimeout(r, 2_800));
      const [workerC, workerD] = await Promise.all([
        spawnLockWorker({
          workDir,
          mode: "acquire-once",
          lockFile,
          lockKey,
          s3,
          staleAfterMs: 30_000,
          distributedTtlMs: 10_000,
          heartbeatIntervalMs: 500,
          holdMs: 600,
          holder: "worker-c",
        }),
        spawnLockWorker({
          workDir,
          mode: "acquire-once",
          lockFile,
          lockKey,
          s3,
          staleAfterMs: 30_000,
          distributedTtlMs: 10_000,
          heartbeatIntervalMs: 500,
          holdMs: 600,
          holder: "worker-d",
        }),
      ]);

      const [exitC, exitD] = await Promise.all([workerC.waitForExit(), workerD.waitForExit()]);
      const exits = [exitC.exitCode, exitD.exitCode].sort((a, b) => a - b);
      expect(exits).toEqual([0, 2]);
    });

    it("3.3 Lease theft onLost fencing aborts live mongodump during UploadPart (activeUploads === 0, zero manifest)", async () => {
      const encKey = parseKey(generateKey());
      const storage = makeS3Storage(s3Server.endpoint, { partSizeMb: 5, queueSize: 1 });
      const fencingWorkDir = path.join(workDir, "fencing-work");
      await fs.mkdir(fencingWorkDir, { recursive: true });
      const lockFile = path.join(fencingWorkDir, ".backup.lock");
      const lockKey = "f3-fencing/.locks/backup.lock.json";

      const client = new MongoClient(rsF3.uri);
      await client.connect();

      try {
        const docs = Array.from({ length: 110 }, (_, idx) => ({
          _id: `fence-${idx}`,
          blob: randomBytes(64 * 1024),
        }));
        await client.db("fencingdb").collection("items").insertMany(docs);

        s3Server.addFault({
          op: "UploadPart",
          keyPattern: /f3-fencing/,
          type: "delay",
          delayMs: 300,
          count: 10,
        });

        const controller = new AbortController();
        const release = await acquireHybridLock({
          file: lockFile,
          storage,
          lockKey,
          staleAfterMs: 30_000,
          distributedTtlMs: 800,
          heartbeatIntervalMs: 100,
          holder: "live-backup",
          logger,
          onLost: (err) => controller.abort(err),
        });
        expect(release).not.toBeNull();

        const cfg = makeS3Config({
          uri: rsF3.uri,
          s3Endpoint: s3Server.endpoint,
          workDir: fencingWorkDir,
          encKey,
          prefix: "f3-fencing",
          partSizeMb: 5,
          queueSize: 1,
          database: "fencingdb",
          oplog: false,
          pitrEnabled: false,
        });

        const backupPromise = runBackup({
          config: cfg,
          storage,
          logger,
          signal: controller.signal,
        });

        const sawUpload = await waitForCondition(
          () => s3Server.stats.activeUploads === 1 && s3Server.stats.uploadParts >= 1,
          15_000,
        );
        expect(sawUpload).toBe(true);

        // Steal the S3 distributed lock while multipart upload is actively in flight
        s3Server.putRaw(
          "e2e-bucket",
          lockKey,
          JSON.stringify({
            token: "thief-token",
            holder: "thief-host",
            acquiredAt: new Date().toISOString(),
            expiresAt: Date.now() + 60_000,
          }),
          { contentType: "application/json" },
        );

        await expect(backupPromise).rejects.toThrow();
        await release();

        const abortSettled = await waitForCondition(
          () => s3Server.stats.activeUploads === 0 && s3Server.stats.multipartAborts >= 1,
          5_000,
        );
        expect(abortSettled).toBe(true);
        expect(s3Server.stats.activeUploads).toBe(0);
        expect(s3Server.stats.multipartAborts).toBeGreaterThanOrEqual(1);

        // Zero manifest written & thief's lock remains intact
        const objs = [];
        for await (const o of storage.list("f3-fencing/fencingdb/")) {
          objs.push(o.key);
        }
        expect(objs.filter((k) => k.endsWith(".manifest.json"))).toEqual([]);
        const thiefLock = await storage.getJson(lockKey);
        expect(thiefLock?.data?.token).toBe("thief-token");

        // Verify no leftover temp mongodump config files in fencingWorkDir
        const leftOverFiles = await fs.readdir(fencingWorkDir);
        expect(leftOverFiles.filter((f) => f.endsWith(".yaml"))).toEqual([]);
      } finally {
        await client
          .db("fencingdb")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("3.4 Host ClockSkew distributed lock protection (F-TST-015 / F-SCL-010)", async () => {
      const workDirA = path.join(workDir, "skew-a");
      const workDirB = path.join(workDir, "skew-b");
      await fs.mkdir(workDirA, { recursive: true });
      await fs.mkdir(workDirB, { recursive: true });
      const lockKey = "f3-skew/.locks/backup.lock.json";
      const s3 = {
        endpoint: s3Server.endpoint,
        bucket: "e2e-bucket",
        region: "us-east-1",
        accessKeyId: "test-access",
        secretAccessKey: "test-secret",
        forcePathStyle: true,
      };

      const workerA = await spawnLockWorker({
        workDir: workDirA,
        mode: "hold",
        lockFile: path.join(workDirA, ".backup.lock"),
        lockKey,
        s3,
        staleAfterMs: 60_000,
        distributedTtlMs: 30_000,
        heartbeatIntervalMs: 1_000,
        holder: "host-a",
      });
      await workerA.waitForEvent("acquired");

      // Host B clock is +120s ahead of S3 server's HTTP Date header; MUST still respect active lease
      const workerB = await spawnLockWorker({
        workDir: workDirB,
        clockSkewMs: 120_000,
        mode: "acquire-once",
        lockFile: path.join(workDirB, ".backup.lock"),
        lockKey,
        s3,
        staleAfterMs: 60_000,
        distributedTtlMs: 30_000,
        heartbeatIntervalMs: 1_000,
        holder: "host-b-skewed",
      });
      const exitB = await workerB.waitForExit();
      await workerA.kill("SIGTERM").catch(() => {});
      await workerA.waitForExit().catch(() => {});

      expect(exitB.exitCode).toBe(2);
      expect(workerB.events.some((e) => e.event === "busy" && e.acquired === false)).toBe(true);
    });
  });

  describe("F4 — Catalog, GFS Retention, PITR Anchors, Sharded Cascade & WORM", () => {
    let rsF4;
    let workDir;

    beforeAll(async () => {
      const f4Dir = path.join(rootDir, "f4");
      workDir = path.join(f4Dir, "work");
      await fs.mkdir(workDir, { recursive: true });
      rsF4 = await startReplicaSet({ rootDir: f4Dir, name: "rsF4" });
    });

    afterAll(async () => {
      await rsF4?.stop().catch(() => {});
    });

    async function seedBackupRecord(
      storage,
      scope,
      date,
      { engine = "logical", oplog = true, oplogWindow = null, complete = true } = {},
    ) {
      const iso = date.toISOString();
      const day = iso.slice(0, 10);
      const stamp = `${iso.slice(0, 19).replace(/[-:]/g, "")}Z`;
      const ext = engine === "physical" ? ".physical.tar.gz" : ".archive.gz";
      const prefix = scope.prefix ? `${scope.prefix}/_cluster/` : "_cluster/";
      const key = `${prefix}${day}/${stamp}${ext}`;
      const body = Buffer.from(`archive-${stamp}`);
      s3Server.putRaw("e2e-bucket", key, body, { lastModified: date });
      if (complete) {
        const manifest = {
          version: MANIFEST_VERSION,
          engine,
          key,
          database: scope.database,
          createdAt: iso,
          completedAt: iso,
          durationMs: 100,
          size: body.length,
          sha256: createHash("sha256").update(body).digest("hex"),
          compression: "gzip",
          encryption: null,
          oplog,
          ...(oplogWindow ? { oplogWindow } : {}),
          excludedCollections: [],
          collections: {},
        };
        await writeManifest(storage, manifest, null);
      }
      return key;
    }

    it("4.1a Live GFS prune (keepLast/Daily/Weekly/Monthly, dryRun)", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      const now = Date.UTC(2026, 10, 25, 12, 0, 0); // 2026-11-25T12:00:00Z (Wed W48)

      const cfgA = makeS3Config({
        uri: rsF4.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        prefix: "f4-gfs",
        retention: { keepLast: 1, keepDaily: 2, keepWeekly: 2, keepMonthly: 2 },
      });
      const scopeA = scopeOf(cfgA);
      const kNow = await seedBackupRecord(storage, scopeA, new Date("2026-11-25T10:00:00Z"));
      const kDay2 = await seedBackupRecord(storage, scopeA, new Date("2026-11-24T10:00:00Z"));
      const kSameDayExtra = await seedBackupRecord(
        storage,
        scopeA,
        new Date("2026-11-24T08:00:00Z"),
      );
      const kWeek2 = await seedBackupRecord(storage, scopeA, new Date("2026-11-17T10:00:00Z")); // W47
      const kMonth2 = await seedBackupRecord(storage, scopeA, new Date("2026-10-15T10:00:00Z")); // Oct
      const kTooOld = await seedBackupRecord(storage, scopeA, new Date("2026-09-10T10:00:00Z")); // Sep
      const kOrphan = await seedBackupRecord(storage, scopeA, new Date("2026-11-25T09:00:00Z"), {
        complete: false,
      });

      const dryRes = await prune({ config: cfgA, storage, logger }, { dryRun: true, now });
      expect(dryRes.remove.map((r) => r.key).sort()).toEqual(
        [kSameDayExtra, kTooOld, kOrphan].sort(),
      );
      expect(await storage.head(kTooOld)).not.toBeNull();
      expect(await storage.head(kOrphan)).not.toBeNull();

      const realRes = await prune({ config: cfgA, storage, logger }, { dryRun: false, now });
      expect(realRes.keep.map((k) => k.key).sort()).toEqual([kNow, kDay2, kWeek2, kMonth2].sort());
      expect(await storage.head(kNow)).not.toBeNull();
      expect(await storage.head(kDay2)).not.toBeNull();
      expect(await storage.head(kWeek2)).not.toBeNull();
      expect(await storage.head(kMonth2)).not.toBeNull();
      expect(await storage.head(kSameDayExtra)).toBeNull();
      expect(await storage.head(kTooOld)).toBeNull();
      expect(await storage.head(kOrphan)).toBeNull();
    });

    it("4.1b Live GFS prune PITR base-anchor protection (F-SCL-001)", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      const now = Date.UTC(2026, 10, 25, 12, 0, 0);

      const cfgB = makeS3Config({
        uri: rsF4.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        prefix: "f4-anchor-prune",
        retention: { keepLast: 1, keepDaily: 0, keepWeekly: 0, keepMonthly: 0 },
      });
      const scopeB = scopeOf(cfgB);
      const tB1Sec = Math.floor(Date.UTC(2026, 10, 25, 10, 0, 0) / 1000);
      const tSliceEndSec = Math.floor(Date.UTC(2026, 10, 25, 11, 20, 0) / 1000);
      const b1Key = await seedBackupRecord(storage, scopeB, new Date("2026-11-25T10:00:00Z"), {
        engine: "logical",
        oplog: true,
        oplogWindow: { startTs: `${tB1Sec - 10}:1`, endTs: `${tB1Sec}:1` },
      });
      await seedBackupRecord(storage, scopeB, new Date("2026-11-25T11:00:00Z"), {
        engine: "physical",
        oplog: false,
      });

      const sliceKey = oplogSliceKey(scopeB, { t: tB1Sec, i: 1 }, { t: tSliceEndSec, i: 1 });
      const sliceBody = Buffer.from("slice-payload");
      s3Server.putRaw("e2e-bucket", sliceKey, sliceBody);
      await writeManifest(
        storage,
        {
          version: MANIFEST_VERSION,
          type: "oplog-slice",
          key: sliceKey,
          database: "*",
          createdAt: new Date("2026-11-25T11:20:00Z").toISOString(),
          completedAt: new Date("2026-11-25T11:20:00Z").toISOString(),
          durationMs: 10,
          size: sliceBody.length,
          sha256: createHash("sha256").update(sliceBody).digest("hex"),
          compression: "gzip",
          encryption: null,
          oplog: true,
          oplogWindow: { startTs: `${tB1Sec}:1`, endTs: `${tSliceEndSec}:1`, entries: 1 },
          excludedCollections: [],
          collections: {},
        },
        null,
      );

      await prune({ config: cfgB, storage, logger }, { dryRun: false, now });
      expect(await storage.head(b1Key)).not.toBeNull();
      expect(await storage.head(manifestKey(b1Key))).not.toBeNull();
    });

    it("4.1c Live GFS prune active lock orphan guard (F-SCL-002)", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      const now = Date.UTC(2026, 10, 25, 12, 0, 0);

      const cfgC = makeS3Config({
        uri: rsF4.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        prefix: "f4-orphan-lock",
        retention: { keepLast: 1, keepDaily: 0, keepWeekly: 0, keepMonthly: 0 },
      });
      const scopeC = scopeOf(cfgC);
      const activeOrphanKey = await seedBackupRecord(
        storage,
        scopeC,
        new Date("2026-11-25T10:00:00Z"),
        { complete: false },
      );
      s3Server.putRaw(
        "e2e-bucket",
        `${databasePrefix(scopeC)}.locks/backup.lock.json`,
        JSON.stringify({
          token: "active-backup-token",
          holder: "long-running-backup",
          acquiredAt: new Date(now - 2 * 3_600_000).toISOString(),
          expiresAt: now + 60_000,
        }),
        { contentType: "application/json" },
      );

      await prune({ config: cfgC, storage, logger }, { dryRun: false, now });
      expect(await storage.head(activeOrphanKey)).not.toBeNull();
    });

    it("4.2a Live RS PITR anchor protection on deleteBackup (--force vs no --force)", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      const client = new MongoClient(rsF4.uri);
      await client.connect();

      try {
        await client.db("pitr_del").collection("items").insertOne({ _id: "i1" });
        await waitForMajorityCatchup(rsF4.uri, await getLatestOplogTs(rsF4.uri));

        const cfgA = makeS3Config({
          uri: rsF4.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          prefix: "f4-del-live",
        });
        const b1 = await runBackup({ config: cfgA, storage, logger });

        await client.db("pitr_del").collection("items").insertOne({ _id: "i2" });
        await waitForMajorityCatchup(rsF4.uri, await getLatestOplogTs(rsF4.uri));
        const slice = await captureOplogSlice({ config: cfgA, storage, logger });
        expect(slice).not.toBeNull();

        await expect(
          deleteBackup({ config: cfgA, storage, logger }, { key: b1.key, force: false }),
        ).rejects.toThrow(/PITR|anchor|--force/i);
        expect(await storage.head(b1.key)).not.toBeNull();

        const forced = await deleteBackup(
          { config: cfgA, storage, logger },
          { key: b1.key, force: true },
        );
        expect(forced.deleted).toBe(true);
        expect(await storage.head(b1.key)).toBeNull();
        expect(await storage.head(manifestKey(b1.key))).toBeNull();
      } finally {
        await client
          .db("pitr_del")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });

    it("4.2b Unsharded non-oplog newer backup anchor check on deleteBackup (F-SCL-005)", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      const cfgB = makeS3Config({
        uri: rsF4.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        prefix: "f4-del-nonoplog",
      });
      const scopeB = scopeOf(cfgB);
      const anchorKey = await seedBackupRecord(storage, scopeB, new Date(100_000), {
        oplog: true,
        oplogWindow: { startTs: "100:1", endTs: "110:1" },
      });
      await seedBackupRecord(storage, scopeB, new Date(500_000), { oplog: false });
      const sKey = oplogSliceKey(scopeB, { t: 100, i: 1 }, { t: 200, i: 1 });
      const sBody = Buffer.from("slice-100-200");
      s3Server.putRaw("e2e-bucket", sKey, sBody);
      await writeManifest(
        storage,
        {
          version: MANIFEST_VERSION,
          type: "oplog-slice",
          key: sKey,
          database: "*",
          createdAt: new Date(200_000).toISOString(),
          completedAt: new Date(200_000).toISOString(),
          durationMs: 10,
          size: sBody.length,
          sha256: createHash("sha256").update(sBody).digest("hex"),
          compression: "gzip",
          encryption: null,
          oplog: true,
          oplogWindow: { startTs: "100:1", endTs: "200:1", entries: 1 },
          excludedCollections: [],
          collections: {},
        },
        null,
      );

      await expect(
        deleteBackup({ config: cfgB, storage, logger }, { key: anchorKey, force: false }),
      ).rejects.toThrow(/PITR|anchor|--force/i);
    });

    it("4.2c Sharded PITR anchor protection across configsvr + shards/* scopes (F-SCL-005)", async () => {
      const storage = makeS3Storage(s3Server.endpoint);
      const cfgC = makeS3Config({
        uri: rsF4.uri,
        s3Endpoint: s3Server.endpoint,
        workDir,
        prefix: "f4-del-sharded-anchor",
        sharding: { enabled: true },
      });
      const c1Key = "f4-del-sharded-anchor/_cluster/1970-01-01/19700101T000140Z.cluster";
      const c2Key = "f4-del-sharded-anchor/_cluster/1970-01-01/19700101T000050Z.cluster";
      for (const [cKey, tsSec, ct] of [
        [c1Key, 100, "100:1"],
        [c2Key, 50, "50:1"],
      ]) {
        const buf = Buffer.from(JSON.stringify({ clusterTime: ct }));
        s3Server.putRaw("e2e-bucket", cKey, buf);
        await writeManifest(
          storage,
          {
            version: MANIFEST_VERSION,
            type: "sharded-cluster-backup",
            engine: "logical",
            key: cKey,
            database: "*",
            createdAt: new Date(tsSec * 1000).toISOString(),
            completedAt: new Date(tsSec * 1000).toISOString(),
            durationMs: 10,
            size: buf.length,
            sha256: createHash("sha256").update(buf).digest("hex"),
            compression: "none",
            encryption: null,
            oplog: true,
            excludedCollections: [],
            collections: {},
            sharding: {
              clusterTime: ct,
              configsvr: { backupKey: `${cKey}.cfg`, manifestKey: `${cKey}.cfg.manifest.json` },
              shards: {},
            },
          },
          null,
        );
      }
      for (const scopePrefix of [
        "f4-del-sharded-anchor/configsvr",
        "f4-del-sharded-anchor/shards/shard01",
      ]) {
        const shSliceKey = oplogSliceKey(
          { prefix: scopePrefix, database: "*" },
          { t: 100, i: 1 },
          { t: 200, i: 1 },
        );
        const shSliceBuf = Buffer.from("sharded-slice");
        s3Server.putRaw("e2e-bucket", shSliceKey, shSliceBuf);
        await writeManifest(
          storage,
          {
            version: MANIFEST_VERSION,
            type: "oplog-slice",
            key: shSliceKey,
            database: "*",
            createdAt: new Date(200_000).toISOString(),
            completedAt: new Date(200_000).toISOString(),
            durationMs: 10,
            size: shSliceBuf.length,
            sha256: createHash("sha256").update(shSliceBuf).digest("hex"),
            compression: "gzip",
            encryption: null,
            oplog: true,
            oplogWindow: { startTs: "100:1", endTs: "200:1", entries: 1 },
            excludedCollections: [],
            collections: {},
          },
          null,
        );
      }

      await expect(
        deleteBackup({ config: cfgC, storage, logger }, { key: c1Key, force: false }),
      ).rejects.toThrow(/PITR|anchor|--force/i);
    });

    it("4.3 Sharded .cluster cascade delete & failure-ordering (F-SCL-007)", async () => {
      const shardedDir = path.join(rootDir, "f4-sharded");
      await fs.mkdir(shardedDir, { recursive: true });
      const cluster = await startShardedCluster(shardedDir);
      const storage = makeS3Storage(s3Server.endpoint);
      const encKey = parseKey(generateKey());
      const client = new MongoClient(cluster.mongosUri);

      try {
        await client.connect();
        await client.db("shop").collection("orders").insertOne({ _id: "ord-1", amount: 50 });

        const cfg = makeS3Config({
          uri: cluster.mongosUri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          encKey,
          prefix: "f4-sharded-cascade",
          sharding: { enabled: true },
        });

        // (a) Full sharded backup + cascade delete
        const res1 = await runShardedClusterBackup({ config: cfg, storage, logger });
        const clusterKey1 = res1.manifest.key;
        const m1 = await readManifest(storage, clusterKey1);
        const expectedDeleted = [
          clusterKey1,
          manifestKey(clusterKey1),
          m1.sharding.configsvr.backupKey,
          manifestKey(m1.sharding.configsvr.backupKey),
          ...(m1.sharding.configsvr.bridgingSliceKeys ?? []).flatMap((k) => [k, manifestKey(k)]),
          ...Object.values(m1.sharding.shards).flatMap((s) => [
            s.backupKey,
            manifestKey(s.backupKey),
            ...(s.bridgingSliceKeys ?? []).flatMap((k) => [k, manifestKey(k)]),
          ]),
        ];
        for (const k of expectedDeleted) {
          expect(await storage.head(k)).not.toBeNull();
        }

        const del1 = await deleteBackup(
          { config: cfg, storage, logger },
          { key: clusterKey1, force: true },
        );
        expect(del1.deleted).toBe(true);
        for (const k of expectedDeleted) {
          expect(await storage.head(k)).toBeNull();
        }

        // (b) Failure-ordering: shard01 delete fails -> .cluster.enc.manifest.json MUST survive for retry (F-SCL-007)
        await new Promise((r) => setTimeout(r, 1_100));
        const res2 = await runShardedClusterBackup({ config: cfg, storage, logger });
        const clusterKey2 = res2.manifest.key;
        s3Server.addFault({
          op: "DeleteObjects",
          method: "POST",
          keyPattern: /shard01/,
          type: "delete-key-error",
          code: "InternalError",
          message: "Simulated shard01 delete failure",
          once: true,
        });
        s3Server.addFault({
          op: "DeleteObject",
          method: "DELETE",
          keyPattern: /shard01/,
          type: "http-error",
          status: 500,
          code: "InternalError",
          message: "Simulated shard01 delete failure",
          count: 10,
        });

        await expect(
          deleteBackup({ config: cfg, storage, logger }, { key: clusterKey2, force: true }),
        ).rejects.toThrow();

        expect(await storage.head(manifestKey(clusterKey2))).not.toBeNull();

        s3Server.clearFaults();
        const retryDel = await deleteBackup(
          { config: cfg, storage, logger },
          { key: clusterKey2, force: true },
        );
        expect(retryDel.deleted).toBe(true);
        expect(await storage.head(clusterKey2)).toBeNull();
        expect(await storage.head(manifestKey(clusterKey2))).toBeNull();
      } finally {
        await client.close().catch(() => {});
        await cluster.stop().catch(() => {});
      }
    });

    it("4.4 S3 WORM Object Lock (COMPLIANCE) blocks delete --force while prune warns & continues (F-SCL-006)", async () => {
      const objectLock = { enabled: true, mode: "COMPLIANCE", retentionDays: 30 };
      const storage = makeS3Storage(s3Server.endpoint, { objectLock });
      const client = new MongoClient(rsF4.uri);
      await client.connect();

      try {
        await client.db("lockeddb").collection("records").insertOne({ _id: "w1" });
        const cfg = makeS3Config({
          uri: rsF4.uri,
          s3Endpoint: s3Server.endpoint,
          workDir,
          prefix: "f4-objlock",
          database: "lockeddb",
          oplog: false,
          pitrEnabled: false,
          objectLock,
          retention: { keepLast: 1, keepDaily: 0, keepWeekly: 0, keepMonthly: 0 },
        });

        const b1 = await runBackup({ config: cfg, storage, logger });
        await new Promise((r) => setTimeout(r, 1_100));
        await runBackup({ config: cfg, storage, logger });

        // Verify Object Lock applied to archive + manifest, but NEVER to mutable .locks/ or state.json
        const rawArchive = s3Server.getRaw("e2e-bucket", b1.key);
        const rawManifest = s3Server.getRaw("e2e-bucket", manifestKey(b1.key));
        expect(rawArchive?.objectLock?.mode).toBe("COMPLIANCE");
        expect(rawArchive?.objectLock?.retainUntilDate).toBeInstanceOf(Date);
        expect(rawManifest?.objectLock?.mode).toBe("COMPLIANCE");

        const release = await acquireHybridLock({
          file: path.join(workDir, "objlock.lock"),
          storage,
          lockKey: `${databasePrefix(scopeOf(cfg))}.locks/backup.lock.json`,
          staleAfterMs: 30_000,
          distributedTtlMs: 30_000,
          heartbeatIntervalMs: 5_000,
          logger,
        });
        expect(release).not.toBeNull();
        const rawLock = s3Server.getRaw(
          "e2e-bucket",
          `${databasePrefix(scopeOf(cfg))}.locks/backup.lock.json`,
        );
        expect(rawLock?.objectLock).toBeUndefined();
        await release();

        await storage.putJson("f4-objlock/_cluster/_pitr/state.json", { lastTs: "100:1" });
        const rawState = s3Server.getRaw("e2e-bucket", "f4-objlock/_cluster/_pitr/state.json");
        expect(rawState?.objectLock).toBeUndefined();

        // deleteBackup --force MUST fail with WORM protection error
        await expect(
          deleteBackup({ config: cfg, storage, logger }, { key: b1.key, force: true }),
        ).rejects.toThrow(/WORM|Object Lock/i);

        // prune MUST warn and complete without throwing (F-SCL-006)
        const warnMessages = [];
        const spyLogger = {
          fatal: () => {},
          error: () => {},
          warn: (...args) => warnMessages.push(args),
          info: () => {},
          debug: () => {},
          trace: () => {},
          child: () => spyLogger,
        };
        await expect(
          prune({ config: cfg, storage, logger: spyLogger }, { dryRun: false }),
        ).resolves.toBeDefined();
        expect(warnMessages.length).toBeGreaterThanOrEqual(1);
      } finally {
        await client
          .db("lockeddb")
          .dropDatabase()
          .catch(() => {});
        await client.close().catch(() => {});
      }
    });
  });
});
