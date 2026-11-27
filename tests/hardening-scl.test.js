import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { describe, expect, it, vi } from "vitest";
import { linkedController, settleAll } from "../src/abort.js";
import {
  archiveKey,
  databasePrefix,
  listBackups,
  manifestKey,
  parseCreatedAt,
  readManifest,
  writeManifest,
} from "../src/catalog.js";
import {
  CHUNKED_ALGORITHM,
  ChunkedAeadDecryptStream,
  ChunkedAeadEncryptStream,
  createDecryptor,
  generateKey,
  keyId,
  parseKey,
  signManifest,
  verifyManifest,
} from "../src/crypto.js";
import { DigestStream } from "../src/digest.js";
import {
  acquireDistributedLock,
  acquireHybridLock,
  acquireLock,
  isStale,
  readLock,
} from "../src/lock.js";
import { oplogSliceKey, pitrPrefix } from "../src/oplog.js";
import { deleteBackup, planRetention, prune, pruneOplogSlices } from "../src/retention.js";
import { FilesystemStorageClient, S3StorageClient } from "../src/storage.js";

function makeSilentLogger() {
  return {
    child() {
      return this;
    },
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  };
}

function makeBaseConfig(overrides = {}) {
  return {
    mongo: { database: "*" },
    storage: { prefix: "mv" },
    schedule: { timezone: "UTC" },
    retention: { keepLast: 1 },
    ...overrides,
  };
}

function makeManifest(overrides = {}) {
  return {
    version: 2,
    key: "mv/_cluster/1970-01-01/19700101T000000Z.archive.gz",
    database: "*",
    createdAt: "1970-01-01T00:00:00.000Z",
    completedAt: "1970-01-01T00:00:05.000Z",
    durationMs: 5000,
    size: 128,
    sha256: "abc123",
    compression: "gzip",
    encryption: null,
    oplog: true,
    excludedCollections: [],
    collections: { items: 10 },
    ...overrides,
  };
}

async function startHttpServer(handler) {
  const server = http.createServer(handler);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  const endpoint = `http://127.0.0.1:${addr.port}`;
  return {
    endpoint,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

describe("M2 storage-crypto-lock hardening regression & contract suite", () => {
  it("[F-SCL-001] prune() protects unsharded PITR anchor backup even when newer physical/non-oplog backup exists and uses per-component cutoffs for sharded clusters", async () => {
    // 1. Unsharded: B1 (logical, t=1000) + S1 (1000..1500) + B2 (physical, oplog: false, t=1200)
    const dir1 = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-001-unsharded-"));
    try {
      const storage = new FilesystemStorageClient(dir1);
      const logger = makeSilentLogger();
      const scope = { prefix: "mv", database: "*" };
      const config = makeBaseConfig({ retention: { keepLast: 1 } });

      const b1 = "mv/_cluster/1970-01-01/19700101T001640Z.archive.gz";
      const b2 = "mv/_cluster/1970-01-01/19700101T002000Z.physical.tar.gz";
      const s1 = oplogSliceKey(scope, { t: 1000, i: 5 }, { t: 1500, i: 1 });

      await storage.upload(b1, Buffer.from("logical-b1"));
      await writeManifest(
        storage,
        makeManifest({
          key: b1,
          engine: "logical",
          createdAt: "1970-01-01T00:16:40.000Z",
          completedAt: "1970-01-01T00:16:45.000Z",
          oplog: true,
          oplogWindow: { startTs: "1000:1", endTs: "1000:5" },
        }),
      );

      await storage.upload(s1, Buffer.from("slice-1"));
      await writeManifest(
        storage,
        makeManifest({
          key: s1,
          type: "oplog-slice",
          oplog: true,
          oplogWindow: { startTs: "1000:5", endTs: "1500:1" },
        }),
      );

      await storage.upload(b2, Buffer.from("physical-b2"));
      await writeManifest(
        storage,
        makeManifest({
          key: b2,
          engine: "physical",
          createdAt: "1970-01-01T00:20:00.000Z",
          completedAt: "1970-01-01T00:20:05.000Z",
          oplog: false,
          oplogWindow: null,
        }),
      );

      const res = await prune({ config, storage, logger });
      expect(res.keep.map((b) => b.key)).toContain(b1);
      expect(res.keep.map((b) => b.key)).toContain(b2);
      expect(await storage.head(b1)).not.toBeNull();
      expect(await storage.head(s1)).not.toBeNull();
    } finally {
      await fs.rm(dir1, { recursive: true, force: true });
    }

    // 2. Sharded: .cluster (clusterTime 2000:1, no oplogWindow) + configsvr (1750:1..1750:5) + shard01 (1800:1..1800:5) + configsvr slice (1750:5..1900:1)
    const dir2 = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-001-sharded-"));
    try {
      const storage = new FilesystemStorageClient(dir2);
      const logger = makeSilentLogger();
      const config = makeBaseConfig({
        sharding: { enabled: true },
        retention: { keepLast: 1 },
      });

      const clusterKey = "mv/_cluster/1970-01-01/19700101T003320Z.cluster";
      const cfgKey = "mv/configsvr/_cluster/1970-01-01/19700101T002910Z.archive.gz";
      const s1Key = "mv/shards/shard01/_cluster/1970-01-01/19700101T003000Z.archive.gz";
      const cfgSlice = oplogSliceKey(
        { prefix: "mv/configsvr", database: "*" },
        { t: 1750, i: 5 },
        { t: 1900, i: 1 },
      );

      await storage.upload(cfgKey, Buffer.from("cfg"));
      await writeManifest(
        storage,
        makeManifest({
          key: cfgKey,
          oplog: true,
          oplogWindow: { startTs: "1750:1", endTs: "1750:5" },
        }),
      );

      await storage.upload(s1Key, Buffer.from("shard01"));
      await writeManifest(
        storage,
        makeManifest({
          key: s1Key,
          oplog: true,
          oplogWindow: { startTs: "1800:1", endTs: "1800:5" },
        }),
      );

      await storage.upload(clusterKey, Buffer.from("cluster"));
      await writeManifest(
        storage,
        makeManifest({
          type: "sharded-cluster-backup",
          key: clusterKey,
          createdAt: "1970-01-01T00:33:20.000Z",
          completedAt: "1970-01-01T00:33:25.000Z",
          oplog: true,
          oplogWindow: null,
          sharding: {
            clusterTime: "2000:1",
            configsvr: { backupKey: cfgKey, manifestKey: manifestKey(cfgKey) },
            shards: {
              shard01: {
                shardId: "shard01",
                host: "rs1",
                backupKey: s1Key,
                manifestKey: manifestKey(s1Key),
                status: "success",
              },
            },
          },
        }),
      );

      await storage.upload(cfgSlice, Buffer.from("cfg-slice"));
      await writeManifest(
        storage,
        makeManifest({
          key: cfgSlice,
          type: "oplog-slice",
          oplog: true,
          oplogWindow: { startTs: "1750:5", endTs: "1900:1" },
        }),
      );

      const res = await prune({ config, storage, logger });
      expect(res.remove).toEqual([]);
      expect(await storage.head(cfgSlice)).not.toBeNull();
    } finally {
      await fs.rm(dir2, { recursive: true, force: true });
    }
  });

  it("[F-SCL-002] prune() preserves >1h incomplete archives under active backup lock and incomplete slices under active oplog lock or missing lastModified", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-002-"));
    try {
      const storage = new FilesystemStorageClient(dir);
      const logger = makeSilentLogger();
      const scope = { prefix: "mv", database: "prod" };
      const config = makeBaseConfig({
        mongo: { database: "prod" },
        retention: { keepLast: 1 },
      });

      const realNow = Date.now();
      const oldDate = new Date(realNow - 7_200_000);
      const activeDate = new Date(realNow - 5_400_000); // 90 min ago (> 1h ORPHAN_AGE_MS)
      const fmtStamp = (d) => `${d.toISOString().slice(0, 19).replace(/[-:]/g, "")}Z`;
      const bOld = `${databasePrefix(scope)}${oldDate.toISOString().slice(0, 10)}/${fmtStamp(oldDate)}.archive.gz`;
      const bActive = `${databasePrefix(scope)}${activeDate.toISOString().slice(0, 10)}/${fmtStamp(activeDate)}.archive.gz`;

      await storage.upload(bOld, Buffer.from("old-complete"));
      await writeManifest(storage, makeManifest({ key: bOld, database: "prod" }));
      await storage.upload(bActive, Buffer.from("in-progress-upload")); // no manifest

      const backupLockKey = `${databasePrefix(scope)}.locks/backup.lock.json`;
      const backupToken = await storage.acquireLock(backupLockKey, {
        holder: "active-backup-worker",
        ttlMs: 90_000,
      });
      expect(backupToken).toBeTruthy();

      const incompleteSlice = oplogSliceKey(scope, { t: 1000, i: 1 }, { t: 1010, i: 1 });
      await storage.upload(incompleteSlice, Buffer.from("partial-slice")); // no manifest

      const oplogLockKey = `${pitrPrefix(scope)}.locks/oplog.lock.json`;
      const oplogToken = await storage.acquireLock(oplogLockKey, {
        holder: "active-oplog-archiver",
        ttlMs: 90_000,
      });
      expect(oplogToken).toBeTruthy();

      // Wrapper omitting lastModified on incompleteSlice to test missing lastModified safety too
      const storageNoLastMod = {
        bucket: storage.bucket,
        head: (k) => storage.head(k),
        getJson: (k) => storage.getJson(k),
        remove: (k) => storage.remove(k),
        removeMany: (ks) => storage.removeMany(ks),
        async *list(prefix) {
          for await (const obj of storage.list(prefix)) {
            if (obj.key === incompleteSlice) {
              yield { key: obj.key, size: obj.size, lastModified: undefined };
            } else {
              yield obj;
            }
          }
        },
      };

      const resLocked = await prune(
        { config, storage: storageNoLastMod, logger },
        { now: realNow },
      );
      expect(resLocked.remove.map((r) => r.key)).not.toContain(bActive);
      expect(resLocked.remove.map((r) => r.key)).not.toContain(incompleteSlice);
      expect(await storage.head(bActive)).not.toBeNull();
      expect(await storage.head(incompleteSlice)).not.toBeNull();

      // Release oplog lock; missing lastModified alone must still protect incomplete slice from immediate deletion
      await storage.releaseLock(oplogLockKey, oplogToken);
      const resNoMod = await prune({ config, storage: storageNoLastMod, logger }, { now: realNow });
      expect(resNoMod.remove.map((r) => r.key)).not.toContain(incompleteSlice);
      expect(await storage.head(incompleteSlice)).not.toBeNull();

      // Release backup lock; now >1h incomplete archive bActive should be cleaned up
      await storage.releaseLock(backupLockKey, backupToken);
      const resUnlocked = await prune({ config, storage, logger }, { now: realNow });
      expect(resUnlocked.remove.map((r) => r.key)).toContain(bActive);
      expect(await storage.head(bActive)).toBeNull();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  it("[F-SCL-003] verifyManifest() fails closed when signature and manifestHmac are stripped from encrypted or unencrypted manifests", () => {
    const masterKey = parseKey(generateKey());

    const encryptedManifest = makeManifest({
      key: "mv/prod/2026-01-01/20260101T000000Z.archive.gz.enc",
      database: "prod",
      encryption: {
        algorithm: CHUNKED_ALGORITHM,
        keyId: keyId(masterKey),
        wrappedKey: "wrapped",
        iv: Buffer.alloc(12).toString("base64"),
        chunkSize: 65536,
      },
    });
    const encHmac = signManifest(masterKey, encryptedManifest);
    encryptedManifest.encryption.manifestHmac = encHmac;
    encryptedManifest.signature = {
      algorithm: "HMAC-SHA256",
      keyId: keyId(masterKey),
      hmac: encHmac,
    };
    expect(verifyManifest([masterKey], encryptedManifest)).toBe(true);

    const strippedEncrypted = structuredClone(encryptedManifest);
    delete strippedEncrypted.signature;
    delete strippedEncrypted.encryption.manifestHmac;

    expect(() => verifyManifest([masterKey], strippedEncrypted)).toThrow();
    expect(() => verifyManifest([], strippedEncrypted)).toThrow();

    const unencryptedManifest = makeManifest({
      key: "mv/prod/2026-01-01/20260101T000000Z.archive.gz",
      database: "prod",
      encryption: null,
    });
    unencryptedManifest.signature = {
      algorithm: "HMAC-SHA256",
      keyId: keyId(masterKey),
      hmac: signManifest(masterKey, unencryptedManifest),
    };
    expect(verifyManifest([masterKey], unencryptedManifest)).toBe(true);

    const strippedUnencrypted = structuredClone(unencryptedManifest);
    delete strippedUnencrypted.signature;
    expect(() => verifyManifest([masterKey], strippedUnencrypted)).toThrow();
  });

  it("[F-SCL-004] FS lock refreshLock, stale takeover, and delayed release() TOCTOU races do NOT overwrite or evict a new owner", async () => {
    // 1. FilesystemStorageClient.refreshLock delayed after expiry
    const dirFs = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-004-fs-"));
    const origRename = fs.rename.bind(fs);
    const renameSpy = vi.spyOn(fs, "rename");

    try {
      const storageA = new FilesystemStorageClient(dirFs);
      const storageB = new FilesystemStorageClient(dirFs);

      const tokenA = await storageA.acquireLock("op.lock", { holder: "A", ttlMs: 5_000 });
      expect(tokenA).toBeTruthy();

      const lockPath = path.join(dirFs, "op.lock");
      const rec = JSON.parse(await fs.readFile(lockPath, "utf8"));
      rec.expiresAt = Date.now() - 5_000;
      await fs.writeFile(lockPath, JSON.stringify(rec, null, 2));

      let interceptCount = 0;
      let paused = false;
      renameSpy.mockImplementation(async (src, dst) => {
        interceptCount++;
        if (!paused && String(src).includes(".refresh.")) {
          paused = true;
          await new Promise((r) => setTimeout(r, 60));
        }
        return origRename(src, dst);
      });

      const refreshP = storageA.refreshLock("op.lock", tokenA, 10_000);
      await new Promise((r) => setTimeout(r, 15));
      const acquireBP = storageB.acquireLock("op.lock", { holder: "B", ttlMs: 10_000 });
      const [refreshedA, tokenB] = await Promise.all([refreshP, acquireBP]);

      expect(interceptCount).toBeGreaterThan(0);
      expect(refreshedA && tokenB !== null).toBe(false);
      const finalRecord = (await storageB.getJson("op.lock"))?.data;
      if (tokenB) {
        expect(finalRecord?.token).toBe(tokenB);
      } else {
        expect(refreshedA).toBe(true);
        expect(finalRecord?.token).toBe(tokenA);
      }
    } finally {
      renameSpy.mockRestore();
      await fs.rm(dirFs, { recursive: true, force: true });
    }

    // 2. Local acquireLock stale-takeover race & delayed release() race
    const dirLocal = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-004-local-"));
    const lockFile = path.join(dirLocal, ".backup.lock");
    const origRm = fs.rm.bind(fs);
    const renameSpy2 = vi.spyOn(fs, "rename");
    const rmSpy = vi.spyOn(fs, "rm");

    try {
      await fs.writeFile(
        lockFile,
        JSON.stringify({
          pid: 999_999,
          token: "dead",
          acquiredAt: new Date(Date.now() - 60_000).toISOString(),
        }),
      );

      let staleRenameIntercepts = 0;
      let delayedOnce = false;
      renameSpy2.mockImplementation(async (src, dst) => {
        if (String(dst).includes(".stale.")) {
          staleRenameIntercepts++;
          if (!delayedOnce) {
            delayedOnce = true;
            await new Promise((r) => setTimeout(r, 60));
          }
        }
        return origRename(src, dst);
      });

      const pB = acquireLock(lockFile, { staleAfterMs: 60_000 });
      await new Promise((r) => setTimeout(r, 15));
      const pC = acquireLock(lockFile, { staleAfterMs: 60_000 });
      const [relB, relC] = await Promise.all([pB, pC]);

      expect(staleRenameIntercepts).toBeGreaterThan(0);
      expect([relB, relC].filter(Boolean)).toHaveLength(1);
      await (relB ?? relC)();

      // Delayed release() race
      renameSpy2.mockRestore();
      const relA = await acquireLock(lockFile, {
        staleAfterMs: 40,
        heartbeatIntervalMs: 1_000,
      });
      expect(relA).toBeTypeOf("function");
      const past = new Date(Date.now() - 120_000);
      await fs.utimes(lockFile, past, past);

      let rmDelayed = false;
      let rmIntercepts = 0;
      rmSpy.mockImplementation(async (target, opts) => {
        if (target === lockFile) {
          rmIntercepts++;
          if (!rmDelayed) {
            rmDelayed = true;
            await new Promise((r) => setTimeout(r, 60));
          }
        }
        return origRm(target, opts);
      });

      const relAPromise = relA();
      await new Promise((r) => setTimeout(r, 15));
      const relB2 = await acquireLock(lockFile, {
        staleAfterMs: 40,
        heartbeatIntervalMs: 1_000,
      });
      await relAPromise;

      expect(rmIntercepts).toBeGreaterThan(0);
      expect(relB2).toBeTypeOf("function");
      expect(await readLock(lockFile)).not.toBeNull();
      const relC2 = await acquireLock(lockFile, { staleAfterMs: 40 });
      expect(relC2).toBeNull();
      await relB2();
    } finally {
      renameSpy2.mockRestore();
      rmSpy.mockRestore();
      await fs.rm(dirLocal, { recursive: true, force: true });
    }
  });

  it("[F-SCL-004-ENOENT-Race] prevents dual lock acquisition when isStale hits ENOENT during concurrent acquireLock()", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-004-enoent-"));
    const lockFile = path.join(dir, ".backup.lock");
    const mutexDir = `${lockFile}.lck`;

    const origRm = fs.rm.bind(fs);
    const origMkdir = fs.mkdir.bind(fs);
    const origStat = fs.stat.bind(fs);
    const origLink = fs.link.bind(fs);
    const origRename = fs.rename.bind(fs);

    try {
      // Scenario 1: P1 releases lock right as P2 waits on withFileLockMutex; P3 arrives concurrently
      const rel1 = await acquireLock(lockFile, { staleAfterMs: 60_000 });
      expect(rel1).toBeTypeOf("function");

      let notifyP1InRm;
      const p1InRmPromise = new Promise((r) => {
        notifyP1InRm = r;
      });
      let notifyP2WaitingMutex;
      const p2WaitingMutexPromise = new Promise((r) => {
        notifyP2WaitingMutex = r;
      });

      let p1RmPaused = false;
      const rmSpy = vi.spyOn(fs, "rm").mockImplementation(async (target, opts) => {
        if (!p1RmPaused && target === lockFile) {
          p1RmPaused = true;
          notifyP1InRm();
          await p2WaitingMutexPromise;
        }
        return origRm(target, opts);
      });

      let p2MutexSeen = false;
      const mkdirSpy = vi.spyOn(fs, "mkdir").mockImplementation(async (target, opts) => {
        try {
          return await origMkdir(target, opts);
        } catch (err) {
          if (!p2MutexSeen && target === mutexDir && err?.code === "EEXIST") {
            p2MutexSeen = true;
            notifyP2WaitingMutex();
          }
          throw err;
        }
      });

      let p3Promise = null;
      let statEnoentHooked = false;
      const statSpy = vi.spyOn(fs, "stat").mockImplementation(async (target, opts) => {
        try {
          return await origStat(target, opts);
        } catch (err) {
          if (!statEnoentHooked && target === lockFile && err?.code === "ENOENT") {
            statEnoentHooked = true;
            p3Promise = acquireLock(lockFile, { staleAfterMs: 60_000 });
            await Promise.race([p3Promise, new Promise((r) => setTimeout(r, 50))]);
          }
          throw err;
        }
      });

      const rel1Promise = rel1();
      await p1InRmPromise;

      const p2Promise = acquireLock(lockFile, { staleAfterMs: 60_000 });
      await rel1Promise;

      const rel2 = await p2Promise;
      if (!p3Promise) {
        p3Promise = acquireLock(lockFile, { staleAfterMs: 60_000 });
      }
      const rel3 = await p3Promise;

      rmSpy.mockRestore();
      mkdirSpy.mockRestore();
      statSpy.mockRestore();

      expect([rel2, rel3].filter(Boolean)).toHaveLength(1);
      await rel2?.();
      await rel3?.();

      // Scenario 2: External ENOENT between writeLockExclusiveLocal(EEXIST) and isStale() must not tombstone-rename a concurrent P3 lock
      await fs.writeFile(
        lockFile,
        JSON.stringify({
          pid: 999_999,
          token: "external-dead",
          acquiredAt: new Date().toISOString(),
        }),
      );

      let removedOnEexist = false;
      let p3Promise2 = null;
      let statEnoentHooked2 = false;
      let staleRenameCount = 0;

      const linkSpy2 = vi.spyOn(fs, "link").mockImplementation(async (existingPath, newPath) => {
        try {
          return await origLink(existingPath, newPath);
        } catch (err) {
          if (!removedOnEexist && newPath === lockFile && err?.code === "EEXIST") {
            removedOnEexist = true;
            await origRm(lockFile, { force: true });
          }
          throw err;
        }
      });

      const statSpy2 = vi.spyOn(fs, "stat").mockImplementation(async (target, opts) => {
        try {
          return await origStat(target, opts);
        } catch (err) {
          if (!statEnoentHooked2 && target === lockFile && err?.code === "ENOENT") {
            statEnoentHooked2 = true;
            p3Promise2 = acquireLock(lockFile, { staleAfterMs: 60_000 });
            await Promise.race([p3Promise2, new Promise((r) => setTimeout(r, 50))]);
          }
          throw err;
        }
      });

      const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (src, dst) => {
        if (src === lockFile && String(dst).includes(".stale.")) {
          staleRenameCount++;
        }
        return origRename(src, dst);
      });

      const rel2b = await acquireLock(lockFile, { staleAfterMs: 60_000 });
      const rel3b = await p3Promise2;

      linkSpy2.mockRestore();
      statSpy2.mockRestore();
      renameSpy.mockRestore();

      expect(staleRenameCount).toBe(0);
      expect([rel2b, rel3b].filter(Boolean)).toHaveLength(1);
      await rel2b?.();
      await rel3b?.();
    } finally {
      vi.restoreAllMocks();
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  it("[F-SCL-005] deleteBackup() blocks deletion when PITR slices exist only under sharded paths and protects older anchor when newer backup is oplog:false", async () => {
    // 1. Sharded PITR scopes under shards/rs1/_cluster/_pitr/
    const dir1 = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-005-sharded-"));
    try {
      const storage = new FilesystemStorageClient(dir1);
      const logger = makeSilentLogger();
      const config = makeBaseConfig({ sharding: { enabled: true } });

      const clusterKey = "mv/_cluster/2026-01-01/20260101T000000Z.cluster";
      const cfgKey = "mv/configsvr/_cluster/2026-01-01/20260101T000000Z.archive.gz";
      const s1Key = "mv/shards/rs1/_cluster/2026-01-01/20260101T000000Z.archive.gz";
      const rs1Slice = oplogSliceKey(
        { prefix: "mv/shards/rs1", database: "*" },
        { t: 100, i: 1 },
        { t: 200, i: 1 },
      );

      await storage.upload(cfgKey, Buffer.from("cfg"));
      await writeManifest(
        storage,
        makeManifest({
          key: cfgKey,
          oplog: true,
          oplogWindow: { startTs: "95:1", endTs: "100:1" },
        }),
      );
      await storage.upload(s1Key, Buffer.from("s1"));
      await writeManifest(
        storage,
        makeManifest({ key: s1Key, oplog: true, oplogWindow: { startTs: "95:1", endTs: "100:1" } }),
      );
      await storage.upload(clusterKey, Buffer.from("cluster"));
      await writeManifest(
        storage,
        makeManifest({
          type: "sharded-cluster-backup",
          key: clusterKey,
          oplog: true,
          sharding: {
            clusterTime: "100:1",
            configsvr: { backupKey: cfgKey, manifestKey: manifestKey(cfgKey) },
            shards: {
              rs1: {
                shardId: "rs1",
                host: "rs1",
                backupKey: s1Key,
                manifestKey: manifestKey(s1Key),
                status: "success",
              },
            },
          },
        }),
      );
      await storage.upload(rs1Slice, Buffer.from("rs1-slice"));
      await writeManifest(
        storage,
        makeManifest({
          key: rs1Slice,
          type: "oplog-slice",
          oplogWindow: { startTs: "100:1", endTs: "200:1" },
        }),
      );

      await expect(deleteBackup({ config, storage, logger }, { key: clusterKey })).rejects.toThrow(
        /anchoring PITR/i,
      );
    } finally {
      await fs.rm(dir1, { recursive: true, force: true });
    }

    // 2. B1 (100:1..110:1) + S1 (100:1..200:1) + B2 (oplog: false, t=500s)
    const dir2 = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-005-anchor-"));
    try {
      const storage = new FilesystemStorageClient(dir2);
      const logger = makeSilentLogger();
      const scope = { prefix: "mv", database: "prod" };
      const config = makeBaseConfig({ mongo: { database: "prod" } });

      const b1 = "mv/prod/1970-01-01/19700101T000140Z.archive.gz";
      const b2 = "mv/prod/1970-01-01/19700101T000820Z.archive.gz";
      const s1 = oplogSliceKey(scope, { t: 100, i: 1 }, { t: 200, i: 1 });

      await storage.upload(b1, Buffer.from("b1"));
      await writeManifest(
        storage,
        makeManifest({
          key: b1,
          database: "prod",
          createdAt: "1970-01-01T00:01:40.000Z",
          completedAt: "1970-01-01T00:01:50.000Z",
          oplog: true,
          oplogWindow: { startTs: "100:1", endTs: "110:1" },
        }),
      );

      await storage.upload(s1, Buffer.from("s1"));
      await writeManifest(
        storage,
        makeManifest({
          key: s1,
          database: "prod",
          type: "oplog-slice",
          oplog: true,
          oplogWindow: { startTs: "100:1", endTs: "200:1" },
        }),
      );

      await storage.upload(b2, Buffer.from("b2"));
      await writeManifest(
        storage,
        makeManifest({
          key: b2,
          database: "prod",
          createdAt: "1970-01-01T00:08:20.000Z",
          completedAt: "1970-01-01T00:08:30.000Z",
          oplog: false,
          oplogWindow: null,
        }),
      );

      await expect(deleteBackup({ config, storage, logger }, { key: b1 })).rejects.toThrow(
        /anchoring PITR/i,
      );
      const delB2 = await deleteBackup({ config, storage, logger }, { key: b2 });
      expect(delB2.deleted).toBe(true);
    } finally {
      await fs.rm(dir2, { recursive: true, force: true });
    }
  });

  it("[F-SCL-006] handles real S3 XML AccessDenied on WORM bucket cleanly in prune()/deleteBackup() and fails on mixed [AccessDenied, InternalError] batch", async () => {
    const oldKey = "mv/prod/2026-03-01/20260301T020000Z.archive.gz";
    const newKey = "mv/prod/2026-03-10/20260310T020000Z.archive.gz";
    let deleteMode = "worm-only";

    const srv = await startHttpServer(async (req, res) => {
      const url = new URL(req.url, "http://127.0.0.1");
      if (req.method === "GET" && url.searchParams.has("list-type")) {
        res.writeHead(200, { "Content-Type": "application/xml" });
        res.end(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>worm-bkt</Name>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>${oldKey}</Key><Size>5000</Size><LastModified>2026-03-01T02:00:00Z</LastModified></Contents>
  <Contents><Key>${manifestKey(oldKey)}</Key><Size>100</Size><LastModified>2026-03-01T02:00:00Z</LastModified></Contents>
  <Contents><Key>${newKey}</Key><Size>5000</Size><LastModified>2026-03-10T02:00:00Z</LastModified></Contents>
  <Contents><Key>${manifestKey(newKey)}</Key><Size>100</Size><LastModified>2026-03-10T02:00:00Z</LastModified></Contents>
</ListBucketResult>`);
        return;
      }
      if (req.method === "HEAD") {
        res.writeHead(200, { "Content-Length": "5000", ETag: '"e1"' });
        res.end();
        return;
      }
      if (req.method === "GET") {
        res.writeHead(404, { "Content-Type": "application/xml" });
        res.end("<Error><Code>NoSuchKey</Code></Error>");
        return;
      }
      if (req.method === "POST" && url.searchParams.has("delete")) {
        await readRequestBody(req);
        res.writeHead(200, { "Content-Type": "application/xml" });
        if (deleteMode === "worm-only") {
          res.end(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Error>
    <Key>${oldKey}</Key>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
  </Error>
</DeleteResult>`);
        } else {
          res.end(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Error>
    <Key>${oldKey}</Key>
    <Code>AccessDenied</Code>
    <Message>Object Lock WORM</Message>
  </Error>
  <Error>
    <Key>${manifestKey(oldKey)}</Key>
    <Code>InternalError</Code>
    <Message>We encountered an internal error</Message>
  </Error>
</DeleteResult>`);
        }
        return;
      }
      res.writeHead(404);
      res.end();
    });

    try {
      const storage = new S3StorageClient({
        region: "us-east-1",
        bucket: "worm-bkt",
        endpoint: srv.endpoint,
        forcePathStyle: true,
        accessKeyId: "test",
        secretAccessKey: "test",
        objectLock: { enabled: true, mode: "COMPLIANCE", retentionDays: 30 },
      });

      const config = makeBaseConfig({
        mongo: { database: "prod" },
        storage: { prefix: "mv", objectLock: { enabled: true } },
        retention: { keepLast: 1 },
      });
      const logger = makeSilentLogger();

      // 1. WORM AccessDenied skips cleanly in prune()
      const pruneRes = await prune({ config, storage, logger });
      expect(pruneRes.remove.map((r) => r.key)).toContain(oldKey);
      expect(logger.warn).toHaveBeenCalled();

      // 2. WORM AccessDenied explains WORM Compliance in deleteBackup()
      await expect(
        deleteBackup({ config, storage, logger }, { key: oldKey, force: true }),
      ).rejects.toThrow(/S3 WORM Compliance Object Lock/i);

      // 3. Mixed [AccessDenied, InternalError] batch MUST throw in prune()
      deleteMode = "mixed";
      await expect(prune({ config, storage, logger })).rejects.toThrow(
        /InternalError|S3 batch delete failed/i,
      );
    } finally {
      await srv.close();
    }
  });

  it("[F-SCL-007] sharded deleteBackup() failing on shard01 preserves .cluster.manifest.json so retry succeeds", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-007-"));
    try {
      const storage = new FilesystemStorageClient(dir);
      const logger = makeSilentLogger();
      const config = makeBaseConfig({ sharding: { enabled: true } });

      const clusterKey = "mv/_cluster/2026-01-01/20260101T000000Z.cluster";
      const cfgKey = "mv/configsvr/_cluster/2026-01-01/20260101T000000Z.archive.gz";
      const s1Key = "mv/shards/shard01/_cluster/2026-01-01/20260101T000000Z.archive.gz";
      const brKey = "mv/shards/shard01/_bridging/20260101T000000Z.oplog.gz";

      await storage.upload(cfgKey, Buffer.from("cfg"));
      await storage.upload(s1Key, Buffer.from("s1"));
      await storage.upload(brKey, Buffer.from("br"));
      await storage.upload(clusterKey, Buffer.from("cluster"));
      await writeManifest(
        storage,
        makeManifest({
          type: "sharded-cluster-backup",
          key: clusterKey,
          sharding: {
            clusterTime: "100:1",
            configsvr: { backupKey: cfgKey, manifestKey: manifestKey(cfgKey) },
            shards: {
              shard01: {
                shardId: "shard01",
                host: "rs1",
                backupKey: s1Key,
                manifestKey: manifestKey(s1Key),
                bridgingSliceKeys: [brKey],
                status: "success",
              },
            },
          },
        }),
      );

      let failShard01 = true;
      const origRemoveMany = storage.removeMany.bind(storage);
      storage.removeMany = async (keys) => {
        if (failShard01 && keys.includes(s1Key)) {
          const otherKeys = keys.filter((k) => k !== s1Key);
          await origRemoveMany(otherKeys);
          throw new Error("S3 503 ServiceUnavailable");
        }
        return origRemoveMany(keys);
      };

      await expect(
        deleteBackup({ config, storage, logger }, { key: clusterKey, force: true }),
      ).rejects.toThrow(/ServiceUnavailable/);

      // .cluster.manifest.json MUST remain intact when shard01 deletion failed
      expect(await storage.head(manifestKey(clusterKey))).not.toBeNull();

      // Retry after clearing fault succeeds and deletes .cluster.manifest.json too
      failShard01 = false;
      const retryRes = await deleteBackup(
        { config, storage, logger },
        { key: clusterKey, force: true },
      );
      expect(retryRes.deleted).toBe(true);
      expect(await storage.head(manifestKey(clusterKey))).toBeNull();
      expect(await storage.head(s1Key)).toBeNull();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  it("[F-SCL-008] verifyManifest() detects tampering of schema, objectLock, signature.algorithm downgrade, and mismatched dual HMACs", () => {
    const masterKey = parseKey(generateKey());
    const base = makeManifest({
      key: "mv/prod/2026-01-01/20260101T000000Z.archive.gz.enc",
      database: "prod",
      schema: {
        collections: {
          users: {
            indexes: [{ name: "email_1", key: { email: 1 }, unique: true }],
          },
        },
        views: [],
      },
      objectLock: {
        mode: "COMPLIANCE",
        retainUntil: "2026-02-01T00:00:00.000Z",
        legalHold: true,
      },
      encryption: {
        algorithm: CHUNKED_ALGORITHM,
        keyId: keyId(masterKey),
        wrappedKey: "wrapped",
        iv: Buffer.alloc(12).toString("base64"),
        chunkSize: 65536,
      },
    });
    const hmac = signManifest(masterKey, base);
    base.encryption.manifestHmac = hmac;
    base.signature = {
      algorithm: "HMAC-SHA256",
      keyId: keyId(masterKey),
      hmac,
    };
    expect(verifyManifest(masterKey, base)).toBe(true);

    // 1. Mutate schema
    const badSchema = structuredClone(base);
    badSchema.schema.collections.users.indexes[0].unique = false;
    expect(() => verifyManifest(masterKey, badSchema)).toThrow();

    // 2. Mutate objectLock
    const badLock = structuredClone(base);
    badLock.objectLock.retainUntil = "2099-01-01T00:00:00.000Z";
    expect(() => verifyManifest(masterKey, badLock)).toThrow();

    // 3. Downgrade signature.algorithm
    const badAlg = structuredClone(base);
    badAlg.signature.algorithm = "none";
    expect(() => verifyManifest(masterKey, badAlg)).toThrow();

    // 4. Mismatch signature.hmac vs encryption.manifestHmac
    const badDual = structuredClone(base);
    badDual.encryption.manifestHmac = "0".repeat(64);
    expect(() => verifyManifest(masterKey, badDual)).toThrow();
  });

  it("[F-SCL-009] does not fire onLost after clean release(), triggers onLost on persistent fs.utimes error, clamps hybrid heartbeat, and exposes assertValid()/signal", async () => {
    // 1. Slow refreshLock resolving false after release()
    let resolveRefresh;
    let lostCount = 0;
    const mockStorage = {
      async acquireLock() {
        return "t1";
      },
      refreshLock: () =>
        new Promise((r) => {
          resolveRefresh = r;
        }),
      async releaseLock() {},
    };
    const rel = await acquireDistributedLock(mockStorage, "l.json", {
      ttlMs: 500,
      heartbeatIntervalMs: 15,
      onLost: () => {
        lostCount++;
      },
    });
    await new Promise((r) => setTimeout(r, 35));
    await rel();
    resolveRefresh(false);
    await new Promise((r) => setTimeout(r, 20));
    expect(lostCount).toBe(0);

    // 2. Persistent fs.utimes failure past staleAfterMs triggers onLost
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-009-local-"));
    const lockFile = path.join(dir, ".backup.lock");
    const utimesSpy = vi
      .spyOn(fs, "utimes")
      .mockRejectedValue(Object.assign(new Error("EROFS"), { code: "EROFS" }));
    let localLost = null;
    try {
      const relLocal = await acquireLock(lockFile, {
        staleAfterMs: 50,
        heartbeatIntervalMs: 15,
        onLost: (err) => {
          localLost = err;
        },
      });
      await new Promise((r) => setTimeout(r, 95));
      expect(localLost).toBeInstanceOf(Error);
      await relLocal();
    } finally {
      utimesSpy.mockRestore();
      await fs.rm(dir, { recursive: true, force: true });
    }

    // 3. acquireHybridLock clamps heartbeat when distributedTtlMs=120 and exposes .assertValid() + .signal
    let refreshes = 0;
    const dir2 = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-009-hybrid-"));
    try {
      const hybridRel = await acquireHybridLock({
        file: path.join(dir2, ".backup.lock"),
        lockKey: "k.lock.json",
        distributedTtlMs: 120,
        storage: {
          async acquireLock() {
            return "ht1";
          },
          async refreshLock() {
            refreshes++;
            return true;
          },
          async releaseLock() {},
        },
      });
      await new Promise((r) => setTimeout(r, 150));
      expect(refreshes).toBeGreaterThanOrEqual(2);
      expect(typeof hybridRel.assertValid).toBe("function");
      expect(hybridRel.signal).toBeInstanceOf(AbortSignal);
      hybridRel.assertValid();
      await hybridRel();
    } finally {
      await fs.rm(dir2, { recursive: true, force: true });
    }
  });

  it("[F-SCL-010] S3 lock handles HTTP Date skew, weak/null ETag, corrupt JSON recovery, and 501 releaseLock expiry guard over node:http", async () => {
    const state = {
      scenario: null,
      storedBody: "",
      storedEtag: '"e1"',
      sendEtagOnGet: true,
      sendEtagOnHead: true,
      headEtag: '"head-1"',
      serverDateMs: Date.now(),
      lastIfMatch: null,
      sawIfNoneMatchStar: false,
      sawUnconditionalDelete: false,
      requireExactWeakEtag: false,
      return501OnConditionalDelete: false,
    };

    const srv = await startHttpServer(async (req, res) => {
      const dateHeader = new Date(state.serverDateMs).toUTCString();
      if (req.method === "GET") {
        if (!state.storedBody) {
          res.writeHead(404, { Date: dateHeader, "Content-Type": "application/xml" });
          res.end("<Error><Code>NoSuchKey</Code></Error>");
          return;
        }
        const headers = {
          Date: dateHeader,
          "Content-Type": "application/json",
          "Content-Length": String(Buffer.byteLength(state.storedBody)),
        };
        if (state.sendEtagOnGet && state.storedEtag) headers.ETag = state.storedEtag;
        res.writeHead(200, headers);
        res.end(state.storedBody);
        return;
      }

      if (req.method === "HEAD") {
        const headers = {
          Date: dateHeader,
          "Content-Length": String(Buffer.byteLength(state.storedBody)),
        };
        if (state.sendEtagOnHead && state.headEtag) headers.ETag = state.headEtag;
        res.writeHead(200, headers);
        res.end();
        return;
      }

      if (req.method === "PUT") {
        const body = await readRequestBody(req);
        if (req.headers["if-none-match"] === "*") {
          state.sawIfNoneMatchStar = true;
        }
        if (req.headers["if-match"]) {
          state.lastIfMatch = req.headers["if-match"];
        }
        if (state.requireExactWeakEtag && req.headers["if-match"] !== 'W/"weak-1"') {
          res.writeHead(412, { Date: dateHeader, "Content-Type": "application/xml" });
          res.end("<Error><Code>PreconditionFailed</Code></Error>");
          return;
        }
        state.storedBody = body;
        state.storedEtag = `"e-${state.scenario}-${Date.now()}"`;
        res.writeHead(200, { Date: dateHeader, ETag: state.storedEtag });
        res.end();
        return;
      }

      if (req.method === "DELETE") {
        if (!req.headers["if-match"]) {
          state.sawUnconditionalDelete = true;
          state.storedBody = "";
          res.writeHead(204, { Date: dateHeader });
          res.end();
          return;
        }
        if (state.return501OnConditionalDelete) {
          res.writeHead(501, { Date: dateHeader, "Content-Type": "application/xml" });
          res.end("<Error><Code>NotImplemented</Code><Message>Not Implemented</Message></Error>");
          return;
        }
        state.storedBody = "";
        res.writeHead(204, { Date: dateHeader });
        res.end();
        return;
      }

      res.writeHead(404, { Date: dateHeader });
      res.end();
    });

    try {
      const s3 = new S3StorageClient({
        region: "us-east-1",
        bucket: "lock-bkt",
        endpoint: srv.endpoint,
        forcePathStyle: true,
        accessKeyId: "test",
        secretAccessKey: "test",
      });

      // 1a. Server clock 10 min behind local Date.now(), active lock expiresAt = T_s3 + 20_000 -> MUST stay locked (null)
      state.scenario = "skew-behind";
      state.serverDateMs = Date.now() - 600_000;
      state.storedBody = JSON.stringify({
        token: "tok-a",
        holder: "A",
        acquiredAt: new Date(state.serverDateMs).toISOString(),
        expiresAt: state.serverDateMs + 20_000,
      });
      state.storedEtag = '"e-skew-1"';
      expect(await s3.acquireLock("op.lock.json", { holder: "B", ttlMs: 30_000 })).toBeNull();

      // 1b. Server clock 10 min ahead of local Date.now(), expired lock expiresAt = T_s3 - 1_000 -> MUST take over
      state.scenario = "skew-ahead";
      state.serverDateMs = Date.now() + 600_000;
      state.storedBody = JSON.stringify({
        token: "tok-a",
        holder: "A",
        acquiredAt: new Date(state.serverDateMs - 30_000).toISOString(),
        expiresAt: state.serverDateMs - 1_000,
      });
      state.storedEtag = '"e-skew-2"';
      const tokAhead = await s3.acquireLock("op.lock.json", { holder: "B", ttlMs: 30_000 });
      expect(tokAhead).toBeTypeOf("string");

      // 2a. Weak ETag W/"weak-1" preserved verbatim in If-Match
      state.scenario = "weak-etag";
      state.serverDateMs = Date.now();
      state.storedBody = JSON.stringify({
        token: "tok-old",
        holder: "old",
        acquiredAt: new Date(state.serverDateMs - 60_000).toISOString(),
        expiresAt: state.serverDateMs - 1_000,
      });
      state.storedEtag = 'W/"weak-1"';
      state.lastIfMatch = null;
      state.requireExactWeakEtag = true;
      const tokWeak = await s3.acquireLock("op.lock.json", { holder: "B", ttlMs: 30_000 });
      expect(tokWeak).toBeTypeOf("string");
      expect(state.lastIfMatch).toBe('W/"weak-1"');
      state.requireExactWeakEtag = false;

      // 2b. Missing ETag on GET never sends If-None-Match: "*"
      state.scenario = "missing-etag";
      state.storedBody = JSON.stringify({
        token: "tok-old2",
        holder: "old",
        acquiredAt: new Date(state.serverDateMs - 60_000).toISOString(),
        expiresAt: state.serverDateMs - 1_000,
      });
      state.sendEtagOnGet = false;
      state.sendEtagOnHead = true;
      state.headEtag = '"head-1"';
      state.sawIfNoneMatchStar = false;
      state.lastIfMatch = null;
      const tokHead = await s3.acquireLock("op.lock.json", { holder: "B", ttlMs: 30_000 });
      expect(tokHead).toBeTypeOf("string");
      expect(state.sawIfNoneMatchStar).toBe(false);
      expect(state.lastIfMatch).toBe('"head-1"');
      state.sendEtagOnGet = true;

      // 3. Corrupt JSON lock recovers via conditional PutObject
      state.scenario = "corrupt-json";
      state.storedBody = "{corrupt";
      state.storedEtag = '"bad-etag"';
      state.lastIfMatch = null;
      const tokRecovered = await s3.acquireLock("op.lock.json", { holder: "B", ttlMs: 30_000 });
      expect(tokRecovered).toBeTypeOf("string");
      expect(state.lastIfMatch).toBe('"bad-etag"');

      // 4. 501 fallback in releaseLock does NOT unconditionally delete an already-expired lock
      state.scenario = "501-expired";
      state.serverDateMs = Date.now();
      state.storedBody = JSON.stringify({
        token: "tok-exp",
        holder: "A",
        acquiredAt: new Date(state.serverDateMs - 60_000).toISOString(),
        expiresAt: state.serverDateMs - 1_000,
      });
      state.storedEtag = '"exp-etag"';
      state.return501OnConditionalDelete = true;
      state.sawUnconditionalDelete = false;
      await s3.releaseLock("op.lock.json", "tok-exp");
      expect(state.sawUnconditionalDelete).toBe(false);
    } finally {
      await srv.close();
    }
  });

  it("[F-SCL-011] S3 download() rejects 200 OK full body on Range resume, completes cleanly on trailing error at exact EOF, and aborts backoff immediately", async () => {
    let mode = "range-200";
    let reqCount = 0;

    const srv = await startHttpServer((req, res) => {
      reqCount++;
      if (mode === "range-200") {
        if (!req.headers.range) {
          res.writeHead(200, { "Content-Length": "10", ETag: '"e1"' });
          res.write("hello");
          setTimeout(() => req.socket.destroy(), 10);
        } else {
          // Misbehaving server ignores Range and sends 200 OK full body without Content-Range
          res.writeHead(200, { "Content-Length": "10", ETag: '"e1"' });
          res.end("helloworld");
        }
        return;
      }

      if (mode === "eof-reset") {
        if (req.headers.range) {
          res.writeHead(416, { "Content-Type": "application/xml" });
          res.end("<Error><Code>InvalidRange</Code></Error>");
          return;
        }
        res.writeHead(200, { "Content-Length": "10", ETag: '"e2"' });
        res.end("helloworld");
        return;
      }

      if (mode === "abort-backoff") {
        res.writeHead(200, { "Content-Length": "10", ETag: '"e3"' });
        res.write("hello");
        setTimeout(() => req.socket.destroy(), 10);
      }
    });

    try {
      // 1. Resumed GET returning 200 OK full body without Content-Range must reject
      const s3A = new S3StorageClient({
        region: "us-east-1",
        bucket: "dl-bkt",
        endpoint: srv.endpoint,
        forcePathStyle: true,
        accessKeyId: "test",
        secretAccessKey: "test",
      });
      mode = "range-200";
      reqCount = 0;
      const st1 = await s3A.download("k1");
      await expect(
        (async () => {
          const chunks = [];
          for await (const c of st1) chunks.push(c);
          return Buffer.concat(chunks).toString();
        })(),
      ).rejects.toThrow(/206|Content-Range|Range/i);

      // 2. Trailing socket reset after exact Content-Length completes cleanly without Range: bytes=10-
      const s3B = new S3StorageClient({
        region: "us-east-1",
        bucket: "dl-bkt",
        endpoint: srv.endpoint,
        forcePathStyle: true,
        accessKeyId: "test",
        secretAccessKey: "test",
      });
      let eofInjected = false;
      s3B.client.middlewareStack.add(
        (next) => async (args) => {
          const result = await next(args);
          if (result.output?.Body && !eofInjected) {
            eofInjected = true;
            const orig = result.output.Body;
            async function* failAtEof() {
              for await (const c of orig) yield c;
              throw Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" });
            }
            result.output.Body = Readable.from(failAtEof());
          }
          return result;
        },
        { step: "deserialize", name: "injectEofReset", priority: "high" },
      );
      mode = "eof-reset";
      reqCount = 0;
      const st2 = await s3B.download("k2");
      const chunks2 = [];
      for await (const c of st2) chunks2.push(c);
      expect(Buffer.concat(chunks2).toString()).toBe("helloworld");
      expect(reqCount).toBe(1);

      // 3. signal.abort() during retry backoff rejects immediately (< 150ms)
      mode = "abort-backoff";
      reqCount = 0;
      const ac = new AbortController();
      const st3 = await s3A.download("k3", { signal: ac.signal });
      const startAbort = Date.now();
      await expect(
        (async () => {
          const chunks = [];
          for await (const c of st3) {
            chunks.push(c);
            ac.abort(new Error("user cancelled"));
          }
          return Buffer.concat(chunks);
        })(),
      ).rejects.toThrow();
      expect(Date.now() - startAbort).toBeLessThan(150);
    } finally {
      await srv.close();
    }
  }, 25_000);

  it("[F-SCL-012] FilesystemStorageClient blocks symlink escapes, rejects empty/root keys without parent .tmp-, and excludes lock tombstones from list()", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-012-root-"));
    const outside = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-012-out-"));
    const storage = new FilesystemStorageClient(root);

    try {
      // 1. Symlink escape with non-existent child directory
      await fs.symlink(outside, path.join(root, "escape"));
      await expect(storage.upload("escape/nested/pwn.txt", Buffer.from("x"))).rejects.toThrow(
        /symlink|escapes/i,
      );
      await expect(storage.putJson("escape/nested/pwn.json", { bad: true })).rejects.toThrow(
        /symlink|escapes/i,
      );
      await expect(fs.stat(path.join(outside, "nested"))).rejects.toThrow();

      // 2. Empty / root keys must reject without writing .tmp-* into parent of basePath
      await expect(storage.putJson("", { x: 1 })).rejects.toThrow(/empty|invalid|escapes/i);
      await expect(storage.upload("/", Buffer.from("x"))).rejects.toThrow(/empty|invalid|escapes/i);
      const parentEntries = await fs.readdir(path.dirname(root));
      expect(parentEntries.some((n) => n.startsWith(`${path.basename(root)}.tmp-`))).toBe(false);

      // 3. list() excludes .stale., .refresh., .release., .init., and .lck
      await fs.writeFile(path.join(root, "good.txt"), "ok");
      await fs.writeFile(path.join(root, "op.lock.stale.123"), "{}");
      await fs.writeFile(path.join(root, "op.lock.refresh.123"), "{}");
      await fs.writeFile(path.join(root, "op.lock.release.123"), "{}");
      await fs.writeFile(path.join(root, "op.lock.init.123"), "{}");
      await fs.mkdir(path.join(root, "op.lock.lck"));
      await fs.writeFile(path.join(root, "op.lock.lck", "ghost.txt"), "nope");

      const keys = [];
      for await (const o of storage.list("")) keys.push(o.key);
      expect(keys).toEqual(["good.txt"]);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
      await fs.rm(outside, { recursive: true, force: true });
    }
  });

  it("[F-SCL-013] settleAll() throws root-cause error instead of secondary AbortError when tasks[1] aborts controller", async () => {
    const ctrl = linkedController();
    const slowAbortable = new Promise((_resolve, reject) => {
      ctrl.signal.addEventListener(
        "abort",
        () =>
          reject(
            Object.assign(new Error("The operation was aborted"), {
              name: "AbortError",
              code: "ABORT_ERR",
            }),
          ),
        { once: true },
      );
    });
    const fastRootFailure = Promise.reject(new Error("IntegrityError: AEAD tag mismatch"));

    await expect(settleAll(ctrl, [slowAbortable, fastRootFailure])).rejects.toThrow(
      "IntegrityError: AEAD tag mismatch",
    );
  });

  it("[F-TST-015] enforces mutual exclusion across child processes and recovers immediately after SIGKILL", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-tst-015-"));
    const lockFile = path.join(dir, ".backup.lock");
    const activeCountFile = path.join(dir, "active-count.txt");
    const maxCountFile = path.join(dir, "max-count.txt");
    await fs.writeFile(activeCountFile, "0");
    await fs.writeFile(maxCountFile, "0");

    try {
      // 1. Multi-process contention: 5 child processes race to acquire/hold/release local lock
      const workerScript = `
        import fs from "node:fs/promises";
        import { acquireLock } from "./src/lock.js";
        const [lockFile, activeFile, maxFile] = process.argv.slice(1);
        for (let attempt = 0; attempt < 25; attempt++) {
          const release = await acquireLock(lockFile, { staleAfterMs: 10000, heartbeatIntervalMs: 2000 });
          if (!release) {
            await new Promise((r) => setTimeout(r, 15));
            continue;
          }
          const cur = Number(await fs.readFile(activeFile, "utf8")) + 1;
          await fs.writeFile(activeFile, String(cur));
          const prevMax = Number(await fs.readFile(maxFile, "utf8"));
          if (cur > prevMax) await fs.writeFile(maxFile, String(cur));
          await new Promise((r) => setTimeout(r, 25));
          const after = Number(await fs.readFile(activeFile, "utf8")) - 1;
          await fs.writeFile(activeFile, String(after));
          await release();
          process.exit(0);
        }
        process.exit(2);
      `;

      const children = Array.from(
        { length: 5 },
        () =>
          new Promise((resolve, reject) => {
            const child = spawn(
              process.execPath,
              ["--import", "tsx", "-e", workerScript, lockFile, activeCountFile, maxCountFile],
              { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] },
            );
            let stderr = "";
            child.stderr.on("data", (d) => {
              stderr += d.toString();
            });
            child.on("error", reject);
            child.on("exit", (code) => {
              if (code === 0) resolve();
              else reject(new Error(`Worker exited with ${code}: ${stderr}`));
            });
          }),
      );
      await Promise.all(children);
      expect(Number(await fs.readFile(maxCountFile, "utf8"))).toBe(1);
      expect(Number(await fs.readFile(activeCountFile, "utf8"))).toBe(0);

      // 2. SIGKILL recovery: dead PID lock recovered immediately despite 60s staleAfterMs
      const holderScript = `
        import { acquireLock } from "./src/lock.js";
        const [lockFile] = process.argv.slice(1);
        const rel = await acquireLock(lockFile, { staleAfterMs: 60000 });
        if (!rel) process.exit(2);
        process.stdout.write("LOCKED\\n");
        setInterval(() => {}, 1000);
      `;
      const victim = spawn(process.execPath, ["--import", "tsx", "-e", holderScript, lockFile], {
        cwd: process.cwd(),
        stdio: ["ignore", "pipe", "pipe"],
      });
      await new Promise((resolve, reject) => {
        let out = "";
        victim.stdout.on("data", (d) => {
          out += d.toString();
          if (out.includes("LOCKED")) resolve();
        });
        victim.on("exit", (code) => reject(new Error(`Victim exited prematurely: ${code}`)));
      });
      victim.kill("SIGKILL");
      await new Promise((resolve) => victim.once("exit", resolve));

      const startRecovery = Date.now();
      const recovered = await acquireLock(lockFile, { staleAfterMs: 60_000 });
      expect(recovered).toBeTypeOf("function");
      expect(Date.now() - startRecovery).toBeLessThan(2_000);
      await recovered();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  }, 20_000);

  it("[F-SCL-014] S3StorageClient.head() returns null for DeleteMarker and finite size 0 when ContentLength is missing", async () => {
    let mode = "delete-marker";
    const srv = await startHttpServer((req, res) => {
      if (req.method === "HEAD") {
        if (mode === "delete-marker") {
          res.writeHead(200, { "x-amz-delete-marker": "true", ETag: '"dm1"' });
          res.end();
        } else {
          res.writeHead(200, { ETag: '"no-len"' });
          res.end();
        }
        return;
      }
      res.writeHead(404);
      res.end();
    });

    try {
      const s3 = new S3StorageClient({
        region: "us-east-1",
        bucket: "head-bkt",
        endpoint: srv.endpoint,
        forcePathStyle: true,
        accessKeyId: "test",
        secretAccessKey: "test",
      });

      mode = "delete-marker";
      expect(await s3.head("deleted.gz")).toBeNull();

      mode = "missing-length";
      const res = await s3.head("unknown-len.gz");
      expect(res).not.toBeNull();
      expect(Number.isFinite(res.size)).toBe(true);
      expect(res.size).toBe(0);
    } finally {
      await srv.close();
    }
  });

  it("[F-SCL-015] S3StorageClient.upload() rejects pre-aborted signal without sending HTTP requests and aborts mid-flight uploads cleanly", async () => {
    let requestCount = 0;
    const srv = await startHttpServer(async (req, res) => {
      requestCount++;
      await readRequestBody(req);
      res.writeHead(200, { ETag: '"u1"' });
      res.end();
    });

    try {
      const s3 = new S3StorageClient({
        region: "us-east-1",
        bucket: "up-bkt",
        endpoint: srv.endpoint,
        forcePathStyle: true,
        accessKeyId: "test",
        secretAccessKey: "test",
      });

      const preAc = new AbortController();
      preAc.abort(new Error("pre-aborted"));
      await expect(
        s3.upload("k.gz", Buffer.from("payload"), { signal: preAc.signal }),
      ).rejects.toThrow(/aborted/i);
      expect(requestCount).toBe(0);

      // Mid-flight stream abort
      const midAc = new AbortController();
      const slowStream = new Readable({
        read() {
          setTimeout(() => midAc.abort(new Error("mid-flight-abort")), 15);
        },
      });
      await expect(s3.upload("k-mid.gz", slowStream, { signal: midAc.signal })).rejects.toThrow();
    } finally {
      await srv.close();
    }
  });

  it("[F-SCL-016] local lock isStale() handles container PID-1 restart (instanceId), remote hostname, and same-process active locks", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-f-scl-016-"));
    const lockFile = path.join(dir, ".backup.lock");

    try {
      // 1. Container PID-1 restart: same PID + same hostname + different instanceId -> stale immediately
      await fs.writeFile(
        lockFile,
        JSON.stringify({
          pid: process.pid,
          hostname: os.hostname(),
          instanceId: "prior-container-instance",
          token: "old-container-token",
          acquiredAt: new Date().toISOString(),
        }),
      );
      expect(await isStale(lockFile, 60_000)).toBe(true);
      const rel = await acquireLock(lockFile, { staleAfterMs: 60_000 });
      expect(rel).toBeTypeOf("function");
      // Same-process active lock stays non-stale
      expect(await isStale(lockFile, 60_000)).toBe(false);
      expect(await acquireLock(lockFile, { staleAfterMs: 60_000 })).toBeNull();
      await rel();

      // 2. Different hostname with dead local PID is NOT stale until staleAfterMs elapses
      await fs.writeFile(
        lockFile,
        JSON.stringify({
          pid: 999_999,
          hostname: "remote-host.internal",
          instanceId: "remote-inst",
          token: "remote-tok",
          acquiredAt: new Date().toISOString(),
        }),
      );
      expect(await isStale(lockFile, 60_000)).toBe(false);
      const past = new Date(Date.now() - 120_000);
      await fs.utimes(lockFile, past, past);
      expect(await isStale(lockFile, 60_000)).toBe(true);
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  it("[F-SCL-017] readManifest() unwraps GetJsonResult safely and validates manifest shape and version", async () => {
    const makeMockStorage = (val) => ({
      async getJson() {
        return val;
      },
    });

    // Unwraps GetJsonResult with etag or without key
    expect(
      await readManifest(makeMockStorage({ data: { key: "k1", version: 2 }, etag: "e1" }), "k1"),
    ).toEqual({ key: "k1", version: 2 });
    expect(await readManifest(makeMockStorage({ data: { key: "k2" } }), "k2")).toEqual({
      key: "k2",
    });

    // Does NOT unwrap direct manifest containing a literal "data" field
    expect(
      await readManifest(makeMockStorage({ key: "k3", version: 2, data: "literal" }), "k3"),
    ).toEqual({ key: "k3", version: 2, data: "literal" });

    // Rejects invalid shapes and unsupported versions
    await expect(readManifest(makeMockStorage({}), "k4")).rejects.toThrow(
      /missing required "key"/i,
    );
    await expect(readManifest(makeMockStorage({ data: [] }), "k5")).rejects.toThrow(
      /expected JSON object/i,
    );
    await expect(
      readManifest(makeMockStorage({ data: { key: "k6", version: 99 } }), "k6"),
    ).rejects.toThrow(/Unsupported manifest version/i);
    await expect(
      readManifest(makeMockStorage({ data: { key: "k7", version: 0 } }), "k7"),
    ).rejects.toThrow(/Unsupported manifest version/i);
  });

  it("[F-SCL-018] archiveKey() avoids sub-second collisions, parseCreatedAt() preserves ms, and listBackups()/planRetention() handle missing timestamps safely", async () => {
    const scope = { prefix: "mv", database: "prod" };
    const d1 = new Date("2026-05-10T14:30:00.123Z");
    const d2 = new Date("2026-05-10T14:30:00.456Z");
    const dZero = new Date("2026-05-10T14:30:00.000Z");

    const k1 = archiveKey(scope, d1, { encrypted: false });
    const k2 = archiveKey(scope, d2, { encrypted: false });
    const kZero = archiveKey(scope, dZero, { encrypted: false });
    expect(k1).not.toBe(k2);
    expect(kZero).toBe("mv/prod/2026-05-10/20260510T143000Z.archive.gz");
    expect(parseCreatedAt(k1)?.toISOString()).toBe("2026-05-10T14:30:00.123Z");

    const fakeStorage = {
      async *list() {
        yield { key: "mv/prod/custom-backup.archive.gz", size: 100 };
        yield { key: "mv/prod/custom-backup.archive.gz.manifest.json", size: 10 };
      },
    };
    const backups = await listBackups(fakeStorage, scope);
    expect(backups).toHaveLength(1);
    expect(Number.isNaN(backups[0].createdAt.getTime())).toBe(false);
    const plan = planRetention(backups, { keepLast: 1, keepDaily: 1 }, "UTC");
    expect(plan.keep).toHaveLength(1);
  });

  it("[F-SCL-019] listBackups() skips _pitr/.locks/_bridging early and databasePrefix() rejects reserved/unsafe DB names while allowing _cluster", async () => {
    const fakeStorage = {
      async *list() {
        yield { key: "mv/prod/_pitr/2026-01-01/fake.archive.gz", size: 50 };
        yield { key: "mv/prod/_pitr/2026-01-01/fake.archive.gz.manifest.json", size: 10 };
        yield { key: "mv/prod/.locks/backup.lock.json", size: 20 };
        yield { key: "mv/prod/_bridging/b.archive.gz", size: 30 };
        yield { key: "mv/prod/2026-01-01/20260101T000000Z.archive.gz", size: 100 };
      },
    };
    const backups = await listBackups(fakeStorage, { prefix: "mv", database: "prod" });
    expect(backups.map((b) => b.key)).toEqual(["mv/prod/2026-01-01/20260101T000000Z.archive.gz"]);

    expect(databasePrefix({ prefix: "mv", database: "_cluster" })).toBe("mv/_cluster/");
    expect(() => databasePrefix({ prefix: "mv", database: "../bad" })).toThrow(
      /Invalid or reserved/i,
    );
    expect(() => databasePrefix({ prefix: "mv", database: "a/b" })).toThrow(/Invalid or reserved/i);
    expect(() => databasePrefix({ prefix: "mv", database: "shards" })).toThrow(
      /Invalid or reserved/i,
    );
    expect(() => databasePrefix({ prefix: "mv", database: "configsvr" })).toThrow(
      /Invalid or reserved/i,
    );
  });

  it("[F-SCL-020] parseKey() enforces strict base64/hex and createDecryptor()/unwrap() validate envelope base64 and buffer lengths", () => {
    const validKey = parseKey(generateKey());
    expect(() => parseKey(`${generateKey().slice(0, -2)}!a`)).toThrow(
      /Encryption key must be 32 bytes/i,
    );
    expect(() => parseKey("   ")).toThrow(/Encryption key must be 32 bytes/i);

    const kId = keyId(validKey);
    expect(() =>
      createDecryptor([], { algorithm: CHUNKED_ALGORITHM, keyId: kId, wrappedKey: "", iv: "" }),
    ).toThrow(/No encryption keys/i);
    expect(() =>
      createDecryptor(validKey, {
        algorithm: CHUNKED_ALGORITHM,
        keyId: kId,
        wrappedKey: Buffer.alloc(60).toString("base64"),
        iv: Buffer.alloc(4).toString("base64"),
      }),
    ).toThrow(/Invalid encryption IV length/i);
    expect(() =>
      createDecryptor(validKey, {
        algorithm: CHUNKED_ALGORITHM,
        keyId: kId,
        wrappedKey: "not!base64!",
        iv: Buffer.alloc(12).toString("base64"),
      }),
    ).toThrow(/Invalid base64 encoding/i);
    expect(() =>
      createDecryptor(validKey, {
        algorithm: CHUNKED_ALGORITHM,
        keyId: kId,
        wrappedKey: Buffer.alloc(16).toString("base64"),
        iv: Buffer.alloc(12).toString("base64"),
      }),
    ).toThrow(/Invalid wrapped data key length/i);
  });

  it("[F-SCL-021] ChunkedAeadDecryptStream decrypts a 1 MiB frame fed in 4 KiB chunks with O(1) Buffer.concat calls", async () => {
    const dataKey = randomBytes(32);
    const baseNonce = randomBytes(12);
    const frameChunkSize = 1024 * 1024; // 1 MiB single frame
    const payload = randomBytes(frameChunkSize);

    const encChunks = [];
    const encryptor = new ChunkedAeadEncryptStream(dataKey, baseNonce, frameChunkSize);
    await pipeline(Readable.from([payload]), encryptor, async function* (src) {
      for await (const c of src) encChunks.push(c);
    });
    const encrypted = Buffer.concat(encChunks);

    // Slice encrypted 1 MiB frame into 4 KiB pieces (~256 chunks)
    const smallChunks = [];
    for (let i = 0; i < encrypted.length; i += 4096) {
      smallChunks.push(encrypted.subarray(i, i + 4096));
    }
    expect(smallChunks.length).toBeGreaterThan(200);

    const origConcat = Buffer.concat;
    const concatSpy = vi
      .spyOn(Buffer, "concat")
      .mockImplementation((...args) => origConcat(...args));
    try {
      const decChunks = [];
      const decryptor = new ChunkedAeadDecryptStream(dataKey, baseNonce);
      await pipeline(Readable.from(smallChunks), decryptor, async function* (src) {
        for await (const c of src) decChunks.push(c);
      });
      // O(1) concats per frame (1 for frame buffer assembly + 1 for cipher update/final + 1 in test)
      expect(concatSpy.mock.calls.length).toBeLessThan(10);
      expect(origConcat(decChunks)).toEqual(payload);
    } finally {
      concatSpy.mockRestore();
    }
  });

  it("[Challenger-1.1] prune() promotes sole post-gap PITR anchor (B_mid) even when older pre-gap backup (B_old) is kept", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-challenger-1-1-"));
    try {
      const storage = new FilesystemStorageClient(dir);
      const logger = makeSilentLogger();
      const scope = { prefix: "mv", database: "*" };
      const config = makeBaseConfig({ retention: { keepLast: 1, keepDaily: 2 } });

      const bOld = "mv/_cluster/1970-01-01/19700101T001640Z.archive.gz";
      const bMid = "mv/_cluster/1970-01-02/19700102T000100Z.archive.gz";
      const bNew = "mv/_cluster/1970-01-02/19700102T000500Z.physical.tar.gz";
      const s1 = oplogSliceKey(scope, { t: 1005, i: 1 }, { t: 86420, i: 1 });
      const s2 = oplogSliceKey(scope, { t: 86500, i: 1 }, { t: 86800, i: 1 });

      await storage.upload(bOld, Buffer.from("b-old"));
      await writeManifest(
        storage,
        makeManifest({
          key: bOld,
          engine: "logical",
          createdAt: "1970-01-01T00:16:40.000Z",
          completedAt: "1970-01-01T00:16:45.000Z",
          oplog: true,
          oplogWindow: { startTs: "1000:1", endTs: "1005:1" },
        }),
      );

      await storage.upload(s1, Buffer.from("slice-1"));
      await writeManifest(
        storage,
        makeManifest({
          key: s1,
          type: "oplog-slice",
          oplog: true,
          oplogWindow: { startTs: "1005:1", endTs: "86420:1" },
        }),
      );

      await storage.upload(bMid, Buffer.from("b-mid"));
      await writeManifest(
        storage,
        makeManifest({
          key: bMid,
          engine: "logical",
          createdAt: "1970-01-02T00:01:00.000Z",
          completedAt: "1970-01-02T00:01:05.000Z",
          oplog: true,
          oplogWindow: { startTs: "86460:1", endTs: "86465:1" },
        }),
      );

      await storage.upload(s2, Buffer.from("slice-2"));
      await writeManifest(
        storage,
        makeManifest({
          key: s2,
          type: "oplog-slice",
          oplog: true,
          oplogWindow: {
            startTs: "86500:1",
            endTs: "86800:1",
            previousEndTs: "86420:1",
            gapDetected: true,
          },
        }),
      );

      await storage.upload(bNew, Buffer.from("b-new"));
      await writeManifest(
        storage,
        makeManifest({
          key: bNew,
          engine: "physical",
          createdAt: "1970-01-02T00:05:00.000Z",
          completedAt: "1970-01-02T00:05:05.000Z",
          oplog: false,
          oplogWindow: null,
        }),
      );

      const res = await prune({ config, storage, logger });
      expect(res.keep.map((b) => b.key)).toEqual(expect.arrayContaining([bNew, bMid, bOld]));
      expect(res.remove.map((b) => b.key)).not.toContain(bMid);
      expect(await storage.head(bMid)).not.toBeNull();
      expect(await storage.head(s2)).not.toBeNull();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  it("[Challenger-3.4] S3StorageClient.download() aborts immediately under downstream backpressure and removeMany() aggregates multi-chunk errors", async () => {
    const totalBytes = 5 * 1024 * 1024;
    const chunk2MiB = Buffer.alloc(2 * 1024 * 1024, 0x61);
    const deleteChunks = [];
    let openServerRes = null;

    const srv = await startHttpServer(async (req, res) => {
      const url = new URL(req.url, "http://127.0.0.1");
      if (req.method === "GET") {
        openServerRes = res;
        res.writeHead(200, {
          "Content-Length": String(totalBytes),
          ETag: '"big"',
        });
        // Immediately push 2 MiB to saturate PassThrough({ highWaterMark: 512 KiB })
        res.write(chunk2MiB);
        return;
      }
      if (req.method === "POST" && url.searchParams.has("delete")) {
        const body = await readRequestBody(req);
        deleteChunks.push(body);
        const chunkIdx = deleteChunks.length;
        res.writeHead(200, { "Content-Type": "application/xml" });
        if (chunkIdx === 1) {
          res.end(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Error><Key>k-0</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>
</DeleteResult>`);
        } else {
          res.end(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Error><Key>k-1000</Key><Code>InternalError</Code><Message>Internal error</Message></Error>
</DeleteResult>`);
        }
        return;
      }
      res.writeHead(404);
      res.end();
    });

    try {
      const s3 = new S3StorageClient({
        region: "us-east-1",
        bucket: "bp-bkt",
        endpoint: srv.endpoint,
        forcePathStyle: true,
        accessKeyId: "test",
        secretAccessKey: "test",
        objectLock: { enabled: true, mode: "COMPLIANCE", retentionDays: 30 },
      });

      const ac = new AbortController();
      const stream = await s3.download("big.bin", { signal: ac.signal });
      stream.pause();

      const streamResult = new Promise((resolve, reject) => {
        stream.once("error", reject);
        stream.once("end", resolve);
      });

      await new Promise((r) => setTimeout(r, 80));
      const abortStart = Date.now();
      ac.abort(new Error("user cancelled during backpressure"));

      await expect(streamResult).rejects.toThrow(/user cancelled during backpressure|aborted/i);
      expect(Date.now() - abortStart).toBeLessThan(250);

      // Verify removeMany() processes all >1000-key chunks and aggregates batchErrors across chunks
      const keys = Array.from({ length: 1200 }, (_, i) => `k-${i}`);
      let batchErr = null;
      try {
        await s3.removeMany(keys);
      } catch (err) {
        batchErr = err;
      }
      expect(batchErr).not.toBeNull();
      expect(deleteChunks).toHaveLength(2);
      expect(batchErr.errors).toHaveLength(2);
      expect(batchErr.errors.map((e) => e.code)).toEqual(["AccessDenied", "InternalError"]);
    } finally {
      openServerRes?.destroy();
      await srv.close();
    }
  });

  it("[Challenger-2.3] ChunkedAeadDecryptStream rejects invalid frame flag byte (0x02..0xff)", async () => {
    const dataKey = randomBytes(32);
    const baseNonce = randomBytes(12);
    const encChunks = [];
    // 100 bytes with chunkSize=64 produces 2 frames: frame 0 has flag 0x00, frame 1 has flag 0x01.
    // Flipping frame 0 flag 0x00 -> 0x02 leaves isFinal=false so AAD would silently match without explicit flag validation.
    const encryptor = new ChunkedAeadEncryptStream(dataKey, baseNonce, 64);
    await pipeline(Readable.from([Buffer.alloc(100, 0x42)]), encryptor, async function* (src) {
      for await (const c of src) encChunks.push(c);
    });
    const tampered = Buffer.concat(encChunks);
    expect(tampered[4]).toBe(0x00);
    tampered[4] = 0x02;

    const decryptor = new ChunkedAeadDecryptStream(dataKey, baseNonce);
    await expect(
      pipeline(Readable.from([tampered]), decryptor, async function* (src) {
        for await (const _ of src) {
          // drain
        }
      }),
    ).rejects.toThrow(/Invalid AEAD frame flag: 2/i);
  });

  it("[Challenger-Iter2-UploadAbort] S3StorageClient.upload() aborts in-flight multipart uploads, destroys unclosed Readable, and awaits AbortMultipartUpload before settling", async () => {
    const partSizeBytes = 5 * 1024 * 1024;
    const chunk1 = Buffer.alloc(partSizeBytes + 512 * 1024, 0x61); // 5.5 MiB -> triggers multipart Part 1
    const chunk2 = Buffer.alloc(partSizeBytes + 512 * 1024, 0x62); // 5.5 MiB -> total 11 MiB

    // Case 1: Unclosed Readable pausing after Part 1 completes
    {
      let abortCalled = false;
      let abortFinishedAt = 0;
      let uploadSettledAt = 0;
      let part1Completed = false;
      let partUploadsAfterSettle = 0;
      let notifyPart1;
      const part1DonePromise = new Promise((resolve) => {
        notifyPart1 = resolve;
      });

      const srv = await startHttpServer(async (req, res) => {
        const url = new URL(req.url, "http://127.0.0.1");
        if (req.method === "POST" && url.searchParams.has("uploads")) {
          res.writeHead(200, { "Content-Type": "application/xml" });
          res.end(`<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>test-bucket</Bucket>
  <Key>big-upload.bin</Key>
  <UploadId>up-iter2-open</UploadId>
</InitiateMultipartUploadResult>`);
          return;
        }
        if (req.method === "PUT" && url.searchParams.has("partNumber")) {
          const partNum = Number(url.searchParams.get("partNumber"));
          for await (const _ of req) {
            // drain part body
          }
          if (uploadSettledAt > 0) {
            partUploadsAfterSettle += 1;
          }
          res.writeHead(200, { ETag: `"part-${partNum}"` });
          res.end();
          if (partNum === 1 && !part1Completed) {
            part1Completed = true;
            notifyPart1();
          }
          return;
        }
        if (req.method === "DELETE" && url.searchParams.get("uploadId") === "up-iter2-open") {
          abortCalled = true;
          await new Promise((r) => setTimeout(r, 150));
          abortFinishedAt = performance.now();
          res.writeHead(200);
          res.end();
          return;
        }
        res.writeHead(404);
        res.end();
      });

      let pushTimer;
      try {
        const s3 = new S3StorageClient({
          region: "us-east-1",
          bucket: "test-bucket",
          endpoint: srv.endpoint,
          forcePathStyle: true,
          accessKeyId: "test",
          secretAccessKey: "test",
          partSizeMb: 5,
          queueSize: 1,
        });

        let pushedInitial = false;
        const body = new Readable({
          read() {
            if (!pushedInitial) {
              pushedInitial = true;
              this.push(chunk1);
              return;
            }
            // Hold stream open without ending until destroyed/aborted
            if (!pushTimer) {
              pushTimer = setTimeout(() => {
                if (!this.destroyed) {
                  this.push(chunk2);
                  this.push(null);
                }
              }, 2_000);
            }
          },
        });

        const ac = new AbortController();
        const uploadPromise = s3
          .upload("big-upload.bin", body, {
            signal: ac.signal,
            expectedSizeBytes: 11 * 1024 * 1024,
          })
          .finally(() => {
            uploadSettledAt = performance.now();
          });

        await part1DonePromise;
        ac.abort(new Error("mid-flight abort after part 1"));

        await expect(uploadPromise).rejects.toThrow(/abort/i);
        expect(abortCalled).toBe(true);
        expect(abortFinishedAt).toBeGreaterThan(0);
        expect(uploadSettledAt).toBeGreaterThanOrEqual(abortFinishedAt);
        expect(body.destroyed).toBe(true);

        await new Promise((r) => setTimeout(r, 60));
        expect(partUploadsAfterSettle).toBe(0);
      } finally {
        if (pushTimer) clearTimeout(pushTimer);
        await srv.close();
      }
    }

    // Case 2: 11 MiB Readable with slow in-flight Part 2 aborted mid-flight
    {
      let abortCalled = false;
      let abortFinishedAt = 0;
      let uploadSettledAt = 0;
      let partUploadsAfterSettle = 0;
      let notifyPart2Started;
      const part2StartedPromise = new Promise((resolve) => {
        notifyPart2Started = resolve;
      });

      const srv = await startHttpServer(async (req, res) => {
        const url = new URL(req.url, "http://127.0.0.1");
        if (req.method === "POST" && url.searchParams.has("uploads")) {
          res.writeHead(200, { "Content-Type": "application/xml" });
          res.end(`<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>test-bucket</Bucket>
  <Key>big-upload-slow-p2.bin</Key>
  <UploadId>up-iter2-p2</UploadId>
</InitiateMultipartUploadResult>`);
          return;
        }
        if (req.method === "PUT" && url.searchParams.has("partNumber")) {
          const partNum = Number(url.searchParams.get("partNumber"));
          if (partNum === 2) {
            notifyPart2Started();
            await new Promise((r) => setTimeout(r, 300));
            if (req.destroyed || res.destroyed) return;
          }
          for await (const _ of req) {
            // drain
          }
          if (uploadSettledAt > 0) {
            partUploadsAfterSettle += 1;
          }
          if (!res.writableEnded && !res.destroyed) {
            res.writeHead(200, { ETag: `"part-${partNum}"` });
            res.end();
          }
          return;
        }
        if (req.method === "DELETE" && url.searchParams.get("uploadId") === "up-iter2-p2") {
          abortCalled = true;
          await new Promise((r) => setTimeout(r, 150));
          abortFinishedAt = performance.now();
          res.writeHead(200);
          res.end();
          return;
        }
        res.writeHead(404);
        res.end();
      });

      try {
        const s3 = new S3StorageClient({
          region: "us-east-1",
          bucket: "test-bucket",
          endpoint: srv.endpoint,
          forcePathStyle: true,
          accessKeyId: "test",
          secretAccessKey: "test",
          partSizeMb: 5,
          queueSize: 1,
        });

        const body = Readable.from([chunk1, chunk2]);
        const ac = new AbortController();
        const uploadPromise = s3
          .upload("big-upload-slow-p2.bin", body, {
            signal: ac.signal,
            expectedSizeBytes: chunk1.length + chunk2.length,
          })
          .finally(() => {
            uploadSettledAt = performance.now();
          });

        await part2StartedPromise;
        ac.abort(new Error("abort during slow part 2"));

        await expect(uploadPromise).rejects.toThrow(/abort/i);
        expect(abortCalled).toBe(true);
        expect(abortFinishedAt).toBeGreaterThan(0);
        expect(uploadSettledAt).toBeGreaterThanOrEqual(abortFinishedAt);

        await new Promise((r) => setTimeout(r, 100));
        expect(partUploadsAfterSettle).toBe(0);
      } finally {
        await srv.close();
      }
    }
  });

  it("[Challenger-Iter2-MissingFirstSeg] prune() promotes post-gap PITR anchor (B_mid) when first segment S1 is absent and S2 records gapDetected + previousEndTs", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-challenger-iter2-missing-seg-"));
    try {
      const storage = new FilesystemStorageClient(dir);
      const logger = makeSilentLogger();
      const scope = { prefix: "mv", database: "*" };
      const config = makeBaseConfig({ retention: { keepLast: 1, keepDaily: 2 } });

      const bOld = "mv/_cluster/1970-01-01/19700101T001640Z.archive.gz";
      const bMid = "mv/_cluster/1970-01-02/19700102T000100Z.archive.gz";
      const bNew = "mv/_cluster/1970-01-02/19700102T000500Z.physical.tar.gz";
      // S1 (1005:1..86420:1) is absent from storage; S2 records gapDetected + previousEndTs
      const s2 = oplogSliceKey(scope, { t: 86500, i: 1 }, { t: 86800, i: 1 });

      await storage.upload(bOld, Buffer.from("b-old"));
      await writeManifest(
        storage,
        makeManifest({
          key: bOld,
          engine: "logical",
          createdAt: "1970-01-01T00:16:40.000Z",
          completedAt: "1970-01-01T00:16:45.000Z",
          oplog: true,
          oplogWindow: { startTs: "1000:1", endTs: "1005:1" },
        }),
      );

      await storage.upload(bMid, Buffer.from("b-mid"));
      await writeManifest(
        storage,
        makeManifest({
          key: bMid,
          engine: "logical",
          createdAt: "1970-01-02T00:01:00.000Z",
          completedAt: "1970-01-02T00:01:05.000Z",
          oplog: true,
          oplogWindow: { startTs: "86460:1", endTs: "86465:1" },
        }),
      );

      await storage.upload(s2, Buffer.from("slice-2"));
      await writeManifest(
        storage,
        makeManifest({
          key: s2,
          type: "oplog-slice",
          oplog: true,
          oplogWindow: {
            startTs: "86500:1",
            endTs: "86800:1",
            previousEndTs: "86420:1",
            gapDetected: true,
          },
        }),
      );

      await storage.upload(bNew, Buffer.from("b-new"));
      await writeManifest(
        storage,
        makeManifest({
          key: bNew,
          engine: "physical",
          createdAt: "1970-01-02T00:05:00.000Z",
          completedAt: "1970-01-02T00:05:05.000Z",
          oplog: false,
          oplogWindow: null,
        }),
      );

      const res = await prune({ config, storage, logger });
      expect(res.keep.map((b) => b.key)).toEqual(expect.arrayContaining([bNew, bMid, bOld]));
      expect(res.remove.map((b) => b.key)).not.toContain(bMid);
      expect(await storage.head(bMid)).not.toBeNull();
      expect(await storage.head(s2)).not.toBeNull();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  it("[F-SCL-022] DigestStream.digest() caches final digest and can be called multiple times safely", async () => {
    const digest = new DigestStream();
    await pipeline(Readable.from([Buffer.from("hello-mongovault")]), digest, async (src) => {
      for await (const _chunk of src) {
        // drain
      }
    });
    const first = digest.digest();
    const second = digest.digest();
    expect(first).toHaveLength(64);
    expect(second).toBe(first);
  });

  it("[F-SCL-023] S3StorageClient.list() strips leading slashes and honors AbortSignal", async () => {
    let seenPrefix = null;
    const server = http.createServer((req, res) => {
      const url = new URL(req.url, "http://127.0.0.1");
      seenPrefix = url.searchParams.get("prefix");
      res.writeHead(200, { "Content-Type": "application/xml" });
      res.end(
        `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name><IsTruncated>false</IsTruncated><Contents><Key>mv/a.txt</Key><Size>1</Size></Contents></ListBucketResult>`,
      );
    });
    await new Promise((r) => server.listen(0, "127.0.0.1", r));
    try {
      const port = server.address().port;
      const s3 = new S3StorageClient({
        bucket: "b",
        region: "us-east-1",
        endpoint: `http://127.0.0.1:${port}`,
        forcePathStyle: true,
        credentials: { accessKeyId: "test", secretAccessKey: "test" },
      });
      const items = [];
      for await (const obj of s3.list("/mv/")) {
        items.push(obj.key);
      }
      expect(seenPrefix).toBe("mv/");
      expect(items).toEqual(["mv/a.txt"]);

      const ac = new AbortController();
      ac.abort(new Error("cancelled"));
      await expect(async () => {
        for await (const _obj of s3.list("mv/", { signal: ac.signal })) {
          // should abort
        }
      }).rejects.toThrow();
    } finally {
      await new Promise((r) => server.close(r));
    }
  });

  it("[F-SCL-024] pruneOplogSlices() compares sub-second ordinal endTs.i and warns on base-backup overlap", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-scl-024-"));
    try {
      const storage = new FilesystemStorageClient(dir);
      const logger = makeSilentLogger();
      const scope = { prefix: "mv", database: "*" };
      const config = makeBaseConfig();

      const b1 = "mv/_cluster/1970-01-01/19700101T000140Z.archive.gz";
      await storage.upload(b1, Buffer.from("base"));
      await writeManifest(
        storage,
        makeManifest({
          key: b1,
          oplogWindow: { startTs: "100:1", endTs: "100:10" },
        }),
      );

      // Slice ending at 100:5 (< cutoff 100:8) overlaps base [100:1..100:10]
      const sOld = oplogSliceKey(scope, { t: 100, i: 1 }, { t: 100, i: 5 });
      await storage.upload(sOld, Buffer.from("s-old"));
      await writeManifest(
        storage,
        makeManifest({ key: sOld, oplogWindow: { startTs: "100:1", endTs: "100:5" } }),
      );

      // Slice ending at 100:12 (>= cutoff 100:8, same second t=100) must be kept
      const sNew = oplogSliceKey(scope, { t: 100, i: 6 }, { t: 100, i: 12 });
      await storage.upload(sNew, Buffer.from("s-new"));
      await writeManifest(
        storage,
        makeManifest({ key: sNew, oplogWindow: { startTs: "100:6", endTs: "100:12" } }),
      );

      const res = await pruneOplogSlices({ config, storage, logger }, { olderThan: "100:8" });
      expect(res.deletedSlices).toEqual([sOld]);
      expect(await storage.head(sNew)).not.toBeNull();
      expect(logger.warn).toHaveBeenCalled();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });
});
