import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import { MongoCursorExhaustedError } from "mongodb";
import { afterEach, describe, expect, it, vi } from "vitest";

const { spawnOverride, mongoClientOverride } = vi.hoisted(() => ({
  spawnOverride: { fn: null },
  mongoClientOverride: { factory: null },
}));

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    spawn: (...args) => (spawnOverride.fn ? spawnOverride.fn(...args) : actual.spawn(...args)),
  };
});

vi.mock("mongodb", async (importOriginal) => {
  const actual = await importOriginal();
  class PatchedMongoClient {
    constructor(uri, opts) {
      if (mongoClientOverride.factory) {
        return mongoClientOverride.factory(uri, opts);
      }
      return new actual.MongoClient(uri, opts);
    }
  }
  return {
    ...actual,
    MongoClient: PatchedMongoClient,
  };
});

import { archiveKey, manifestKey, scopeOf, writeManifest } from "../src/catalog.js";
import { createEncryptor, generateKey } from "../src/crypto.js";
import * as physicalMod from "../src/physical.js";
import {
  PhysicalRecoveryCoordinator,
  createTarHeader,
  createTarPackStream,
  extractTarStream,
  restorePhysicalBackup,
  runPhysicalBackup,
  scanWiredTigerFiles,
  trimDbPathPrefix,
  verifyTarStream,
} from "../src/physical.js";
import { IntegrityError } from "../src/restore.js";
import { createFilesystemStorage } from "../src/storage.js";

const tempDirs = [];

async function makeTempDir(prefix) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function makeLogger() {
  const logger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: () => logger,
  };
  return logger;
}

function sha256Hex(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function buildRawTarHeaderBuffer(name, size, typeflag = "0", prefix = "") {
  const buf = Buffer.alloc(512, 0);
  buf.write(name, 0, 100, "utf8");
  if (prefix) {
    buf.write(prefix, 345, 155, "utf8");
  }
  buf.write("0000644\0", 100, "ascii");
  buf.write("0000000\0", 108, "ascii");
  buf.write("0000000\0", 116, "ascii");
  buf.write(size.toString(8).padStart(11, "0") + "\0", 124, "ascii");
  buf.write("00000000000\0", 136, "ascii");
  buf.write(typeflag, 156, "ascii");
  buf.write("ustar\0", 257, "ascii");
  buf.write("00", 263, "ascii");
  buf.fill(32, 148, 156);
  let sum = 0;
  for (let i = 0; i < 512; i++) sum += buf[i];
  buf.write(sum.toString(8).padStart(6, "0") + "\0 ", 148, "ascii");
  return buf;
}

function padTarBlock(data) {
  const rem = data.length % 512;
  if (rem === 0) return data;
  return Buffer.concat([data, Buffer.alloc(512 - rem, 0)]);
}

async function seedPhysicalBackup(storage, config, files, options = {}) {
  const srcDir = await makeTempDir("mv-phys-seed-");
  const tarEntries = [];
  for (const f of files) {
    const fullPath = path.join(srcDir, f.name);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    const data = Buffer.isBuffer(f.data) ? f.data : Buffer.from(f.data, "utf8");
    await fs.writeFile(fullPath, data);
    tarEntries.push({ name: f.name, fullPath, size: data.length });
  }

  const tarChunks = [];
  for await (const chunk of createTarPackStream(tarEntries)) {
    tarChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  const tarBuf = Buffer.concat(tarChunks);
  const gzBuf = gzipSync(tarBuf);

  let payloadBuf = gzBuf;
  let encryptionEnvelope = null;
  if (options.encryptionKey) {
    const enc = createEncryptor(options.encryptionKey);
    const encChunks = [];
    enc.stream.on("data", (c) => encChunks.push(Buffer.from(c)));
    await new Promise((resolve, reject) => {
      enc.stream.on("error", reject);
      enc.stream.on("end", resolve);
      enc.stream.end(gzBuf);
    });
    payloadBuf = Buffer.concat(encChunks);
    encryptionEnvelope = enc.envelope();
  }

  const date = options.date ?? new Date();
  const key = archiveKey(scopeOf(config), date, {
    encrypted: Boolean(options.encryptionKey),
  }).replace(/\.archive\.gz(\.enc)?$/, ".physical.tar.gz$1");

  await storage.upload(key, Readable.from([payloadBuf]), {
    contentType: options.encryptionKey ? "application/octet-stream" : "application/gzip",
    expectedSizeBytes: payloadBuf.length,
  });

  const manifest = {
    version: 2,
    type: "physical-hot-backup",
    engine: "physical",
    key,
    database: config.mongo.database,
    createdAt: date.toISOString(),
    completedAt: date.toISOString(),
    durationMs: 10,
    size: payloadBuf.length,
    sha256: sha256Hex(payloadBuf),
    compression: "gzip",
    encryption: encryptionEnvelope,
    oplog: false,
    excludedCollections: [],
    collections: {},
    physical: {
      dbPath: srcDir,
      cursorType: "$backupCursor",
      filesCount: tarEntries.length,
      files: tarEntries.map((e) => ({ name: e.name, size: e.size })),
    },
  };

  if (options.skipWriteManifest !== true) {
    await writeManifest(storage, manifest, options.encryptionKey ?? null);
  }

  return { key, manifest, payloadBuf };
}

afterEach(async () => {
  spawnOverride.fn = null;
  mongoClientOverride.factory = null;
  vi.useRealTimers();
  vi.restoreAllMocks();
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
});

describe("physical-engine hardening fail-first suite", () => {
  it("[F-PHY-001] retains rollbackDir across PhysicalRecoveryCoordinator failure and rolls back mid-move rename faults", async () => {
    const workDir = await makeTempDir("mv-phy-001-");
    const storeDir = path.join(workDir, "store");
    const targetDbA = path.join(workDir, "db-a");
    const targetDbB = path.join(workDir, "db-b");
    await fs.mkdir(path.join(targetDbA, "journal"), { recursive: true });
    await fs.writeFile(path.join(targetDbA, "WiredTiger.wt"), "ORIGINAL_WT_DATA");
    await fs.writeFile(
      path.join(targetDbA, "journal", "WiredTigerLog.0000000001"),
      "ORIGINAL_JOURNAL",
    );

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const config = {
      engine: "physical",
      workDir,
      timeoutMs: 30_000,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
    };

    const { key } = await seedPhysicalBackup(storage, config, [
      { name: "WiredTiger.wt", data: "NEW_BACKUP_WT_DATA" },
    ]);

    // Case A: recoveryRunner throws AFTER staged swap — original dbPath must be restored from rollbackDir
    await expect(
      restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: targetDbA,
        ref: key,
        recoveryRunner: async () => {
          throw new Error("Simulated recovery crash");
        },
      }),
    ).rejects.toThrow(/Simulated recovery crash/);

    expect(await fs.readFile(path.join(targetDbA, "WiredTiger.wt"), "utf8")).toBe(
      "ORIGINAL_WT_DATA",
    );
    expect(
      await fs.readFile(path.join(targetDbA, "journal", "WiredTigerLog.0000000001"), "utf8"),
    ).toBe("ORIGINAL_JOURNAL");
    const entriesAfterA = await fs.readdir(targetDbA);
    expect(entriesAfterA.some((e) => e.startsWith(".mongovault-"))).toBe(false);

    // Case B: fs.rename fails on the 2nd entry moved into rollbackDir — already-moved entry must be rolled back
    await fs.mkdir(targetDbB, { recursive: true });
    await fs.writeFile(path.join(targetDbB, "a.wt"), "OLD_A");
    await fs.writeFile(path.join(targetDbB, "b.wt"), "OLD_B");

    const origRename = fs.rename.bind(fs);
    let movesIntoRollback = 0;
    vi.spyOn(fs, "rename").mockImplementation(async (oldPath, newPath) => {
      if (String(newPath).includes(".mongovault-rollback-")) {
        movesIntoRollback++;
        if (movesIntoRollback === 2) {
          throw Object.assign(new Error("Simulated rename EIO"), { code: "EIO" });
        }
      }
      return origRename(oldPath, newPath);
    });

    await expect(
      restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: targetDbB,
        ref: key,
        skipRecovery: true,
      }),
    ).rejects.toThrow(/Simulated rename EIO/);

    expect(await fs.readFile(path.join(targetDbB, "a.wt"), "utf8")).toBe("OLD_A");
    expect(await fs.readFile(path.join(targetDbB, "b.wt"), "utf8")).toBe("OLD_B");
    const entriesAfterB = await fs.readdir(targetDbB);
    expect(entriesAfterB.some((e) => e.startsWith(".mongovault-"))).toBe(false);
  });

  it("[F-PHY-002] guards destructive 1x direct mode behind explicit opt-in, validates TAR in zero-disk dry-run, and re-checks mongod.lock before 1x wipe", async () => {
    const workDir = await makeTempDir("mv-phy-002-");
    const storeDir = path.join(workDir, "store");
    const dstDb = path.join(workDir, "dst-db");
    await fs.mkdir(dstDb, { recursive: true });
    await fs.writeFile(path.join(dstDb, "old.wt"), Buffer.alloc(600, 0x42));

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const config = {
      engine: "physical",
      workDir,
      timeoutMs: 30_000,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
    };

    const { key: validKey } = await seedPhysicalBackup(storage, config, [
      { name: "WiredTiger.wt", data: Buffer.alloc(1000, 0x41) },
    ]);

    const statfsSpy = vi.spyOn(fs, "statfs").mockResolvedValue({
      bavail: 700,
      bsize: 1,
    });

    // Case A: 1x direct mode is needed (700 < 1100, 700 + 600 >= 1100), but allowDirectMode is NOT opted in
    await expect(
      restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: dstDb,
        ref: validKey,
        skipRecovery: true,
      }),
    ).rejects.toThrow(/allowDirectMode|direct mode/i);
    expect(await fs.readFile(path.join(dstDb, "old.wt"))).toHaveLength(600);

    // Case B: allowDirectMode: true, valid gzip + sha256 wrapping a CORRUPTED TAR checksum -> must fail in dry-run BEFORE wiping old.wt
    const badTarHeader = createTarHeader("WiredTiger.wt", 10);
    badTarHeader[148] = 0x58; // corrupt checksum
    const badTarStreamBytes = Buffer.concat([
      badTarHeader,
      Buffer.alloc(512, 0x41),
      Buffer.alloc(1024, 0),
    ]);
    const badGz = gzipSync(badTarStreamBytes);
    const badKey = "mv/_cluster/2026-11-26/20261126T000001Z.physical.tar.gz";
    await storage.upload(badKey, Readable.from([badGz]), {
      contentType: "application/gzip",
      expectedSizeBytes: badGz.length,
    });
    await writeManifest(storage, {
      version: 2,
      type: "physical-hot-backup",
      engine: "physical",
      key: badKey,
      database: "*",
      createdAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      durationMs: 5,
      size: badGz.length,
      sha256: sha256Hex(badGz),
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: {},
      physical: {
        dbPath: "/tmp/ignored",
        cursorType: "$backupCursor",
        filesCount: 1,
        files: [{ name: "WiredTiger.wt", size: 1000 }],
      },
    });

    await expect(
      restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: dstDb,
        ref: badKey,
        skipRecovery: true,
        allowDirectMode: true,
      }),
    ).rejects.toThrow(/checksum mismatch|corrupted tar/i);
    expect(await fs.readFile(path.join(dstDb, "old.wt"))).toHaveLength(600);

    // Case C: allowDirectMode: true, valid archive, mongod.lock appears during zero-disk pre-verify -> must fail before wiping old.wt
    const lockRaceStorage = {
      ...storage,
      getJson: storage.getJson.bind(storage),
      list: storage.list.bind(storage),
      download: vi.fn(async (k, opts) => {
        await fs.writeFile(path.join(dstDb, "mongod.lock"), "99999\n");
        return storage.download(k, opts);
      }),
    };

    await expect(
      restorePhysicalBackup({
        config,
        storage: lockRaceStorage,
        logger,
        targetDbPath: dstDb,
        ref: validKey,
        skipRecovery: true,
        allowDirectMode: true,
      }),
    ).rejects.toThrow(/running mongod/i);
    expect(await fs.readFile(path.join(dstDb, "old.wt"))).toHaveLength(600);
    statfsSpy.mockRestore();
  });

  it("[F-PHY-003] drains finite $backupCursorExtend without MongoCursorExhaustedError and serializes keepAliveTimer tryNext() calls", async () => {
    const workDir = await makeTempDir("mv-phy-003-");
    const dbPath = path.join(workDir, "db");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(path.join(dbPath, "journal"), { recursive: true });
    const wtFile = path.join(dbPath, "WiredTiger.wt");
    const jrnFile = path.join(dbPath, "journal", "WiredTigerLog.0000000001");
    await fs.writeFile(wtFile, "WT_CONTENT");
    await fs.writeFile(jrnFile, "JOURNAL_CONTENT");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const config = {
      engine: "physical",
      workDir,
      timeoutMs: 180_000,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
    };

    // Part (a): Finite $backupCursorExtend cursor throws MongoCursorExhaustedError if tryNext() is called past cursorId === 0
    const makePartAClient = () => ({
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: (name) => {
        if (name !== "admin") return {};
        return {
          command: vi.fn(async () => ({ ok: 1 })),
          aggregate: (pipeline) => {
            if (pipeline[0]?.$backupCursor) {
              const docs = [
                {
                  metadata: {
                    backupId: "b-ext-1",
                    dbpath: dbPath,
                    checkpointTimestamp: { t: 1700000000, i: 1 },
                  },
                },
                { filename: wtFile, fileSize: 10 },
              ];
              let idx = 0;
              return {
                closed: false,
                next: vi.fn(async () => docs[idx++] ?? null),
                tryNext: vi.fn(async () => docs[idx++] ?? null),
                close: vi.fn(async function () {
                  this.closed = true;
                }),
              };
            }
            if (pipeline[0]?.$backupCursorExtend) {
              let tryCalls = 0;
              return {
                closed: false,
                tryNext: vi.fn(async () => {
                  tryCalls++;
                  if (tryCalls === 1) return { filename: jrnFile, fileSize: 15 };
                  throw new MongoCursorExhaustedError();
                }),
                async *[Symbol.asyncIterator]() {
                  yield { filename: jrnFile, fileSize: 15 };
                },
                close: vi.fn(async function () {
                  this.closed = true;
                }),
              };
            }
            throw new Error("Unexpected pipeline");
          },
        };
      },
    });

    const resA = await runPhysicalBackup({
      config,
      storage,
      logger,
      dbPath,
      createMongoClient: makePartAClient,
    });
    expect(resA.filesCount).toBe(2);

    // Part (b): Overlapping keepAliveTimer tryNext() calls must be serialized (maxInFlight === 1)
    vi.useFakeTimers({
      toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"],
    });

    let inFlight = 0;
    let maxInFlight = 0;
    let cursorDocsIdx = 0;
    const cursorDocs = [
      {
        metadata: {
          backupId: "b-keepalive-1",
          dbpath: dbPath,
          checkpointTimestamp: { t: 1700000010, i: 1 },
        },
      },
      { filename: wtFile, fileSize: 10 },
    ];

    const makePartBClient = () => ({
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: (name) => {
        if (name !== "admin") return {};
        return {
          command: vi.fn(async () => ({ ok: 1 })),
          aggregate: (pipeline) => {
            if (pipeline[0]?.$backupCursor) {
              return {
                closed: false,
                next: vi.fn(async () => cursorDocs[cursorDocsIdx++] ?? null),
                tryNext: vi.fn(async () => {
                  // Initial drain returns wtFile then null immediately
                  if (cursorDocsIdx < cursorDocs.length) {
                    return cursorDocs[cursorDocsIdx++];
                  }
                  if (cursorDocsIdx === cursorDocs.length) {
                    cursorDocsIdx++;
                    return null;
                  }
                  // Subsequent calls come from keepAliveTimer
                  inFlight++;
                  maxInFlight = Math.max(maxInFlight, inFlight);
                  if (inFlight > 1) {
                    inFlight--;
                    throw new Error("Overlapping tryNext() on cursor");
                  }
                  await new Promise((r) => setTimeout(r, 70_000));
                  inFlight--;
                  return null;
                }),
                close: vi.fn(async function () {
                  this.closed = true;
                }),
              };
            }
            if (pipeline[0]?.$backupCursorExtend) {
              return {
                async *[Symbol.asyncIterator]() {
                  yield { filename: jrnFile, fileSize: 15 };
                },
                close: vi.fn(async () => {}),
              };
            }
            throw new Error("Unexpected pipeline");
          },
        };
      },
    });

    let notifyUploadStarted;
    const uploadStarted = new Promise((resolve) => {
      notifyUploadStarted = resolve;
    });
    const slowStorage = {
      ...storage,
      upload: vi.fn(async (_key, body, { signal }) => {
        for await (const _chunk of body) {
          // drain stream
        }
        notifyUploadStarted();
        await new Promise((resolve, reject) => {
          const timer = setTimeout(resolve, 110_000);
          signal?.addEventListener(
            "abort",
            () => {
              clearTimeout(timer);
              reject(signal.reason ?? new Error("aborted"));
            },
            { once: true },
          );
        });
      }),
      head: vi.fn(async () => null),
      putJson: vi.fn(async () => {}),
    };

    const backupPromise = runPhysicalBackup({
      config,
      storage: slowStorage,
      logger,
      dbPath,
      createMongoClient: makePartBClient,
    });
    // Prevent unhandled rejection warning if backup rejects before await
    const settledPromise = backupPromise.then(
      (value) => ({ ok: true, value }),
      (error) => ({ ok: false, error }),
    );

    await uploadStarted;
    await vi.advanceTimersByTimeAsync(45_000);
    await vi.advanceTimersByTimeAsync(45_000);
    await vi.advanceTimersByTimeAsync(30_000);

    const outcome = await settledPromise;
    expect(outcome.ok).toBe(true);
    expect(maxInFlight).toBe(1);
  });

  it("[F-PHY-004] pins physical backup and fsyncUnlock retries to the selected secondary node honoring readPreference", async () => {
    const workDir = await makeTempDir("mv-phy-004-");
    const dbPath = path.join(workDir, "db");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(dbPath, { recursive: true });
    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT_DATA");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const createdUris = [];
    const primaryCommands = [];
    const secondaryCommands = [];
    let secondaryUnlockAttempts = 0;

    const createMongoClient = (uri) => {
      createdUris.push(uri);
      const [beforeQuery, queryStr = ""] = uri.split("?");
      const params = new URLSearchParams(queryStr);
      const isDirect = params.get("directConnection") === "true";
      const authority = beforeQuery.replace(/^mongodb(?:\+srv)?:\/\//, "").split("/")[0];
      const hostPart = authority.includes("@") ? authority.split("@")[1] : authority;
      const hosts = hostPart.split(",");
      const pinnedToSecondary = isDirect && hosts.length === 1 && hosts[0] === "n2:27017";
      const pinnedToPrimary = isDirect && hosts.length === 1 && hosts[0] === "n1:27017";

      return {
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: (dbName) => {
          if (dbName === "local") {
            return {
              collection: () => ({
                find: () => ({
                  sort: () => ({
                    limit: () => ({ next: vi.fn(async () => ({ ts: { t: 1000, i: 1 } })) }),
                  }),
                }),
              }),
            };
          }
          if (dbName === "config") {
            return {
              collection: () => ({
                countDocuments: vi.fn(async () => 0),
              }),
            };
          }
          return {
            aggregate: () => ({
              next: vi.fn(async () => {
                throw Object.assign(new Error("Command $backupCursor not found"), { code: 59 });
              }),
              close: vi.fn(async () => {}),
            }),
            command: vi.fn(async (cmd, opts) => {
              const cmdName = Object.keys(cmd)[0];
              if (cmd.hello) {
                const useSecondary =
                  pinnedToSecondary || (!pinnedToPrimary && opts?.readPreference === "secondary");
                if (useSecondary) {
                  secondaryCommands.push(cmdName);
                  return {
                    ok: 1,
                    setName: "rs0",
                    isWritablePrimary: false,
                    secondary: true,
                    me: "n2:27017",
                    hosts: ["n1:27017", "n2:27017"],
                  };
                }
                primaryCommands.push(cmdName);
                return {
                  ok: 1,
                  setName: "rs0",
                  isWritablePrimary: true,
                  me: "n1:27017",
                  hosts: ["n1:27017", "n2:27017"],
                };
              }

              if (!pinnedToSecondary) {
                primaryCommands.push(cmdName);
              } else {
                secondaryCommands.push(cmdName);
              }

              if (cmd.buildInfo) return { version: "8.0.0" };
              if (cmd.getCmdLineOpts) return { parsed: { storage: { dbPath } } };
              if (cmd.fsync) return { ok: 1 };
              if (cmd.fsyncUnlock) {
                secondaryUnlockAttempts++;
                if (secondaryUnlockAttempts === 1) {
                  throw new Error("transient unlock disconnect");
                }
                return { ok: 1, lockCount: 0 };
              }
              return { ok: 1 };
            }),
          };
        },
      };
    };

    const res = await runPhysicalBackup({
      config: {
        engine: "physical",
        workDir,
        timeoutMs: 30_000,
        storage: { prefix: "mv" },
        mongo: {
          uri: "mongodb://n1:27017,n2:27017/?replicaSet=rs0",
          database: "*",
          readPreference: "secondary",
        },
        physical: { dbPath, allowFsyncLock: true, allowPrimaryFsyncLock: false },
      },
      storage,
      logger,
      createMongoClient,
    });

    expect(res.manifest.physical?.cursorType).toBe("fsyncLock");
    expect(primaryCommands).not.toContain("fsync");
    expect(secondaryCommands).toContain("fsync");
    expect(secondaryCommands).toContain("fsyncUnlock");
    // Must pin to n2:27017 with directConnection=true and use that same pinned URI for fsyncUnlock retry
    expect(createdUris.length).toBeGreaterThanOrEqual(3);
    for (const postDiscoveryUri of createdUris.slice(1)) {
      expect(postDiscoveryUri).toContain("n2:27017");
      expect(postDiscoveryUri).not.toContain("n1:27017");
      expect(postDiscoveryUri).toContain("directConnection=true");
    }
  });

  it("[F-PHY-005] registers SIGINT/SIGTERM cleanup guard while fsyncLock is held so fsyncUnlock runs before exit", async () => {
    const workDir = await makeTempDir("mv-phy-005-");
    const dbPath = path.join(workDir, "db");
    await fs.mkdir(dbPath, { recursive: true });
    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT_DATA");

    const logger = makeLogger();
    const registered = new Map();

    const origOn = process.on.bind(process);
    const origOnce = process.once.bind(process);
    const origRemove = process.removeListener.bind(process);
    const origOff = process.off.bind(process);

    const addCaptured = (event, fn) => {
      if (!registered.has(event)) registered.set(event, new Set());
      registered.get(event).add(fn);
    };
    const removeCaptured = (event, fn) => {
      registered.get(event)?.delete(fn);
      if (registered.get(event)?.size === 0) registered.delete(event);
    };

    vi.spyOn(process, "on").mockImplementation((event, fn) => {
      if (event === "SIGINT" || event === "SIGTERM") {
        addCaptured(event, fn);
        return process;
      }
      return origOn(event, fn);
    });
    vi.spyOn(process, "once").mockImplementation((event, fn) => {
      if (event === "SIGINT" || event === "SIGTERM") {
        addCaptured(event, fn);
        return process;
      }
      return origOnce(event, fn);
    });
    vi.spyOn(process, "removeListener").mockImplementation((event, fn) => {
      if (event === "SIGINT" || event === "SIGTERM") {
        removeCaptured(event, fn);
        return process;
      }
      return origRemove(event, fn);
    });
    vi.spyOn(process, "off").mockImplementation((event, fn) => {
      if (event === "SIGINT" || event === "SIGTERM") {
        removeCaptured(event, fn);
        return process;
      }
      return origOff(event, fn);
    });

    let unlocked = false;
    const fakeClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: (name) => {
        if (name === "local") {
          return {
            collection: () => ({
              find: () => ({
                sort: () => ({ limit: () => ({ next: vi.fn(async () => null) }) }),
              }),
            }),
          };
        }
        if (name === "config") {
          return { collection: () => ({ countDocuments: vi.fn(async () => 0) }) };
        }
        return {
          aggregate: () => ({
            next: vi.fn(async () => {
              throw Object.assign(new Error("Command $backupCursor not found"), { code: 59 });
            }),
            close: vi.fn(async () => {}),
          }),
          command: vi.fn(async (cmd) => {
            if (cmd.buildInfo) return { version: "8.0.0" };
            if (cmd.hello) return { ok: 1 };
            if (cmd.fsync) return { ok: 1 };
            if (cmd.fsyncUnlock) {
              unlocked = true;
              return { ok: 1, lockCount: 0 };
            }
            return { ok: 1 };
          }),
        };
      },
    };

    let notifyUploadStarted;
    const uploadStarted = new Promise((resolve) => {
      notifyUploadStarted = resolve;
    });
    const blockingStorage = {
      bucket: "test",
      upload: vi.fn((_key, _stream, { signal }) => {
        notifyUploadStarted();
        return new Promise((_, reject) => {
          signal?.addEventListener("abort", () => reject(signal.reason ?? new Error("aborted")), {
            once: true,
          });
        });
      }),
      remove: vi.fn(async () => {}),
    };

    const ac = new AbortController();
    const backupPromise = runPhysicalBackup({
      config: {
        engine: "physical",
        workDir,
        timeoutMs: 30_000,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
        physical: { dbPath, allowFsyncLock: true },
      },
      storage: blockingStorage,
      logger,
      signal: ac.signal,
      createMongoClient: () => fakeClient,
    });
    const settled = backupPromise.then(
      (value) => ({ ok: true, value }),
      (error) => ({ ok: false, error }),
    );

    try {
      await uploadStarted;
      const sigintHandlers = [...(registered.get("SIGINT") ?? [])];
      const sigtermHandlers = [...(registered.get("SIGTERM") ?? [])];
      expect(sigintHandlers.length).toBeGreaterThan(0);
      expect(sigtermHandlers.length).toBeGreaterThan(0);

      sigintHandlers[0]("SIGINT");
      const outcome = await settled;
      expect(outcome.ok).toBe(false);
      expect(outcome.error?.message).toMatch(/SIGINT/i);
      expect(unlocked).toBe(true);
      expect(registered.has("SIGINT")).toBe(false);
      expect(registered.has("SIGTERM")).toBe(false);
    } finally {
      ac.abort(new Error("test cleanup abort"));
      await settled;
    }
  });

  it("[F-PHY-006] enforces physical manifest presence/engine/sha256/HMAC integrity and filters ref='latest' to physical backups", async () => {
    const workDir = await makeTempDir("mv-phy-006-");
    const storeDir = path.join(workDir, "store");
    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const encKey = generateKey();

    const baseConfig = {
      engine: "physical",
      workDir,
      timeoutMs: 30_000,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
    };

    // (A) Missing manifest (null) must fail with IntegrityError BEFORE creating targetDbPath
    const nonExistentTarget = path.join(workDir, "never-created-db");
    await expect(
      restorePhysicalBackup({
        config: baseConfig,
        storage,
        logger,
        targetDbPath: nonExistentTarget,
        ref: "mv/_cluster/2026-11-26/missing.physical.tar.gz",
        skipRecovery: true,
      }),
    ).rejects.toThrow(IntegrityError);
    await expect(fs.stat(nonExistentTarget)).rejects.toThrow();

    // (B) Wrong engine ("logical") or missing sha256 must fail with IntegrityError
    const logicalKey = "mv/_cluster/2026-11-26/20261126T000100Z.archive.gz";
    await storage.putJson(manifestKey(logicalKey), {
      version: 2,
      type: "logical-backup",
      engine: "logical",
      key: logicalKey,
      database: "*",
      createdAt: new Date().toISOString(),
      size: 10,
      sha256: "abc",
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: {},
    });
    await expect(
      restorePhysicalBackup({
        config: baseConfig,
        storage,
        logger,
        targetDbPath: nonExistentTarget,
        ref: logicalKey,
        skipRecovery: true,
      }),
    ).rejects.toThrow(IntegrityError);

    // (C) Encrypted physical backup with stripped signature & manifestHmac must fail with IntegrityError (F-SCL-003)
    const { key: encPhysKey, manifest: encManifest } = await seedPhysicalBackup(
      storage,
      baseConfig,
      [{ name: "WiredTiger.wt", data: "ENCRYPTED_WT" }],
      { encryptionKey: encKey, skipWriteManifest: true, date: new Date("2026-11-26T00:02:00Z") },
    );
    const strippedManifest = {
      ...encManifest,
      encryption: encManifest.encryption
        ? { ...encManifest.encryption, manifestHmac: undefined }
        : null,
    };
    delete strippedManifest.signature;
    await storage.putJson(manifestKey(encPhysKey), strippedManifest);

    await expect(
      restorePhysicalBackup({
        config: { ...baseConfig, encryptionKey: encKey },
        storage,
        logger,
        targetDbPath: path.join(workDir, "dst-stripped"),
        ref: encPhysKey,
        skipRecovery: true,
      }),
    ).rejects.toThrow(IntegrityError);

    // (D) ref="latest" must pick older .physical.tar.gz rather than newer .archive.gz logical backup
    const olderPhysDate = new Date("2026-11-26T01:00:00Z");
    const newerLogicalDate = new Date("2026-11-26T02:00:00Z");
    await seedPhysicalBackup(
      storage,
      baseConfig,
      [{ name: "WiredTiger.wt", data: "OLDER_PHYS_OK" }],
      { date: olderPhysDate },
    );
    const newerLogicalKey = archiveKey(scopeOf(baseConfig), newerLogicalDate, { encrypted: false });
    const fakeArchiveBytes = gzipSync(Buffer.from("NOT_A_TAR_LOGICAL_ARCHIVE"));
    await storage.upload(newerLogicalKey, Readable.from([fakeArchiveBytes]), {
      contentType: "application/gzip",
      expectedSizeBytes: fakeArchiveBytes.length,
    });
    await writeManifest(storage, {
      version: 2,
      type: "logical-backup",
      engine: "logical",
      key: newerLogicalKey,
      database: "*",
      createdAt: newerLogicalDate.toISOString(),
      completedAt: newerLogicalDate.toISOString(),
      durationMs: 5,
      size: fakeArchiveBytes.length,
      sha256: sha256Hex(fakeArchiveBytes),
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: {},
    });

    const latestDst = path.join(workDir, "dst-latest");
    const latestRes = await restorePhysicalBackup({
      config: baseConfig,
      storage,
      logger,
      targetDbPath: latestDst,
      ref: "latest",
      skipRecovery: true,
    });
    expect(latestRes.restoredFiles).toBe(1);
    expect(await fs.readFile(path.join(latestDst, "WiredTiger.wt"), "utf8")).toBe("OLDER_PHYS_OK");
  });

  it("[F-PHY-007] deletes local.replset.election in Step 1 and sets replset.minvalid.ts to Timestamp(0, 1)", async () => {
    const workDir = await makeTempDir("mv-phy-007-");
    const coordinator = new PhysicalRecoveryCoordinator();
    const stepOps = [];
    let currentStep = 0;

    vi.spyOn(coordinator, "runWithEphemeralMongod").mockImplementation(
      async (_wd, _dbPath, _args, fn) => {
        currentStep++;
        const step = currentStep;
        const fakeClient = {
          db: (dbName) => ({
            command: vi.fn(async () => ({ ok: 1 })),
            listCollections: () => ({ toArray: vi.fn(async () => []) }),
            collection: (coll) => ({
              deleteMany: vi.fn(async (filter) => {
                stepOps.push({ step, op: "deleteMany", ns: `${dbName}.${coll}`, filter });
                return { deletedCount: 1 };
              }),
              insertOne: vi.fn(async (doc) => {
                stepOps.push({ step, op: "insertOne", ns: `${dbName}.${coll}`, doc });
                return { acknowledged: true };
              }),
              drop: vi.fn(async () => ({})),
            }),
          }),
        };
        await fn(fakeClient);
      },
    );

    await coordinator.run({
      config: {
        workDir,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
      },
      storage: {},
      logger: makeLogger(),
      targetDbPath: workDir,
      manifest: {
        physical: {
          cursorType: "$backupCursor",
          oplogEnd: "1000:5",
        },
      },
      pitrPlan: null,
    });

    const step1ElectionDeletes = stepOps.filter(
      (op) => op.step === 1 && op.op === "deleteMany" && op.ns === "local.replset.election",
    );
    expect(step1ElectionDeletes.length).toBeGreaterThanOrEqual(1);

    const step1Minvalid = stepOps.find(
      (op) => op.step === 1 && op.op === "insertOne" && op.ns === "local.replset.minvalid",
    );
    expect(step1Minvalid).toBeDefined();
    expect(step1Minvalid.doc.ts.t).toBe(0);
    expect(step1Minvalid.doc.ts.i).toBe(1);
  });

  it("[F-PHY-008] excludes volatile diagnostic.data/lock/staging files and traverses symlinks safely without cycles", async () => {
    const workDir = await makeTempDir("mv-phy-008-");
    const dbPath = path.join(workDir, "db");
    const extJournal = path.join(workDir, "external-journal");

    await fs.mkdir(path.join(dbPath, "diagnostic.data"), { recursive: true });
    await fs.mkdir(path.join(dbPath, "_tmp"), { recursive: true });
    await fs.mkdir(path.join(dbPath, ".mongovault-staging-1"), { recursive: true });
    await fs.mkdir(extJournal, { recursive: true });

    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT");
    await fs.writeFile(path.join(dbPath, "diagnostic.data", "metrics.interim"), "FTDC");
    await fs.writeFile(path.join(dbPath, "WiredTiger.lock"), "lock");
    await fs.writeFile(path.join(dbPath, "ongoingBackup.lock"), "lock");
    await fs.writeFile(path.join(dbPath, "_tmp", "x.wt"), "tmp");
    await fs.writeFile(path.join(dbPath, ".mongovault-staging-1", "y.wt"), "stg");
    await fs.writeFile(path.join(extJournal, "WiredTigerLog.0000000001"), "JRN");

    await fs.symlink(extJournal, path.join(dbPath, "journal"), "dir");
    await fs.symlink(dbPath, path.join(dbPath, "loop"), "dir");

    const scanned = await scanWiredTigerFiles(dbPath);
    const names = scanned.map((f) => f.name).sort();
    expect(names).toEqual(["WiredTiger.wt", "journal/WiredTigerLog.0000000001"]);
  });

  it("[F-PHY-009] canonicalizes symlinked/non-normalized dbPath in trimDbPathPrefix and rejects out-of-dbPath paths", async () => {
    const workDir = await makeTempDir("mv-phy-009-");
    const realDb = path.join(workDir, "real-db");
    const symlinkDb = path.join(workDir, "link-db");
    await fs.mkdir(path.join(realDb, "journal"), { recursive: true });
    await fs.writeFile(path.join(realDb, "journal", "WiredTigerLog.0000000001"), "JRN");
    await fs.symlink(realDb, symlinkDb, "dir");

    expect(
      trimDbPathPrefix(path.join(realDb, "journal", "WiredTigerLog.0000000001"), symlinkDb),
    ).toBe("journal/WiredTigerLog.0000000001");

    expect(
      trimDbPathPrefix("/var/lib/mongodb/journal/WiredTigerLog.0000000001", "/var/lib/mongodb/."),
    ).toBe("journal/WiredTigerLog.0000000001");

    expect(() => trimDbPathPrefix("/etc/passwd", "/var/lib/mongodb")).toThrow();
  });

  it("[F-PHY-010] rejects traversal entries, parses PAX size=, enforces typeflag whitelist, and requires 2x 512B zero EOF blocks", async () => {
    const workDir = await makeTempDir("mv-phy-010-");

    // (A) Path traversal entries must throw and never write files
    for (const evilName of ["../../etc/passwd", "a/../../etc/passwd", "/etc/passwd"]) {
      const hdr = buildRawTarHeaderBuffer(evilName, 3, "0");
      const payload = padTarBlock(Buffer.from("123", "utf8"));
      const stream = Readable.from([Buffer.concat([hdr, payload, Buffer.alloc(1024, 0)])]);
      const outDir = path.join(workDir, "out-a");
      await expect(extractTarStream(stream, outDir)).rejects.toThrow(/escapes target directory/i);
      await expect(fs.stat(path.join(outDir, "etc", "passwd"))).rejects.toThrow();
    }

    // (B) PAX extended header size=<n> overrides UStar size=0
    const paxBody = Buffer.from("10 size=7\n", "utf8");
    const paxHdr = buildRawTarHeaderBuffer("PaxHeaders/file.wt", paxBody.length, "x");
    const fileHdrZero = buildRawTarHeaderBuffer("pax-sized.wt", 0, "0");
    const filePayload = padTarBlock(Buffer.from("PAXSIZE", "utf8"));
    const paxStream = Readable.from([
      Buffer.concat([
        paxHdr,
        padTarBlock(paxBody),
        fileHdrZero,
        filePayload,
        Buffer.alloc(1024, 0),
      ]),
    ]);
    const outB = path.join(workDir, "out-b");
    const resB = await extractTarStream(paxStream, outB);
    expect(resB.extractedCount).toBe(1);
    expect(await fs.readFile(path.join(outB, "pax-sized.wt"), "utf8")).toBe("PAXSIZE");

    // (C) Reject unsupported typeflags ('1', '2', 'L') and skip 'g' global PAX header cleanly
    for (const badType of ["2", "1", "L"]) {
      const hdr = buildRawTarHeaderBuffer("bad-entry", 4, badType);
      const stream = Readable.from([
        Buffer.concat([hdr, padTarBlock(Buffer.from("DATA")), Buffer.alloc(1024, 0)]),
      ]);
      await expect(
        extractTarStream(stream, path.join(workDir, `out-c-${badType}`)),
      ).rejects.toThrow(/unsupported tar entry typeflag/i);
    }

    const gPayload = Buffer.from("15 comment=hi\n", "utf8");
    const gHdr = buildRawTarHeaderBuffer("pax_global_header", gPayload.length, "g");
    const gStream = Readable.from([
      Buffer.concat([gHdr, padTarBlock(gPayload), Buffer.alloc(1024, 0)]),
    ]);
    const outG = path.join(workDir, "out-g");
    const resG = await extractTarStream(gStream, outG);
    expect(resG.extractedCount).toBe(0);
    await expect(fs.stat(path.join(outG, "pax_global_header"))).rejects.toThrow();

    // (D) Single 512B zero block at EOF (missing 2nd zero block) must be rejected as truncated
    const validHdr = buildRawTarHeaderBuffer("ok.wt", 3, "0");
    const validBody = padTarBlock(Buffer.from("ABC", "utf8"));
    const oneZeroEofStream = Readable.from([
      Buffer.concat([validHdr, validBody, Buffer.alloc(512, 0)]),
    ]);
    await expect(extractTarStream(oneZeroEofStream, path.join(workDir, "out-d"))).rejects.toThrow(
      /truncated tar archive/i,
    );
  });

  it("[F-PHY-011] enforces dbPath co-location and restricts fsyncLock fallback to capability errors", async () => {
    const workDir = await makeTempDir("mv-phy-011-");
    const dirA = path.join(workDir, "dir-a");
    const dirB = path.join(workDir, "dir-b");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(dirA, { recursive: true });
    await fs.mkdir(dirB, { recursive: true });
    await fs.writeFile(path.join(dirA, "WiredTiger.wt"), "WT_A");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const baseConfig = {
      engine: "physical",
      workDir,
      timeoutMs: 30_000,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
    };

    // (A) Explicit dbPath differs from $backupCursor metadata.dbpath -> must reject
    await expect(
      runPhysicalBackup({
        config: baseConfig,
        storage,
        logger,
        dbPath: dirA,
        createMongoClient: () => ({
          connect: vi.fn(async () => {}),
          close: vi.fn(async () => {}),
          db: () => ({
            command: vi.fn(async () => ({ ok: 1 })),
            aggregate: () => {
              const docs = [
                {
                  metadata: {
                    backupId: "b-mismatch",
                    dbpath: dirB,
                    checkpointTimestamp: { t: 100, i: 1 },
                  },
                },
                { filename: "WiredTiger.wt" },
              ];
              let i = 0;
              return {
                next: vi.fn(async () => docs[i++] ?? null),
                tryNext: vi.fn(async () => docs[i++] ?? null),
                close: vi.fn(async () => {}),
              };
            },
          }),
        }),
      }),
    ).rejects.toThrow(/dbPath mismatch/i);

    // (B) Auth error (code: 13) on $backupCursor must NOT fall back to fsyncLock even if fsyncLockFallback=true
    let fsyncCalledB = false;
    await expect(
      runPhysicalBackup({
        config: baseConfig,
        storage,
        logger,
        dbPath: dirA,
        fsyncLockFallback: true,
        createMongoClient: () => ({
          connect: vi.fn(async () => {}),
          close: vi.fn(async () => {}),
          db: () => ({
            aggregate: () => ({
              next: vi.fn(async () => {
                throw Object.assign(new Error("not authorized on admin to execute command"), {
                  code: 13,
                });
              }),
              close: vi.fn(async () => {}),
            }),
            command: vi.fn(async (cmd) => {
              if (cmd.fsync) fsyncCalledB = true;
              if (cmd.buildInfo) return { version: "8.0.0" };
              if (cmd.hello) return { ok: 1 };
              return { ok: 1 };
            }),
          }),
        }),
      }),
    ).rejects.toThrow(/not authorized/i);
    expect(fsyncCalledB).toBe(false);

    // (C) Retriable checkpoint error (code: 50915) exhausting retries must NOT fall back to fsyncLock
    vi.useFakeTimers({
      toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"],
    });
    let fsyncCalledC = false;
    const retryPromise = runPhysicalBackup({
      config: baseConfig,
      storage,
      logger,
      dbPath: dirA,
      fsyncLockFallback: true,
      createMongoClient: () => ({
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: () => ({
          aggregate: () => ({
            next: vi.fn(async () => {
              throw Object.assign(new Error("BackupCursorOpenConflictWithCheckpoint"), {
                code: 50915,
              });
            }),
            close: vi.fn(async () => {}),
          }),
          command: vi.fn(async (cmd) => {
            if (cmd.fsync) fsyncCalledC = true;
            if (cmd.buildInfo) return { version: "8.0.0" };
            if (cmd.hello) return { ok: 1 };
            return { ok: 1 };
          }),
        }),
      }),
    });
    const retrySettled = retryPromise.then(
      (v) => ({ ok: true, v }),
      (e) => ({ ok: false, e }),
    );
    for (let i = 0; i < 12; i++) {
      await vi.advanceTimersByTimeAsync(15_000);
    }
    const retryOutcome = await retrySettled;
    expect(retryOutcome.ok).toBe(false);
    expect(retryOutcome.e?.message).toMatch(/BackupCursorOpenConflictWithCheckpoint/i);
    expect(fsyncCalledC).toBe(false);
  });

  it("[F-PHY-012] propagates AbortSignal into ephemeral mongod recovery, rejects on waitForLockFileCleared timeout, passes required mongod args, and validates encryptionKeyFile", async () => {
    const workDir = await makeTempDir("mv-phy-012-");
    const dbDir = path.join(workDir, "db");
    const lockDir = path.join(workDir, "lock-dir");
    await fs.mkdir(dbDir, { recursive: true });
    await fs.mkdir(lockDir, { recursive: true });
    await fs.writeFile(path.join(lockDir, "mongod.lock"), "12345\n");

    const logger = makeLogger();

    // (A) AbortSignal propagation into runWithEphemeralMongod / waitForMongodReady
    let spawnedProc = null;
    spawnOverride.fn = () => {
      const proc = new EventEmitter();
      proc.exitCode = null;
      proc.signalCode = null;
      proc.kill = vi.fn((sig = "SIGTERM") => {
        proc.signalCode = sig;
        proc.emit("close", null, sig);
        return true;
      });
      spawnedProc = proc;
      return proc;
    };
    mongoClientOverride.factory = () => ({
      connect: vi.fn(async () => {
        throw new Error("ECONNREFUSED");
      }),
      close: vi.fn(async () => {}),
      db: () => ({ command: vi.fn(async () => ({ ok: 1 })) }),
    });

    const coordinatorA = new PhysicalRecoveryCoordinator();
    const ac = new AbortController();
    const abortTimer = setTimeout(() => ac.abort(new Error("aborted by test")), 40);

    const runPromise = coordinatorA.run({
      config: {
        workDir,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
      },
      storage: {},
      logger,
      targetDbPath: dbDir,
      manifest: { physical: { cursorType: "fsyncLock" } },
      pitrPlan: null,
      signal: ac.signal,
    });
    const runSettled = runPromise.then(
      (v) => ({ status: "resolved", v }),
      (e) => ({ status: "rejected", e }),
    );

    try {
      const raced = await Promise.race([
        runSettled,
        new Promise((resolve) => setTimeout(() => resolve({ status: "HANG_TIMEOUT" }), 350)),
      ]);
      expect(raced.status).toBe("rejected");
      expect(spawnedProc?.kill).toHaveBeenCalledWith("SIGTERM");
    } finally {
      clearTimeout(abortTimer);
      if (spawnedProc && spawnedProc.exitCode === null && spawnedProc.signalCode === null) {
        spawnedProc.kill("SIGKILL");
      }
      await runSettled;
      spawnOverride.fn = null;
      mongoClientOverride.factory = null;
    }

    // (B) waitForLockFileCleared must be exported and throw on timeout when mongod.lock remains non-empty
    expect(physicalMod.waitForLockFileCleared).toBeTypeOf("function");
    await expect(physicalMod.waitForLockFileCleared(lockDir, 50)).rejects.toThrow(/mongod\.lock/i);

    // (C) buildBaseMongodArgs includes skipShardingConfigurationChecks + compressor flags, and missing encryptionKeyFile fails pre-spawn
    const coordinatorC = new PhysicalRecoveryCoordinator();
    const capturedArgs = [];
    vi.spyOn(coordinatorC, "runWithEphemeralMongod").mockImplementation(
      async (_wd, _db, args, fn) => {
        capturedArgs.push(args);
        await fn({
          db: () => ({
            command: vi.fn(async () => ({ ok: 1 })),
            listCollections: () => ({ toArray: vi.fn(async () => []) }),
            collection: () => ({
              deleteMany: vi.fn(async () => ({})),
              insertOne: vi.fn(async () => ({})),
              drop: vi.fn(async () => ({})),
            }),
          }),
        });
      },
    );

    await coordinatorC.run({
      config: {
        workDir,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
      },
      storage: {},
      logger,
      targetDbPath: dbDir,
      manifest: {
        physical: {
          cursorType: "fsyncLock",
          storageOptions: {
            journalCompressor: "zstd",
            blockCompressor: "snappy",
          },
        },
      },
      pitrPlan: null,
    });

    expect(capturedArgs[0]).toContain("--setParameter=skipShardingConfigurationChecks=true");
    expect(capturedArgs[0]).toContain("--wiredTigerJournalCompressor=zstd");
    expect(capturedArgs[0]).toContain("--wiredTigerCollectionBlockCompressor=snappy");

    const spawnSpyNever = vi.fn(() => {
      throw new Error("spawn must not be called when encryptionKeyFile is missing");
    });
    spawnOverride.fn = spawnSpyNever;
    const coordinatorD = new PhysicalRecoveryCoordinator();
    await expect(
      coordinatorD.run({
        config: {
          workDir,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          physical: {
            encryptionKeyFile: path.join(workDir, "nonexistent.key"),
          },
        },
        storage: {},
        logger,
        targetDbPath: dbDir,
        manifest: { physical: { cursorType: "fsyncLock" } },
        pitrPlan: null,
      }),
    ).rejects.toThrow(/encryptionKeyFile/i);
    expect(spawnSpyNever).not.toHaveBeenCalled();
  });

  it("[F-PHY-013] supports BackupCursorFile.length and records actual TAR stat.size in manifest.physical.files", async () => {
    const workDir = await makeTempDir("mv-phy-013-");
    const dbPath = path.join(workDir, "db");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(dbPath, { recursive: true });
    const wtFile = path.join(dbPath, "WiredTiger.wt");
    await fs.writeFile(wtFile, "ACTUAL_12_BY"); // 12 bytes

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const res = await runPhysicalBackup({
      config: {
        engine: "physical",
        workDir,
        timeoutMs: 30_000,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
      },
      storage,
      logger,
      dbPath,
      createMongoClient: () => ({
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: (name) => {
          if (name !== "admin") return {};
          return {
            command: vi.fn(async () => ({ ok: 1 })),
            aggregate: (pipeline) => {
              if (pipeline[0]?.$backupCursor) {
                const docs = [
                  {
                    metadata: {
                      backupId: "b-013",
                      dbpath: dbPath,
                      checkpointTimestamp: { t: 1700000000, i: 1 },
                    },
                  },
                  { filename: wtFile, length: 5 },
                ];
                let idx = 0;
                return {
                  next: vi.fn(async () => docs[idx++] ?? null),
                  tryNext: vi.fn(async () => docs[idx++] ?? null),
                  close: vi.fn(async () => {}),
                };
              }
              return {
                async *[Symbol.asyncIterator]() {},
                close: vi.fn(async () => {}),
              };
            },
          };
        },
      }),
    });

    const wtEntry = res.manifest.physical?.files.find((f) => f.name === "WiredTiger.wt");
    expect(wtEntry?.size).toBe(12);
  });

  it("[F-PHY-014] re-verifies mongod.lock before staged swap and surfaces unexpected fs.statfs errors", async () => {
    const workDir = await makeTempDir("mv-phy-014-");
    const storeDir = path.join(workDir, "store");
    const targetDb = path.join(workDir, "db");
    await fs.mkdir(targetDb, { recursive: true });
    await fs.writeFile(path.join(targetDb, "old.wt"), "KEEP_OLD");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const config = {
      engine: "physical",
      workDir,
      timeoutMs: 30_000,
      storage: { prefix: "mv" },
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
    };
    const { key } = await seedPhysicalBackup(storage, config, [
      { name: "WiredTiger.wt", data: "NEW_DATA" },
    ]);

    // (A) Staged restore TOCTOU: mongod.lock appears during archive download before staged swap
    const raceStorage = {
      ...storage,
      getJson: storage.getJson.bind(storage),
      list: storage.list.bind(storage),
      download: vi.fn(async (k, opts) => {
        await fs.writeFile(path.join(targetDb, "mongod.lock"), "54321\n");
        return storage.download(k, opts);
      }),
    };

    await expect(
      restorePhysicalBackup({
        config,
        storage: raceStorage,
        logger,
        targetDbPath: targetDb,
        ref: key,
        skipRecovery: true,
      }),
    ).rejects.toThrow(/running mongod/i);
    expect(await fs.readFile(path.join(targetDb, "old.wt"), "utf8")).toBe("KEEP_OLD");
    await fs.rm(path.join(targetDb, "mongod.lock"), { force: true });

    // (B) Unexpected fs.statfs EACCES error must surface instead of being silently swallowed
    const statfsSpy = vi
      .spyOn(fs, "statfs")
      .mockRejectedValueOnce(
        Object.assign(new Error("EACCES: permission denied"), { code: "EACCES" }),
      );
    await expect(
      restorePhysicalBackup({
        config,
        storage,
        logger,
        targetDbPath: targetDb,
        ref: key,
        skipRecovery: true,
      }),
    ).rejects.toThrow(/EACCES/i);
    statfsSpy.mockRestore();
  });

  it("[F-PHY-015] propagates AbortSignal into createBackupCursorWithRetries and cleans up cursor/unlock errors in probePhysicalCapability", async () => {
    // (A) createBackupCursorWithRetries aborts during 50915 retry sleep
    const ac = new AbortController();
    let attempts = 0;
    const mockAdmin = {
      aggregate: () => ({
        next: vi.fn(async () => {
          attempts++;
          throw Object.assign(new Error("BackupCursorOpenConflictWithCheckpoint"), { code: 50915 });
        }),
        close: vi.fn(async () => {}),
      }),
    };
    const abortTimer = setTimeout(() => ac.abort(new Error("cancelled cursor wait")), 30);
    const startMs = Date.now();
    await expect(
      physicalMod.createBackupCursorWithRetries(mockAdmin, 5, ac.signal),
    ).rejects.toThrow();
    clearTimeout(abortTimer);
    expect(Date.now() - startMs).toBeLessThan(500);
    expect(attempts).toBe(1);

    // (B) probePhysicalCapability closes cursor when next() throws and surfaces FsyncUnlockError on failed probe unlock
    const cursorClose = vi.fn(async () => {});
    const probeClient = {
      db: () => ({
        aggregate: () => ({
          next: vi.fn(async () => {
            throw Object.assign(new Error("Command $backupCursor not found"), { code: 59 });
          }),
          close: cursorClose,
        }),
        command: vi.fn(async (cmd) => {
          if (cmd.fsync) return { ok: 1, lock: true };
          if (cmd.fsyncUnlock) throw new Error("unlock failed permanently");
          return { ok: 1 };
        }),
      }),
    };

    await expect(
      physicalMod.probePhysicalCapability(probeClient, { probeFsyncLock: true }),
    ).rejects.toThrow(physicalMod.FsyncUnlockError);
    expect(cursorClose).toHaveBeenCalledTimes(1);
  });

  it("[F-PHY-016] falls back to physicalMeta.oplogEnd in effectiveEndTs and scopes takeUnstableCheckpointOnShutdown to Step 2 and PITR Step 3", async () => {
    const workDir = await makeTempDir("mv-phy-016-");
    const dbPath = path.join(workDir, "db");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(dbPath, { recursive: true });
    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();

    // (A) When extendedToTs is null, effectiveEndTs uses physicalMeta.oplogEnd before checkpointTimestamp
    const backupRes = await runPhysicalBackup({
      config: {
        engine: "physical",
        workDir,
        timeoutMs: 30_000,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
      },
      storage,
      logger,
      dbPath,
      createMongoClient: () => ({
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: (name) => {
          if (name !== "admin") return {};
          return {
            command: vi.fn(async () => ({ ok: 1 })),
            aggregate: () => {
              const docs = [
                {
                  metadata: {
                    dbpath: dbPath,
                    checkpointTimestamp: { t: 1000, i: 1 },
                    oplogStart: { ts: { t: 1000, i: 1 }, t: 1 },
                    oplogEnd: { ts: { t: 2000, i: 7 }, t: 1 },
                  },
                },
                { filename: path.join(dbPath, "WiredTiger.wt") },
              ];
              let idx = 0;
              return {
                next: vi.fn(async () => docs[idx++] ?? null),
                tryNext: vi.fn(async () => docs[idx++] ?? null),
                close: vi.fn(async () => {}),
              };
            },
          };
        },
      }),
    });
    expect(backupRes.manifest.oplogWindow?.endTs).toBe("2000:7");

    // (B) takeUnstableCheckpointOnShutdown=true only on Step 2 (and Step 3 when PITR has slices)
    const coordNoPitr = new PhysicalRecoveryCoordinator();
    const argsNoPitr = [];
    vi.spyOn(coordNoPitr, "runWithEphemeralMongod").mockImplementation(
      async (_wd, _db, args, fn) => {
        argsNoPitr.push(args);
        await fn({
          db: () => ({
            command: vi.fn(async () => ({ ok: 1 })),
            listCollections: () => ({ toArray: vi.fn(async () => []) }),
            collection: () => ({
              deleteMany: vi.fn(async () => ({})),
              insertOne: vi.fn(async () => ({})),
              drop: vi.fn(async () => ({})),
            }),
          }),
        });
      },
    );

    await coordNoPitr.run({
      config: {
        workDir,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
      },
      storage: {},
      logger,
      targetDbPath: dbPath,
      manifest: { physical: { cursorType: "$backupCursor", oplogEnd: "2000:7" } },
      pitrPlan: null,
    });

    expect(argsNoPitr).toHaveLength(3);
    expect(argsNoPitr[0]).not.toContain("--setParameter=takeUnstableCheckpointOnShutdown=true");
    expect(argsNoPitr[1]).toContain("--setParameter=takeUnstableCheckpointOnShutdown=true");
    expect(argsNoPitr[2]).not.toContain("--setParameter=takeUnstableCheckpointOnShutdown=true");
  });

  it("[F-PHY-018] enforces cluster scope and fails closed on hello error during fsyncLock fallback", async () => {
    const workDir = await makeTempDir("mv-phy-018-");
    const dbPath = path.join(workDir, "db");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(dbPath, { recursive: true });
    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();

    // (A) Non-cluster database scope rejected before connecting
    const clientFactoryNever = vi.fn(() => {
      throw new Error("Should not connect for non-cluster scope");
    });
    await expect(
      runPhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "appdb" },
        },
        storage,
        logger,
        dbPath,
        createMongoClient: clientFactoryNever,
      }),
    ).rejects.toThrow(/cluster scope/i);
    expect(clientFactoryNever).not.toHaveBeenCalled();

    // (B) hello error fails closed when !allowPrimaryFsyncLock
    let fsyncCalled = false;
    await expect(
      runPhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          physical: { dbPath, allowFsyncLock: true, allowPrimaryFsyncLock: false },
        },
        storage,
        logger,
        createMongoClient: () => ({
          connect: vi.fn(async () => {}),
          close: vi.fn(async () => {}),
          db: () => ({
            aggregate: () => ({
              next: vi.fn(async () => {
                throw Object.assign(new Error("Command $backupCursor not found"), { code: 59 });
              }),
              close: vi.fn(async () => {}),
            }),
            command: vi.fn(async (cmd) => {
              if (cmd.hello) throw new Error("hello network timeout");
              if (cmd.fsync) fsyncCalled = true;
              if (cmd.buildInfo) return { version: "8.0.0" };
              return { ok: 1 };
            }),
          }),
        }),
      }),
    ).rejects.toThrow(/failed to verify replica set role via hello/i);
    expect(fsyncCalled).toBe(false);
  });

  it("[Challenger-Iter1-EdgeCases] retains rollbackDir on swap-rollback failure, preserves mongodb+srv implicit TLS, rejects malformed PAX & invalid GNU base-256 headers, and rejects Windows drive paths", async () => {
    const workDir = await makeTempDir("mv-phy-challenger-");

    // 1. [F-PHY-001] Swap-rollback fs.rename failure must retain rollbackDir instead of deleting it in finally
    {
      const storeDir = path.join(workDir, "store-swap");
      const targetDb = path.join(workDir, "db-swap");
      await fs.mkdir(path.join(targetDb, "db1"), { recursive: true });
      await fs.writeFile(path.join(targetDb, "WiredTiger.wt"), "ORIG_WT");
      await fs.writeFile(path.join(targetDb, "db1", "coll.wt"), "ORIG_DB1_COLL");

      const storage = createFilesystemStorage({ basePath: storeDir });
      const logger = makeLogger();
      const config = {
        engine: "physical",
        workDir,
        timeoutMs: 30_000,
        storage: { prefix: "mv" },
        mongo: { uri: "mongodb://localhost:27017", database: "*" },
      };

      const { key } = await seedPhysicalBackup(storage, config, [
        { name: "WiredTiger.wt", data: "NEW_WT" },
        { name: "db2/coll.wt", data: "NEW_DB2_COLL" },
      ]);

      const origRename = fs.rename.bind(fs);
      const renameSpy = vi.spyOn(fs, "rename").mockImplementation(async (oldPath, newPath) => {
        const oldStr = String(oldPath);
        const newStr = String(newPath);
        // Fail promoting db2 from staging -> targetDb
        if (oldStr.includes(".mongovault-staging-") && path.basename(newStr) === "db2") {
          throw Object.assign(new Error("Simulated promote rename EIO"), { code: "EIO" });
        }
        // Fail restoring db1 from rollback -> targetDb
        if (oldStr.includes(".mongovault-rollback-") && path.basename(newStr) === "db1") {
          throw Object.assign(new Error("Simulated rollback rename EIO"), { code: "EIO" });
        }
        return origRename(oldPath, newPath);
      });

      await expect(
        restorePhysicalBackup({
          config,
          storage,
          logger,
          targetDbPath: targetDb,
          ref: key,
          skipRecovery: true,
        }),
      ).rejects.toThrow(/Simulated promote rename EIO/);
      renameSpy.mockRestore();

      const entries = await fs.readdir(targetDb);
      const rbDirName = entries.find((e) => e.startsWith(".mongovault-rollback-"));
      expect(rbDirName).toBeDefined();
      expect(await fs.readFile(path.join(targetDb, rbDirName, "db1", "coll.wt"), "utf8")).toBe(
        "ORIG_DB1_COLL",
      );
      expect(await fs.readFile(path.join(targetDb, "WiredTiger.wt"), "utf8")).toBe("ORIG_WT");
    }

    // 2. [F-PHY-004] buildDirectNodeUri preserves implicit tls=true when pinning mongodb+srv:// URIs
    {
      const dbPath = path.join(workDir, "db-srv");
      const storeDir = path.join(workDir, "store-srv");
      await fs.mkdir(dbPath, { recursive: true });
      await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT_SRV");

      const storage = createFilesystemStorage({ basePath: storeDir });
      const logger = makeLogger();
      const createdUris = [];

      await runPhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: {
            uri: "mongodb+srv://atlasUser:secret@cluster0.xyz.mongodb.net/mydb?authSource=admin",
            database: "*",
            readPreference: "secondary",
          },
          physical: { dbPath, allowFsyncLock: true, allowPrimaryFsyncLock: false },
        },
        storage,
        logger,
        createMongoClient: (uri) => {
          createdUris.push(uri);
          return {
            connect: vi.fn(async () => {}),
            close: vi.fn(async () => {}),
            db: (dbName) => {
              if (dbName === "local") {
                return {
                  collection: () => ({
                    find: () => ({
                      sort: () => ({
                        limit: () => ({ next: vi.fn(async () => ({ ts: { t: 1000, i: 1 } })) }),
                      }),
                    }),
                  }),
                };
              }
              if (dbName === "config") {
                return { collection: () => ({ countDocuments: vi.fn(async () => 0) }) };
              }
              return {
                aggregate: () => ({
                  next: vi.fn(async () => {
                    throw Object.assign(new Error("Command $backupCursor not found"), { code: 59 });
                  }),
                  close: vi.fn(async () => {}),
                }),
                command: vi.fn(async (cmd) => {
                  if (cmd.hello) {
                    return {
                      ok: 1,
                      setName: "rs0",
                      isWritablePrimary: false,
                      secondary: true,
                      me: "s2.mongodb.net:27017",
                      hosts: ["s1.mongodb.net:27017", "s2.mongodb.net:27017"],
                    };
                  }
                  if (cmd.buildInfo) return { version: "8.0.0" };
                  if (cmd.getCmdLineOpts) return { parsed: { storage: { dbPath } } };
                  if (cmd.fsync) return { ok: 1 };
                  if (cmd.fsyncUnlock) return { ok: 1, lockCount: 0 };
                  return { ok: 1 };
                }),
              };
            },
          };
        },
      });

      expect(createdUris.length).toBeGreaterThanOrEqual(2);
      for (const pinnedUri of createdUris.slice(1)) {
        const params = new URLSearchParams(pinnedUri.split("?")[1] ?? "");
        expect(pinnedUri).toContain("mongodb://atlasUser:secret@s2.mongodb.net:27017/mydb");
        expect(params.get("directConnection")).toBe("true");
        expect(params.get("tls")).toBe("true");
      }
    }

    // 3. [F-PHY-010] parsePaxHeaders rejects malformed PAX records + GNU base-256 negative/overflow rejected before file creation
    {
      const makePaxTar = (paxText, fileSize = 4, fileBody = Buffer.from("DATA")) => {
        const paxPayload = Buffer.from(paxText, "utf8");
        const paxHdr = buildRawTarHeaderBuffer("PaxHeaders/entry", paxPayload.length, "x");
        const fileHdr = buildRawTarHeaderBuffer("ok.txt", fileSize, "0");
        const parts = [paxHdr, padTarBlock(paxPayload), fileHdr];
        if (fileSize > 0) {
          parts.push(padTarBlock(fileBody));
        }
        parts.push(Buffer.alloc(1024, 0));
        return Buffer.concat(parts);
      };

      const malformedPaxPayloads = [
        { tar: makePaxTar("1 path=../evil\n"), desc: "truncated length" },
        { tar: makePaxTar("15abc path=ok\n"), desc: "non-numeric length suffix" },
        { tar: makePaxTar("99 size=512\n", 0), desc: "out-of-bounds length" },
        { tar: makePaxTar("15 path=no_nl_x"), desc: "missing trailing newline" },
        { tar: makePaxTar("path=no_space\n"), desc: "missing length delimiter" },
      ];

      for (const [idx, { tar }] of malformedPaxPayloads.entries()) {
        await expect(verifyTarStream(Readable.from([tar]))).rejects.toThrow(
          /Corrupted tar PAX header/i,
        );
        const outDir = path.join(workDir, `pax-out-${idx}`);
        await expect(extractTarStream(Readable.from([tar]), outDir)).rejects.toThrow(
          /Corrupted tar PAX header/i,
        );
      }

      const makeBase256Tar = (firstByte, restByte, entryName) => {
        const hdr = Buffer.alloc(512, 0);
        hdr.write(entryName, 0, 100, "utf8");
        hdr.write("0000644\0", 100, "ascii");
        hdr.write("0000000\0", 108, "ascii");
        hdr.write("0000000\0", 116, "ascii");
        hdr[124] = firstByte;
        for (let i = 125; i < 136; i++) hdr[i] = restByte;
        hdr.write("00000000000\0", 136, "ascii");
        hdr.write("0", 156, "ascii");
        hdr.write("ustar\0", 257, "ascii");
        hdr.write("00", 263, "ascii");
        hdr.fill(32, 148, 156);
        let sum = 0;
        for (let i = 0; i < 512; i++) sum += hdr[i];
        hdr.write(sum.toString(8).padStart(6, "0") + "\0 ", 148, "ascii");
        return Buffer.concat([hdr, Buffer.alloc(1024, 0)]);
      };

      const negB256 = makeBase256Tar(0xff, 0xff, "neg-b256.wt");
      const overB256 = makeBase256Tar(0x80, 0xff, "over-b256.wt");

      for (const [label, tar] of [
        ["neg", negB256],
        ["over", overB256],
      ]) {
        await expect(verifyTarStream(Readable.from([tar]))).rejects.toThrow(
          /Corrupted tar header: invalid entry size/i,
        );
        const outDir = path.join(workDir, `b256-out-${label}`);
        await expect(extractTarStream(Readable.from([tar]), outDir)).rejects.toThrow(
          /Corrupted tar header: invalid entry size/i,
        );
        // Must reject before opening/creating any 0-byte file on disk
        await expect(fs.stat(path.join(outDir, `${label}-b256.wt`))).rejects.toThrow();
      }
    }

    // 4. [F-PHY-009] trimDbPathPrefix rejects Windows drive-letter paths
    {
      expect(() => trimDbPathPrefix("C:\\evil\\file.wt", "/var/lib/mongodb")).toThrow(
        /Invalid relative backup cursor file path/i,
      );
      expect(() => trimDbPathPrefix("d:/evil/file.wt", "/var/lib/mongodb")).toThrow(
        /Invalid relative backup cursor file path/i,
      );
    }
  });
});
