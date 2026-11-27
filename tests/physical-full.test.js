import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createFilesystemStorage } from "../src/storage.js";
import {
  FsyncUnlockError,
  PhysicalRecoveryCoordinator,
  restorePhysicalBackup,
  runPhysicalBackup,
} from "../src/physical.js";

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

describe("Physical Backup & Recovery (physical.ts full coverage)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("falls back to fsyncLock when $backupCursor fails, warns on RS primary, and unlocks in finally", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-phys-fsync-"));
    const dbPath = path.join(workDir, "db");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(path.join(dbPath, "journal"), { recursive: true });
    await fs.writeFile(path.join(dbPath, "WiredTiger"), "WT");
    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "data");
    await fs.writeFile(path.join(dbPath, "journal", "WiredTigerLog.0000000001"), "jrn");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();
    const commands = [];

    const fakeClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        aggregate: () => ({
          toArray: vi.fn(async () => {
            throw new Error("Command $backupCursor not found");
          }),
          close: vi.fn(async () => {}),
        }),
        command: vi.fn(async (cmd) => {
          commands.push(Object.keys(cmd)[0]);
          if (cmd.buildInfo) return { version: "8.0.0" };
          if (cmd.getCmdLineOpts) return { parsed: { storage: { dbPath } } };
          if (cmd.hello) {
            return { setName: "rs0", isWritablePrimary: true, hosts: ["n1:27017", "n2:27017"] };
          }
          if (cmd.fsync) return { ok: 1 };
          if (cmd.fsyncUnlock) return { ok: 1 };
          return {};
        }),
        collection: () => ({
          countDocuments: vi.fn(async () => 0),
          find: () => ({
            sort: () => ({
              limit: () => ({
                next: vi.fn(async () => ({ ts: { t: 1000, i: 1 } })),
              }),
            }),
          }),
        }),
      }),
    };

    try {
      // 1. Without explicit opt-in, fsyncLock fallback MUST be refused
      await expect(
        runPhysicalBackup({
          config: {
            engine: "physical",
            workDir,
            timeoutMs: 30_000,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
            physical: { dbPath },
          },
          storage,
          logger,
          createMongoClient: () => fakeClient,
        }),
      ).rejects.toThrow(/fsyncLock fallback is disabled/i);

      // 2. Even with allowFsyncLock=true, multi-node RS PRIMARY MUST be refused unless allowPrimaryFsyncLock=true
      await expect(
        runPhysicalBackup({
          config: {
            engine: "physical",
            workDir,
            timeoutMs: 30_000,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
            physical: { dbPath, allowFsyncLock: true },
          },
          storage,
          logger,
          createMongoClient: () => fakeClient,
        }),
      ).rejects.toThrow(
        /Refusing to acquire write-blocking fsyncLock on multi-node replica set PRIMARY/i,
      );

      // 3. With explicit allowFsyncLock + allowPrimaryFsyncLock, backup proceeds, warns, and unlocks
      const res = await runPhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          physical: { dbPath, allowFsyncLock: true, allowPrimaryFsyncLock: true },
        },
        storage,
        logger,
        createMongoClient: () => fakeClient,
      });

      expect(res.manifest.physical.cursorType).toBe("fsyncLock");
      expect(commands).toContain("fsync");
      expect(commands).toContain("fsyncUnlock");
      expect(logger.warn).toHaveBeenCalled();

      // Test live mongod.lock guard on restore
      await fs.writeFile(path.join(dbPath, "mongod.lock"), "12345");
      await expect(
        restorePhysicalBackup({
          config: {
            engine: "physical",
            workDir,
            timeoutMs: 30_000,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
          },
          storage,
          logger,
          targetDbPath: dbPath,
          skipRecovery: true,
        }),
      ).rejects.toThrow(/non-empty mongod\.lock/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("supports small-disk 1x direct extraction mode and fails fast on insufficient disk space", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-phys-1x-"));
    const srcDb = path.join(workDir, "srcdb");
    const dstDb = path.join(workDir, "dstdb");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(srcDb, { recursive: true });
    await fs.mkdir(dstDb, { recursive: true });

    // Create a 1000-byte file in srcDb
    await fs.writeFile(path.join(srcDb, "WiredTiger.wt"), Buffer.alloc(1000, 0x41));
    // Put an existing 600-byte old file in dstDb
    await fs.writeFile(path.join(dstDb, "old.wt"), Buffer.alloc(600, 0x42));

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();

    const fakeClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        aggregate: () => ({
          toArray: vi.fn(async () => {
            throw new Error("no cursor");
          }),
          close: vi.fn(async () => {}),
        }),
        command: vi.fn(async (cmd) => {
          if (cmd.buildInfo) return { version: "8.0.0" };
          if (cmd.fsync || cmd.fsyncUnlock) return { ok: 1 };
          return {};
        }),
        collection: () => ({
          countDocuments: vi.fn(async () => 0),
          find: () => ({
            sort: () => ({
              limit: () => ({
                next: vi.fn(async () => null),
              }),
            }),
          }),
        }),
      }),
    };

    try {
      await runPhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          physical: { dbPath: srcDb, allowFsyncLock: true },
        },
        storage,
        logger,
        createMongoClient: () => fakeClient,
      });

      // Mock fs.statfs: 700 bytes free (< 1100 needed), but 700 + 600 existing = 1300 >= 1100 -> 1x direct mode!
      const statfsSpy = vi.spyOn(fs, "statfs").mockResolvedValueOnce({
        bavail: 700,
        bsize: 1,
      });

      const res1x = await restorePhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
        },
        storage,
        logger,
        targetDbPath: dstDb,
        allowDirectMode: true,
        skipRecovery: true,
      });

      expect(res1x.restoredFiles).toBe(1);
      expect(await fs.stat(path.join(dstDb, "WiredTiger.wt"))).toBeDefined();
      // Old file should have been cleared in 1x mode
      await expect(fs.stat(path.join(dstDb, "old.wt"))).rejects.toThrow();

      // Mock fs.statfs: 100 bytes free + 1000 existing < 1100 needed -> fail fast
      statfsSpy.mockResolvedValueOnce({ bavail: 50, bsize: 1 });
      await expect(
        restorePhysicalBackup({
          config: {
            engine: "physical",
            workDir,
            timeoutMs: 30_000,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
          },
          storage,
          logger,
          targetDbPath: dstDb,
          skipRecovery: true,
        }),
      ).rejects.toThrow(/Insufficient disk space/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("PhysicalRecoveryCoordinator executes 3-step recovery and resetReplicaSetState", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-phys-rec-"));
    try {
      const coordinator = new PhysicalRecoveryCoordinator();
      const insertedMinvalid = [];
      const droppedColls = [];
      const ephemeralCalls = [];

      const fakeClient = {
        db: (dbName) => ({
          command: vi.fn(async () => ({ ok: 1 })),
          listCollections: () => ({
            toArray: vi.fn(async () => [{ name: "cache.chunks.app.users" }]),
          }),
          collection: (coll) => ({
            deleteMany: vi.fn(async () => ({ deletedCount: 1 })),
            insertOne: vi.fn(async (doc) => {
              if (coll === "replset.minvalid") insertedMinvalid.push(doc);
            }),
            drop: vi.fn(async () => {
              droppedColls.push(`${dbName}.${coll}`);
            }),
          }),
        }),
      };

      // Stub private runWithEphemeralMongod so we test all 3 recovery steps without spawning mongod
      vi.spyOn(coordinator, "runWithEphemeralMongod").mockImplementation(
        async (_wd, _dbPath, args, fn) => {
          ephemeralCalls.push(args);
          await fn(fakeClient);
        },
      );

      const res = await coordinator.run({
        config: {
          workDir,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          physical: { directoryPerDb: true },
        },
        storage: {},
        logger: makeLogger(),
        targetDbPath: workDir,
        manifest: {
          physical: {
            cursorType: "$backupCursor",
            oplogEnd: "1000:5",
            storageOptions: { directoryForIndexes: true },
          },
        },
        pitrPlan: null,
      });

      expect(res.replayedEntries).toBe(0);
      expect(ephemeralCalls.length).toBe(3);
      expect(ephemeralCalls[1]).toContain("--setParameter=recoverFromOplogAsStandalone=true");
      expect(ephemeralCalls[0]).toContain("--directoryperdb");
      expect(ephemeralCalls[0]).toContain("--wiredTigerDirectoryForIndexes");
      expect(insertedMinvalid.length).toBe(1);
      expect(droppedColls).toContain("config.cache.chunks.app.users");
      expect(droppedColls).toContain("config.transactions");
      expect(droppedColls).toContain("config.system.sessions");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("retries fsyncUnlock on a fresh MongoClient when original connection drops, and throws FsyncUnlockError when unlock fails permanently", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-phys-unlock-"));
    const dbPath = path.join(workDir, "db");
    const storeDir = path.join(workDir, "store");
    await fs.mkdir(dbPath, { recursive: true });
    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT_DATA");

    const storage = createFilesystemStorage({ basePath: storeDir });
    const logger = makeLogger();

    try {
      // Case A: First unlock fails on original client; retry #2 opens fresh client and succeeds
      let clientCreates = 0;
      let unlockAttempts = 0;
      const makeClient = (succeedOnAttempt) => {
        clientCreates++;
        return {
          connect: vi.fn(async () => {}),
          close: vi.fn(async () => {}),
          db: () => ({
            aggregate: () => ({
              next: vi.fn(async () => {
                throw new Error("$backupCursor unsupported");
              }),
              close: vi.fn(async () => {}),
            }),
            command: vi.fn(async (cmd) => {
              if (cmd.buildInfo) return { version: "8.0.0" };
              if (cmd.hello) return { ok: 1 };
              if (cmd.fsync) return { ok: 1 };
              if (cmd.fsyncUnlock) {
                unlockAttempts++;
                if (unlockAttempts < succeedOnAttempt) {
                  throw new Error("socket reset during fsyncUnlock");
                }
                return { ok: 1, lockCount: 0 };
              }
              return { ok: 1 };
            }),
            collection: () => ({
              countDocuments: vi.fn(async () => 0),
              find: () => ({
                sort: () => ({
                  limit: () => ({ next: vi.fn(async () => null) }),
                }),
              }),
            }),
          }),
        };
      };

      const recovered = await runPhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          physical: { dbPath, allowFsyncLock: true },
        },
        storage,
        logger,
        createMongoClient: () => makeClient(2),
      });
      expect(recovered.manifest.physical.cursorType).toBe("fsyncLock");
      expect(clientCreates).toBe(2); // original client + 1 fresh retry client
      expect(unlockAttempts).toBe(2);

      // Case B: All unlock attempts fail -> MUST throw FsyncUnlockError (never swallow!)
      clientCreates = 0;
      unlockAttempts = 0;
      await expect(
        runPhysicalBackup({
          config: {
            engine: "physical",
            workDir,
            timeoutMs: 30_000,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
            physical: { dbPath, allowFsyncLock: true },
          },
          storage,
          logger,
          createMongoClient: () => makeClient(99),
        }),
      ).rejects.toThrow(FsyncUnlockError);
      expect(unlockAttempts).toBe(3);

      // Case C: Upload fails AND unlock also fails in finally -> MUST surface FsyncUnlockError with cause!
      unlockAttempts = 0;
      const failingStorage = {
        ...storage,
        upload: vi.fn(async () => {
          throw new Error("S3 network disconnect");
        }),
        remove: vi.fn(async () => {}),
      };
      let caught = null;
      try {
        await runPhysicalBackup({
          config: {
            engine: "physical",
            workDir,
            timeoutMs: 30_000,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
            physical: { dbPath, allowFsyncLock: true },
          },
          storage: failingStorage,
          logger,
          createMongoClient: () => makeClient(99),
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(FsyncUnlockError);
      expect(caught.message).toContain("db.adminCommand({ fsyncUnlock: 1 })");
      expect(caught.cause?.message).toMatch(/S3 network disconnect/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("aborts stalled fsyncLock backup when fsyncLockTimeoutMs watchdog expires and unlocks MongoDB", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-phys-watchdog-"));
    const dbPath = path.join(workDir, "db");
    await fs.mkdir(dbPath, { recursive: true });
    await fs.writeFile(path.join(dbPath, "WiredTiger.wt"), "WT_DATA");

    const logger = makeLogger();
    let unlocked = false;

    const fakeClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        aggregate: () => ({
          next: vi.fn(async () => {
            throw new Error("$backupCursor unsupported");
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
        collection: () => ({
          countDocuments: vi.fn(async () => 0),
          find: () => ({
            sort: () => ({
              limit: () => ({ next: vi.fn(async () => null) }),
            }),
          }),
        }),
      }),
    };

    const stalledStorage = {
      bucket: "test",
      upload: vi.fn(
        (_key, _stream, { signal }) =>
          new Promise((_, reject) => {
            signal?.addEventListener("abort", () => {
              reject(signal.reason ?? new Error("aborted"));
            });
          }),
      ),
      remove: vi.fn(async () => {}),
    };

    try {
      await expect(
        runPhysicalBackup({
          config: {
            engine: "physical",
            workDir,
            timeoutMs: 30_000,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
            physical: { dbPath, allowFsyncLock: true },
          },
          storage: stalledStorage,
          logger,
          fsyncLockTimeoutMs: 60,
          createMongoClient: () => fakeClient,
        }),
      ).rejects.toThrow(/watchdog/i);
      expect(unlocked).toBe(true);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
