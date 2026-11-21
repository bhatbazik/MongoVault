import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createFilesystemStorage } from "../src/storage.js";
import {
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
      const res = await runPhysicalBackup({
        config: {
          engine: "physical",
          workDir,
          timeoutMs: 30_000,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          physical: { dbPath, fsyncLockFallback: true },
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
          physical: { dbPath: srcDb, fsyncLockFallback: true },
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
});
