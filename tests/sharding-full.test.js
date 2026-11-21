import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { gzipSync } from "node:zlib";
import { BSON, Binary } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { ARCHIVE_MAGIC_NUMBER } from "../src/archive-stream.js";
import { createFilesystemStorage } from "../src/storage.js";
import { writeManifest } from "../src/catalog.js";
import { CsrsRestorer, restoreShardedCluster } from "../src/sharding.js";

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

/** Builds a minimal valid `mongodump --archive` stream (magic + header + docs + EOF). */
function buildMockArchiveGz(entries) {
  const int32LE = (n) => {
    const b = Buffer.alloc(4);
    b.writeInt32LE(n, 0);
    return b;
  };
  const parts = [];
  const magic = Buffer.alloc(4);
  magic.writeUInt32LE(ARCHIVE_MAGIC_NUMBER >>> 0, 0);
  parts.push(magic);
  parts.push(Buffer.from(BSON.serialize({ version: "0.1", concurrent_collections: 1 })));
  parts.push(int32LE(-1));
  for (const { db, collection, docs } of entries) {
    parts.push(Buffer.from(BSON.serialize({ db, collection })));
    for (const doc of docs) {
      parts.push(Buffer.from(BSON.serialize(doc)));
    }
    parts.push(int32LE(-1));
  }
  parts.push(int32LE(-1));
  return gzipSync(Buffer.concat(parts));
}

describe("Sharded Cluster Restore & CsrsRestorer (sharding.ts full coverage)", () => {
  it("CsrsRestorer streams CSRS collections, preserves target sessions UUID & balancer/automerge", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-csrs-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const backupKey = "mv/configsvr/_cluster/2026-01-01T00-00-00.archive.gz";

    const backupSessionsUuid = new Binary(
      Buffer.from("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "hex"),
      4,
    );
    const targetSessionsUuid = new Binary(
      Buffer.from("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "hex"),
      4,
    );

    const archiveGz = buildMockArchiveGz([
      {
        db: "config",
        collection: "databases",
        docs: [{ _id: "app", primary: "shard1" }],
      },
      {
        db: "config",
        collection: "collections",
        docs: [
          { _id: "config.system.sessions", uuid: backupSessionsUuid },
          { _id: "app.users", key: { _id: 1 } },
        ],
      },
      {
        db: "config",
        collection: "chunks",
        docs: [
          { _id: "sess-chunk", uuid: backupSessionsUuid, min: { _id: 0 } },
          { _id: "user-chunk", ns: "app.users", min: { _id: 0 } },
        ],
      },
      {
        db: "config",
        collection: "settings",
        docs: [
          { _id: "balancer", stopped: true },
          { _id: "chunksize", value: 64 },
        ],
      },
    ]);

    try {
      await storage.upload(backupKey, archiveGz);
      await writeManifest(storage, {
        version: 1,
        key: backupKey,
        database: "*",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:01:00Z",
        durationMs: 1000,
        size: archiveGz.length,
        sha256: createHash("sha256").update(archiveGz).digest("hex"),
        compression: "gzip",
        encryption: null,
        collections: {},
      });

      const bulkOps = {};
      const deleted = [];
      const fakeClient = {
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: () => ({
          collection: (name) => ({
            findOne: vi.fn(async () =>
              name === "collections"
                ? { _id: "config.system.sessions", uuid: targetSessionsUuid }
                : null,
            ),
            deleteMany: vi.fn(async (filter) => {
              deleted.push({ name, filter });
              return { deletedCount: 1 };
            }),
            bulkWrite: vi.fn(async (ops) => {
              bulkOps[name] = (bulkOps[name] || []).concat(ops);
            }),
          }),
        }),
      };

      const res = await new CsrsRestorer().restore({
        config: {
          workDir,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
        },
        storage,
        backupKey,
        targetConfigsvrUri: "mongodb://localhost:27019",
        drop: true,
        logger: makeLogger(),
        createMongoClient: () => fakeClient,
      });

      expect(res.backupSessionsUuidHex).toBe("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
      // config.system.sessions and balancer setting should NOT be overwritten!
      expect(bulkOps.collections.map((o) => o.replaceOne.filter._id)).toEqual(["app.users"]);
      expect(bulkOps.settings.map((o) => o.replaceOne.filter._id)).toEqual(["chunksize"]);
      expect(bulkOps.chunks.map((o) => o.replaceOne.filter._id)).toEqual(["user-chunk"]);
      expect(deleted.length).toBe(6);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("restoreShardedCluster validates manifest, shard mapping, drops user DBs, replays slices, and restarts balancer", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-sharded-res-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const clusterKey = "mv/_cluster/2026-01-01.cluster";

    const makeClusterManifest = (overrides = {}) => ({
      version: 1,
      key: clusterKey,
      engine: "sharded",
      database: "*",
      createdAt: "2026-01-01T00:00:00Z",
      completedAt: "2026-01-01T00:01:00Z",
      durationMs: 1000,
      size: 10,
      sha256: "abc",
      compression: "gzip",
      encryption: null,
      collections: {},
      sharding: {
        clusterTime: "100:1",
        configsvr: { backupKey: "mv/configsvr/base.archive.gz", status: "success" },
        shards: {
          rs1: {
            backupKey: "mv/shards/rs1/base.archive.gz",
            status: "success",
            host: "rs1/s1:27018",
          },
        },
        ...overrides,
      },
    });

    try {
      await writeManifest(storage, makeClusterManifest());

      // 1. Missing target shard mapping fails fast
      const missingShardClient = () => ({
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: () => ({
          command: vi.fn(async (cmd) => {
            if (cmd.hello) return { msg: "isdbgrid" };
            if (cmd.balancerStatus) return { mode: "off", inBalancerRound: false };
            if (cmd.getShardMap) return { map: { config: "csrs/c1:27019" } };
            return {};
          }),
          collection: () => ({
            find: () => ({ toArray: vi.fn(async () => []) }),
          }),
        }),
      });

      await expect(
        restoreShardedCluster({
          config: {
            workDir,
            storage: { prefix: "mv" },
            mongo: { uri: "mongodb://localhost:27017", database: "*" },
          },
          storage,
          logger: makeLogger(),
          ref: clusterKey,
          createMongoClient: missingShardClient,
          restoreRunner: vi.fn(async () => ({ restored: 1 })),
        }),
      ).rejects.toThrow(/missing mapping for shard "rs1"/);

      // 2. Full successful restore with drop=true, balancer stop/start, flushRouterConfig
      const droppedDbs = [];
      const commands = [];

      // After balancerStop, balancerStatus returns mode: "off"
      let stopped = false;
      const dynamicClient = () => ({
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: (name) => ({
          dropDatabase: vi.fn(async () => {
            droppedDbs.push(name);
          }),
          command: vi.fn(async (cmd) => {
            commands.push(Object.keys(cmd)[0]);
            if (cmd.hello) return { msg: "isdbgrid" };
            if (cmd.balancerStop) {
              stopped = true;
              return { ok: 1 };
            }
            if (cmd.balancerStart) {
              stopped = false;
              return { ok: 1 };
            }
            if (cmd.balancerStatus)
              return { mode: stopped ? "off" : "full", inBalancerRound: false };
            if (cmd.listDatabases) return { databases: [{ name: "admin" }, { name: "app" }] };
            if (cmd.flushRouterConfig) return { ok: 1 };
            if (cmd.getShardMap) return { map: { config: "csrs/c1:27019", rs1: "rs1/s1:27018" } };
            return {};
          }),
          collection: () => ({
            find: () => ({
              toArray: vi.fn(async () => [{ _id: "rs1", host: "rs1/s1:27018" }]),
            }),
          }),
        }),
      });

      const restoreRunner = vi.fn(async () => ({ restored: 5 }));
      const res = await restoreShardedCluster({
        config: {
          workDir,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
        },
        storage,
        logger: makeLogger(),
        ref: clusterKey,
        drop: true,
        createMongoClient: dynamicClient,
        restoreRunner,
      });

      expect(res.restoredShards).toEqual(["rs1"]);
      expect(droppedDbs).toEqual(["app"]);
      expect(restoreRunner).toHaveBeenCalledTimes(2); // configsvr + rs1
      expect(commands).toContain("balancerStop");
      expect(commands).toContain("balancerStart");
      expect(commands).toContain("flushRouterConfig");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
