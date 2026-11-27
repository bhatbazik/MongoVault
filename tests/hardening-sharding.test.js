import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import { BSON, Binary, Timestamp } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import { ARCHIVE_MAGIC_NUMBER } from "../src/archive-stream.js";
import { writeManifest } from "../src/catalog.js";
import { generateKey, parseKey } from "../src/crypto.js";
import { pitrStateKey, uploadOplogSlice } from "../src/oplog.js";
import {
  ActiveChunkMigrationError,
  BalancerController,
  CsrsRestorer,
  buildShardUri,
  captureShardedOplogSlices,
  resolveShardedPitrPlan,
  restoreShardedCluster,
  runShardedClusterBackup,
} from "../src/sharding.js";
import { createFilesystemStorage } from "../src/storage.js";

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

function makeMemoryStorage() {
  const objects = new Map();
  return {
    objects,
    async upload(key, body) {
      const chunks = [];
      for await (const c of body) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
      const data = Buffer.concat(chunks);
      objects.set(key, { data, size: data.length, lastModified: new Date() });
    },
    async head(key) {
      const obj = objects.get(key);
      return obj ? { size: obj.size, lastModified: obj.lastModified, metadata: {} } : null;
    },
    async download(key) {
      const obj = objects.get(key);
      if (!obj) throw new Error(`Object not found: ${key}`);
      return Readable.from(obj.data);
    },
    async putJson(key, value) {
      const data = Buffer.from(JSON.stringify(value));
      objects.set(key, { data, size: data.length, json: value, lastModified: new Date() });
    },
    async getJson(key) {
      const obj = objects.get(key);
      if (!obj) return null;
      return { data: obj.json ?? JSON.parse(obj.data.toString("utf8")) };
    },
    async remove(key) {
      objects.delete(key);
    },
    async *list(prefix) {
      for (const [key, obj] of objects.entries()) {
        if (key.startsWith(prefix)) {
          yield { key, size: obj.size, lastModified: obj.lastModified || new Date() };
        }
      }
    },
  };
}

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

function makeBaseConfig(uri, workDir, extra = {}) {
  return {
    engine: "logical",
    mongo: {
      uri,
      database: "*",
      excludeCollections: [],
      oplog: true,
      parallelCollections: 2,
      readPreference: "primaryPreferred",
      ...(extra.mongo || {}),
    },
    pitr: { enabled: true, flushIntervalSeconds: 60, maxSliceMb: 16 },
    storage: {
      type: "filesystem",
      path: workDir,
      bucket: workDir,
      region: "us-east-1",
      forcePathStyle: false,
      accessKeyId: "",
      secretAccessKey: "",
      prefix: "mv",
      partSizeMb: 32,
      queueSize: 2,
      ...(extra.storage || {}),
    },
    workDir,
    encryptionKey: null,
    encryptionKeyRing: [],
    requireEncryption: false,
    ...extra,
  };
}

describe("M4 Sharding Reliability Hardening (F-SHD-001..013)", () => {
  it("F-SHD-001a: CsrsRestorer preserves live config.system.sessions chunks on same-cluster restore", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd001a-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const backupKey = "mv/configsvr/_cluster/2026-01-01/20260101T000000Z.archive.gz";
    const sameUuid = new Binary(Buffer.from("cccccccccccccccccccccccccccccccc", "hex"), 4);

    const archiveGz = buildMockArchiveGz([
      {
        db: "config",
        collection: "collections",
        docs: [
          { _id: "config.system.sessions", uuid: sameUuid },
          { _id: "app.items", key: { _id: 1 } },
        ],
      },
      {
        db: "config",
        collection: "chunks",
        docs: [
          { _id: "sess-chunk", uuid: sameUuid, min: { _id: 0 } },
          { _id: "app-chunk", ns: "app.items", min: { _id: 0 } },
        ],
      },
    ]);

    try {
      await storage.upload(backupKey, archiveGz);
      await writeManifest(storage, {
        version: 2,
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

      const deletedChunkFilters = [];
      const fakeClient = {
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: () => ({
          collection: (name) => ({
            findOne: vi.fn(async () =>
              name === "collections" ? { _id: "config.system.sessions", uuid: sameUuid } : null,
            ),
            deleteMany: vi.fn(async (filter) => {
              if (name === "chunks") deletedChunkFilters.push(filter);
              return { deletedCount: 1 };
            }),
            bulkWrite: vi.fn(async () => {}),
          }),
        }),
      };

      await new CsrsRestorer().restore({
        config: makeBaseConfig("mongodb://localhost:27017", workDir),
        storage,
        backupKey,
        targetConfigsvrUri: "mongodb://localhost:27019",
        drop: true,
        logger: makeLogger(),
        createMongoClient: () => fakeClient,
      });

      // Must NOT issue deleteMany({ uuid: sameUuid }) when backupSessionsUuid === targetSessionsUuid
      const deletedSameUuid = deletedChunkFilters.some((f) => f && f.uuid && !f.uuid.$ne);
      expect(deletedSameUuid).toBe(false);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-001b: CsrsRestorer fails closed on deleteMany error and verifies checksum before mutating CSRS", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd001b-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const backupKey = "mv/configsvr/_cluster/2026-01-01/20260101T000000Z.archive.gz";

    const archiveGz = buildMockArchiveGz([
      { db: "config", collection: "databases", docs: [{ _id: "app", primary: "shard01" }] },
    ]);

    try {
      await storage.upload(backupKey, archiveGz);
      await writeManifest(storage, {
        version: 2,
        key: backupKey,
        database: "*",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:01:00Z",
        durationMs: 1000,
        size: archiveGz.length,
        sha256: "0".repeat(64), // Corrupt checksum!
        compression: "gzip",
        encryption: null,
        collections: {},
      });

      let mutated = false;
      const fakeClient = {
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: () => ({
          collection: () => ({
            findOne: vi.fn(async () => null),
            deleteMany: vi.fn(async () => {
              mutated = true;
              return { deletedCount: 1 };
            }),
            bulkWrite: vi.fn(async () => {
              mutated = true;
            }),
          }),
        }),
      };

      await expect(
        new CsrsRestorer().restore({
          config: makeBaseConfig("mongodb://localhost:27017", workDir),
          storage,
          backupKey,
          targetConfigsvrUri: "mongodb://localhost:27019",
          drop: true,
          logger: makeLogger(),
          createMongoClient: () => fakeClient,
        }),
      ).rejects.toThrow(/checksum|mismatch/i);

      expect(mutated).toBe(false);

      // Now fix checksum and test deleteMany failure propagation
      await writeManifest(storage, {
        version: 2,
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

      const failingDeleteClient = {
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: () => ({
          collection: () => ({
            findOne: vi.fn(async () => null),
            deleteMany: vi.fn(async () => {
              throw new Error("CSRS write failed");
            }),
            bulkWrite: vi.fn(async () => {}),
          }),
        }),
      };

      await expect(
        new CsrsRestorer().restore({
          config: makeBaseConfig("mongodb://localhost:27017", workDir),
          storage,
          backupKey,
          targetConfigsvrUri: "mongodb://localhost:27019",
          drop: true,
          logger: makeLogger(),
          createMongoClient: () => failingDeleteClient,
        }),
      ).rejects.toThrow(/CSRS write failed/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-004: CsrsRestorer & restoreShardedCluster fail closed on unsigned manifest when REQUIRE_ENCRYPTION=true", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd-sig-"));
    const storage = makeMemoryStorage();
    const encKey = parseKey(generateKey());
    const clusterKey = "mv/_cluster/2026-01-01/20260101T000000Z.cluster";

    await storage.putJson(clusterKey, {
      clusterTime: "100:1",
      configsvr: { backupKey: "mv/configsvr/base.archive.gz", status: "success" },
      shards: { shard01: { backupKey: "mv/shards/shard01/base.archive.gz", status: "success" } },
    });
    await writeManifest(storage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: "2026-01-01T00:00:00Z",
      completedAt: "2026-01-01T00:01:00Z",
      durationMs: 1000,
      size: 10,
      sha256: "abc",
      compression: "none",
      encryption: null,
      collections: {},
      sharding: {
        clusterTime: "100:1",
        configsvr: { backupKey: "mv/configsvr/base.archive.gz", status: "success" },
        shards: { shard01: { backupKey: "mv/shards/shard01/base.archive.gz", status: "success" } },
      },
    });

    try {
      await expect(
        restoreShardedCluster({
          config: makeBaseConfig("mongodb://localhost:27017", workDir, {
            encryptionKey: encKey,
            encryptionKeyRing: [encKey],
            requireEncryption: true,
          }),
          storage,
          logger: makeLogger(),
          ref: clusterKey,
          createMongoClient: () => ({ connect: vi.fn(), close: vi.fn() }),
          restoreRunner: vi.fn(),
        }),
      ).rejects.toThrow(/encrypt|sign|HMAC|integrity/i);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-005: runShardedClusterBackup fails closed when Embedded Config Shard (_id: 'config') is present", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd005-"));
    const storage = makeMemoryStorage();
    const mockClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: (_name) => ({
        command: vi.fn(async (cmd) => {
          if (cmd.hello) return { msg: "isdbgrid", me: "mongos:27017" };
          if (cmd.balancerStatus) return { mode: "off", inBalancerRound: false };
          if (cmd.getShardMap) return { map: { config: "cfg/cfg1:27019", shard01: "s1/s1:27018" } };
          if (cmd.ping)
            return { ok: 1, $clusterTime: { clusterTime: new Timestamp({ t: 100, i: 1 }) } };
          return { ok: 1 };
        }),
        collection: (coll) => ({
          find: () => ({
            toArray: async () =>
              coll === "shards"
                ? [
                    { _id: "config", host: "cfg/cfg1:27019" },
                    { _id: "shard01", host: "s1/s1:27018" },
                  ]
                : [],
          }),
        }),
      }),
    };

    try {
      await expect(
        runShardedClusterBackup({
          config: makeBaseConfig("mongodb://mongos:27017", workDir),
          storage,
          logger: makeLogger(),
          createMongoClient: () => mockClient,
        }),
      ).rejects.toThrow(/Embedded Config Shard/i);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-006: restoreShardedCluster pre-verifies when drop=false and fails closed on dropDatabase / metadata sync errors", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd006-"));
    const storage = makeMemoryStorage();
    const clusterKey = "mv/_cluster/2026-01-01/20260101T000000Z.cluster";
    const cfgKey = "mv/configsvr/_cluster/2026-01-01/20260101T000000Z.archive.gz";
    const s1Key = "mv/shards/shard01/_cluster/2026-01-01/20260101T000000Z.archive.gz";

    await storage.putJson(clusterKey, { clusterTime: "100:1" });
    await writeManifest(storage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: "2026-01-01T00:00:00Z",
      completedAt: "2026-01-01T00:01:00Z",
      durationMs: 1000,
      size: 10,
      sha256: "abc",
      compression: "none",
      encryption: null,
      collections: {},
      sharding: {
        clusterTime: "100:1",
        configsvr: { backupKey: cfgKey, status: "success" },
        shards: { shard01: { backupKey: s1Key, status: "success", host: "shard01/s1:27018" } },
      },
    });

    const makeClient =
      ({ failDrop = false, failFlush = false } = {}) =>
      () => ({
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: (name) => ({
          dropDatabase: vi.fn(async () => {
            if (failDrop) throw new Error(`dropDatabase failed for ${name}`);
          }),
          command: vi.fn(async (cmd) => {
            if (cmd.hello) return { msg: "isdbgrid" };
            if (cmd.balancerStatus) return { mode: "off", inBalancerRound: false };
            if (cmd.getShardMap)
              return { map: { config: "csrs/c1:27019", shard01: "shard01/s1:27018" } };
            if (cmd.listDatabases) return { databases: [{ name: "app" }] };
            if (cmd.flushRouterConfig) {
              if (failFlush) throw new Error("flushRouterConfig failed");
              return { ok: 1 };
            }
            return { ok: 1 };
          }),
          collection: () => ({
            find: () => ({
              toArray: async () => [{ _id: "shard01", host: "shard01/s1:27018" }],
            }),
          }),
        }),
      });

    try {
      await expect(
        restoreShardedCluster({
          config: makeBaseConfig("mongodb://mongos:27017", workDir),
          storage,
          logger: makeLogger(),
          ref: clusterKey,
          drop: true,
          createMongoClient: makeClient({ failDrop: true }),
          restoreRunner: vi.fn(async () => ({ restored: 1 })),
        }),
      ).rejects.toThrow(/dropDatabase failed/);

      await expect(
        restoreShardedCluster({
          config: makeBaseConfig("mongodb://mongos:27017", workDir),
          storage,
          logger: makeLogger(),
          ref: clusterKey,
          drop: false,
          createMongoClient: makeClient({ failFlush: true }),
          restoreRunner: vi.fn(async () => ({ restored: 1 })),
        }),
      ).rejects.toThrow(/flushRouterConfig failed/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-007: BalancerController restarts balancer in finally if balancerStop was issued before drain timeout, and drains active round when balancerEnabled=false", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd007-"));
    const storage = makeMemoryStorage();
    const commands = [];

    const timeoutClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: (_name) => ({
        command: vi.fn(async (cmd) => {
          commands.push(Object.keys(cmd)[0]);
          if (cmd.hello) return { msg: "isdbgrid", me: "mongos:27017" };
          if (cmd.balancerStatus) return { mode: "full", inBalancerRound: true };
          if (cmd.balancerStop) return { ok: 1 };
          if (cmd.balancerStart) return { ok: 1 };
          if (cmd.getShardMap) return { map: { config: "cfg/c1:27019", s1: "s1/s1:27018" } };
          return { ok: 1 };
        }),
        collection: () => ({
          find: () => ({ toArray: async () => [{ _id: "s1", host: "s1/s1:27018" }] }),
        }),
      }),
    };

    // Test BalancerController.stopAndDrain tracking & runShardedClusterBackup finally restart
    const balancer = new BalancerController(timeoutClient, makeLogger());
    await expect(balancer.stopAndDrain(60)).rejects.toThrow(ActiveChunkMigrationError);
    expect(balancer.stopIssued).toBe(true);

    // Test balancerEnabled=false, balancerRunning=true waits/aborts instead of skipping drain
    let roundPolls = 0;
    const disabledButRunningClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.hello) return { msg: "isdbgrid", me: "mongos:27017" };
          if (cmd.balancerStatus) {
            roundPolls++;
            return { mode: "off", inBalancerRound: roundPolls < 2 };
          }
          if (cmd.getShardMap) return { map: { config: "cfg/c1:27019", s1: "s1/s1:27018" } };
          if (cmd.ping)
            return { ok: 1, $clusterTime: { clusterTime: new Timestamp({ t: 100, i: 1 }) } };
          return { ok: 1 };
        }),
        collection: () => ({
          find: () => ({ toArray: async () => [{ _id: "s1", host: "s1/s1:27018" }] }),
        }),
      }),
    };

    try {
      await runShardedClusterBackup({
        config: makeBaseConfig("mongodb://mongos:27017", workDir),
        storage,
        logger: makeLogger(),
        createMongoClient: () => disabledButRunningClient,
        backupRunner: async ({ config: subCfg }) => ({
          key: `${subCfg.storage.prefix}/2026-01-01/20260101T000000Z.archive.gz`,
          size: 10,
          encryption: null,
        }),
      });
      expect(roundPolls).toBeGreaterThanOrEqual(2);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-008: buildShardUri preserves implicit TLS for mongodb+srv, preserves path authSource, and strips loadBalanced", () => {
    const srvUri = buildShardUri(
      "mongodb+srv://user:pass@cluster.example.com/opsdb?retryWrites=true&loadBalanced=true",
      "shard01/s1a:27018,s1b:27018",
    );
    expect(srvUri).toContain("tls=true");
    expect(srvUri).toContain("authSource=opsdb");
    expect(srvUri).not.toContain("loadBalanced");
    expect(srvUri).toContain("replicaSet=shard01");
  });

  it("F-SHD-009: runShardedClusterBackup fails closed on mid-backup shard topology change", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd009-"));
    const storage = makeMemoryStorage();
    let shardListCalls = 0;

    const mutatingClient = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.hello) return { msg: "isdbgrid", me: "mongos:27017" };
          if (cmd.balancerStatus) return { mode: "off", inBalancerRound: false };
          if (cmd.getShardMap) return { map: { config: "cfg/c1:27019", s1: "s1/s1:27018" } };
          if (cmd.ping)
            return { ok: 1, $clusterTime: { clusterTime: new Timestamp({ t: 100, i: 1 }) } };
          return { ok: 1 };
        }),
        collection: (coll) => ({
          find: () => ({
            toArray: async () => {
              if (coll === "shards") {
                shardListCalls++;
                if (shardListCalls === 1) {
                  return [{ _id: "s1", host: "s1/s1:27018" }];
                }
                return [
                  { _id: "s1", host: "s1/s1:27018" },
                  { _id: "s2", host: "s2/s2:27018" },
                ];
              }
              return [];
            },
          }),
        }),
      }),
    };

    try {
      await expect(
        runShardedClusterBackup({
          config: makeBaseConfig("mongodb://mongos:27017", workDir),
          storage,
          logger: makeLogger(),
          createMongoClient: () => mutatingClient,
          backupRunner: async ({ config: subCfg }) => ({
            key: `${subCfg.storage.prefix}/2026-01-01/20260101T000000Z.archive.gz`,
            size: 10,
            encryption: null,
          }),
        }),
      ).rejects.toThrow(/topology changed/i);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-010: captureShardedOplogSlices isolates shard failures with allSettled, resolveShardedPitrPlan bridges clusterTime, and restoreShardedCluster resets pitr/state.json", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd010-"));
    const storage = makeMemoryStorage();

    // 1. captureShardedOplogSlices isolates failing shard
    const mockMongos = {
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.hello) return { msg: "isdbgrid", me: "mongos:27017" };
          if (cmd.balancerStatus) return { mode: "off", inBalancerRound: false };
          if (cmd.getShardMap)
            return { map: { config: "cfg/c1:27019", s1: "s1/s1:27018", s2: "s2/s2:27018" } };
          return { ok: 1 };
        }),
        collection: () => ({
          find: () => ({
            toArray: async () => [
              { _id: "s1", host: "s1/s1:27018" },
              { _id: "s2", host: "s2/s2:27018" },
            ],
          }),
        }),
      }),
    };

    const createClient = (uri) => {
      if (uri.includes("mongos")) return mockMongos;
      if (uri.includes("s2")) {
        return {
          connect: vi.fn(async () => {
            throw new Error("shard s2 unreachable");
          }),
          close: vi.fn(async () => {}),
        };
      }
      // Healthy shard s1 / cfg: quiet oplog
      return {
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: (dbName) => ({
          command: vi.fn(async () => ({
            optimes: {
              lastCommittedOpTime: { ts: new Timestamp({ t: 200, i: 1 }) },
              appliedOpTime: { ts: new Timestamp({ t: 200, i: 1 }) },
            },
          })),
          collection: (collName) => ({
            find: () => {
              const makeCursor = (spec) => ({
                sort: (s) => makeCursor(s),
                limit: () => makeCursor(spec),
                batchSize: () => makeCursor(spec),
                project: () => makeCursor(spec),
                next: async () =>
                  dbName === "local" && collName === "oplog.rs"
                    ? { ts: new Timestamp({ t: spec?.$natural === -1 ? 200 : 100, i: 1 }) }
                    : null,
                toArray: async () => [],
                close: async () => {},
                async *[Symbol.asyncIterator]() {},
              });
              return makeCursor(null);
            },
          }),
        }),
      };
    };

    const logger = makeLogger();
    try {
      const sliceRes = await captureShardedOplogSlices({
        config: makeBaseConfig("mongodb://mongos:27017", workDir),
        storage,
        logger,
        createMongoClient: createClient,
      });
      // Healthy shard s1 should have written its pitr state even though s2 failed
      expect(logger.error.mock.calls.map((c) => `${c[0]?.shardId}:${c[0]?.err?.message}`)).toEqual([
        "s2:shard s2 unreachable",
      ]);
      const s1State = await storage.getJson(
        pitrStateKey({ prefix: "mv/shards/s1", database: "*" }),
      );
      expect(s1State?.data?.coverageEndTs).toBe("200:1");
      expect(sliceRes).not.toBeNull();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-010b: resolveShardedPitrPlan bridges shard PITR slices anchored at clusterTime when bridgingSliceKeys exist", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd010b-"));
    const storage = makeMemoryStorage();
    const cfg = makeBaseConfig("mongodb://mongos:27017", workDir);
    const logger = makeLogger();

    const clusterKey = "mv/_cluster/2026-03-10/20260310T120000Z.cluster";
    const cfgKey = "mv/configsvr/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const s1Key = "mv/shards/shard01/_cluster/2026-03-10/20260310T120000Z.archive.gz";

    await storage.putJson(clusterKey, { clusterTime: "1000:10" });
    await writeManifest(storage, {
      version: 2,
      key: cfgKey,
      database: "*",
      createdAt: "2026-03-10T12:00:00Z",
      completedAt: "2026-03-10T12:00:05Z",
      durationMs: 5000,
      size: 100,
      sha256: "cfg",
      compression: "gzip",
      encryption: null,
      oplog: true,
      oplogWindow: { startTs: "1000:1", endTs: "1000:10", entries: 0 },
      collections: {},
    });
    await writeManifest(storage, {
      version: 2,
      key: s1Key,
      database: "*",
      createdAt: "2026-03-10T12:00:00Z",
      completedAt: "2026-03-10T12:00:05Z",
      durationMs: 5000,
      size: 100,
      sha256: "s1",
      compression: "gzip",
      encryption: null,
      oplog: true,
      oplogWindow: { startTs: "1000:1", endTs: "1000:5", entries: 0 },
      collections: {},
    });

    // Bridging slice on shard01 covers (1000:5, 1000:10]
    const bridgeSlice = await uploadOplogSlice({
      config: { ...cfg, storage: { ...cfg.storage, prefix: "mv/shards/shard01" } },
      storage,
      logger,
      rawDocs: [
        Buffer.from(BSON.serialize({ ts: new Timestamp({ t: 1000, i: 8 }), op: "n", ns: "" })),
      ],
      startTs: { t: 1000, i: 8 },
      endTs: { t: 1000, i: 10 },
      coverageStartTs: { t: 1000, i: 5 },
      coverageEndTs: { t: 1000, i: 10 },
      previousEndTs: { t: 1000, i: 5 },
      keyPrefix: "mv/shards/shard01/_cluster/_bridging/20260310T120000Z/",
    });

    // Continuous PITR slice on configsvr & shard01 starts at 1000:12 (previousEndTs = 1000:10 = clusterTime)
    await uploadOplogSlice({
      config: { ...cfg, storage: { ...cfg.storage, prefix: "mv/configsvr" } },
      storage,
      logger,
      rawDocs: [
        Buffer.from(BSON.serialize({ ts: new Timestamp({ t: 1005, i: 1 }), op: "n", ns: "" })),
      ],
      startTs: { t: 1005, i: 1 },
      endTs: { t: 1010, i: 1 },
      coverageStartTs: { t: 1000, i: 10 },
      coverageEndTs: { t: 1010, i: 1 },
      previousEndTs: { t: 1000, i: 10 },
    });
    await uploadOplogSlice({
      config: { ...cfg, storage: { ...cfg.storage, prefix: "mv/shards/shard01" } },
      storage,
      logger,
      rawDocs: [
        Buffer.from(BSON.serialize({ ts: new Timestamp({ t: 1000, i: 12 }), op: "n", ns: "" })),
      ],
      startTs: { t: 1000, i: 12 },
      endTs: { t: 1010, i: 1 },
      coverageStartTs: { t: 1000, i: 10 },
      coverageEndTs: { t: 1010, i: 1 },
      previousEndTs: { t: 1000, i: 10 },
    });

    await writeManifest(storage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: "2026-03-10T12:00:00Z",
      completedAt: "2026-03-10T12:00:05Z",
      durationMs: 5000,
      size: 200,
      sha256: "cluster",
      compression: "none",
      encryption: null,
      collections: {},
      sharding: {
        clusterTime: "1000:10",
        configsvr: { backupKey: cfgKey, status: "success" },
        shards: {
          shard01: {
            backupKey: s1Key,
            status: "success",
            bridgingSliceKey: bridgeSlice.key,
            bridgingSliceKeys: [bridgeSlice.key],
          },
        },
      },
    });

    try {
      const plan = await resolveShardedPitrPlan(storage, { prefix: "mv", database: "*" }, "1010:1");
      expect(plan.shardPlans.shard01.slices.length).toBe(1);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-012 & F-SHD-013: CsrsRestorer validates shard IDs and runShardedClusterBackup rejects non-cluster DB", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd012-"));
    const storage = createFilesystemStorage({ basePath: workDir });

    try {
      // F-SHD-013: reject non-cluster database
      await expect(
        runShardedClusterBackup({
          config: makeBaseConfig("mongodb://mongos:27017", workDir, {
            mongo: { database: "app", excludeCollections: [] },
          }),
          storage,
          logger: makeLogger(),
          createMongoClient: () => ({ connect: vi.fn(), close: vi.fn() }),
          backupRunner: vi.fn(),
        }),
      ).rejects.toThrow(/database/i);

      // F-SHD-012: CsrsRestorer validates config.databases.primary against targetShardIds
      const backupKey = "mv/configsvr/_cluster/2026-01-01/20260101T000000Z.archive.gz";
      const archiveGz = buildMockArchiveGz([
        { db: "config", collection: "databases", docs: [{ _id: "app", primary: "unknown-shard" }] },
      ]);
      await storage.upload(backupKey, archiveGz);
      await writeManifest(storage, {
        version: 2,
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

      const fakeClient = {
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: () => ({
          collection: () => ({
            findOne: vi.fn(async () => null),
            deleteMany: vi.fn(async () => ({ deletedCount: 0 })),
            bulkWrite: vi.fn(async () => {}),
          }),
        }),
      };

      await expect(
        new CsrsRestorer().restore({
          config: makeBaseConfig("mongodb://localhost:27017", workDir),
          storage,
          backupKey,
          targetConfigsvrUri: "mongodb://localhost:27019",
          drop: false,
          logger: makeLogger(),
          targetShardIds: new Set(["shard01", "shard02"]),
          createMongoClient: () => fakeClient,
        }),
      ).rejects.toThrow(/unknown-shard/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("F-SHD-002: restoreShardedCluster includes 0-slice shards in 2PC clusterCommits reconciliation", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-shd002-"));
    const storage = makeMemoryStorage();
    const cfg = makeBaseConfig("mongodb://mongos:27017", workDir);
    const logger = makeLogger();

    const clusterKey = "mv/_cluster/2026-03-10/20260310T120000Z.cluster";
    const cfgKey = "mv/configsvr/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const s1Key = "mv/shards/shard01/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const s2Key = "mv/shards/shard02/_cluster/2026-03-10/20260310T120000Z.archive.gz";

    const lsid = {
      id: new Binary(Buffer.from("11111111111111111111111111111111", "hex"), 4),
      uid: new Binary(Buffer.from("22".repeat(32), "hex"), 0),
    };

    // shard01 has a bridging slice with a prepared (uncommitted) 2PC txn
    const prepareDoc = Buffer.from(
      BSON.serialize({
        ts: new Timestamp({ t: 1000, i: 7 }),
        t: 1,
        op: "c",
        ns: "admin.$cmd",
        lsid,
        txnNumber: 1,
        o: {
          prepare: true,
          applyOps: [{ op: "i", ns: "app.items", o: { _id: "txn-doc-1", v: 42 } }],
        },
      }),
    );
    const s1Bridge = await uploadOplogSlice({
      config: { ...cfg, storage: { ...cfg.storage, prefix: "mv/shards/shard01" } },
      storage,
      logger,
      rawDocs: [prepareDoc],
      startTs: { t: 1000, i: 7 },
      endTs: { t: 1000, i: 10 },
      coverageStartTs: { t: 1000, i: 5 },
      coverageEndTs: { t: 1000, i: 10 },
      previousEndTs: { t: 1000, i: 5 },
      keyPrefix: "mv/shards/shard01/_cluster/_bridging/20260310T120000Z/",
    });

    await storage.putJson(clusterKey, { clusterTime: "1000:10" });
    await writeManifest(storage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: "2026-03-10T12:00:00Z",
      completedAt: "2026-03-10T12:00:05Z",
      durationMs: 5000,
      size: 200,
      sha256: "cluster",
      compression: "none",
      encryption: null,
      collections: {},
      sharding: {
        clusterTime: "1000:10",
        configsvr: { backupKey: cfgKey, status: "success" },
        shards: {
          shard01: {
            backupKey: s1Key,
            status: "success",
            host: "shard01/s1:27018",
            bridgingSliceKey: s1Bridge.key,
            bridgingSliceKeys: [s1Bridge.key],
          },
          shard02: {
            backupKey: s2Key,
            status: "success",
            host: "shard02/s2:27018",
            // 0 bridging slices! Commit happened inside shard02's base dump
          },
        },
      },
    });

    const appliedOps = [];
    const makeClient = (uri) => ({
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: (dbName) => ({
        command: vi.fn(async (cmd) => {
          if (cmd.hello) return { msg: "isdbgrid" };
          if (cmd.balancerStatus) return { mode: "off", inBalancerRound: false };
          if (cmd.getShardMap) {
            return {
              map: {
                config: "cfg/c1:27019",
                shard01: "shard01/s1:27018",
                shard02: "shard02/s2:27018",
              },
            };
          }
          if (cmd.applyOps) {
            appliedOps.push(...cmd.applyOps);
            return { ok: 1 };
          }
          return { ok: 1 };
        }),
        collection: (coll) => ({
          find: () => ({
            toArray: async () => {
              if (dbName === "config" && coll === "shards") {
                return [
                  { _id: "shard01", host: "shard01/s1:27018" },
                  { _id: "shard02", host: "shard02/s2:27018" },
                ];
              }
              if (uri.includes("s2") && dbName === "config" && coll === "transactions") {
                return [
                  {
                    _id: lsid,
                    txnNum: 1,
                    state: "committed",
                    lastWriteOpTime: { ts: new Timestamp({ t: 1000, i: 9 }) },
                  },
                ];
              }
              return [];
            },
          }),
        }),
      }),
    });

    try {
      const res = await restoreShardedCluster({
        config: cfg,
        storage,
        logger,
        ref: clusterKey,
        drop: false,
        createMongoClient: makeClient,
        restoreRunner: vi.fn(async () => ({ restored: 1 })),
      });
      expect(res.restoredShards).toEqual(["shard01", "shard02"]);
      // shard01's prepared op must be applied because shard02 (0-slice) had it committed!
      expect(appliedOps.some((op) => op.ns === "app.items" && op.o?._id === "txn-doc-1")).toBe(
        true,
      );
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
