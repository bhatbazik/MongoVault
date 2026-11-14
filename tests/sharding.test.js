import { describe, expect, it } from "vitest";
import { discoverClusterTopology } from "../src/sharding.js";

describe("multi-shard cluster coordinator", () => {
  it("discovers sharded cluster topology via mongos", async () => {
    const mockClient = {
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.hello || cmd.isMaster) {
                return {
                  msg: "isdbgrid",
                  me: "mongos-router.internal:27017",
                };
              }
              if (cmd.balancerStatus) {
                return { mode: "full", inBalancerRound: false };
              }
              return { ok: 1 };
            },
          };
        }
        if (name === "config") {
          return {
            collection(colName) {
              if (colName === "shards") {
                return {
                  async toArray() {
                    return [
                      {
                        _id: "shard01",
                        host: "shard01/shard01a:27018,shard01b:27018",
                        tags: ["zone-a"],
                      },
                      {
                        _id: "shard02",
                        host: "shard02/shard02a:27018,shard02b:27018",
                        tags: ["zone-b"],
                      },
                    ];
                  },
                  find() {
                    return this;
                  },
                };
              }
              return {};
            },
          };
        }
        return {};
      },
    };

    const topology = await discoverClusterTopology(mockClient);
    expect(topology.isSharded).toBe(true);
    expect(topology.mongosHost).toBe("mongos-router.internal:27017");
    expect(topology.shards.length).toBe(2);
    expect(topology.shards[0].id).toBe("shard01");
    expect(topology.shards[1].id).toBe("shard02");
    expect(topology.balancerEnabled).toBe(true);
  });

  it("detects non-sharded replica set topology properly", async () => {
    const mockClient = {
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.hello || cmd.isMaster) {
                return {
                  setName: "rs0",
                  isWritablePrimary: true,
                };
              }
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    const topology = await discoverClusterTopology(mockClient);
    expect(topology.isSharded).toBe(false);
    expect(topology.shards.length).toBe(0);
  });

  it("waits for cluster balancer to finish active chunk migration rounds", async () => {
    let polls = 0;
    const mockClient = {
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.balancerStatus) {
                polls++;
                if (polls === 1) {
                  return { mode: "off", inBalancerRound: true };
                }
                return { mode: "off", inBalancerRound: false };
              }
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    const { waitForBalancerDisabled } = await import("../src/sharding.js");
    const status = await waitForBalancerDisabled(mockClient, 5000, 50);
    expect(status.inBalancerRound).toBe(false);
    expect(status.mode).toBe("off");
    expect(polls).toBe(2);
  });

  it("aborts backup with ActiveChunkMigrationError if chunk migrations fail to drain within timeout", async () => {
    const mockClient = {
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.balancerStop) return { ok: 1 };
              if (cmd.balancerStatus) {
                return { mode: "off", inBalancerRound: true };
              }
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    const { BalancerController, ActiveChunkMigrationError } = await import("../src/sharding.js");
    const balancer = new BalancerController(mockClient);
    await expect(balancer.stopAndDrain(100)).rejects.toThrow(ActiveChunkMigrationError);
  });

  it("builds shard connection URIs preserving credentials and query options", async () => {
    const { buildShardUri } = await import("../src/sharding.js");
    const uri1 = buildShardUri(
      "mongodb://backupUser:s3cr3t@mongos.internal:27017/admin?authSource=admin&tls=true",
      "shard01/s1a.internal:27018,s1b.internal:27018",
    );
    expect(uri1).toBe(
      "mongodb://backupUser:s3cr3t@s1a.internal:27018,s1b.internal:27018/?authSource=admin&tls=true&replicaSet=shard01",
    );

    const uri2 = buildShardUri(
      "mongodb://localhost:27017/?replicaSet=oldRs",
      "standalone-shard:27018",
    );
    expect(uri2).toBe("mongodb://standalone-shard:27018/");
  });

  it("coordinates distributed sharded cluster backup and restore with isolated shard prefixes", async () => {
    const { runShardedClusterBackup, restoreShardedCluster } = await import("../src/sharding.js");

    const mockClient = {
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.hello || cmd.isMaster) {
                return { msg: "isdbgrid", me: "mongos:27017" };
              }
              if (cmd.balancerStatus) {
                return { mode: "off", inBalancerRound: false };
              }
              if (cmd.ping) {
                return {
                  ok: 1,
                  $clusterTime: { clusterTime: { t: 1774092000, i: 5 } },
                };
              }
              return { ok: 1 };
            },
          };
        }
        if (name === "config") {
          return {
            collection() {
              return {
                find() {
                  return this;
                },
                async toArray() {
                  return [
                    { _id: "config", host: "cfg/cfg1:27019" },
                    { _id: "shard01", host: "shard01/s1a:27018,s1b:27018" },
                    { _id: "shard02", host: "shard02/s2a:27018,s2b:27018" },
                  ];
                },
              };
            },
          };
        }
        return {};
      },
    };

    const jsonStore = new Map();
    const mockStorage = {
      async putJson(k, v) {
        jsonStore.set(k, v);
      },
      async getJson(k) {
        return jsonStore.has(k) ? { data: jsonStore.get(k) } : null;
      },
    };

    const mockLogger = {
      child() {
        return this;
      },
      info() {},
      warn() {},
      error() {},
      debug() {},
    };

    const recordedBackups = [];
    const config = {
      mongo: {
        uri: "mongodb://admin:pass@mongos:27017/?authSource=admin",
        database: "*",
      },
      storage: { prefix: "prod" },
    };

    const res = await runShardedClusterBackup({
      config,
      storage: mockStorage,
      logger: mockLogger,
      createMongoClient: () => mockClient,
      backupRunner: async ({ config: subConfig }) => {
        recordedBackups.push({
          prefix: subConfig.storage.prefix,
          uri: subConfig.mongo.uri,
          db: subConfig.mongo.database,
        });
        return {
          key: `${subConfig.storage.prefix}/2026-03-10/20260310T120000Z.archive.gz`,
          size: 1024,
          encryption: null,
        };
      },
    });

    expect(recordedBackups.length).toBe(3);
    expect(recordedBackups[0].prefix).toBe("prod/configsvr");
    expect(recordedBackups[1].prefix).toBe("prod/shards/shard01");
    expect(recordedBackups[2].prefix).toBe("prod/shards/shard02");
    expect(res.clusterMeta.clusterTime).toBe("1774092000:5");
    expect(res.manifest.size).toBe(3072);

    const restoredTargets = [];
    const restoreRes = await restoreShardedCluster({
      config,
      storage: mockStorage,
      logger: mockLogger,
      ref: res.manifest.key,
      createMongoClient: () => mockClient,
      restoreRunner: async (_ctx, opts) => {
        restoredTargets.push(opts);
        return { key: opts.ref, restoredCollections: 1 };
      },
    });

    expect(restoreRes.configsvrRestored).toBe(true);
    expect(restoreRes.restoredShards).toEqual(["shard01", "shard02"]);
    expect(restoredTargets.length).toBe(3);
    // Config Server restore
    expect(restoredTargets[0].database).toBe("config");
    // Shard restores are anchored to clusterTime with oplogReplay
    expect(restoredTargets[1].database).toBe("*");
    expect(restoredTargets[1].oplogReplay).toBe(true);
    expect(restoredTargets[1].oplogLimit).toBe("1774092000:6");
    expect(restoredTargets[2].database).toBe("*");
    expect(restoredTargets[2].oplogReplay).toBe(true);
    expect(restoredTargets[2].oplogLimit).toBe("1774092000:6");
  });

  it("fails closed in stopAndDrain if balancer mode remains enabled even if not in a round", async () => {
    const mockClient = {
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.balancerStop) return { ok: 1 };
              // Balancer is not currently in a balancing round, but mode is still "full" (enabled)
              if (cmd.balancerStatus) {
                return { mode: "full", inBalancerRound: false };
              }
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    const { BalancerController, ActiveChunkMigrationError } = await import("../src/sharding.js");
    const balancer = new BalancerController(mockClient);
    await expect(balancer.stopAndDrain(100)).rejects.toThrow(ActiveChunkMigrationError);
  });

  it("fails closed in stopAndDrain if balancerStatus throws an error", async () => {
    const mockClient = {
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.balancerStop) return { ok: 1 };
              if (cmd.balancerStatus) {
                throw new Error("Cluster unreachable");
              }
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    const { BalancerController, ActiveChunkMigrationError } = await import("../src/sharding.js");
    const balancer = new BalancerController(mockClient);
    await expect(balancer.stopAndDrain(100)).rejects.toThrow(ActiveChunkMigrationError);
  });

  it("resolves sharded PITR plan across configsvr and all shards and fails closed on missing coverage", async () => {
    const { resolveShardedPitrPlan } = await import("../src/sharding.js");
    const { writeManifest } = await import("../src/catalog.js");
    const { uploadOplogSlice, toBsonTimestamp } = await import("../src/oplog.js");
    const { BSON } = await import("mongodb");
    const { Readable } = await import("node:stream");

    const objects = new Map();
    const mockStorage = {
      async upload(key, body) {
        const chunks = [];
        for await (const c of body) chunks.push(c);
        const data = Buffer.concat(chunks);
        objects.set(key, { data, size: data.length, lastModified: new Date() });
      },
      async head(key) {
        const obj = objects.get(key);
        return obj ? { size: obj.size, lastModified: obj.lastModified, metadata: {} } : null;
      },
      async download(key) {
        return Readable.from(objects.get(key).data);
      },
      async putJson(key, value) {
        const data = Buffer.from(JSON.stringify(value));
        objects.set(key, { data, size: data.length, json: value, lastModified: new Date() });
      },
      async getJson(key) {
        const obj = objects.get(key);
        return obj?.json ? { data: obj.json } : null;
      },
      async *list(prefix) {
        for (const [key, obj] of objects.entries()) {
          if (key.startsWith(prefix)) {
            yield { key, size: obj.size, lastModified: obj.lastModified || new Date() };
          }
        }
      },
    };
    const mockLogger = {
      child() {
        return this;
      },
      info() {},
      warn() {},
      error() {},
      debug() {},
    };

    const clusterKey = "prod/_cluster/2026-03-10/20260310T120000Z.cluster";
    const cfgKey = "prod/configsvr/_cluster/2026-03-10/20260310T120000Z.archive.gz";
    const s1Key = "prod/shards/shard01/_cluster/2026-03-10/20260310T120000Z.archive.gz";

    await mockStorage.putJson(clusterKey, { clusterTime: "1000:5" });
    await writeManifest(mockStorage, {
      version: 2,
      type: "sharded-cluster-backup",
      engine: "logical",
      key: clusterKey,
      database: "*",
      createdAt: "2026-03-10T12:00:00.000Z",
      completedAt: "2026-03-10T12:01:00.000Z",
      durationMs: 60000,
      size: 100,
      sha256: "sharded-cluster",
      compression: "gzip",
      encryption: null,
      oplog: true,
      excludedCollections: [],
      collections: {},
      sharding: {
        clusterTime: "1000:5",
        configsvr: {
          backupKey: cfgKey,
          manifestKey: `${cfgKey}.manifest.json`,
          host: "cfg/c1:27019",
        },
        shards: {
          shard01: {
            shardId: "shard01",
            host: "shard01/s1:27018",
            backupKey: s1Key,
            status: "success",
          },
        },
      },
    });

    await mockStorage.upload(cfgKey, Readable.from([Buffer.from("cfg")]));
    await writeManifest(mockStorage, {
      version: 2,
      key: cfgKey,
      database: "*",
      createdAt: "2026-03-10T12:00:00.000Z",
      completedAt: "2026-03-10T12:01:00.000Z",
      durationMs: 1000,
      size: 3,
      sha256: "cfg",
      compression: "gzip",
      encryption: null,
      oplog: true,
      oplogWindow: { startTs: "1000:1", endTs: "1000:5" },
      excludedCollections: [],
      collections: {},
    });

    await mockStorage.upload(s1Key, Readable.from([Buffer.from("s1")]));
    await writeManifest(mockStorage, {
      version: 2,
      key: s1Key,
      database: "*",
      createdAt: "2026-03-10T12:00:00.000Z",
      completedAt: "2026-03-10T12:01:00.000Z",
      durationMs: 1000,
      size: 2,
      sha256: "s1",
      compression: "gzip",
      encryption: null,
      oplog: true,
      oplogWindow: { startTs: "1000:1", endTs: "1000:5" },
      excludedCollections: [],
      collections: {},
    });

    await expect(
      resolveShardedPitrPlan(mockStorage, { prefix: "prod", database: "*" }, "1010:1"),
    ).rejects.toThrow("no oplog slices are available");

    await uploadOplogSlice({
      config: {
        mongo: { uri: "mongodb://c1:27019", database: "*" },
        storage: { prefix: "prod/configsvr" },
      },
      storage: mockStorage,
      logger: mockLogger,
      rawDocs: [
        Buffer.from(
          BSON.serialize({
            ts: toBsonTimestamp({ t: 1008, i: 1 }),
            op: "i",
            ns: "config.databases",
            o: { _id: "app" },
          }),
        ),
      ],
      startTs: { t: 1000, i: 6 },
      endTs: { t: 1012, i: 1 },
      previousEndTs: { t: 1000, i: 5 },
    });

    await uploadOplogSlice({
      config: {
        mongo: { uri: "mongodb://s1:27018", database: "*" },
        storage: { prefix: "prod/shards/shard01" },
      },
      storage: mockStorage,
      logger: mockLogger,
      rawDocs: [
        Buffer.from(
          BSON.serialize({
            ts: toBsonTimestamp({ t: 1009, i: 1 }),
            op: "i",
            ns: "app.users",
            o: { _id: 1 },
          }),
        ),
      ],
      startTs: { t: 1000, i: 6 },
      endTs: { t: 1012, i: 1 },
      previousEndTs: { t: 1000, i: 5 },
    });

    const plan = await resolveShardedPitrPlan(
      mockStorage,
      { prefix: "prod", database: "*" },
      "1010:1",
    );
    expect(plan.clusterBackupKey).toBe(clusterKey);
    expect(plan.configsvrPlan.slices.length).toBe(1);
    expect(plan.shardPlans.shard01.slices.length).toBe(1);
  });

  it("restarts cluster balancer in finally when restoreShardedCluster fails mid-restore", async () => {
    const { restoreShardedCluster } = await import("../src/sharding.js");
    let balancerStarted = false;
    let balancerMode = "full";

    const mockClient = {
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.hello || cmd.isMaster) return { msg: "isdbgrid", me: "mongos:27017" };
              if (cmd.balancerStop) {
                balancerMode = "off";
                return { ok: 1 };
              }
              if (cmd.balancerStart) {
                balancerStarted = true;
                balancerMode = "full";
                return { ok: 1 };
              }
              if (cmd.balancerStatus) return { mode: balancerMode, inBalancerRound: false };
              return { ok: 1 };
            },
          };
        }
        if (name === "config") {
          return {
            collection() {
              return {
                find() {
                  return this;
                },
                async toArray() {
                  return [{ _id: "shard01", host: "shard01/s1:27018" }];
                },
              };
            },
          };
        }
        return {};
      },
    };

    const manifest = {
      version: 2,
      type: "sharded-cluster-backup",
      engine: "logical",
      key: "prod/2026-03-10/20260310T120000Z.cluster",
      database: "*",
      sharding: {
        clusterTime: "1774092000:5",
        configsvr: {
          backupKey: "cfg-key",
          manifestKey: "cfg-key.manifest.json",
          host: "cfg/c1:27019",
        },
        shards: {
          shard01: {
            shardId: "shard01",
            host: "shard01/s1:27018",
            backupKey: "s1-key",
            status: "success",
          },
        },
      },
    };

    const mockStorage = {
      async getJson() {
        return { data: manifest };
      },
    };
    const mockLogger = {
      child() {
        return this;
      },
      info() {},
      warn() {},
      error() {},
      debug() {},
    };

    await expect(
      restoreShardedCluster({
        config: {
          mongo: { uri: "mongodb://mongos:27017", database: "*" },
          storage: { prefix: "prod" },
        },
        storage: mockStorage,
        logger: mockLogger,
        ref: manifest.key,
        createMongoClient: () => mockClient,
        restoreRunner: async () => {
          throw new Error("Simulated shard restore failure");
        },
      }),
    ).rejects.toThrow("Simulated shard restore failure");

    expect(balancerStarted).toBe(true);
  });
});
