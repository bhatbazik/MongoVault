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
  });
});
