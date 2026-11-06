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
});
