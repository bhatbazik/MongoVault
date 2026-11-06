import { describe, expect, it } from "vitest";
import { formatStatus, getStatus } from "../src/status.js";

describe("status", () => {
  const config = {
    mongo: {
      uri: "mongodb://localhost:27017",
      database: "prod",
      excludeCollections: [],
      oplog: true,
      parallelCollections: 2,
      readPreference: "secondaryPreferred",
    },
    pitr: {
      enabled: true,
      flushIntervalSeconds: 60,
      maxSliceMb: 16,
    },
    storage: {
      type: "s3",
      bucket: "my-bucket",
      prefix: "mongodb",
      region: "us-east-1",
      forcePathStyle: false,
      accessKeyId: "key",
      secretAccessKey: "secret",
      partSizeMb: 32,
      queueSize: 2,
    },
    workDir: "/tmp/test-vault-work",
  };

  it("gathers and formats system status for a replica set", async () => {
    const mockClient = {
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.hello || cmd.isMaster) {
                return {
                  setName: "rs0",
                  isWritablePrimary: true,
                  secondary: false,
                  me: "mongo1.internal:27017",
                  primary: "mongo1.internal:27017",
                  hosts: ["mongo1.internal:27017", "mongo2.internal:27017"],
                };
              }
              if (cmd.buildInfo) {
                return { version: "7.0.5" };
              }
              return { ok: 1 };
            },
          };
        }
        if (name === "local") {
          return {
            collection() {
              return {
                find() {
                  return {
                    sort() {
                      return {
                        limit() {
                          return {
                            async next() {
                              return { ts: { t: 1774000000, i: 1 } };
                            },
                          };
                        },
                      };
                    },
                  };
                },
              };
            },
          };
        }
        return {};
      },
    };

    const backupKey = "mongodb/prod/2026-03-10/20260310T020000Z.archive.gz.enc";
    const sliceKey =
      "mongodb/prod_pitr/2026-03-10/1774000000_00000001__1774003600_00000001.oplog.bson.gz.enc";
    const mockStorage = {
      bucket: "my-bucket",
      async *list(prefix) {
        if (prefix.includes("_pitr")) {
          yield { key: sliceKey, size: 50000, lastModified: new Date() };
          yield { key: `${sliceKey}.manifest.json`, size: 500 };
        } else {
          yield { key: backupKey, size: 1000000, lastModified: new Date() };
          yield { key: `${backupKey}.manifest.json`, size: 1000 };
        }
      },
      async getJson(k) {
        if (k.endsWith(".manifest.json")) {
          return {
            data: {
              key: k.replace(".manifest.json", ""),
              database: "prod",
              createdAt: "2026-03-10T02:00:00Z",
              completedAt: "2026-03-10T02:02:00Z",
              size: 1000000,
              encryption: { algorithm: "aes-256-gcm-chunked", keyId: "test-id" },
              oplog: true,
              oplogWindow: { startTs: "1774000000:1", endTs: "1774003600:1" },
            },
          };
        }
        return null;
      },
      async head() {
        return { size: 1000000, lastModified: new Date() };
      },
    };

    const status = await getStatus({
      config,
      storage: mockStorage,
      createMongoClient: () => mockClient,
    });

    expect(status.cluster.connected).toBe(true);
    expect(status.cluster.role).toBe("primary");
    expect(status.cluster.version).toBe("7.0.5");
    expect(status.cluster.setName).toBe("rs0");
    expect(status.storage.accessible).toBe(true);
    expect(status.storage.totalBackups).toBe(1);
    expect(status.pitr.enabled).toBe(true);
    expect(status.pitr.slicesCount).toBe(1);

    const formatted = formatStatus(status);
    expect(formatted).toContain("Replica Set [rs0]");
    expect(formatted).toContain("MongoDB v7.0.5");
    expect(formatted).toContain("PRIMARY");
    expect(formatted).toContain("Point-in-Time Recovery (PITR)");
    expect(formatted).toContain("Running Operations");
  });

  it("handles offline MongoDB gracefully", async () => {
    const offlineClient = {
      async connect() {
        throw new Error("ECONNREFUSED 127.0.0.1:27017");
      },
      async close() {},
    };

    const mockStorage = {
      bucket: "my-bucket",
      async *list() {},
      async getJson() {
        return null;
      },
      async head() {
        return null;
      },
    };

    const status = await getStatus({
      config,
      storage: mockStorage,
      createMongoClient: () => offlineClient,
    });

    expect(status.cluster.connected).toBe(false);
    expect(status.cluster.error).toContain("ECONNREFUSED");

    const formatted = formatStatus(status);
    expect(formatted).toContain("Connection: FAILED");
  });
});
