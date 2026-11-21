import { describe, expect, it, vi } from "vitest";
import { inspectMongoCluster, maskMongoUri } from "../src/mongo.js";

describe("Mongo helpers (maskMongoUri & inspectMongoCluster)", () => {
  it("maskMongoUri masks user:pass in connection string", () => {
    expect(
      maskMongoUri("mongodb://admin:secret123@db1.example.com:27017/app?authSource=admin"),
    ).toBe("mongodb://***:***@db1.example.com:27017/app?authSource=admin");
    expect(maskMongoUri("mongodb://localhost:27017/app")).toBe("mongodb://localhost:27017/app");
    expect(maskMongoUri(null)).toBe("masked");
  });

  it("inspectMongoCluster handles standalone, mongos, primary, secondary, arbiter, unknown, and errors", async () => {
    const makeClient = (helloRes, buildInfoRes = { version: "8.0.0" }) => ({
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.hello) return helloRes;
          if (cmd.isMaster) return helloRes;
          if (cmd.buildInfo) return buildInfoRes;
          return {};
        }),
      }),
    });

    const standalone = await inspectMongoCluster(makeClient({}), "mongodb://localhost:27017");
    expect(standalone.connected).toBe(true);
    expect(standalone.role).toBe("standalone");

    const mongos = await inspectMongoCluster(
      makeClient({ msg: "isdbgrid" }),
      "mongodb://localhost:27017",
    );
    expect(mongos.role).toBe("mongos");

    // Replica set with failing oplog bounds
    const makeReplicaClient = (hello) => ({
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.hello) return hello;
          if (cmd.buildInfo) return { version: "8.0.1" };
          return {};
        }),
      }),
    });
    const fakeCreateClient = () => ({
      connect: vi.fn(async () => {
        throw new Error("no oplog");
      }),
      close: vi.fn(async () => {}),
    });

    const sec = await inspectMongoCluster(
      makeReplicaClient({
        setName: "rs0",
        secondary: true,
        primary: "p1:27017",
        hosts: ["p1:27017"],
      }),
      "mongodb://localhost:27017",
      { createMongoClient: fakeCreateClient },
    );
    expect(sec.role).toBe("secondary");
    expect(sec.oplog).toEqual({ available: false });

    const arb = await inspectMongoCluster(
      makeReplicaClient({ setName: "rs0", arbiterOnly: true }),
      "mongodb://localhost:27017",
      { createMongoClient: fakeCreateClient },
    );
    expect(arb.role).toBe("arbiter");

    const unk = await inspectMongoCluster(
      makeReplicaClient({ setName: "rs0" }),
      "mongodb://localhost:27017",
      { createMongoClient: fakeCreateClient },
    );
    expect(unk.role).toBe("unknown");

    // Connection failure
    const errClient = {
      db: () => ({
        command: vi.fn(async () => {
          throw new Error("ECONNREFUSED");
        }),
      }),
    };
    const failed = await inspectMongoCluster(errClient, "mongodb://localhost:27017");
    expect(failed.connected).toBe(false);
    expect(failed.error).toBe("ECONNREFUSED");
  });
});
