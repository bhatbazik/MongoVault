import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";

const { mockCheckDbTools, mockCheckTool } = vi.hoisted(() => ({
  mockCheckDbTools: vi.fn(),
  mockCheckTool: vi.fn(),
}));

vi.mock("../src/tools.js", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    checkDatabaseTools: mockCheckDbTools,
    checkTool: mockCheckTool,
  };
});

import { formatPreflightCheck, runPreflightCheck } from "../src/check.js";
import { generateKey } from "../src/crypto.js";

describe("Preflight Check (check.ts full coverage)", () => {
  const baseConfig = {
    engine: "logical",
    workDir: path.join(os.tmpdir(), `mv-check-${Date.now()}`),
    encryptionKey: generateKey(),
    storage: { type: "filesystem", prefix: "mv" },
    mongo: { uri: "mongodb://localhost:27017", database: "*", oplog: true },
    pitr: { enabled: true },
    sharding: { enabled: true },
  };

  it("checks tools, mongo, oplog, pitr, sharding (mongos + shards), physical, storage, encryption, and scratch", async () => {
    mockCheckDbTools.mockResolvedValueOnce({
      mongodump: { available: true, version: "100.9.4" },
      mongorestore: { available: true, version: "100.9.4" },
    });
    mockCheckTool.mockResolvedValueOnce({ available: true, version: "8.0.0" });

    const fakeMongoClient = () => ({
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.ping) return { ok: 1 };
          if (cmd.hello) return { msg: "isdbgrid", me: "mongos:27017", setName: "rs0" };
          return {};
        }),
        collection: () => ({
          estimatedDocumentCount: vi.fn(async () => 42),
          findOne: vi.fn(async () => null),
          find: () => ({ toArray: vi.fn(async () => [{ _id: "shard1" }]) }),
        }),
      }),
    });

    const storeMap = new Map();
    const storage = {
      bucket: "test-bkt",
      putJson: vi.fn(async (k, v) => storeMap.set(k, v)),
      getJson: vi.fn(async (k) => ({ data: storeMap.get(k) })),
      head: vi.fn(async () => ({ size: 12 })),
      remove: vi.fn(async (k) => storeMap.delete(k)),
    };

    const res = await runPreflightCheck({
      config: { ...baseConfig, engine: "physical", physical: { dbPath: os.tmpdir() } },
      storage,
      createMongoClient: fakeMongoClient,
    });

    expect(res.allPassed).toBe(true);
    const formatted = formatPreflightCheck(res);
    expect(formatted).toContain("All system prerequisites PASSED");
  });

  it("reports failures across tools, mongo, oplog, pitr, sharding, physical dbPath, storage, and encryption", async () => {
    mockCheckDbTools.mockRejectedValueOnce(new Error("spawn error"));
    mockCheckTool.mockResolvedValueOnce({ available: false, error: "mongod missing" });

    const fakeMongoClient = () => ({
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.ping) return { ok: 1 };
          if (cmd.hello) return { me: "node1:27017" }; // not mongos!
          return {};
        }),
        collection: () => ({
          estimatedDocumentCount: vi.fn(async () => {
            throw new Error("no oplog");
          }),
          findOne: vi.fn(async () => {
            throw new Error("no txns");
          }),
        }),
      }),
    });

    const badStorage = {
      bucket: "bad-bkt",
      putJson: vi.fn(async () => {
        throw new Error("S3 access denied");
      }),
    };

    const res = await runPreflightCheck({
      config: {
        ...baseConfig,
        engine: "physical",
        physical: { dbPath: "/nonexistent/path/xyz-123" },
        encryptionKey: "invalid-short-key",
      },
      storage: badStorage,
      createMongoClient: fakeMongoClient,
    });

    expect(res.allPassed).toBe(false);
    expect(formatPreflightCheck(res)).toContain("One or more prerequisite checks FAILED");
  });

  it("handles mongo connection failure and mongos with 0 shards / config.shards error", async () => {
    mockCheckDbTools.mockResolvedValue({
      mongodump: { available: false },
      mongorestore: { available: false },
    });

    const failingClient = () => ({
      connect: vi.fn(async () => {
        throw new Error("ECONNREFUSED");
      }),
      close: vi.fn(async () => {}),
    });

    const storage = {
      bucket: "b",
      putJson: vi.fn(async () => {}),
      getJson: vi.fn(async () => ({ data: { probe: true } })),
      head: vi.fn(async () => ({ size: 10 })),
      remove: vi.fn(async () => {}),
    };

    const r1 = await runPreflightCheck({
      config: { ...baseConfig, encryptionKey: undefined },
      storage,
      createMongoClient: failingClient,
    });
    expect(r1.allPassed).toBe(false);

    // mongos with 0 shards
    const zeroShardClient = () => ({
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: () => ({
        command: vi.fn(async (cmd) => {
          if (cmd.ping) return { ok: 1 };
          if (cmd.hello) return { msg: "isdbgrid" };
          return {};
        }),
        collection: () => ({
          estimatedDocumentCount: vi.fn(async () => 0),
          findOne: vi.fn(async () => null),
          find: () => ({ toArray: vi.fn(async () => []) }),
        }),
      }),
    });

    const r2 = await runPreflightCheck({
      config: baseConfig,
      storage,
      createMongoClient: zeroShardClient,
    });
    expect(r2.checks.find((c) => c.name === "Sharded Cluster (mongos)")?.passed).toBe(false);
  });
});
