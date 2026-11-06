import { describe, expect, it } from "vitest";
import { formatPreflightCheck, runPreflightCheck } from "../src/check.js";
import { generateKey } from "../src/crypto.js";

describe("check (preflight verification)", () => {
  const validKey = generateKey();
  const config = {
    mongo: {
      uri: "mongodb://localhost:27017",
      database: "prod",
      excludeCollections: [],
      oplog: false,
      parallelCollections: 2,
    },
    storage: {
      type: "s3",
      bucket: "my-bucket",
      prefix: "mongodb",
    },
    workDir: "/tmp/preflight-test-work",
    encryptionKey: Buffer.from(validKey, "base64"),
    encryptionKeyRing: [Buffer.from(validKey, "base64")],
  };

  it("verifies all checks pass under healthy conditions", async () => {
    const mockClient = {
      async connect() {},
      async close() {},
      db() {
        return {
          async command() {
            return { ok: 1, me: "node1:27017" };
          },
        };
      },
    };

    let probeStored = null;
    const mockStorage = {
      bucket: "my-bucket",
      async putJson(key, data) {
        probeStored = data;
      },
      async getJson() {
        return { data: probeStored };
      },
      async head() {
        return { size: 42 };
      },
      async remove() {
        probeStored = null;
      },
    };

    const result = await runPreflightCheck({
      config,
      storage: mockStorage,
      createMongoClient: () => mockClient,
    });

    expect(result.checks.length).toBeGreaterThanOrEqual(4);
    const mongoCheck = result.checks.find((c) => c.name.includes("MongoDB Connectivity"));
    expect(mongoCheck?.passed).toBe(true);

    const storageCheck = result.checks.find((c) => c.name.includes("Storage Backend"));
    expect(storageCheck?.passed).toBe(true);

    const cryptoCheck = result.checks.find((c) => c.name.includes("Client-Side Encryption"));
    expect(cryptoCheck?.passed).toBe(true);

    const formatted = formatPreflightCheck(result);
    expect(formatted).toContain("Preflight System Verification");
  });

  it("identifies failing checks with clear diagnostic errors", async () => {
    const failingClient = {
      async connect() {
        throw new Error("Authentication failed for user admin");
      },
      async close() {},
    };

    const failingStorage = {
      bucket: "my-bucket",
      async putJson() {
        throw new Error("AccessDenied: PutObject permission missing");
      },
      async getJson() {
        return null;
      },
      async head() {
        return null;
      },
      async remove() {},
    };

    const result = await runPreflightCheck({
      config,
      storage: failingStorage,
      createMongoClient: () => failingClient,
    });

    expect(result.allPassed).toBe(false);

    const mongoCheck = result.checks.find((c) => c.name.includes("MongoDB Connectivity"));
    expect(mongoCheck?.passed).toBe(false);
    expect(mongoCheck?.message).toContain("Authentication failed");

    const storageCheck = result.checks.find((c) => c.name.includes("Storage Backend"));
    expect(storageCheck?.passed).toBe(false);
    expect(storageCheck?.message).toContain("AccessDenied");

    const formatted = formatPreflightCheck(result);
    expect(formatted).toContain("FAIL");
    expect(formatted).toContain("One or more prerequisite checks FAILED");
  });
});
