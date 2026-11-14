import { Buffer } from "node:buffer";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { gzipSync } from "node:zlib";
import pino from "pino";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { runBackup } from "../src/backup.js";
import { runDrill } from "../src/drill.js";

describe("restore drill", () => {
  let tmpDir;
  let logger;
  let objects;
  let mockStorage;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "drill-test-"));
    logger = pino({ level: "silent" });
    objects = new Map();

    mockStorage = {
      bucket: "test-bucket",
      async upload(key, body) {
        const chunks = [];
        for await (const chunk of body) chunks.push(chunk);
        const data = Buffer.concat(chunks);
        objects.set(key, { data, size: data.length, metadata: {} });
      },
      async head(key) {
        const obj = objects.get(key);
        if (!obj) return null;
        return { size: obj.size, lastModified: new Date(), metadata: obj.metadata };
      },
      async download(key) {
        const obj = objects.get(key);
        if (!obj) throw new Error("NoSuchKey");
        return Readable.from(obj.data);
      },
      async putJson(key, value) {
        objects.set(key, {
          data: Buffer.from(JSON.stringify(value)),
          size: Buffer.byteLength(JSON.stringify(value)),
          json: value,
        });
      },
      async getJson(key) {
        const obj = objects.get(key);
        if (!obj?.json) return null;
        return obj.json;
      },
      async remove(key) {
        objects.delete(key);
      },
      async *list(prefix) {
        for (const [key, obj] of objects.entries()) {
          if (key.startsWith(prefix)) {
            yield { key, size: obj.size, lastModified: new Date() };
          }
        }
      },
    };
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  function createMockTools(collections) {
    const rawGzip = gzipSync(Buffer.from("dummy-drill-archive"));
    return {
      mongodump: async () => ({
        stdout: Readable.from(rawGzip),
        done: Promise.resolve({ collections }),
      }),
      mongorestore: async () => ({
        stdin: new PassThrough().on("data", () => {}),
        done: Promise.resolve({ restored: 15, failed: 0 }),
      }),
    };
  }

  it("fails when a collection is missing in target DB even if countDocuments returns 0", async () => {
    const tools = createMockTools({ users: 10, orders: 5 });
    const config = {
      mongo: { uri: "mongodb://prod:27017", database: "app", excludeCollections: [] },
      drill: { uri: "mongodb://staging:27017" },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    await runBackup({ config, storage: mockStorage, logger, tools });

    // Only "users" exists; "orders" is missing (and MongoDB countDocuments() returns 0 on missing collections)
    const mockClient = {
      async connect() {},
      async close() {},
      db() {
        return {
          async dropDatabase() {},
          listCollections() {
            return {
              async toArray() {
                return [{ name: "users" }];
              },
            };
          },
          collection(name) {
            return {
              async countDocuments() {
                return name === "users" ? 10 : 0;
              },
            };
          },
        };
      },
    };

    await expect(
      runDrill({
        config,
        storage: mockStorage,
        logger,
        tools,
        createMongoClient: () => mockClient,
      }),
    ).rejects.toThrow(/orders: expected 5, restored missing/);
  });

  it("fails cluster oplog drill if a collection has 0 documents or is missing, but allows non-zero oplog shifts", async () => {
    const tools = createMockTools({ "app.users": 100, "app.orders": 50 });
    const config = {
      mongo: { uri: "mongodb://prod:27017", database: "*", excludeCollections: [], oplog: true },
      drill: { uri: "mongodb://staging:27017" },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    await runBackup({ config, storage: mockStorage, logger, tools });

    // Case 1: app.orders restored 0 documents -> must fail even when oplog is true
    const failingClient = {
      async connect() {},
      async close() {},
      db() {
        return {
          async dropDatabase() {},
          listCollections() {
            return {
              async toArray() {
                return [{ name: "users" }, { name: "orders" }];
              },
            };
          },
          collection(name) {
            return {
              async countDocuments() {
                return name === "users" ? 102 : 0;
              },
            };
          },
        };
      },
    };

    await expect(
      runDrill({
        config,
        storage: mockStorage,
        logger,
        tools,
        createMongoClient: () => failingClient,
      }),
    ).rejects.toThrow(/app\.orders: expected 50, restored 0/);

    // Case 2: app.users has 102 and app.orders has 51 (legitimate oplog shift) -> passes
    const passingClient = {
      async connect() {},
      async close() {},
      db() {
        return {
          async dropDatabase() {},
          listCollections() {
            return {
              async toArray() {
                return [{ name: "users" }, { name: "orders" }];
              },
            };
          },
          collection(name) {
            return {
              async countDocuments() {
                return name === "users" ? 102 : 51;
              },
            };
          },
        };
      },
    };

    const result = await runDrill({
      config,
      storage: mockStorage,
      logger,
      tools,
      createMongoClient: () => passingClient,
    });
    expect(result.oplogVariances).toHaveLength(2);
  });

  it("blocks drill when DRILL_MONGODB_URI resolves via hello to the same cluster as MONGODB_URI and protects system DBs", async () => {
    const tools = createMockTools({ "app.users": 10, "admin.system.users": 1 });
    const config = {
      mongo: { uri: "mongodb://10.0.0.1:27017", database: "*", excludeCollections: [] },
      drill: { uri: "mongodb://prod-alias.internal:27017" },
      storage: { prefix: "backups" },
      workDir: tmpDir,
      timeoutMs: 5000,
      encryptionKey: null,
      schedule: { timezone: "UTC" },
    };

    await runBackup({ config, storage: mockStorage, logger, tools });

    const sameClusterClient = {
      async connect() {},
      async close() {},
      db() {
        return {
          async command(cmd) {
            if (cmd.hello) {
              return { setName: "rs0", me: "10.0.0.1:27017", hosts: ["10.0.0.1:27017"] };
            }
            return { ok: 1 };
          },
          async dropDatabase() {},
        };
      },
    };

    await expect(
      runDrill({
        config,
        storage: mockStorage,
        logger,
        tools,
        createMongoClient: () => sameClusterClient,
      }),
    ).rejects.toThrow(/Safety violation: DRILL_MONGODB_URI resolves to the same MongoDB cluster/);
  });
});
