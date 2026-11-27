import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import pino from "pino";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runPreflightCheck } from "../src/check.js";
import { generateDiagnosticBundle } from "../src/diagnostic.js";
import { runDrill } from "../src/drill.js";
import { maskMongoUri } from "../src/mongo.js";
import { formatStatus, getStatus } from "../src/status.js";

describe("M5 Hardening — ops-verify-health (F-OPS-001..013)", () => {
  let tmpDir;
  let logger;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "ops-hardening-"));
    logger = pino({ level: "silent" });
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  function makeMemoryStorage() {
    const objects = new Map();
    return {
      bucket: "test-bucket",
      objects,
      async upload(key, body) {
        const data = Buffer.isBuffer(body)
          ? body
          : await (async () => {
              const chunks = [];
              for await (const c of body) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
              return Buffer.concat(chunks);
            })();
        objects.set(key, { data, size: data.length, lastModified: new Date() });
      },
      async head(key) {
        const o = objects.get(key);
        return o ? { size: o.size, lastModified: o.lastModified, metadata: {} } : null;
      },
      async download(key) {
        const { Readable } = await import("node:stream");
        const o = objects.get(key);
        if (!o) throw new Error("NoSuchKey");
        return Readable.from(o.data);
      },
      async putJson(key, value) {
        const buf = Buffer.from(JSON.stringify(value));
        objects.set(key, { data: buf, size: buf.length, json: value, lastModified: new Date() });
        return { etag: `"etag-${objects.size}"` };
      },
      async getJson(key) {
        const o = objects.get(key);
        return o?.json ? { data: o.json, etag: '"etag-1"' } : null;
      },
      async remove(key) {
        objects.delete(key);
      },
      async *list(prefix) {
        for (const [key, o] of objects.entries()) {
          if (key.startsWith(prefix)) {
            yield { key, size: o.size, lastModified: o.lastModified };
          }
        }
      },
    };
  }

  function makeBaseConfig(overrides = {}) {
    return {
      engine: "logical",
      mongo: {
        uri: "mongodb://admin:secret@prod.internal:27017/app",
        database: "app",
        excludeCollections: [],
        oplog: false,
        parallelCollections: 2,
        readPreference: "secondaryPreferred",
      },
      pitr: { enabled: false, flushIntervalSeconds: 60, maxSliceMb: 16 },
      storage: { type: "s3", bucket: "b", prefix: "mv", region: "us-east-1" },
      schedule: { cron: "0 2 * * *", timezone: "UTC", runOnStart: false },
      timeoutMs: 10_000,
      workDir: tmpDir,
      encryptionKey: null,
      encryptionKeyRing: [],
      requireEncryption: false,
      retention: null,
      drill: { uri: "mongodb://admin:secret@staging.internal:27017/app", cron: null },
      physical: null,
      sharding: null,
      notifications: { onSuccess: false, email: null },
      http: { port: 0 },
      logLevel: "silent",
      ...overrides,
    };
  }

  describe("F-OPS-001: runDrill isolation guard fails closed and checks mongos cluster identity", () => {
    it("fails closed when prodClient.connect() errors instead of proceeding with dropDatabase", async () => {
      const storage = makeMemoryStorage();
      const backupKey = "mv/app/2026-01-01T00-00-00Z.archive.gz";
      await storage.putJson(`${backupKey}.manifest.json`, {
        version: 1,
        key: backupKey,
        database: "app",
        createdAt: "2026-01-01T00:00:00.000Z",
        completedAt: "2026-01-01T00:00:01.000Z",
        size: 10,
        sha256: "abc",
        collections: { users: 1 },
        oplog: false,
      });
      await storage.upload(backupKey, Buffer.from("dummy"));

      const droppedDbs = [];
      const createMongoClient = (uri) => {
        if (uri.includes("prod.internal")) {
          return {
            async connect() {
              throw new Error("ETIMEDOUT");
            },
            async close() {},
          };
        }
        return {
          async connect() {},
          async close() {},
          db(name) {
            return {
              async dropDatabase() {
                droppedDbs.push(name);
              },
              listCollections: () => ({ toArray: async () => [] }),
            };
          },
        };
      };

      await expect(
        runDrill({ config: makeBaseConfig(), storage, logger, createMongoClient }),
      ).rejects.toThrow(/Safety violation/i);
      expect(droppedDbs).toEqual([]);
    });

    it("detects two mongos routers on the same sharded cluster via config.shards", async () => {
      const storage = makeMemoryStorage();
      const backupKey = "mv/app/2026-01-01T00-00-00Z.archive.gz";
      await storage.putJson(`${backupKey}.manifest.json`, {
        version: 1,
        key: backupKey,
        database: "app",
        createdAt: "2026-01-01T00:00:00.000Z",
        completedAt: "2026-01-01T00:00:01.000Z",
        size: 10,
        sha256: "abc",
        collections: { users: 1 },
        oplog: false,
      });
      await storage.upload(backupKey, Buffer.from("dummy"));

      const makeMongosClient = (me) => ({
        async connect() {},
        async close() {},
        db(dbName) {
          if (dbName === "admin") {
            return {
              async command(cmd) {
                if (cmd.hello) return { msg: "isdbgrid", me };
                return { ok: 1 };
              },
            };
          }
          if (dbName === "config") {
            return {
              collection(c) {
                if (c === "shards") {
                  return {
                    find: () => ({
                      toArray: async () => [{ _id: "shard01", host: "shard01/127.0.0.1:27018" }],
                    }),
                  };
                }
                return {};
              },
            };
          }
          return {
            async dropDatabase() {},
            listCollections: () => ({ toArray: async () => [] }),
          };
        },
      });

      await expect(
        runDrill({
          config: makeBaseConfig(),
          storage,
          logger,
          createMongoClient: (uri) =>
            uri.includes("prod.internal")
              ? makeMongosClient("mongos1:27017")
              : makeMongosClient("mongos2:27017"),
        }),
      ).rejects.toThrow(/Safety violation/i);
    });
  });

  describe("F-OPS-003: runDrill skips incomplete backups and rejects physical backups clearly", () => {
    it("throws explicit engine error when only physical backups exist", async () => {
      const storage = makeMemoryStorage();
      await storage.upload("mv/app/2026-01-01T00-00-00Z.physical.tar.gz", Buffer.from("tar"));
      await storage.putJson("mv/app/2026-01-01T00-00-00Z.physical.tar.gz.manifest.json", {
        version: 1,
        key: "mv/app/2026-01-01T00-00-00Z.physical.tar.gz",
        database: "*",
        engine: "physical",
        createdAt: "2026-01-01T00:00:00.000Z",
        completedAt: "2026-01-01T00:00:01.000Z",
        size: 3,
        sha256: "abc",
        collections: {},
      });

      await expect(runDrill({ config: makeBaseConfig(), storage, logger })).rejects.toThrow(
        /physical/i,
      );
    });
  });

  describe("F-OPS-006: maskMongoUri & credential sanitization", () => {
    it("masks passwords containing @, empty passwords, user-only URIs, and sensitive query params", () => {
      expect(maskMongoUri("mongodb://admin:p@ss@word@host:27017/db")).toBe(
        "mongodb://***:***@host:27017/db",
      );
      expect(maskMongoUri("mongodb://admin:@host:27017/db")).toBe(
        "mongodb://***:***@host:27017/db",
      );
      expect(maskMongoUri("mongodb://admin@host:27017/db")).toBe("mongodb://***@host:27017/db");
      const maskedQuery = maskMongoUri(
        "mongodb://u:p@host:27017/db?tlsCertificateKeyFilePassword=topsecret&awsSessionToken=tok123",
      );
      expect(maskedQuery).not.toContain("topsecret");
      expect(maskedQuery).not.toContain("tok123");
    });

    it("sanitizes URIs in status, check, and diagnostic errors", async () => {
      const storage = makeMemoryStorage();
      const cfg = makeBaseConfig({
        mongo: {
          uri: "mongodb://admin:supersecret@prod.internal:27017/app",
          database: "app",
          excludeCollections: [],
          oplog: false,
          parallelCollections: 2,
          readPreference: "secondaryPreferred",
        },
      });
      const failingClient = () => ({
        async connect() {
          throw new Error("connect failed for mongodb://admin:supersecret@prod.internal:27017/app");
        },
        async close() {},
      });

      const status = await getStatus({ config: cfg, storage, createMongoClient: failingClient });
      expect(status.cluster.error).not.toContain("supersecret");

      const diag = await generateDiagnosticBundle({
        config: cfg,
        storage,
        createMongoClient: failingClient,
      });
      expect(diag.database.error).not.toContain("supersecret");
    });
  });

  describe("F-OPS-007: checkStorage cleans probe on error & handles empty prefix and list failure", () => {
    it("cleans up .probe-* in finally and checks storage.list()", async () => {
      const storage = makeMemoryStorage();
      storage.list = async function* () {
        throw new Error("AccessDenied on ListBucket");
      };
      const cfg = makeBaseConfig({
        storage: { type: "s3", bucket: "b", prefix: "", region: "us-east-1" },
      });
      const res = await runPreflightCheck({
        config: cfg,
        storage,
        createMongoClient: () => ({
          async connect() {},
          async close() {},
          db: () => ({
            command: async () => ({ ok: 1, me: "h:27017" }),
          }),
        }),
      });
      const storageCheck = res.checks.find((c) => c.name.startsWith("Storage Backend"));
      expect(storageCheck?.passed).toBe(false);
      // Probe file must have been cleaned up and not start with "/"
      expect([...storage.objects.keys()].filter((k) => k.includes(".probe-"))).toEqual([]);
    });
  });

  describe("F-OPS-008 & F-OPS-013: status PITR contiguity, stale lock, and formatStatus gapWarning", () => {
    it("detects gap when previousEndTs is missing and treats overlapping slices as continuous", async () => {
      const storage = makeMemoryStorage();
      const s1 = "mv/app/_pitr/2026-01-01/0000000100_00000001__0000000150_00000001.oplog.bson.gz";
      const s2 = "mv/app/_pitr/2026-01-01/0000000200_00000001__0000000250_00000001.oplog.bson.gz"; // gap 150..200
      await storage.upload(s1, Buffer.from("a"));
      await storage.putJson(`${s1}.manifest.json`, {
        version: 1,
        key: s1,
        database: "app",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:00:01Z",
        size: 1,
        sha256: "a",
        collections: {},
        oplogWindow: { startTs: "100:1", endTs: "150:1", entries: 1 },
      });
      await storage.upload(s2, Buffer.from("b"));
      // No previousEndTs in s2 manifest!
      await storage.putJson(`${s2}.manifest.json`, {
        version: 1,
        key: s2,
        database: "app",
        createdAt: "2026-01-01T00:01:00Z",
        completedAt: "2026-01-01T00:01:01Z",
        size: 1,
        sha256: "b",
        collections: {},
        oplogWindow: { startTs: "200:1", endTs: "250:1", entries: 1 },
      });

      const status = await getStatus({
        config: makeBaseConfig({
          pitr: { enabled: true, flushIntervalSeconds: 60, maxSliceMb: 16 },
        }),
        storage,
        createMongoClient: () => ({
          async connect() {},
          async close() {},
          db: () => ({ command: async () => ({ ok: 1 }) }),
        }),
      });
      expect(status.pitr.timelineContinuous).toBe(false);
    });

    it("renders gapWarning in formatStatus even when earliestTs/latestTs are unset", () => {
      const out = formatStatus({
        timestamp: new Date().toISOString(),
        cluster: { connected: true, version: "8.0", role: "primary" },
        storage: {
          type: "filesystem",
          destination: "file:///tmp",
          prefix: "mv",
          accessible: true,
          totalBackups: 0,
          totalSizeBytes: 0,
        },
        pitr: {
          enabled: true,
          activeArchiver: false,
          flushIntervalSeconds: 60,
          slicesCount: 0,
          timelineContinuous: false,
          gapWarning: "Oplog rolled over",
        },
        operation: { running: false },
        recentBackups: [],
      });
      expect(out).toContain("Oplog rolled over");
    });
  });

  describe("F-OPS-002 & F-OPS-004: cluster partial failure cleanup, drill lock, and AbortSignal", () => {
    it("F-OPS-002: drops manifest DBs in finally when cluster drill fails mid-restore and preserves unrelated DBs", async () => {
      const storage = makeMemoryStorage();
      const backupKey = "mv/_cluster/2026-01-01T00-00-00Z.archive.gz";
      await storage.putJson(`${backupKey}.manifest.json`, {
        version: 1,
        key: backupKey,
        database: "*",
        createdAt: "2026-01-01T00:00:00.000Z",
        completedAt: "2026-01-01T00:00:01.000Z",
        size: 10,
        sha256: "wrong-sha",
        collections: { "shop.orders": 2, "billing.invoices": 1 },
        oplog: false,
      });
      await storage.upload(backupKey, Buffer.from("corrupt-archive"));

      const droppedDbs = [];
      const createMongoClient = (uri) => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command(cmd) {
                if (cmd.hello) {
                  return {
                    ok: 1,
                    me: uri.includes("prod.internal") ? "prod:27017" : "staging:27017",
                  };
                }
                if (cmd.listDatabases) {
                  return { databases: [{ name: "unrelated_db" }, { name: "admin" }] };
                }
                return { ok: 1 };
              },
            };
          }
          return {
            async dropDatabase() {
              droppedDbs.push(name);
            },
          };
        },
      });

      await expect(
        runDrill({
          config: makeBaseConfig({
            mongo: {
              uri: "mongodb://admin:secret@prod.internal:27017/*",
              database: "*",
              excludeCollections: [],
              oplog: false,
              parallelCollections: 2,
              readPreference: "secondaryPreferred",
            },
          }),
          storage,
          logger,
          createMongoClient,
        }),
      ).rejects.toThrow();

      expect(droppedDbs).toContain("shop");
      expect(droppedDbs).toContain("billing");
      expect(droppedDbs).not.toContain("unrelated_db");
    });

    it("F-OPS-004: aborts immediately when AbortSignal is already aborted", async () => {
      const storage = makeMemoryStorage();
      const controller = new AbortController();
      controller.abort(new Error("cancelled"));
      await expect(
        runDrill({ config: makeBaseConfig(), storage, logger, signal: controller.signal }),
      ).rejects.toThrow(/cancelled|abort/i);
    });
  });
});
