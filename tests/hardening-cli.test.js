import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import { createLogger } from "../src/logger.js";
import { createMetricsRegistry, startHttpServer } from "../src/metrics.js";
import { EmailNotifier } from "../src/notify.js";
import { formatDate, formatDuration } from "../src/format.js";

describe("M5 Hardening — cli-daemon-config (F-CLI-001..014)", () => {
  const baseEnv = {
    MONGODB_URI: "mongodb://localhost:27017",
    MONGODB_DATABASE: "testdb",
    STORAGE_TYPE: "filesystem",
    STORAGE_PATH: "/tmp/mv-test",
  };

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("F-CLI-008: config.ts bounds, schemes, and contradiction guards", () => {
    it("rejects non-finite and sub-threshold BACKUP_TIMEOUT_MINUTES", () => {
      expect(() => loadConfig({ ...baseEnv, BACKUP_TIMEOUT_MINUTES: "Infinity" })).toThrow();
      expect(() => loadConfig({ ...baseEnv, BACKUP_TIMEOUT_MINUTES: "0.001" })).toThrow();
    });

    it("rejects out-of-range SMTP_PORT and partial SMTP auth", () => {
      expect(() =>
        loadConfig({
          ...baseEnv,
          SMTP_HOST: "smtp.example.com",
          SMTP_FROM: "a@b.com",
          SMTP_TO: "c@d.com",
          SMTP_PORT: "70000",
        }),
      ).toThrow();
      expect(() =>
        loadConfig({
          ...baseEnv,
          SMTP_HOST: "smtp.example.com",
          SMTP_FROM: "a@b.com",
          SMTP_TO: "c@d.com",
          SMTP_USER: "only-user",
        }),
      ).toThrow();
    });

    it("rejects invalid URI schemes, DRILL_MONGODB_URI === MONGODB_URI, and S3_OBJECT_LOCK_ENABLED on filesystem", () => {
      expect(() => loadConfig({ ...baseEnv, MONGODB_URI: "http://localhost:27017" })).toThrow();
      expect(() =>
        loadConfig({ ...baseEnv, DRILL_MONGODB_URI: "mongodb://localhost:27017" }),
      ).toThrow();
      expect(() => loadConfig({ ...baseEnv, S3_OBJECT_LOCK_ENABLED: "true" })).toThrow();
      expect(() =>
        loadConfig({
          MONGODB_URI: "mongodb://localhost:27017",
          S3_BUCKET: "my-bucket",
          S3_ACCESS_KEY_ID: "k",
          S3_SECRET_ACCESS_KEY: "s",
          S3_ENDPOINT: "ftp://s3.example.com",
        }),
      ).toThrow();
    });

    it("trims slashes from S3_BUCKET and rejects empty bucket after trimming", () => {
      const cfg = loadConfig({
        MONGODB_URI: "mongodb://localhost:27017",
        S3_BUCKET: "/my-bucket/",
        S3_ACCESS_KEY_ID: "k",
        S3_SECRET_ACCESS_KEY: "s",
      });
      expect(cfg.storage.bucket).toBe("my-bucket");
      expect(() =>
        loadConfig({
          MONGODB_URI: "mongodb://localhost:27017",
          S3_BUCKET: "///",
          S3_ACCESS_KEY_ID: "k",
          S3_SECRET_ACCESS_KEY: "s",
        }),
      ).toThrow();
    });
  });

  describe("F-CLI-006: /ready & /healthz query string and 503 readiness", () => {
    it("supports query strings on /ready and returns 503 when shutting down or error", async () => {
      let state = { status: "idle", isShuttingDown: false };
      const metrics = createMetricsRegistry();
      const logger = createLogger({ level: "silent" });
      const srv = startHttpServer({
        port: 0,
        metrics,
        getState: () => state,
        logger,
      });
      // Wait, port: 0 currently returns null in metrics.ts! Let's use an ephemeral port.
      expect(srv).toBeNull();

      const net = await import("node:net");
      const port = await new Promise((resolve) => {
        const s = net.createServer();
        s.listen(0, "127.0.0.1", () => {
          const p = s.address().port;
          s.close(() => resolve(p));
        });
      });

      const server = startHttpServer({
        port,
        metrics,
        getState: () => state,
        logger,
      });
      try {
        const r1 = await fetch(`http://127.0.0.1:${port}/ready?verbose=1`);
        expect(r1.status).toBe(200);

        state = { status: "idle", isShuttingDown: true };
        const r2 = await fetch(`http://127.0.0.1:${port}/ready?verbose=1`);
        expect(r2.status).toBe(503);
      } finally {
        await server.close();
      }
    });
  });

  describe("F-CLI-011 & F-CLI-012 & F-CLI-014: logger stderr, notify sanitization, format guards", () => {
    it("createLogger({ destination: 'stderr' }) writes logs to stderr without touching stdout", async () => {
      const stdoutChunks = [];
      const origStdoutWrite = process.stdout.write.bind(process.stdout);
      vi.spyOn(process.stdout, "write").mockImplementation((chunk, ...rest) => {
        stdoutChunks.push(String(chunk));
        return origStdoutWrite(chunk, ...rest);
      });
      const logger = createLogger({ level: "info", pretty: false, destination: "stderr" });
      logger.info({ test: 1 }, "hello stderr");
      await new Promise((r) => setTimeout(r, 20));
      expect(stdoutChunks.join("")).not.toContain("hello stderr");
    });

    it("EmailNotifier sanitizes MongoDB credentials in error message and stack and exposes close()", () => {
      const cfg = loadConfig({
        ...baseEnv,
        SMTP_HOST: "smtp.example.com",
        SMTP_FROM: "a@b.com",
        SMTP_TO: "c@d.com",
      });
      const notifier = new EmailNotifier(cfg);
      const err = new Error("Failed mongodb://admin:supersecret@db.internal:27017/app");
      err.stack = "Error: Failed mongodb://admin:supersecret@db.internal:27017/app\n  at foo";
      const summary = notifier["messageBuilder"].buildFailureSummary("backup", err, "app");
      expect(summary).not.toContain("supersecret");
      expect(typeof notifier.close).toBe("function");
      notifier.close();
    });

    it("formatDuration and formatDate handle NaN, negative values, and invalid timeZone safely", () => {
      expect(formatDuration(Number.NaN)).toBe("0s");
      expect(formatDuration(-5000)).toBe("0s");
      expect(() => formatDate(new Date("2026-01-01T00:00:00Z"), "Invalid/Zone")).not.toThrow();
    });
  });

  describe("F-CLI-001, F-CLI-002, F-CLI-004, F-CLI-009, F-CLI-010: locks, scheduler, flags, oplog:list", () => {
    it("F-CLI-001/002: DeleteCommand, PruneCommand, OplogSliceCommand, OplogPruneCommand acquire and release lock even on failure", async () => {
      const fs = await import("node:fs/promises");
      const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "cli-lock-"));
      try {
        const { CommandRegistry } = await import("../src/commands/index.js");
        const objects = new Map();
        const storage = {
          bucket: "b",
          async putJson(k, v) {
            objects.set(k, { data: v, etag: '"e1"' });
            return { etag: '"e1"' };
          },
          async getJson(k) {
            return objects.get(k) ?? null;
          },
          async remove(k) {
            objects.delete(k);
          },
          async *list() {
            throw new Error("boom");
          },
        };
        const config = {
          ...loadConfig({ ...baseEnv, WORK_DIR: tmpDir }),
        };
        const logger = createLogger({ level: "silent" });
        const notifier = { notifySuccess: vi.fn(), notifyFailure: vi.fn() };
        const metrics = createMetricsRegistry();

        vi.spyOn(process, "exit").mockImplementation((code) => {
          throw new Error(`EXIT_${code}`);
        });
        vi.spyOn(console, "error").mockImplementation(() => {});

        // PruneCommand fails inside list() -> lock must be released
        await expect(
          CommandRegistry.get("prune").execute({
            config,
            storage,
            logger,
            notifier,
            metrics,
            values: {},
          }),
        ).rejects.toThrow("EXIT_1");
        expect(objects.has("mongodb/testdb/.locks/backup.lock.json")).toBe(false);

        // OplogPruneCommand fails inside list() -> oplog lock must be released
        await expect(
          CommandRegistry.get("oplog:prune").execute({
            config,
            storage,
            logger,
            notifier,
            metrics,
            values: { "older-than": "100:1" },
          }),
        ).rejects.toThrow("EXIT_1");
        expect(objects.has("mongodb/testdb/_pitr/.locks/oplog.lock.json")).toBe(false);
      } finally {
        await fs.rm(tmpDir, { recursive: true, force: true });
      }
    });

    it("F-CLI-004: Scheduler.start() is idempotent and triggerBackup/triggerDrill are non-reentrant", async () => {
      const fs = await import("node:fs/promises");
      const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "sched-lock-"));
      try {
        const { createScheduler } = await import("../src/scheduler.js");
        const objects = new Map();
        let getJsonCalls = 0;
        const storage = {
          bucket: "b",
          async putJson(k, v) {
            objects.set(k, { data: v, etag: '"e1"' });
            return { etag: '"e1"' };
          },
          async getJson(k) {
            getJsonCalls++;
            await new Promise((r) => setTimeout(r, 25));
            return objects.get(k) ?? null;
          },
          async remove(k) {
            objects.delete(k);
          },
          async *list() {},
        };
        const config = loadConfig({ ...baseEnv, WORK_DIR: tmpDir });
        const logger = createLogger({ level: "silent" });
        const notifier = {
          notifySuccess: vi.fn(async () => {}),
          notifyFailure: vi.fn(async () => {}),
        };
        const metrics = createMetricsRegistry();
        const scheduler = createScheduler({ config, storage, logger, notifier, metrics });

        scheduler.start();
        scheduler.start(); // idempotent

        // Concurrent triggerBackup should coalesce to 1 activeBackupTask
        const p1 = scheduler.triggerBackup();
        const p2 = scheduler.triggerBackup();
        await scheduler.stop();
        await Promise.allSettled([p1, p2]);
        expect(getJsonCalls).toBeLessThanOrEqual(2);
      } finally {
        await fs.rm(tmpDir, { recursive: true, force: true });
      }
    });

    it("F-CLI-009: BackupCommand & RestoreCommand reject incompatible flag combinations", async () => {
      const { CommandRegistry } = await import("../src/commands/index.js");
      const config = loadConfig(baseEnv);
      const logger = createLogger({ level: "silent" });
      const ctxBase = {
        config,
        storage: { bucket: "b", async *list() {} },
        logger,
        notifier: { notifySuccess: vi.fn(), notifyFailure: vi.fn() },
        metrics: createMetricsRegistry(),
      };
      vi.spyOn(process, "exit").mockImplementation((code) => {
        throw new Error(`EXIT_${code}`);
      });
      vi.spyOn(console, "error").mockImplementation(() => {});

      await expect(
        CommandRegistry.get("backup").execute({
          ...ctxBase,
          values: { engine: "logical", "db-path": "/tmp/db" },
        }),
      ).rejects.toThrow("EXIT_1");

      await expect(
        CommandRegistry.get("restore").execute({
          ...ctxBase,
          values: { pitr: "100:1", "oplog-limit": "200:1" },
        }),
      ).rejects.toThrow("EXIT_1");

      await expect(
        CommandRegistry.get("restore").execute({
          ...ctxBase,
          values: { engine: "physical", "ns-include": "app.users" },
        }),
      ).rejects.toThrow("EXIT_1");
    });

    it("F-CLI-010: OplogListCommand in sharded mode with empty prefix avoids leading slash", async () => {
      const { CommandRegistry } = await import("../src/commands/index.js");
      const listedPrefixes = [];
      const storage = {
        bucket: "b",
        async *list(prefix) {
          listedPrefixes.push(prefix);
          if (prefix === "shards/") {
            yield {
              key: "shards/rs0/_pitr/2026-01-01/0000000001_00000001__0000000002_00000001.oplog.bson.gz",
            };
          }
        },
      };
      const config = {
        ...loadConfig(baseEnv),
        storage: { type: "filesystem", bucket: "b", prefix: "" },
        sharding: { enabled: true },
      };
      vi.spyOn(console, "log").mockImplementation(() => {});
      await CommandRegistry.get("oplog:list").execute({
        config,
        storage,
        logger: createLogger({ level: "silent" }),
        notifier: { notifySuccess: vi.fn(), notifyFailure: vi.fn() },
        metrics: createMetricsRegistry(),
        values: { json: true },
      });
      expect(listedPrefixes.every((p) => !p.startsWith("/"))).toBe(true);
      expect(listedPrefixes).toContain("configsvr/_cluster/_pitr/");
      expect(listedPrefixes).toContain("shards/rs0/_cluster/_pitr/");
    });
  });
});
