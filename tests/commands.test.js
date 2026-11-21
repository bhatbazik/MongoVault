import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const {
  mockAcquireHybridLock,
  mockEngineResolveBackup,
  mockEngineResolveRestore,
  mockRunPreflightCheck,
  mockFormatPreflightCheck,
  mockDeleteBackup,
  mockPrune,
  mockPruneOplogSlices,
  mockDescribeBackup,
  mockVerifyBackup,
  mockGenerateDiagnosticBundle,
  mockRunDrill,
  mockListBackups,
  mockGetStatus,
  mockFormatStatus,
  mockCaptureOplogSlice,
  mockListOplogSlices,
  mockStartOplogArchiver,
  mockCaptureShardedOplogSlices,
  mockCreateScheduler,
  mockStartHttpServer,
} = vi.hoisted(() => ({
  mockAcquireHybridLock: vi.fn(),
  mockEngineResolveBackup: vi.fn(),
  mockEngineResolveRestore: vi.fn(),
  mockRunPreflightCheck: vi.fn(),
  mockFormatPreflightCheck: vi.fn(() => "PREFLIGHT OK"),
  mockDeleteBackup: vi.fn(),
  mockPrune: vi.fn(),
  mockPruneOplogSlices: vi.fn(),
  mockDescribeBackup: vi.fn(),
  mockVerifyBackup: vi.fn(),
  mockGenerateDiagnosticBundle: vi.fn(),
  mockRunDrill: vi.fn(),
  mockListBackups: vi.fn(),
  mockGetStatus: vi.fn(),
  mockFormatStatus: vi.fn(() => "STATUS OK"),
  mockCaptureOplogSlice: vi.fn(),
  mockListOplogSlices: vi.fn(),
  mockStartOplogArchiver: vi.fn(),
  mockCaptureShardedOplogSlices: vi.fn(),
  mockCreateScheduler: vi.fn(),
  mockStartHttpServer: vi.fn(),
}));

vi.mock("../src/lock.js", () => ({ acquireHybridLock: mockAcquireHybridLock }));
vi.mock("../src/engines.js", () => ({
  EngineRegistry: {
    resolveForBackup: mockEngineResolveBackup,
    resolveForRestore: mockEngineResolveRestore,
  },
}));
vi.mock("../src/check.js", () => ({
  runPreflightCheck: mockRunPreflightCheck,
  formatPreflightCheck: mockFormatPreflightCheck,
}));
vi.mock("../src/retention.js", () => ({
  deleteBackup: mockDeleteBackup,
  prune: mockPrune,
  pruneOplogSlices: mockPruneOplogSlices,
}));
vi.mock("../src/restore.js", () => ({
  describeBackup: mockDescribeBackup,
  verifyBackup: mockVerifyBackup,
}));
vi.mock("../src/diagnostic.js", () => ({
  generateDiagnosticBundle: mockGenerateDiagnosticBundle,
}));
vi.mock("../src/drill.js", () => ({
  runDrill: mockRunDrill,
}));
vi.mock("../src/catalog.js", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    listBackups: mockListBackups,
  };
});
vi.mock("../src/status.js", () => ({
  getStatus: mockGetStatus,
  formatStatus: mockFormatStatus,
}));
vi.mock("../src/oplog.js", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    captureOplogSlice: mockCaptureOplogSlice,
    listOplogSlices: mockListOplogSlices,
    startOplogArchiver: mockStartOplogArchiver,
  };
});
vi.mock("../src/sharding.js", () => ({
  captureShardedOplogSlices: mockCaptureShardedOplogSlices,
}));
vi.mock("../src/scheduler.js", () => ({
  createScheduler: mockCreateScheduler,
}));
vi.mock("../src/metrics.js", () => ({
  startHttpServer: mockStartHttpServer,
}));

import { CommandRegistry } from "../src/commands/index.js";

function makeCtx(values = {}, configOverrides = {}) {
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
  const notifier = { notifySuccess: vi.fn(async () => {}), notifyFailure: vi.fn(async () => {}) };
  const metrics = {};
  const storage = {
    async *list() {},
  };
  const config = {
    workDir: os.tmpdir(),
    timeoutMs: 10_000,
    backupAllDatabases: false,
    mongo: {
      uri: "mongodb://localhost:27017",
      database: "app",
      oplog: false,
      excludeCollections: [],
    },
    storage: { prefix: "mv" },
    schedule: { timezone: "UTC" },
    http: { port: 9090 },
    ...configOverrides,
  };
  return { config, storage, logger, notifier, metrics, values };
}

describe("CLI Commands & CommandRegistry", () => {
  let logSpy;
  let errSpy;
  let exitSpy;

  beforeEach(() => {
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    exitSpy = vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`EXIT_${code}`);
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.restoreAllMocks();
  });

  it("CommandRegistry has all built-in commands and supports register()", () => {
    expect(CommandRegistry.has("backup")).toBe(true);
    expect(CommandRegistry.has("restore")).toBe(true);
    expect(CommandRegistry.has("oplog:tail")).toBe(true);
    const custom = { execute: vi.fn() };
    CommandRegistry.register("custom:cmd", custom);
    expect(CommandRegistry.get("custom:cmd")).toBe(custom);
  });

  it("BackupCommand validates flags, acquires lock, prints text/json, and handles failure", async () => {
    const cmd = CommandRegistry.get("backup");

    await expect(cmd.execute(makeCtx({ oplog: true, database: "app" }))).rejects.toThrow("EXIT_1");
    await expect(
      cmd.execute(
        makeCtx(
          { database: "*" },
          { mongo: { uri: "mongodb://localhost", database: "*", excludeCollections: ["a"] } },
        ),
      ),
    ).rejects.toThrow("EXIT_1");

    // Lock contention
    mockAcquireHybridLock.mockResolvedValueOnce(null);
    await expect(cmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    // Success (text + json)
    const release = vi.fn(async () => {});
    mockAcquireHybridLock.mockResolvedValue(release);
    mockEngineResolveBackup.mockReturnValue({
      backup: vi.fn(async () => ({
        engine: "logical",
        key: "app/b1.archive.gz",
        size: 1024,
        durationMs: 1200,
        sha256: "abc",
        encryption: null,
        oplog: false,
      })),
    });
    await cmd.execute(makeCtx({}));
    await cmd.execute(makeCtx({ json: true }));
    expect(release).toHaveBeenCalledTimes(2);

    // Backup failure
    mockEngineResolveBackup.mockReturnValue({
      backup: vi.fn(async () => {
        throw new Error("backup fail");
      }),
    });
    await expect(cmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");
    expect(errSpy).toHaveBeenCalled();
  });

  it("RestoreCommand handles logical, physical, sharded, json, and error", async () => {
    const cmd = CommandRegistry.get("restore");

    mockEngineResolveRestore.mockReturnValueOnce({
      name: "logical",
      restore: vi.fn(async () => ({
        key: "k1",
        target: "app",
        restored: 10,
        pitrTarget: "100:1",
        replayedSlices: 2,
        replayedEntries: 5,
      })),
    });
    await cmd.execute(makeCtx({}));

    mockEngineResolveRestore.mockReturnValueOnce({
      name: "physical",
      restore: vi.fn(async () => ({
        key: "k2",
        targetDbPath: "/db",
        restoredFiles: 4,
        totalBytes: 512,
      })),
    });
    await cmd.execute(makeCtx({}));

    mockEngineResolveRestore.mockReturnValueOnce({
      name: "sharded",
      restore: vi.fn(async () => ({
        key: "k3",
        clusterTime: "100:1",
        restoredShards: ["s1", "s2"],
      })),
    });
    await cmd.execute(makeCtx({}));

    mockEngineResolveRestore.mockReturnValueOnce({
      name: "logical",
      restore: vi.fn(async () => ({ ok: true })),
    });
    await cmd.execute(makeCtx({ json: true }));

    mockEngineResolveRestore.mockReturnValueOnce({
      name: "logical",
      restore: vi.fn(async () => {
        throw new Error("restore err");
      }),
    });
    await expect(cmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");
  });

  it("CheckCommand, DeleteCommand, DrillCommand, PruneCommand, StatusCommand, VerifyCommand", async () => {
    // Check
    const checkCmd = CommandRegistry.get("check");
    mockRunPreflightCheck.mockResolvedValueOnce({ allPassed: true });
    await checkCmd.execute(makeCtx({}));
    mockRunPreflightCheck.mockResolvedValueOnce({ allPassed: false });
    await expect(checkCmd.execute(makeCtx({ json: true }))).rejects.toThrow("EXIT_1");
    mockRunPreflightCheck.mockRejectedValueOnce(new Error("check err"));
    await expect(checkCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    // Delete
    const deleteCmd = CommandRegistry.get("delete");
    await expect(deleteCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");
    mockDeleteBackup.mockResolvedValueOnce({ message: "Deleted" });
    await deleteCmd.execute(makeCtx({ key: "k1" }));
    mockDeleteBackup.mockResolvedValueOnce({ deleted: true });
    await deleteCmd.execute(makeCtx({ key: "k1", json: true }));
    mockDeleteBackup.mockRejectedValueOnce(new Error("del err"));
    await expect(deleteCmd.execute(makeCtx({ key: "k1" }))).rejects.toThrow("EXIT_1");

    // Drill
    const drillCmd = CommandRegistry.get("drill");
    mockRunDrill.mockResolvedValueOnce({
      key: "k1",
      collections: 2,
      documents: 100,
      durationMs: 500,
    });
    await drillCmd.execute(makeCtx({}));
    mockRunDrill.mockResolvedValueOnce({ key: "k1" });
    await drillCmd.execute(makeCtx({ json: true }));
    mockRunDrill.mockRejectedValueOnce(new Error("drill err"));
    await expect(drillCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    // Prune
    const pruneCmd = CommandRegistry.get("prune");
    mockPrune.mockResolvedValueOnce({ keep: [{ key: "k1" }], remove: [{ key: "k2" }] });
    await pruneCmd.execute(makeCtx({ "dry-run": true }));
    mockPrune.mockResolvedValueOnce({ keep: [], remove: [] });
    await pruneCmd.execute(makeCtx({ json: true }));
    mockPrune.mockRejectedValueOnce(new Error("prune err"));
    await expect(pruneCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    // Status
    const statusCmd = CommandRegistry.get("status");
    mockGetStatus.mockResolvedValueOnce({ ok: true });
    await statusCmd.execute(makeCtx({}));
    mockGetStatus.mockResolvedValueOnce({ ok: true });
    await statusCmd.execute(makeCtx({ json: true }));
    mockGetStatus.mockRejectedValueOnce(new Error("status err"));
    await expect(statusCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    // Verify
    const verifyCmd = CommandRegistry.get("verify");
    mockVerifyBackup.mockResolvedValueOnce({
      key: "k1",
      size: 512,
      sha256: "abc",
      encrypted: true,
    });
    await verifyCmd.execute(makeCtx({}));
    mockVerifyBackup.mockResolvedValueOnce({ key: "k1" });
    await verifyCmd.execute(makeCtx({ json: true }));
    mockVerifyBackup.mockRejectedValueOnce(new Error("verify err"));
    await expect(verifyCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");
  });

  it("DescribeCommand, DiagnosticCommand, and ListCommand", async () => {
    const descCmd = CommandRegistry.get("describe");
    mockDescribeBackup.mockResolvedValueOnce({
      key: "k1",
      checksum: "abc",
      manifest: {
        key: "k1",
        engine: "physical",
        database: "app",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:01:00Z",
        durationMs: 60000,
        size: 1024,
        sha256: "abc",
        compression: "gzip",
        encryption: { algorithm: "aes-256-gcm", keyId: "kid1" },
        oplogWindow: { startTs: "1:1", endTs: "2:1" },
        physical: { cursorType: "$backupCursor", filesCount: 5, dbPath: "/db" },
        sharding: { shards: { s1: {} }, clusterTime: "2:1" },
        excludedCollections: ["tmp"],
        collections: { users: 42 },
      },
    });
    await descCmd.execute(makeCtx({}));

    mockDescribeBackup.mockResolvedValueOnce({ key: "k2", checksum: null, manifest: null });
    await descCmd.execute(makeCtx({}));
    mockDescribeBackup.mockResolvedValueOnce({ key: "k3", checksum: "c", manifest: {} });
    await descCmd.execute(makeCtx({ json: true }));
    mockDescribeBackup.mockRejectedValueOnce(new Error("desc err"));
    await expect(descCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    const diagCmd = CommandRegistry.get("diagnostic");
    const tmpOut = path.join(os.tmpdir(), `diag-${Date.now()}.json`);
    mockGenerateDiagnosticBundle.mockResolvedValueOnce({ hello: "world" });
    await diagCmd.execute(makeCtx({ output: tmpOut }));
    expect(await fs.readFile(tmpOut, "utf8")).toContain('"hello": "world"');
    await fs.rm(tmpOut, { force: true });

    mockGenerateDiagnosticBundle.mockResolvedValueOnce({ hello: "stdout" });
    await diagCmd.execute(makeCtx({}));
    mockGenerateDiagnosticBundle.mockRejectedValueOnce(new Error("diag err"));
    await expect(diagCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    const listCmd = CommandRegistry.get("list");
    mockListBackups.mockResolvedValueOnce([]);
    await listCmd.execute(makeCtx({}));
    mockListBackups.mockResolvedValueOnce([
      { key: "k1", createdAt: "2026-01-01T00:00:00Z", size: 100, encrypted: true, complete: true },
    ]);
    await listCmd.execute(makeCtx({}));
    mockListBackups.mockResolvedValueOnce([]);
    await listCmd.execute(makeCtx({ json: true }));
    expect(logSpy).toHaveBeenCalled();
  });

  it("Oplog commands (slice, list, prune, tail) and DaemonCommand", async () => {
    const sliceCmd = CommandRegistry.get("oplog:slice");
    mockCaptureOplogSlice.mockResolvedValueOnce(null);
    await sliceCmd.execute(makeCtx({}));
    mockCaptureOplogSlice.mockResolvedValueOnce({
      key: "s1",
      size: 100,
      oplogWindow: {
        startTs: { t: 10, i: 1 },
        endTs: { t: 20, i: 1 },
        entries: 5,
        gapDetected: true,
      },
    });
    await sliceCmd.execute(makeCtx({}));
    mockCaptureOplogSlice.mockResolvedValueOnce({ key: "s2" });
    await sliceCmd.execute(makeCtx({ json: true }));

    // Sharded oplog slice
    mockCaptureShardedOplogSlices.mockResolvedValueOnce(null);
    await sliceCmd.execute(makeCtx({}, { sharding: { enabled: true } }));
    mockCaptureShardedOplogSlices.mockResolvedValueOnce({
      configsvr: {
        size: 50,
        oplogWindow: { startTs: { t: 1, i: 1 }, endTs: { t: 2, i: 1 }, entries: 1 },
      },
      shards: { rs1: null },
    });
    await sliceCmd.execute(makeCtx({}, { sharding: { enabled: true } }));
    mockCaptureShardedOplogSlices.mockResolvedValueOnce({ configsvr: null, shards: {} });
    await sliceCmd.execute(makeCtx({ json: true }, { sharding: { enabled: true } }));
    mockCaptureOplogSlice.mockRejectedValueOnce(new Error("slice err"));
    await expect(sliceCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");

    // Oplog list
    const oplogListCmd = CommandRegistry.get("oplog:list");
    mockListOplogSlices.mockResolvedValueOnce([]);
    await oplogListCmd.execute(makeCtx({}));
    mockListOplogSlices.mockResolvedValueOnce([
      { key: "sl1", startTs: { t: 1, i: 1 }, endTs: { t: 2, i: 1 }, size: 64 },
    ]);
    await oplogListCmd.execute(makeCtx({}));
    mockListOplogSlices.mockResolvedValueOnce([]);
    await oplogListCmd.execute(makeCtx({ json: true }));

    const shardedStorage = {
      async *list() {
        yield { key: "mv/shards/shard1/oplog/s1.bson.gz" };
      },
    };
    const shardedListCtx = makeCtx({}, { sharding: { enabled: true } });
    shardedListCtx.storage = shardedStorage;
    mockListOplogSlices.mockResolvedValue([]);
    await oplogListCmd.execute(shardedListCtx);
    mockListOplogSlices.mockResolvedValue([
      { key: "sl1", startTs: { t: 1, i: 1 }, endTs: { t: 2, i: 1 }, size: 64 },
    ]);
    await oplogListCmd.execute(shardedListCtx);
    shardedListCtx.values = { json: true };
    await oplogListCmd.execute(shardedListCtx);

    // Oplog prune
    const oplogPruneCmd = CommandRegistry.get("oplog:prune");
    await expect(oplogPruneCmd.execute(makeCtx({}))).rejects.toThrow("EXIT_1");
    mockPruneOplogSlices.mockResolvedValueOnce({ deletedSlices: ["s1"], cutoffTs: "10:1" });
    await oplogPruneCmd.execute(makeCtx({ "older-than": "10:1" }));
    mockPruneOplogSlices.mockResolvedValueOnce({ deletedSlices: [], cutoffTs: "10:1" });
    await oplogPruneCmd.execute(makeCtx({ "older-than": "10:1", json: true }));
    mockPruneOplogSlices.mockRejectedValueOnce(new Error("oplog prune err"));
    await expect(oplogPruneCmd.execute(makeCtx({ "older-than": "10:1" }))).rejects.toThrow(
      "EXIT_1",
    );

    // Oplog tail & Daemon signal handlers
    const handlers = {};
    vi.spyOn(process, "on").mockImplementation((ev, fn) => {
      handlers[ev] = fn;
      return process;
    });
    const archiver = { start: vi.fn(), stop: vi.fn(async () => {}) };
    mockStartOplogArchiver.mockReturnValue(archiver);
    const tailCmd = CommandRegistry.get("oplog:tail");
    await tailCmd.execute(makeCtx({}));
    expect(archiver.start).toHaveBeenCalled();

    const scheduler = { start: vi.fn(), stop: vi.fn(async () => {}), getState: vi.fn(() => ({})) };
    const httpServer = { close: vi.fn(async () => {}) };
    mockCreateScheduler.mockReturnValue(scheduler);
    mockStartHttpServer.mockReturnValue(httpServer);
    const daemonCmd = CommandRegistry.get("daemon");
    await daemonCmd.execute(makeCtx({}));
    expect(scheduler.start).toHaveBeenCalled();
    expect(typeof handlers.SIGINT).toBe("function");
    exitSpy.mockImplementationOnce(() => {});
    handlers.SIGINT();
    await new Promise((r) => setTimeout(r, 10));
    expect(scheduler.stop).toHaveBeenCalled();
    expect(httpServer.close).toHaveBeenCalled();
    expect(exitSpy).toBeDefined();
  });
});
