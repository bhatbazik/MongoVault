import { describe, expect, it, vi } from "vitest";

const {
  mockSchedule,
  mockEngineBackup,
  mockAcquireHybridLock,
  mockPrune,
  mockRunDrill,
  mockStartOplogArchiver,
} = vi.hoisted(() => ({
  mockSchedule: vi.fn((_expr, _cb) => ({ stop: vi.fn() })),
  mockEngineBackup: vi.fn(async () => ({
    database: "app",
    key: "app/2026.archive.gz",
    size: 1024,
    durationMs: 100,
    completedAt: "2026-01-01T00:00:00.000Z",
  })),
  mockAcquireHybridLock: vi.fn(),
  mockPrune: vi.fn(async () => {}),
  mockRunDrill: vi.fn(async () => ({
    database: "app",
    key: "app/2026.archive.gz",
    durationMs: 50,
    documents: 42,
  })),
  mockStartOplogArchiver: vi.fn(() => ({
    start: vi.fn(),
    stop: vi.fn(async () => {}),
  })),
}));

vi.mock("node-cron", () => ({
  default: { schedule: mockSchedule },
}));

vi.mock("../src/engines.js", () => ({
  EngineRegistry: {
    resolveForBackup: vi.fn(() => ({ backup: mockEngineBackup })),
  },
}));

vi.mock("../src/lock.js", () => ({
  acquireHybridLock: mockAcquireHybridLock,
}));

vi.mock("../src/retention.js", () => ({
  prune: mockPrune,
}));

vi.mock("../src/drill.js", () => ({
  runDrill: mockRunDrill,
}));

vi.mock("../src/oplog.js", () => ({
  startOplogArchiver: mockStartOplogArchiver,
}));

import { createScheduler } from "../src/scheduler.js";

function createTestCtx(overrides = {}) {
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
  const notifier = { notifySuccess: vi.fn(async () => {}), notifyFailure: vi.fn(async () => {}) };
  const metrics = {
    recordBackupSuccess: vi.fn(),
    recordBackupFailure: vi.fn(),
    recordDrillSuccess: vi.fn(),
    recordDrillFailure: vi.fn(),
  };
  const config = {
    workDir: "/tmp/mv-sched-test",
    timeoutMs: 10000,
    backupAllDatabases: false,
    storage: { prefix: "mv" },
    mongo: { database: "app", uri: "mongodb://localhost:27017" },
    schedule: { cron: "0 * * * *", timezone: "UTC", runOnStart: false },
    retention: { daily: 7 },
    drill: { cron: "0 1 * * *" },
    pitr: { enabled: true },
    ...overrides,
  };
  return { config, storage: {}, logger, notifier, metrics };
}

describe("Scheduler", () => {
  it("runs backup, retention prune, drill, and updates state/metrics/notifier", async () => {
    const release = vi.fn(async () => {});
    mockAcquireHybridLock.mockResolvedValueOnce(release);

    const ctx = createTestCtx();
    const scheduler = createScheduler(ctx);
    expect(scheduler.getState().status).toBe("idle");

    await scheduler.triggerBackup();
    expect(ctx.metrics.recordBackupSuccess).toHaveBeenCalledWith("app", 1024, 100);
    expect(ctx.notifier.notifySuccess).toHaveBeenCalledWith("backup", expect.any(Object));
    expect(mockPrune).toHaveBeenCalled();
    expect(release).toHaveBeenCalled();
    expect(scheduler.getState().lastError).toBeNull();

    await scheduler.triggerDrill();
    expect(ctx.metrics.recordDrillSuccess).toHaveBeenCalled();
    expect(ctx.notifier.notifySuccess).toHaveBeenCalledWith("drill", expect.any(Object));
  });

  it("handles lock contention, lock acquisition failure, backup error, prune error, and drill error", async () => {
    const ctx = createTestCtx();
    const scheduler = createScheduler(ctx);

    // 1. Lock held by another process
    mockAcquireHybridLock.mockResolvedValueOnce(null);
    await scheduler.triggerBackup();
    expect(ctx.logger.warn).toHaveBeenCalled();

    // 2. Lock acquisition throws
    mockAcquireHybridLock.mockRejectedValueOnce(new Error("S3 down"));
    await scheduler.triggerBackup();
    expect(scheduler.getState().status).toBe("error");
    expect(scheduler.getState().lastError).toBe("S3 down");

    // 3. Backup throws
    const release = vi.fn(async () => {});
    mockAcquireHybridLock.mockResolvedValueOnce(release);
    mockEngineBackup.mockRejectedValueOnce(new Error("dump crashed"));
    await scheduler.triggerBackup();
    expect(ctx.metrics.recordBackupFailure).toHaveBeenCalledWith("app");
    expect(ctx.notifier.notifyFailure).toHaveBeenCalledWith("backup", expect.any(Error), {
      database: "app",
    });
    expect(release).toHaveBeenCalled();

    // 4. Prune error doesn't fail backup
    mockAcquireHybridLock.mockResolvedValueOnce(release);
    mockPrune.mockRejectedValueOnce(new Error("prune err"));
    await scheduler.triggerBackup();
    expect(ctx.logger.error).toHaveBeenCalled();

    // 5. Drill error
    mockRunDrill.mockRejectedValueOnce(new Error("drill err"));
    await scheduler.triggerDrill();
    expect(ctx.metrics.recordDrillFailure).toHaveBeenCalled();
    expect(ctx.notifier.notifyFailure).toHaveBeenCalledWith("drill", expect.any(Error));
  });

  it("registers cron jobs, starts pitr/runOnStart, aborts active backup on stop", async () => {
    const release = vi.fn(async () => {});
    mockAcquireHybridLock.mockImplementation(async ({ onLost }) => {
      onLost(new Error("lease expired"));
      return release;
    });
    mockEngineBackup.mockImplementation(
      () =>
        new Promise((resolve) =>
          setTimeout(
            () => resolve({ database: "app", size: 1, durationMs: 1, completedAt: "now" }),
            30,
          ),
        ),
    );

    const ctx = createTestCtx({
      schedule: { cron: "0 * * * *", timezone: "UTC", runOnStart: true },
    });
    const scheduler = createScheduler(ctx);
    scheduler.start();

    expect(mockSchedule).toHaveBeenCalledTimes(2);
    expect(mockStartOplogArchiver).toHaveBeenCalled();

    await new Promise((r) => setTimeout(r, 10));
    await scheduler.stop();
    expect(scheduler.getState().isShuttingDown).toBe(true);
  });
});
