import path from "node:path";
import cron, { type ScheduledTask } from "node-cron";
import { databasePrefix, scopeOf } from "./catalog.js";
import { runDrill } from "./drill.js";
import { EngineRegistry } from "./engines.js";
import { acquireHybridLock } from "./lock.js";
import { startOplogArchiver } from "./oplog.js";
import { prune } from "./retention.js";
import type { BackupManifest } from "./types/catalog.js";
import type { OplogArchiver } from "./types/oplog.js";
import type { Scheduler, SchedulerContext, SchedulerState } from "./types/scheduler.js";

export function createScheduler({
  config,
  storage,
  logger,
  notifier,
  metrics,
}: SchedulerContext): Scheduler {
  const lockFile = path.join(config.workDir, ".backup.lock");
  const distLockKey = `${databasePrefix(scopeOf(config))}.locks/backup.lock.json`;
  let activeBackupPromise: Promise<BackupManifest> | null = null;
  let activeAbortController: AbortController | null = null;
  let oplogArchiver: OplogArchiver | null = null;
  let isShuttingDown = false;

  const state: SchedulerState = {
    status: "idle",
    lastRunAt: null,
    lastSuccessAt: null,
    lastError: null,
    lastManifest: null,
    lastDrillAt: null,
  };

  async function executeBackup(): Promise<void> {
    if (isShuttingDown) return;

    let release: (() => Promise<void>) | null;
    try {
      release = await acquireHybridLock({
        file: lockFile,
        storage,
        lockKey: distLockKey,
        staleAfterMs: config.timeoutMs + 60_000,
        logger,
      });
    } catch (lockError) {
      const err = lockError instanceof Error ? lockError : new Error(String(lockError));
      logger.error({ err }, "Failed to acquire lock for backup");
      state.status = "error";
      state.lastError = err.message;
      return;
    }

    if (!release) {
      logger.warn("Skipping backup: another process holds the local or distributed lock");
      return;
    }

    state.status = "running";
    state.lastRunAt = new Date().toISOString();
    activeAbortController = new AbortController();

    try {
      const engine = EngineRegistry.resolveForBackup({ config });
      activeBackupPromise = engine.backup({
        config,
        storage,
        logger,
        signal: activeAbortController.signal,
      });

      const manifest = await activeBackupPromise;
      state.status = "idle";
      state.lastSuccessAt = manifest.completedAt;
      state.lastError = null;
      state.lastManifest = manifest;

      metrics.recordBackupSuccess(manifest.database, manifest.size, manifest.durationMs);
      await notifier.notifySuccess("backup", manifest);

      if (config.retention) {
        try {
          await prune({ config, storage, logger });
        } catch (pruneError) {
          logger.error({ err: pruneError }, "Retention prune failed after backup");
        }
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      state.status = "error";
      state.lastError = err.message;
      metrics.recordBackupFailure(config.mongo.database);
      await notifier.notifyFailure("backup", err, { database: config.mongo.database });
      logger.error({ err }, "Scheduled backup run failed");
    } finally {
      activeBackupPromise = null;
      activeAbortController = null;
      await release();
    }
  }

  async function executeDrill(): Promise<void> {
    if (isShuttingDown || !config.drill) return;

    logger.info("Executing scheduled restore drill");
    try {
      const result = await runDrill({ config, storage, logger });
      state.lastDrillAt = new Date().toISOString();
      metrics.recordDrillSuccess();
      await notifier.notifySuccess("drill", {
        database: result.database,
        key: result.key,
        size: 0,
        durationMs: result.durationMs,
        collections: { restoredCount: result.documents },
        sha256: "verified",
        encryption: false,
      });
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      metrics.recordDrillFailure();
      await notifier.notifyFailure("drill", err);
      logger.error({ err }, "Scheduled restore drill failed");
    }
  }

  const tasks: ScheduledTask[] = [];

  return {
    getState(): SchedulerState {
      return {
        ...state,
        uptimeSeconds: Math.floor(process.uptime()),
        isShuttingDown,
      };
    },

    triggerBackup(): Promise<void> {
      return executeBackup();
    },

    triggerDrill(): Promise<void> {
      return executeDrill();
    },

    start(): void {
      logger.info(
        { cron: config.schedule.cron, timezone: config.schedule.timezone },
        "Registering backup schedule",
      );

      const backupTask = cron.schedule(
        config.schedule.cron,
        () => {
          logger.info("Backup schedule triggered by cron");
          executeBackup().catch((err) => {
            logger.error({ err }, "Unhandled error during scheduled backup execution");
          });
        },
        { timezone: config.schedule.timezone },
      );
      tasks.push(backupTask);

      if (config.drill?.cron) {
        logger.info(
          { cron: config.drill.cron, timezone: config.schedule.timezone },
          "Registering restore drill schedule",
        );
        const drillTask = cron.schedule(
          config.drill.cron,
          () => {
            logger.info("Drill schedule triggered by cron");
            executeDrill().catch((err) => {
              logger.error({ err }, "Unhandled error during scheduled drill execution");
            });
          },
          { timezone: config.schedule.timezone },
        );
        tasks.push(drillTask);
      }

      if (config.pitr?.enabled) {
        oplogArchiver = startOplogArchiver({ config, storage, logger, notifier });
        oplogArchiver.start();
      }

      if (config.schedule.runOnStart) {
        logger.info("BACKUP_ON_START is true; queuing initial backup run");
        executeBackup().catch((err) => {
          logger.error({ err }, "Unhandled error during initial backup run");
        });
      }
    },

    async stop(): Promise<void> {
      isShuttingDown = true;
      for (const task of tasks) {
        task.stop();
      }

      if (oplogArchiver) {
        await oplogArchiver.stop();
      }

      if (activeAbortController) {
        logger.info("Aborting running backup for graceful shutdown");
        activeAbortController.abort(new Error("Daemon is shutting down"));
      }

      if (activeBackupPromise) {
        try {
          await activeBackupPromise;
        } catch {}
      }
    },
  };
}
