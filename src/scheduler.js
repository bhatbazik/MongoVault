import path from "node:path";
import cron from "node-cron";
import { runBackup } from "./backup.js";
import { databasePrefix, scopeOf } from "./catalog.js";
import { runDrill } from "./drill.js";
import { acquireHybridLock } from "./lock.js";
import { prune } from "./retention.js";

export function createScheduler({ config, storage, logger, notifier, metrics }) {
  const lockFile = path.join(config.workDir, ".backup.lock");
  const distLockKey = `${databasePrefix(scopeOf(config))}.locks/backup.lock.json`;
  let activeBackupPromise = null;
  let activeAbortController = null;
  let isShuttingDown = false;

  const state = {
    status: "idle",
    lastRunAt: null,
    lastSuccessAt: null,
    lastError: null,
    lastManifest: null,
    lastDrillAt: null,
  };

  async function executeBackup() {
    if (isShuttingDown) return;

    const release = await acquireHybridLock({
      file: lockFile,
      storage,
      lockKey: distLockKey,
      staleAfterMs: config.timeoutMs + 60_000,
      logger,
    });

    if (!release) {
      logger.warn("Skipping backup: another process holds the local or distributed lock");
      return;
    }

    state.status = "running";
    state.lastRunAt = new Date().toISOString();
    activeAbortController = new AbortController();

    try {
      activeBackupPromise = runBackup({
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
      state.status = "error";
      state.lastError = error.message;
      metrics.recordBackupFailure(config.mongo.database);
      await notifier.notifyFailure("backup", error, { database: config.mongo.database });
      logger.error({ err: error }, "Scheduled backup run failed");
    } finally {
      activeBackupPromise = null;
      activeAbortController = null;
      await release();
    }
  }

  async function executeDrill() {
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
      metrics.recordDrillFailure();
      await notifier.notifyFailure("drill", error);
      logger.error({ err: error }, "Scheduled restore drill failed");
    }
  }

  const tasks = [];

  return {
    getState() {
      return {
        ...state,
        uptimeSeconds: Math.floor(process.uptime()),
        isShuttingDown,
      };
    },

    triggerBackup() {
      return executeBackup();
    },

    triggerDrill() {
      return executeDrill();
    },

    start() {
      logger.info(
        { cron: config.schedule.cron, timezone: config.schedule.timezone },
        "Registering backup schedule",
      );

      const backupTask = cron.schedule(
        config.schedule.cron,
        () => {
          logger.info("Backup schedule triggered by cron");
          void executeBackup();
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
            void executeDrill();
          },
          { timezone: config.schedule.timezone },
        );
        tasks.push(drillTask);
      }

      if (config.schedule.runOnStart) {
        logger.info("BACKUP_ON_START is true; queuing initial backup run");
        void executeBackup();
      }
    },

    async stop() {
      isShuttingDown = true;
      for (const task of tasks) {
        task.stop();
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
