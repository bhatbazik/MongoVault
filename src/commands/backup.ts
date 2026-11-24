import path from "node:path";
import { databasePrefix, isClusterScope, scopeOf } from "../catalog.js";
import { EngineRegistry } from "../engines.js";
import { formatBytes, formatDuration } from "../format.js";
import { acquireHybridLock } from "../lock.js";
import {
  badge,
  createSpinner,
  renderBanner,
  renderErrorBox,
  renderKeyValue,
  renderPanel,
  type KeyValueRow,
} from "../tui.js";
import type { AppConfig } from "../types/config.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class BackupCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, notifier, values } = ctx;

    const targetDb = values.database || config.mongo.database;
    const targetOplog = values.oplog || config.mongo.oplog;

    if (targetOplog && !isClusterScope(targetDb)) {
      console.error(
        renderErrorBox(
          "Invalid Backup Flags",
          new Error("--oplog can only be used with full cluster backup (database: * or all)"),
        ),
      );
      process.exit(1);
    }
    if (isClusterScope(targetDb) && config.mongo.excludeCollections?.length > 0) {
      console.error(
        renderErrorBox(
          "Invalid Backup Configuration",
          new Error(
            "MONGODB_EXCLUDE_COLLECTIONS cannot be used with full cluster backup (database: * or all)",
          ),
        ),
      );
      process.exit(1);
    }

    const backupConfig: AppConfig = {
      ...config,
      mongo: {
        ...config.mongo,
        database: targetDb,
        oplog: targetOplog,
        readPreference:
          (values["read-preference"] as AppConfig["mongo"]["readPreference"]) ||
          config.mongo.readPreference,
        parallelCollections: values.parallel
          ? Number(values.parallel)
          : config.mongo.parallelCollections,
      },
    };

    const controller = new AbortController();
    const releaseLock = await acquireHybridLock({
      file: path.join(config.workDir, ".backup.lock"),
      storage,
      lockKey: `${databasePrefix(scopeOf(backupConfig))}.locks/backup.lock.json`,
      staleAfterMs: Math.max(config.timeoutMs + 60_000, 120_000),
      logger,
      onLost: () => {
        logger.error("Lost distributed backup lock during CLI backup; aborting");
        controller.abort(new Error("Lost distributed backup lock"));
      },
    });

    if (!releaseLock) {
      console.error(
        renderErrorBox(
          "Backup Lock Busy",
          new Error("Another backup is already in progress (failed to acquire backup lock)"),
        ),
      );
      process.exit(1);
    }

    const spinner = createSpinner(
      `Streaming ${values.engine || config.engine || "logical"} backup`,
      {
        enabled: !values.json && Boolean(process.stderr.isTTY),
      },
    );

    try {
      const engine = EngineRegistry.resolveForBackup({
        engine: values.engine,
        sharding: values.sharding,
        config: backupConfig,
      });

      const manifest = await engine.backup({
        config: backupConfig,
        storage,
        logger,
        signal: controller.signal,
        dbPath: values["db-path"],
      });

      spinner.succeed("Backup uploaded and verified");

      if (values.json) {
        console.log(JSON.stringify(manifest, null, 2));
      } else {
        const rows: KeyValueRow[] = [
          { label: "Engine", value: (manifest.engine || "logical").toUpperCase(), tone: "info" },
          { label: "Key", value: manifest.key },
          { label: "Size", value: formatBytes(manifest.size) },
          { label: "Duration", value: formatDuration(manifest.durationMs) },
          { label: "SHA-256", value: manifest.sha256 },
          {
            label: "Encrypted",
            value: manifest.encryption
              ? badge(manifest.encryption.algorithm || "AES-256-GCM", "success")
              : badge("false (unencrypted)", "warn"),
          },
          {
            label: "Oplog",
            value: manifest.oplog
              ? badge(
                  manifest.oplogWindow
                    ? `enabled (${manifest.oplogWindow.startTs} -> ${manifest.oplogWindow.endTs})`
                    : "enabled",
                  "success",
                )
              : "false",
          },
        ];
        console.log(
          `\n${renderBanner("Backup")}\n\n${renderPanel(renderKeyValue(rows, { indent: 1 }), {
            title: "Backup Completed Successfully",
            tone: "success",
          })}`,
        );
      }
      await notifier.notifySuccess("backup", manifest);
    } catch (error) {
      spinner.fail("Backup failed");
      await notifier.notifyFailure(
        "backup",
        error instanceof Error ? error : new Error(String(error)),
      );
      logger.error({ err: error }, "Manual backup execution failed");
      process.exit(1);
    } finally {
      await releaseLock();
    }
  }
}
