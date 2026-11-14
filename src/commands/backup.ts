import { isClusterScope } from "../catalog.js";
import { EngineRegistry } from "../engines.js";
import { formatBytes, formatDuration } from "../format.js";
import type { AppConfig } from "../types/config.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class BackupCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, notifier, values } = ctx;

    const targetDb = values.database || config.mongo.database;
    const targetOplog = values.oplog || config.mongo.oplog;

    if (targetOplog && !isClusterScope(targetDb)) {
      console.error(
        "Error: --oplog can only be used with full cluster backup (database: * or all)",
      );
      process.exit(1);
    }
    if (isClusterScope(targetDb) && config.mongo.excludeCollections?.length > 0) {
      console.error(
        "Error: MONGODB_EXCLUDE_COLLECTIONS cannot be used with full cluster backup (database: * or all)",
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

    try {
      const engine = EngineRegistry.resolveForBackup({
        engine: values.engine,
        sharding: values.sharding,
        config,
      });

      const manifest = await engine.backup({
        config: backupConfig,
        storage,
        logger,
        dbPath: values["db-path"],
      });

      if (values.json) {
        console.log(JSON.stringify(manifest, null, 2));
      } else {
        console.log("\nBackup Completed Successfully");
        console.log(`Engine:     ${manifest.engine || "logical"}`);
        console.log(`Key:        ${manifest.key}`);
        console.log(`Size:       ${formatBytes(manifest.size)}`);
        console.log(`Duration:   ${formatDuration(manifest.durationMs)}`);
        console.log(`SHA-256:    ${manifest.sha256}`);
        console.log(`Encrypted:  ${Boolean(manifest.encryption)}`);
        console.log(`Oplog:      ${Boolean(manifest.oplog)}`);
      }
      await notifier.notifySuccess("backup", manifest);
    } catch (error) {
      await notifier.notifyFailure(
        "backup",
        error instanceof Error ? error : new Error(String(error)),
      );
      logger.error({ err: error }, "Manual backup execution failed");
      process.exit(1);
    }
  }
}
