import { parseArgs } from "node:util";
import { runBackup } from "./backup.js";
import { isClusterScope, listBackups, scopeOf } from "./catalog.js";
import { generateKey } from "./crypto.js";
import { runDrill } from "./drill.js";
import { formatBytes, formatDate, formatDuration } from "./format.js";
import { createLogger } from "./logger.js";
import { createMetricsRegistry, startHttpServer } from "./metrics.js";
import { createNotifier } from "./notify.js";
import { restoreBackup, verifyBackup } from "./restore.js";
import { prune } from "./retention.js";
import { createScheduler } from "./scheduler.js";
import { createStorage } from "./storage.js";

function printHelp() {
  console.log(`
mongodb-backup-service CLI

Usage:
  mongodb-backup <command> [options]
  node src/index.js <command> [options]

Commands:
  daemon               Run backup service in daemon mode with scheduler and metrics
  backup               Execute an immediate backup to configured storage
  restore              Restore a backup archive to MongoDB
  verify               Stream and verify backup checksum and decompression
  list                 List available backups in configured bucket
  prune                Apply retention policy and clean up expired backups
  drill                Run automated restore drill and verify document counts
  key:generate         Generate a new random AES-256 base64 encryption key

Options:
  --key <string>          Target backup key (default: latest)
  --uri <string>          Target MongoDB URI for restore (default: MONGODB_URI)
  --database <string>     Target database name for restore (default: original database)
  --drop                  Drop existing collections before restoring
  --oplog                 Capture point-in-time oplog during backup
  --oplog-replay          Replay captured oplog during restore
  --oplog-limit <ts:inc>  Target timestamp for point-in-time recovery (e.g. 1774092000:1)
  --parallel <number>     Number of collections to dump/restore concurrently (default: 2)
  --dry-run               Simulate retention prune without deleting objects
  --json                  Output machine-readable JSON format
  --help, -h              Show this help message
`);
}

export async function runCli(config, argv = process.argv.slice(2)) {
  const command = argv[0];

  if (!command || command === "--help" || command === "-h" || command === "help") {
    printHelp();
    return;
  }

  if (command === "key:generate") {
    const key = generateKey();
    console.log(key);
    return;
  }

  const { values } = parseArgs({
    args: argv.slice(1),
    options: {
      key: { type: "string" },
      uri: { type: "string" },
      database: { type: "string" },
      drop: { type: "boolean", default: false },
      oplog: { type: "boolean", default: false },
      "oplog-replay": { type: "boolean", default: false },
      "oplog-limit": { type: "string" },
      parallel: { type: "string" },
      "dry-run": { type: "boolean", default: false },
      json: { type: "boolean", default: false },
      help: { type: "boolean", short: "h" },
    },
    strict: true,
  });

  if (values.help) {
    printHelp();
    return;
  }

  const logger = createLogger({
    level: config.logLevel,
    pretty: process.stdout.isTTY && !values.json,
  });
  const storage = createStorage(config.storage);
  const notifier = createNotifier(config, logger);
  const metrics = createMetricsRegistry();

  switch (command) {
    case "daemon": {
      const scheduler = createScheduler({ config, storage, logger, notifier, metrics });
      const httpServer = startHttpServer({
        port: config.http.port,
        metrics,
        getState: () => scheduler.getState(),
        logger,
      });

      scheduler.start();

      const shutdown = async (signal) => {
        logger.info({ signal }, "Received shutdown signal, stopping daemon...");
        await scheduler.stop();
        if (httpServer) await httpServer.close();
        process.exit(0);
      };

      process.on("SIGINT", () => void shutdown("SIGINT"));
      process.on("SIGTERM", () => void shutdown("SIGTERM"));
      break;
    }

    case "backup": {
      const targetDb = values.database || config.mongo.database;
      const targetOplog = values.oplog || config.mongo.oplog;
      if (targetOplog && !isClusterScope(targetDb)) {
        console.error("Error: --oplog can only be used with full cluster backup (database: * or all)");
        process.exit(1);
      }
      if (isClusterScope(targetDb) && config.mongo.excludeCollections?.length > 0) {
        console.error(
          "Error: MONGODB_EXCLUDE_COLLECTIONS cannot be used with full cluster backup (database: * or all)",
        );
        process.exit(1);
      }
      const backupConfig = {
        ...config,
        mongo: {
          ...config.mongo,
          database: targetDb,
          oplog: targetOplog,
          parallelCollections: values.parallel
            ? Number(values.parallel)
            : config.mongo.parallelCollections,
        },
      };

      try {
        const manifest = await runBackup({ config: backupConfig, storage, logger });
        if (values.json) {
          console.log(JSON.stringify(manifest, null, 2));
        } else {
          console.log("\nBackup Completed Successfully");
          console.log(`Key:        ${manifest.key}`);
          console.log(`Size:       ${formatBytes(manifest.size)}`);
          console.log(`Duration:   ${formatDuration(manifest.durationMs)}`);
          console.log(`SHA-256:    ${manifest.sha256}`);
          console.log(`Encrypted:  ${Boolean(manifest.encryption)}`);
          console.log(`Oplog:      ${Boolean(manifest.oplog)}`);
        }
        await notifier.notifySuccess("backup", manifest);
      } catch (error) {
        await notifier.notifyFailure("backup", error);
        logger.error({ err: error }, "Manual backup execution failed");
        process.exit(1);
      }
      break;
    }

    case "restore": {
      const targetUri = values.uri || config.mongo.uri;
      try {
        const result = await restoreBackup(
          { config, storage, logger },
          {
            ref: values.key || "latest",
            uri: targetUri,
            database: values.database,
            drop: values.drop,
            oplogReplay: values["oplog-replay"],
            oplogLimit: values["oplog-limit"],
            parallelCollections: values.parallel ? Number(values.parallel) : undefined,
          },
        );
        if (values.json) {
          console.log(JSON.stringify(result, null, 2));
        } else {
          console.log("\nRestore Completed Successfully");
          console.log(`Key:        ${result.key}`);
          console.log(`Target DB:  ${result.target}`);
          console.log(`Restored:   ${result.restored} documents`);
        }
      } catch (error) {
        logger.error({ err: error }, "Restore execution failed");
        process.exit(1);
      }
      break;
    }

    case "verify": {
      try {
        const result = await verifyBackup(
          { config, storage, logger },
          values.key || "latest",
        );
        if (values.json) {
          console.log(JSON.stringify(result, null, 2));
        } else {
          console.log("\nBackup Verification Passed");
          console.log(`Key:        ${result.key}`);
          console.log(`Size:       ${formatBytes(result.size)}`);
          console.log(`SHA-256:    ${result.sha256}`);
          console.log(`Encrypted:  ${result.encrypted}`);
        }
      } catch (error) {
        logger.error({ err: error }, "Verification failed");
        process.exit(1);
      }
      break;
    }

    case "list": {
      const backups = await listBackups(storage, scopeOf(config));
      if (values.json) {
        console.log(JSON.stringify(backups, null, 2));
      } else {
        if (backups.length === 0) {
          console.log("No backups found in target prefix.");
          return;
        }
        console.log(
          ["Key".padEnd(52), "Created".padEnd(22), "Size".padEnd(12), "Encrypted", "Status"].join(" "),
        );
        console.log("-".repeat(100));
        for (const b of backups) {
          console.log(
            [
              b.key.padEnd(52),
              formatDate(b.createdAt, config.schedule.timezone).padEnd(22),
              formatBytes(b.size).padEnd(12),
              String(b.encrypted).padEnd(9),
              b.complete ? "complete" : "unverified",
            ].join(" "),
          );
        }
      }
      break;
    }

    case "prune": {
      try {
        const result = await prune(
          { config, storage, logger },
          { dryRun: values["dry-run"] },
        );
        if (values.json) {
          console.log(JSON.stringify(result, null, 2));
        } else {
          console.log(`Retention prune completed (${values["dry-run"] ? "dry-run" : "applied"}):`);
          console.log(`Kept:    ${result.keep.length}`);
          console.log(`Removed: ${result.remove.length}`);
          for (const item of result.remove) {
            console.log(`  - ${item.key}`);
          }
        }
      } catch (error) {
        logger.error({ err: error }, "Prune failed");
        process.exit(1);
      }
      break;
    }

    case "drill": {
      try {
        const result = await runDrill({ config, storage, logger });
        if (values.json) {
          console.log(JSON.stringify(result, null, 2));
        } else {
          console.log("\nRestore Drill Passed Successfully");
          console.log(`Key:         ${result.key}`);
          console.log(`Collections: ${result.collections}`);
          console.log(`Documents:   ${result.documents}`);
          console.log(`Duration:    ${formatDuration(result.durationMs)}`);
        }
      } catch (error) {
        logger.error({ err: error }, "Restore drill failed");
        process.exit(1);
      }
      break;
    }

    default: {
      console.error(`Unknown command: ${command}`);
      printHelp();
      process.exit(1);
    }
  }
}
