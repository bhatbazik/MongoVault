import fs from "node:fs/promises";
import { parseArgs } from "node:util";
import { runBackup } from "./backup.js";
import { isClusterScope, listBackups, scopeOf } from "./catalog.js";
import { formatPreflightCheck, runPreflightCheck } from "./check.js";
import { generateKey } from "./crypto.js";
import { generateDiagnosticBundle } from "./diagnostic.js";
import { runDrill } from "./drill.js";
import { formatBytes, formatDate, formatDuration } from "./format.js";
import { createLogger } from "./logger.js";
import { createMetricsRegistry, startHttpServer } from "./metrics.js";
import { createNotifier } from "./notify.js";
import {
  captureOplogSlice,
  formatOplogTimestamp,
  listOplogSlices,
  startOplogArchiver,
} from "./oplog.js";
import { describeBackup, restoreBackup, verifyBackup } from "./restore.js";
import { runPhysicalBackup, restorePhysicalBackup } from "./physical.js";
import { runCloudSnapshot, restoreCloudSnapshot } from "./snapshot.js";
import { runShardedClusterBackup, restoreShardedCluster } from "./sharding.js";
import { deleteBackup, prune, pruneOplogSlices } from "./retention.js";
import { createScheduler } from "./scheduler.js";
import { formatStatus, getStatus } from "./status.js";
import { createStorage } from "./storage.js";
import type { AppConfig } from "./types/config.js";

function printHelp(): void {
  console.log(`
MongoVault CLI - Enterprise-Grade MongoDB Backup & PITR Daemon

Usage:
  mongodb-backup <command> [options]
  node src/index.js <command> [options]

Commands:
  status               Display real-time cluster topology, storage health, and PITR status
  check                Verify system prerequisites (MongoDB, tools, storage, and crypto)
  backup               Execute an immediate backup to configured storage
  restore              Restore a backup archive (or continuous PITR target) to MongoDB
  describe             Show detailed metadata and collection breakdown of a backup
  verify               Stream and verify backup checksum and decompression
  list                 List available backups in configured storage
  delete               Safely delete a specific backup (protected against PITR invalidation)
  prune                Apply retention policy and clean up expired backups
  diagnostic           Generate sanitized environment and database diagnostic report
  drill                Run automated restore drill and verify document counts
  oplog:tail           Run continuous oplog archiver daemon for second-level PITR
  oplog:slice          Capture and upload a single incremental oplog slice now
  oplog:list           List archived PITR oplog slices in configured storage
  oplog:prune          Prune archived PITR oplog slices older than a target timestamp
  key:generate         Generate a new random AES-256 base64 encryption key

Options:
  --key <string>            Target backup key (default: latest)
  --engine <string>         Backup engine: logical, physical, or snapshot (default: logical)
  --db-path <path>          WiredTiger database path for physical backup or restore
  --volume-id <string>      Cloud volume ID for cloud snapshot engine
  --sharding                Execute synchronized multi-shard cluster backup
  --uri <string>            Target MongoDB URI for restore (default: MONGODB_URI)
  --database <string>       Target database name for restore (default: original database)
  --ns-include <string>     Selective restore collection pattern (e.g. "mydb.users,mydb.orders*")
  --ns-exclude <string>     Selective restore exclusion pattern (e.g. "mydb.audit_log")
  --read-preference <str>   Read preference for backup dumps (secondaryPreferred, secondary, etc.)
  --drop                    Drop existing collections before restoring
  --oplog                   Capture point-in-time oplog during backup
  --oplog-replay            Replay captured oplog during restore
  --oplog-limit <ts:inc>    Target timestamp for snapshot oplog replay (e.g. 1774092000:1)
  --pitr <iso|ts:inc>       Restore to exact point-in-time using base backup + oplog slices
  --older-than <ts|iso>     Cutoff timestamp for pruning oplog slices
  --parallel <number>       Number of collections to dump/restore concurrently (default: 2)
  --force                   Bypass safety checks when deleting backups
  --dry-run                 Simulate deletion/prune without modifying storage
  --output <path>           File path for diagnostic output bundle
  --json                    Output machine-readable JSON format
  --help, -h                Show this help message
`);
}

export async function runCli(
  config: AppConfig | null,
  argv: string[] = process.argv.slice(2),
): Promise<void> {
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
      engine: { type: "string" },
      "db-path": { type: "string" },
      "volume-id": { type: "string" },
      sharding: { type: "boolean", default: false },
      uri: { type: "string" },
      database: { type: "string" },
      "ns-include": { type: "string" },
      "ns-exclude": { type: "string" },
      "read-preference": { type: "string" },
      "older-than": { type: "string" },
      output: { type: "string" },
      force: { type: "boolean", default: false },
      drop: { type: "boolean", default: false },
      oplog: { type: "boolean", default: false },
      "oplog-replay": { type: "boolean", default: false },
      "oplog-limit": { type: "string" },
      pitr: { type: "string" },
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

  if (!config) {
    console.error("Error: Configuration is required for command:", command);
    process.exit(1);
  }

  const logger = createLogger({
    level: config.logLevel,
    pretty: process.stdout.isTTY && !values.json,
  });
  const storage = createStorage(config.storage);
  const notifier = createNotifier(config, logger);
  const metrics = createMetricsRegistry();

  switch (command) {
    case "status": {
      try {
        const status = await getStatus({ config, storage, logger });
        if (values.json) {
          console.log(JSON.stringify(status, null, 2));
        } else {
          console.log("\n" + formatStatus(status));
        }
      } catch (error) {
        logger.error({ err: error }, "Failed to retrieve system status");
        process.exit(1);
      }
      break;
    }

    case "check": {
      try {
        const result = await runPreflightCheck({ config, storage });
        if (values.json) {
          console.log(JSON.stringify(result, null, 2));
        } else {
          console.log("\n" + formatPreflightCheck(result));
        }
        if (!result.allPassed) {
          process.exit(1);
        }
      } catch (error) {
        logger.error({ err: error }, "Preflight check failed");
        process.exit(1);
      }
      break;
    }

    case "describe": {
      try {
        const { key, manifest, checksum } = await describeBackup(
          { config, storage, logger },
          values.key || "latest",
        );
        if (values.json) {
          console.log(JSON.stringify({ key, checksum, manifest }, null, 2));
        } else if (manifest) {
          console.log("\nBackup Metadata Details");
          console.log("=======================");
          console.log(`Key:          ${manifest.key}`);
          console.log(`Database:     ${manifest.database}`);
          console.log(`Created At:   ${formatDate(manifest.createdAt)}`);
          console.log(`Completed At: ${formatDate(manifest.completedAt)}`);
          console.log(`Duration:     ${formatDuration(manifest.durationMs)}`);
          console.log(`Archive Size: ${formatBytes(manifest.size)} (${manifest.size} bytes)`);
          console.log(`SHA-256:      ${manifest.sha256}`);
          console.log(`Compression:  ${manifest.compression}`);
          if (manifest.encryption) {
            console.log(
              `Encryption:   ${manifest.encryption.algorithm} (Key ID: ${manifest.encryption.keyId})`,
            );
          } else {
            console.log("Encryption:   None (Unencrypted)");
          }
          if (manifest.oplogWindow) {
            console.log(
              `Oplog Window: ${manifest.oplogWindow.startTs} -> ${manifest.oplogWindow.endTs}`,
            );
          }
          if (manifest.excludedCollections && manifest.excludedCollections.length > 0) {
            console.log(`Excluded:     ${manifest.excludedCollections.join(", ")}`);
          }
          if (manifest.collections && Object.keys(manifest.collections).length > 0) {
            console.log("\nCollections & Document Counts:");
            console.log("------------------------------");
            for (const [coll, count] of Object.entries(manifest.collections)) {
              console.log(`  • ${coll.padEnd(35)} : ${count.toLocaleString()} docs`);
            }
          }
        } else {
          console.log(`Backup found: ${key} (No .manifest.json available)`);
        }
      } catch (error) {
        logger.error({ err: error }, "Failed to describe backup");
        process.exit(1);
      }
      break;
    }

    case "delete": {
      if (!values.key) {
        console.error("Error: --key <string> is required for delete command");
        process.exit(1);
      }
      try {
        const result = await deleteBackup(
          { config, storage, logger },
          { key: values.key, force: values.force, dryRun: values["dry-run"] },
        );
        if (values.json) {
          console.log(JSON.stringify(result, null, 2));
        } else {
          console.log(result.message);
        }
      } catch (error) {
        logger.error({ err: error }, "Delete failed");
        process.exit(1);
      }
      break;
    }

    case "diagnostic": {
      try {
        const bundle = await generateDiagnosticBundle({ config, storage });
        const jsonOutput = JSON.stringify(bundle, null, 2);
        if (values.output) {
          await fs.writeFile(values.output, jsonOutput, "utf8");
          console.log(`Diagnostic bundle written to ${values.output}`);
        } else {
          console.log(jsonOutput);
        }
      } catch (error) {
        logger.error({ err: error }, "Failed to generate diagnostic bundle");
        process.exit(1);
      }
      break;
    }

    case "daemon": {
      const scheduler = createScheduler({ config, storage, logger, notifier, metrics });
      const httpServer = startHttpServer({
        port: config.http.port,
        metrics,
        getState: () => scheduler.getState(),
        logger,
      });

      scheduler.start();

      const shutdown = async (signal: string) => {
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
        let manifest;
        const selectedEngine = values.engine || config.engine || "logical";

        if (selectedEngine === "physical") {
          const res = await runPhysicalBackup({
            config: backupConfig,
            storage,
            logger,
            dbPath: values["db-path"],
          });
          manifest = res.manifest;
        } else if (selectedEngine === "snapshot") {
          const res = await runCloudSnapshot({
            config: backupConfig,
            storage,
            logger,
            volumeId: values["volume-id"],
          });
          manifest = res.manifest;
        } else if (values.sharding || config.sharding?.enabled) {
          const res = await runShardedClusterBackup({
            config: backupConfig,
            storage,
            logger,
          });
          manifest = res.manifest;
        } else {
          manifest = await runBackup({ config: backupConfig, storage, logger });
        }

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
      break;
    }

    case "restore": {
      if (values["db-path"]) {
        try {
          const res = await restorePhysicalBackup({
            config,
            storage,
            logger,
            targetDbPath: values["db-path"],
            ref: values.key || "latest",
          });
          if (values.json) {
            console.log(JSON.stringify(res, null, 2));
          } else {
            console.log("\nPhysical Restore Completed Successfully");
            console.log(`Key:        ${res.key}`);
            console.log(`Target:     ${res.targetDbPath}`);
            console.log(
              `Files:      ${res.restoredFiles} restored (${formatBytes(res.totalBytes)})`,
            );
          }
        } catch (error) {
          logger.error({ err: error }, "Physical restore failed");
          process.exit(1);
        }
        break;
      }

      if (values.engine === "snapshot" || values.key?.endsWith(".snapshot")) {
        try {
          const res = await restoreCloudSnapshot({
            config,
            storage,
            logger,
            ref: values.key || "latest",
          });
          if (values.json) {
            console.log(JSON.stringify(res, null, 2));
          } else {
            console.log("\nCloud Snapshot Restored Successfully");
            console.log(`Snapshot ID:  ${res.snapshotId}`);
            console.log(`Volume ID:    ${res.volumeId}`);
            console.log(`New Volume:   ${res.newVolumeId || "n/a"}`);
          }
        } catch (error) {
          logger.error({ err: error }, "Cloud snapshot restore failed");
          process.exit(1);
        }
        break;
      }

      if (values.sharding) {
        try {
          const res = await restoreShardedCluster({
            config,
            storage,
            logger,
            ref: values.key || "latest",
            drop: values.drop,
          });
          if (values.json) {
            console.log(JSON.stringify(res, null, 2));
          } else {
            console.log("\nSharded Cluster Restored Successfully");
            console.log(`Key:          ${res.key}`);
            console.log(`Cluster Time: ${res.clusterTime}`);
            console.log(`Shards:       ${res.restoredShards.join(", ")}`);
          }
        } catch (error) {
          logger.error({ err: error }, "Sharded cluster restore failed");
          process.exit(1);
        }
        break;
      }

      const targetUri = values.uri || config.mongo.uri;
      try {
        const result = await restoreBackup(
          { config, storage, logger },
          {
            ref: values.key || "latest",
            uri: targetUri,
            database: values.database,
            nsInclude: values["ns-include"],
            nsExclude: values["ns-exclude"],
            drop: values.drop,
            oplogReplay: values["oplog-replay"],
            oplogLimit: values["oplog-limit"],
            pitr: values.pitr,
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
          if (result.pitrTarget) {
            console.log(`PITR Limit: ${result.pitrTarget}`);
            console.log(
              `Slices:     ${result.replayedSlices} replayed (${result.replayedEntries} entries)`,
            );
          }
        }
      } catch (error) {
        logger.error({ err: error }, "Restore execution failed");
        process.exit(1);
      }
      break;
    }

    case "verify": {
      try {
        const result = await verifyBackup({ config, storage, logger }, values.key || "latest");
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
          ["Key".padEnd(52), "Created".padEnd(22), "Size".padEnd(12), "Encrypted", "Status"].join(
            " ",
          ),
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
        const result = await prune({ config, storage, logger }, { dryRun: values["dry-run"] });
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

    case "oplog:slice": {
      try {
        const manifest = await captureOplogSlice({ config, storage, logger });
        if (values.json) {
          console.log(JSON.stringify(manifest, null, 2));
        } else if (manifest) {
          console.log("\nOplog Slice Captured Successfully");
          console.log(`Key:        ${manifest.key}`);
          console.log(
            `Range:      ${formatOplogTimestamp(manifest.oplogWindow!.startTs)} -> ${formatOplogTimestamp(manifest.oplogWindow!.endTs)}`,
          );
          console.log(`Entries:    ${manifest.oplogWindow!.entries}`);
          console.log(`Size:       ${formatBytes(manifest.size)}`);
          if (manifest.oplogWindow!.gapDetected) {
            console.log(
              "WARNING:    Oplog gap detected prior to this slice; take a new base backup.",
            );
          }
        } else {
          console.log("No new oplog entries since last checkpoint.");
        }
      } catch (error) {
        logger.error({ err: error }, "Oplog slice capture failed");
        process.exit(1);
      }
      break;
    }

    case "oplog:tail": {
      const archiver = startOplogArchiver({ config, storage, logger, notifier });
      archiver.start();
      const shutdown = async (signal: string) => {
        logger.info({ signal }, "Stopping continuous oplog archiver...");
        await archiver.stop();
        process.exit(0);
      };
      process.on("SIGINT", () => void shutdown("SIGINT"));
      process.on("SIGTERM", () => void shutdown("SIGTERM"));
      break;
    }

    case "oplog:list": {
      const slices = await listOplogSlices(storage, scopeOf(config));
      if (values.json) {
        console.log(JSON.stringify(slices, null, 2));
      } else {
        if (slices.length === 0) {
          console.log("No PITR oplog slices found in target prefix.");
          return;
        }
        console.log(
          ["Key".padEnd(64), "Start TS".padEnd(16), "End TS".padEnd(16), "Size"].join(" "),
        );
        console.log("-".repeat(110));
        for (const s of slices) {
          console.log(
            [
              s.key.padEnd(64),
              formatOplogTimestamp(s.startTs).padEnd(16),
              formatOplogTimestamp(s.endTs).padEnd(16),
              formatBytes(s.size),
            ].join(" "),
          );
        }
      }
      break;
    }

    case "oplog:prune": {
      if (!values["older-than"]) {
        console.error("Error: --older-than <timestamp|iso> is required for oplog:prune command");
        process.exit(1);
      }
      try {
        const result = await pruneOplogSlices(
          { config, storage, logger },
          { olderThan: values["older-than"], dryRun: values["dry-run"] },
        );
        if (values.json) {
          console.log(JSON.stringify(result, null, 2));
        } else {
          console.log(
            `Pruned ${result.deletedSlices.length} oplog slices older than ${result.cutoffTs}`,
          );
        }
      } catch (error) {
        logger.error({ err: error }, "Oplog prune failed");
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
