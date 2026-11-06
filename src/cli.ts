import { parseArgs } from "node:util";
import { CommandRegistry, type CliParsedValues } from "./commands/index.js";
import { generateKey } from "./crypto.js";
import { createLogger } from "./logger.js";
import { createMetricsRegistry } from "./metrics.js";
import { createNotifier } from "./notify.js";
import { createStorage } from "./storage.js";
import type { AppConfig } from "./types/config.js";

export function printHelp(): void {
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

  const parsedValues = values as CliParsedValues;

  if (parsedValues.help) {
    printHelp();
    return;
  }

  if (!config) {
    console.error("Error: Configuration is required for command:", command);
    process.exit(1);
  }

  const handler = CommandRegistry.get(command);
  if (!handler) {
    console.error(`Unknown command: ${command}`);
    printHelp();
    process.exit(1);
  }

  const logger = createLogger({
    level: config.logLevel,
    pretty: process.stdout.isTTY && !parsedValues.json,
  });
  const storage = createStorage(config.storage);
  const notifier = createNotifier(config, logger);
  const metrics = createMetricsRegistry();

  await handler.execute({
    config,
    storage,
    logger,
    notifier,
    metrics,
    values: parsedValues,
  });
}
