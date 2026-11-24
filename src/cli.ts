import { parseArgs } from "node:util";
import { CommandRegistry, type CliParsedValues } from "./commands/index.js";
import { generateKey } from "./crypto.js";
import { createLogger } from "./logger.js";
import { createMetricsRegistry } from "./metrics.js";
import { createNotifier } from "./notify.js";
import { createStorage } from "./storage.js";
import { padVisibleEnd, renderBanner, renderErrorBox, renderPanel, style } from "./tui.js";
import type { AppConfig } from "./types/config.js";

function formatHelpRows(rows: Array<[string, string]>, colWidth = 24): string[] {
  return rows.map(([cmd, desc]) => `  ${padVisibleEnd(style.cyan(cmd), colWidth)} ${desc}`);
}

export function printHelp(): void {
  const usagePanel = renderPanel(
    [
      `  ${style.bold("mongovault")} ${style.cyan("<command>")} ${style.dim("[options]")}`,
      `  ${style.bold("mongodb-backup")} ${style.cyan("<command>")} ${style.dim("[options]")}`,
    ],
    { title: "Usage", tone: "brand" },
  );

  const commandLines = [
    style.bold("Interactive & Monitoring"),
    ...formatHelpRows([
      ["tui, ui", "Launch interactive live terminal dashboard (status, backups, PITR, checks)"],
      ["status", "Display real-time cluster topology, storage health, and PITR status"],
      ["check", "Verify system prerequisites (MongoDB, oplog window, tools, storage, crypto)"],
      ["diagnostic", "Generate sanitized environment and database diagnostic report"],
      ["daemon", "Run scheduled backups, PITR archiver, restore drills, and /metrics server"],
    ]),
    "",
    style.bold("Backup & Restore"),
    ...formatHelpRows([
      ["backup", "Execute an immediate backup to configured storage"],
      ["restore", "Restore a backup archive (or continuous PITR target) to MongoDB"],
      ["verify", "Stream and verify backup checksum, HMAC signature, and decompression"],
      ["drill", "Run automated restore drill (counts, indexes, validators, views, R/W probe)"],
      ["list", "List available backups in configured storage"],
      ["describe", "Show detailed metadata, schema summary, and collection breakdown"],
    ]),
    "",
    style.bold("Point-in-Time Recovery (PITR)"),
    ...formatHelpRows([
      ["oplog:tail", "Run continuous oplog archiver daemon for second-level PITR"],
      ["oplog:slice", "Capture and upload a single incremental oplog slice now"],
      ["oplog:list", "List archived PITR oplog slices in configured storage"],
      ["oplog:prune", "Prune archived PITR oplog slices older than a target timestamp"],
    ]),
    "",
    style.bold("Maintenance & Security"),
    ...formatHelpRows([
      ["delete", "Safely delete a specific backup (protected against PITR invalidation)"],
      ["prune", "Apply retention policy and clean up expired backups"],
      ["key:generate", "Generate a new random AES-256 base64 encryption key"],
    ]),
  ];

  const optionLines = formatHelpRows(
    [
      ["--key <string>", "Target backup key (default: latest)"],
      ["--engine <string>", "Backup engine: logical or physical (default: logical)"],
      ["--db-path <path>", "WiredTiger database path for physical backup or restore"],
      ["--sharding", "Execute synchronized multi-shard cluster backup/restore via mongos"],
      ["--uri <string>", "Target MongoDB URI for restore (default: MONGODB_URI)"],
      ["--database <string>", "Target database name for restore (default: original database)"],
      [
        "--ns-include <string>",
        'Selective restore include pattern (e.g. "mydb.users,mydb.orders*")',
      ],
      ["--ns-exclude <string>", 'Selective restore exclude pattern (e.g. "mydb.audit_log")'],
      [
        "--read-preference <str>",
        "Read preference for dumps (secondaryPreferred, secondary, etc.)",
      ],
      ["--drop", "Drop existing collections before restoring (pre-verifies slices first)"],
      ["--pre-verify", "Verify archive integrity and SHA-256 before restoring"],
      ["--oplog", "Capture point-in-time oplog during backup"],
      ["--oplog-replay", "Replay captured oplog during restore"],
      ["--oplog-limit <ts:inc>", "Target timestamp for snapshot oplog replay (e.g. 1774092000:1)"],
      ["--pitr <iso|ts:inc>", "Restore/drill to exact point-in-time using base backup + slices"],
      ["--older-than <ts|iso>", "Cutoff timestamp for pruning oplog slices"],
      ["--parallel <number>", "Concurrent collections to dump/restore (default: 2)"],
      ["--force", "Bypass safety checks when deleting backups"],
      ["--dry-run", "Simulate deletion/prune without modifying storage"],
      ["--output <path>", "File path for diagnostic output bundle"],
      ["--json", "Output machine-readable JSON format (disables TUI styling)"],
      ["--help, -h", "Show this help message"],
    ],
    26,
  );

  const examplesPanel = renderPanel(
    [
      `  ${style.dim("$")} mongovault tui`,
      `  ${style.dim("$")} mongovault check`,
      `  ${style.dim("$")} mongovault backup --oplog`,
      `  ${style.dim("$")} mongovault restore --pitr 2026-11-24T12:00:00Z --drop`,
      `  ${style.dim("$")} mongovault drill --pitr latest`,
    ],
    { title: "Quick Examples", tone: "info" },
  );

  console.log(
    [
      "",
      renderBanner("MongoDB Backup, PITR & Sharded Recovery CLI"),
      "",
      usagePanel,
      "",
      renderPanel(commandLines, { title: "Commands" }),
      "",
      renderPanel(optionLines, { title: "Options" }),
      "",
      examplesPanel,
      "",
    ].join("\n"),
  );
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
      "pre-verify": { type: "boolean" },
      oplog: { type: "boolean", default: false },
      "oplog-replay": { type: "boolean" },
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
    console.error(
      renderErrorBox(
        `Configuration required for "${command}"`,
        new Error("Set MONGODB_URI and storage variables in .env or environment."),
      ),
    );
    process.exit(1);
  }

  const handler = CommandRegistry.get(command);
  if (!handler) {
    console.error(
      renderErrorBox(
        `Unknown command: ${command}`,
        new Error('Run "mongovault --help" to see available commands.'),
      ),
    );
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
