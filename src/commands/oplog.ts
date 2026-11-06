import { scopeOf } from "../catalog.js";
import { formatBytes } from "../format.js";
import {
  captureOplogSlice,
  formatOplogTimestamp,
  listOplogSlices,
  startOplogArchiver,
} from "../oplog.js";
import { pruneOplogSlices } from "../retention.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class OplogSliceCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
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
  }
}

export class OplogTailCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, notifier } = ctx;
    const archiver = startOplogArchiver({ config, storage, logger, notifier });
    archiver.start();
    const shutdown = async (signal: string) => {
      logger.info({ signal }, "Stopping continuous oplog archiver...");
      await archiver.stop();
      process.exit(0);
    };
    process.on("SIGINT", () => void shutdown("SIGINT"));
    process.on("SIGTERM", () => void shutdown("SIGTERM"));
  }
}

export class OplogListCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, values } = ctx;
    const slices = await listOplogSlices(storage, scopeOf(config));
    if (values.json) {
      console.log(JSON.stringify(slices, null, 2));
    } else {
      if (slices.length === 0) {
        console.log("No PITR oplog slices found in target prefix.");
        return;
      }
      console.log(["Key".padEnd(64), "Start TS".padEnd(16), "End TS".padEnd(16), "Size"].join(" "));
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
  }
}

export class OplogPruneCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
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
  }
}
