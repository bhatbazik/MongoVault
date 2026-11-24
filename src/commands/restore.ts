import { EngineRegistry } from "../engines.js";
import { formatBytes } from "../format.js";
import {
  badge,
  createSpinner,
  renderBanner,
  renderKeyValue,
  renderPanel,
  type KeyValueRow,
} from "../tui.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class RestoreCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    const spinner = createSpinner(
      values.pitr ? `Running PITR restore to ${values.pitr}` : "Running restore",
      { enabled: !values.json && Boolean(process.stderr.isTTY) },
    );

    try {
      const engine = EngineRegistry.resolveForRestore({
        engine: values.engine,
        key: values.key,
        dbPath: values["db-path"],
        sharding: values.sharding,
        config,
      });

      const result = (await engine.restore({
        config,
        storage,
        logger,
        ref: values.key || "latest",
        uri: values.uri || config.mongo.uri,
        database: values.database,
        dbPath: values["db-path"],
        nsInclude: values["ns-include"],
        nsExclude: values["ns-exclude"],
        drop: values.drop,
        preVerify: values["pre-verify"] !== undefined ? Boolean(values["pre-verify"]) : values.drop,
        oplogReplay: values["oplog-replay"],
        oplogLimit: values["oplog-limit"],
        pitr: values.pitr,
        parallelCollections: values.parallel ? Number(values.parallel) : undefined,
      })) as Record<string, unknown>;

      spinner.succeed("Restore completed");

      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      if (engine.name === "physical") {
        const rows: KeyValueRow[] = [
          { label: "Key", value: String(result.key) },
          { label: "Target", value: String(result.targetDbPath) },
          {
            label: "Files",
            value: `${result.restoredFiles} restored (${formatBytes(result.totalBytes as number)})`,
          },
        ];
        console.log(
          `\n${renderBanner("Physical Restore")}\n\n${renderPanel(
            renderKeyValue(rows, { indent: 1 }),
            { title: "Physical Restore Completed Successfully", tone: "success" },
          )}`,
        );
      } else if (engine.name === "sharded") {
        const rows: KeyValueRow[] = [
          { label: "Key", value: String(result.key) },
          { label: "Cluster Time", value: String(result.clusterTime), tone: "info" },
          { label: "Shards", value: (result.restoredShards as string[]).join(", ") },
        ];
        if (result.pitrTarget) {
          rows.push({
            label: "PITR Target",
            value: `${result.pitrTarget} (${result.replayedSlices ?? 0} slices)`,
            tone: "info",
          });
        }
        console.log(
          `\n${renderBanner("Sharded Restore")}\n\n${renderPanel(
            renderKeyValue(rows, { indent: 1 }),
            { title: "Sharded Cluster Restored Successfully", tone: "success" },
          )}`,
        );
      } else {
        const rows: KeyValueRow[] = [
          { label: "Key", value: String(result.key) },
          { label: "Target DB", value: String(result.target) },
          { label: "Restored", value: `${result.restored} documents`, tone: "success" },
        ];
        if (result.pitrTarget) {
          rows.push(
            { label: "PITR Limit", value: String(result.pitrTarget), tone: "info" },
            {
              label: "Slices",
              value: `${result.replayedSlices} replayed (${result.replayedEntries} entries)`,
            },
          );
        }
        const stats = result.oplogStats as
          | {
              appliedEntries?: number;
              skippedEntries?: number;
              indexesBuilt?: number;
              uncommittedTxns?: number;
              partialTxns?: number;
            }
          | undefined;
        if (stats) {
          rows.push({
            label: "Oplog Replay",
            value: `${stats.appliedEntries ?? 0} applied · ${stats.skippedEntries ?? 0} skipped · ${stats.indexesBuilt ?? 0} indexes built · ${stats.uncommittedTxns ?? 0} uncommitted discarded`,
          });
          if ((stats.partialTxns ?? 0) === 0) {
            rows.push({ label: "Integrity", value: badge("complete (0 partial txns)", "success") });
          }
        }
        console.log(
          `\n${renderBanner("Restore")}\n\n${renderPanel(renderKeyValue(rows, { indent: 1 }), {
            title: "Restore Completed Successfully",
            tone: "success",
          })}`,
        );
      }
    } catch (error) {
      spinner.fail("Restore failed");
      logger.error({ err: error }, "Restore execution failed");
      process.exit(1);
    }
  }
}
