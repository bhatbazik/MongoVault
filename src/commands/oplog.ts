import { scopeOf } from "../catalog.js";
import { formatBytes } from "../format.js";
import { abortOnTerminationSignal, acquireCommandLock } from "./command-lock.js";
import {
  captureOplogSlice,
  formatOplogTimestamp,
  listOplogSlices,
  startOplogArchiver,
} from "../oplog.js";
import { pruneOplogSlices } from "../retention.js";
import {
  badge,
  createSpinner,
  renderBanner,
  renderErrorBox,
  renderKeyValue,
  renderPanel,
  renderTable,
  style,
  type KeyValueRow,
} from "../tui.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class OplogSliceCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    const lock = await acquireCommandLock(ctx, "oplog", "Oplog Slice Lock Busy");
    const disposeSignals = abortOnTerminationSignal(lock.controller);
    let failed = false;
    const spinner = createSpinner("Capturing incremental PITR oplog slice", {
      enabled: !values.json && Boolean(process.stderr.isTTY),
    });

    try {
      if (config.sharding?.enabled) {
        const { captureShardedOplogSlices } = await import("../sharding.js");
        const results = await captureShardedOplogSlices({
          config,
          storage,
          logger,
          signal: lock.controller.signal,
        });
        spinner.stop();
        if (values.json) {
          console.log(JSON.stringify(results, null, 2));
        } else if (!results) {
          console.log(
            `\n${renderBanner("PITR Oplog Slice")}\n\n${renderPanel(
              [`  ${style.dim("Cluster is not sharded; no sharded oplog slices captured.")}`],
              { title: "Sharded Oplog Slice", tone: "muted" },
            )}`,
          );
        } else {
          const entries = [
            ["configsvr", results.configsvr] as const,
            ...Object.entries(results.shards),
          ];
          const tbl = renderTable(
            [
              { header: "Node" },
              { header: "Oplog Window" },
              { header: "Entries", align: "right" },
              { header: "Size", align: "right" },
            ],
            entries.map(([nodeId, manifest]) =>
              manifest?.oplogWindow
                ? [
                    nodeId,
                    `${formatOplogTimestamp(manifest.oplogWindow.startTs)} -> ${formatOplogTimestamp(manifest.oplogWindow.endTs)}`,
                    String(manifest.oplogWindow.entries ?? 0),
                    formatBytes(manifest.size),
                  ]
                : [nodeId, style.dim("no new entries"), "-", "-"],
            ),
            { indent: 1 },
          );
          console.log(
            `\n${renderBanner("PITR Oplog Slice")}\n\n${renderPanel(tbl.split("\n"), {
              title: "Sharded Oplog Slices Captured",
              tone: "success",
            })}`,
          );
        }
        return;
      }

      const manifest = await captureOplogSlice({
        config,
        storage,
        logger,
        signal: lock.controller.signal,
      });
      spinner.stop();
      if (values.json) {
        console.log(JSON.stringify(manifest, null, 2));
      } else if (manifest) {
        const rows: KeyValueRow[] = [
          { label: "Key", value: manifest.key },
          {
            label: "Range",
            value: `${formatOplogTimestamp(manifest.oplogWindow!.startTs)} -> ${formatOplogTimestamp(manifest.oplogWindow!.endTs)}`,
            tone: "info",
          },
          { label: "Entries", value: String(manifest.oplogWindow!.entries ?? 0) },
          { label: "Size", value: formatBytes(manifest.size) },
        ];
        if (manifest.oplogWindow!.gapDetected) {
          rows.push({
            label: "Warning",
            value: badge("Oplog gap detected prior to this slice; take a new base backup.", "warn"),
          });
        }
        console.log(
          `\n${renderBanner("PITR Oplog Slice")}\n\n${renderPanel(
            renderKeyValue(rows, { indent: 1 }),
            {
              title: "Oplog Slice Captured Successfully",
              tone: manifest.oplogWindow!.gapDetected ? "warn" : "success",
            },
          )}`,
        );
      } else {
        console.log(
          `\n${renderBanner("PITR Oplog Slice")}\n\n${renderPanel(
            [`  ${style.dim("No new oplog entries since last checkpoint.")}`],
            { title: "PITR Slice Up-to-Date", tone: "muted" },
          )}`,
        );
      }
    } catch (error) {
      spinner.fail("Oplog slice capture failed");
      logger.error({ err: error }, "Oplog slice capture failed");
      failed = true;
    } finally {
      disposeSignals();
      await lock.release();
    }
    if (failed) process.exit(1);
  }
}

export class OplogTailCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, notifier, metrics } = ctx;
    const archiver = startOplogArchiver({ config, storage, logger, notifier, metrics });
    archiver.start();
    const keepalive = setInterval(() => {}, 60_000);
    let shutdownPromise: Promise<void> | null = null;
    const shutdown = (signal: string) => {
      if (shutdownPromise) return shutdownPromise;
      shutdownPromise = (async () => {
        logger.info({ signal }, "Stopping continuous oplog archiver...");
        clearInterval(keepalive);
        try {
          await archiver.stop();
        } catch (err) {
          logger.error({ err }, "Error while stopping oplog archiver");
        }
        process.exit(0);
      })();
      return shutdownPromise;
    };
    process.on("SIGINT", () => void shutdown("SIGINT"));
    process.on("SIGTERM", () => void shutdown("SIGTERM"));
  }
}

export class OplogListCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, values } = ctx;
    if (config.sharding?.enabled) {
      const basePrefix = config.storage.prefix ? `${config.storage.prefix}/` : "";
      const shardRoot = `${basePrefix}shards/`;
      const shardIds = new Set<string>();
      for await (const obj of storage.list(shardRoot)) {
        const rel = obj.key.slice(shardRoot.length);
        const shardId = rel.split("/")[0];
        if (shardId) {
          shardIds.add(shardId);
        }
      }
      const perNode: Record<string, Awaited<ReturnType<typeof listOplogSlices>>> = {
        configsvr: await listOplogSlices(storage, {
          prefix: `${basePrefix}configsvr`,
          database: "*",
        }),
      };
      for (const shardId of [...shardIds].sort()) {
        perNode[shardId] = await listOplogSlices(storage, {
          prefix: `${basePrefix}shards/${shardId}`,
          database: "*",
        });
      }
      if (values.json) {
        console.log(JSON.stringify(perNode, null, 2));
        return;
      }
      const entries = Object.entries(perNode);
      if (entries.length === 0 || entries.every(([, list]) => list.length === 0)) {
        console.log(
          `\n${renderBanner("PITR Oplog Slices")}\n\n${renderPanel(
            [`  ${style.dim("No sharded PITR oplog slices found in target prefix.")}`],
            { title: "Sharded Oplog Slices", tone: "muted" },
          )}`,
        );
        return;
      }
      const sections: string[] = [renderBanner("Sharded PITR Oplog Slices")];
      for (const [shardId, slices] of entries) {
        const tbl =
          slices.length > 0
            ? renderTable(
                [
                  { header: "Key", maxWidth: 58 },
                  { header: "Start TS" },
                  { header: "End TS" },
                  { header: "Size", align: "right" },
                ],
                slices.map((s) => [
                  s.key,
                  formatOplogTimestamp(s.startTs),
                  formatOplogTimestamp(s.endTs),
                  formatBytes(s.size),
                ]),
                { indent: 1 },
              ).split("\n")
            : [`  ${style.dim("No slices archived")}`];
        sections.push(
          renderPanel(tbl, {
            title: shardId,
            subtitle: `${slices.length} slice(s)`,
            tone: slices.length > 0 ? "info" : "muted",
          }),
        );
      }
      console.log(`\n${sections.join("\n\n")}`);
      return;
    }

    const slices = await listOplogSlices(storage, scopeOf(config));
    if (values.json) {
      console.log(JSON.stringify(slices, null, 2));
      return;
    }
    if (slices.length === 0) {
      console.log(
        `\n${renderBanner("PITR Oplog Slices")}\n\n${renderPanel(
          [`  ${style.dim("No PITR oplog slices found in target prefix.")}`],
          { title: "PITR Oplog Timeline", tone: "muted" },
        )}`,
      );
      return;
    }

    const totalSize = slices.reduce((sum, s) => sum + (s.size || 0), 0);
    const tbl = renderTable(
      [
        { header: "Key", maxWidth: 58 },
        { header: "Start TS" },
        { header: "End TS" },
        { header: "Size", align: "right" },
      ],
      slices.map((s) => [
        s.key,
        formatOplogTimestamp(s.startTs),
        formatOplogTimestamp(s.endTs),
        formatBytes(s.size),
      ]),
      { indent: 1 },
    );
    const lines = [
      ...tbl.split("\n"),
      "",
      ` ${style.dim(`Total: ${slices.length} slice(s) · ${formatBytes(totalSize)}`)}`,
    ];
    console.log(
      `\n${renderBanner("PITR Oplog Slices")}\n\n${renderPanel(lines, {
        title: "PITR Oplog Timeline",
        subtitle: `${slices.length} slice(s)`,
        tone: "info",
      })}`,
    );
  }
}

export class OplogPruneCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    if (!values["older-than"]) {
      console.error(
        renderErrorBox(
          "Missing Required Flag",
          new Error("--older-than <timestamp|iso> is required for oplog:prune"),
        ),
      );
      process.exit(1);
    }
    const lock = values["dry-run"]
      ? null
      : await acquireCommandLock(ctx, "oplog", "Oplog Prune Lock Busy");
    const disposeSignals = lock ? abortOnTerminationSignal(lock.controller) : () => {};
    let failed = false;
    try {
      const result = await pruneOplogSlices(
        { config, storage, logger },
        { olderThan: values["older-than"], dryRun: values["dry-run"] },
      );
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        const rows: KeyValueRow[] = [
          {
            label: "Mode",
            value: values["dry-run"] ? badge("DRY-RUN", "warn") : badge("APPLIED", "success"),
          },
          { label: "Cutoff TS", value: String(result.cutoffTs), tone: "info" },
          { label: "Deleted", value: `${result.deletedSlices.length} slice(s)` },
        ];
        console.log(
          `\n${renderBanner("PITR Oplog Prune")}\n\n${renderPanel(
            renderKeyValue(rows, { indent: 1 }),
            {
              title: "Oplog Prune Completed",
              tone: values["dry-run"] ? "warn" : "success",
            },
          )}`,
        );
      }
    } catch (error) {
      logger.error({ err: error }, "Oplog prune failed");
      failed = true;
    } finally {
      disposeSignals();
      await lock?.release();
    }
    if (failed) process.exit(1);
  }
}
