import { acquireCommandLock } from "./command-lock.js";
import { prune } from "../retention.js";
import {
  badge,
  renderBanner,
  renderKeyValue,
  renderPanel,
  style,
  type KeyValueRow,
} from "../tui.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class PruneCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    const lock = values["dry-run"]
      ? null
      : await acquireCommandLock(ctx, "backup", "Prune Lock Busy");
    let failed = false;
    try {
      const result = await prune({ config, storage, logger }, { dryRun: values["dry-run"] });
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      const mode = values["dry-run"] ? badge("DRY-RUN", "warn") : badge("APPLIED", "success");
      const rows: KeyValueRow[] = [
        { label: "Mode", value: mode },
        { label: "Kept", value: String(result.keep.length), tone: "success" },
        {
          label: "Removed",
          value: String(result.remove.length),
          tone: result.remove.length > 0 ? "warn" : "muted",
        },
      ];
      const lines = renderKeyValue(rows, { indent: 1 });
      if (result.remove.length > 0) {
        lines.push("", ` ${style.bold("Expired Backups:")}`);
        for (const item of result.remove) {
          lines.push(`   ${style.red("-")} ${item.key}`);
        }
      }
      console.log(
        `\n${renderBanner("Retention Prune")}\n\n${renderPanel(lines, {
          title: `Retention Prune Completed (${values["dry-run"] ? "dry-run" : "applied"})`,
          tone: values["dry-run"] ? "warn" : "success",
        })}`,
      );
    } catch (error) {
      logger.error({ err: error }, "Prune failed");
      failed = true;
    } finally {
      await lock?.release();
    }
    if (failed) process.exit(1);
  }
}
