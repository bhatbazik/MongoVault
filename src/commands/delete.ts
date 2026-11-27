import { acquireCommandLock } from "./command-lock.js";
import { deleteBackup } from "../retention.js";
import { badge, renderBanner, renderErrorBox, renderPanel } from "../tui.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class DeleteCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    if (!values.key) {
      console.error(
        renderErrorBox("Missing Required Flag", new Error("--key <string> is required for delete")),
      );
      process.exit(1);
    }
    const lock = values["dry-run"]
      ? null
      : await acquireCommandLock(ctx, "backup", "Delete Lock Busy");
    let failed = false;
    try {
      const result = await deleteBackup(
        { config, storage, logger },
        { key: values.key, force: values.force, dryRun: values["dry-run"] },
      );
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        console.log(
          `\n${renderBanner("Delete Backup")}\n\n${renderPanel(
            [
              `  ${badge(result.message ?? "Backup deleted", values["dry-run"] ? "warn" : "success")}`,
            ],
            {
              title: values["dry-run"] ? "Delete Simulation (Dry Run)" : "Backup Deleted",
              tone: values["dry-run"] ? "warn" : "success",
            },
          )}`,
        );
      }
    } catch (error) {
      logger.error({ err: error }, "Delete failed");
      failed = true;
    } finally {
      await lock?.release();
    }
    if (failed) process.exit(1);
  }
}
