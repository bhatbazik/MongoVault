import { listBackups, scopeOf } from "../catalog.js";
import { formatBytes, formatDate } from "../format.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class ListCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, values } = ctx;
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
  }
}
