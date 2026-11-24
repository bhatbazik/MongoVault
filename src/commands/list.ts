import { listBackups, scopeOf } from "../catalog.js";
import { formatBytes, formatDate } from "../format.js";
import { badge, renderBanner, renderPanel, renderTable, style } from "../tui.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class ListCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, values } = ctx;
    const backups = await listBackups(storage, scopeOf(config));
    if (values.json) {
      console.log(JSON.stringify(backups, null, 2));
      return;
    }

    if (backups.length === 0) {
      console.log(
        `\n${renderBanner("Backups")}\n\n${renderPanel(
          [`  ${style.dim("No backups found in target prefix.")}`],
          { title: "Backup Catalog", tone: "muted" },
        )}`,
      );
      return;
    }

    const totalSize = backups.reduce((sum, b) => sum + (b.size || 0), 0);
    const tbl = renderTable(
      [
        { header: "Key", maxWidth: 56 },
        { header: "Created" },
        { header: "Size", align: "right" },
        { header: "Encryption" },
        { header: "Status" },
      ],
      backups.map((b) => [
        b.key,
        formatDate(b.createdAt, config.schedule.timezone),
        formatBytes(b.size),
        b.encrypted ? badge("AES-256", "success") : style.dim("none"),
        b.complete ? badge("complete", "success") : badge("unverified", "warn"),
      ]),
      { indent: 1 },
    );

    const lines = [
      ...tbl.split("\n"),
      "",
      ` ${style.dim(`Total: ${backups.length} backup(s) · ${formatBytes(totalSize)}`)}`,
    ];
    console.log(
      `\n${renderBanner("Backups")}\n\n${renderPanel(lines, {
        title: "Backup Catalog",
        subtitle: `${backups.length} archive(s)`,
        tone: "info",
      })}`,
    );
  }
}
