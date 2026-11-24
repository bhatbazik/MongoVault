import { formatBytes, formatDate, formatDuration } from "../format.js";
import { describeBackup } from "../restore.js";
import {
  badge,
  renderBanner,
  renderBar,
  renderKeyValue,
  renderPanel,
  renderTable,
  style,
  type KeyValueRow,
} from "../tui.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class DescribeCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    try {
      const { key, manifest, checksum } = await describeBackup(
        { config, storage, logger },
        values.key || "latest",
      );
      if (values.json) {
        console.log(JSON.stringify({ key, checksum, manifest }, null, 2));
        return;
      }

      if (!manifest) {
        console.log(
          `\n${renderBanner("Describe Backup")}\n\n${renderPanel(
            [`  ${badge(`Backup found: ${key} (No .manifest.json available)`, "warn")}`],
            { title: "Backup Metadata", tone: "warn" },
          )}`,
        );
        return;
      }

      const rows: KeyValueRow[] = [
        { label: "Key", value: manifest.key },
        { label: "Engine", value: (manifest.engine || "logical").toUpperCase(), tone: "info" },
        { label: "Database", value: manifest.database },
        { label: "Created At", value: formatDate(manifest.createdAt) },
        { label: "Completed At", value: formatDate(manifest.completedAt) },
        { label: "Duration", value: formatDuration(manifest.durationMs) },
        {
          label: "Archive Size",
          value: `${formatBytes(manifest.size)} ${style.dim(`(${manifest.size.toLocaleString()} bytes)`)}`,
        },
        { label: "SHA-256", value: manifest.sha256 },
        { label: "Compression", value: manifest.compression },
        {
          label: "Encryption",
          value: manifest.encryption
            ? badge(
                `${manifest.encryption.algorithm} (Key ID: ${manifest.encryption.keyId})`,
                "success",
              )
            : badge("None (Unencrypted)", "warn"),
        },
      ];

      if (manifest.oplogWindow) {
        rows.push({
          label: "Oplog Window",
          value: `${manifest.oplogWindow.startTs} -> ${manifest.oplogWindow.endTs}`,
          tone: "info",
        });
      }
      if (manifest.physical) {
        rows.push({
          label: "Physical",
          value: `${manifest.physical.cursorType} (${manifest.physical.filesCount} files, dbPath: ${manifest.physical.dbPath})`,
        });
      }
      if (manifest.sharding) {
        rows.push({
          label: "Sharding",
          value: `${Object.keys(manifest.sharding.shards).length} shards (clusterTime: ${manifest.sharding.clusterTime})`,
        });
      }
      if (manifest.excludedCollections && manifest.excludedCollections.length > 0) {
        rows.push({ label: "Excluded", value: manifest.excludedCollections.join(", ") });
      }
      if (manifest.schema) {
        const schemaColls = Object.values(manifest.schema.collections || {});
        const idxCount = schemaColls.reduce((sum, c) => sum + (c.indexes?.length ?? 0), 0);
        const valCount = schemaColls.filter((c) => Boolean(c.options?.validator)).length;
        const viewCount = manifest.schema.views?.length ?? 0;
        rows.push({
          label: "Schema Recorded",
          value: `${idxCount} indexes · ${valCount} validators · ${viewCount} views`,
          tone: "success",
        });
      }

      const sections = [
        renderBanner("Backup Metadata"),
        renderPanel(renderKeyValue(rows, { indent: 1 }), {
          title: "Backup Metadata Details",
          tone: "brand",
        }),
      ];

      const entries = Object.entries(manifest.collections || {});
      if (entries.length > 0) {
        const totalDocs = entries.reduce((sum, [, c]) => sum + c, 0);
        const sorted = [...entries].sort((a, b) => b[1] - a[1]);
        const tbl = renderTable(
          [
            { header: "Namespace", maxWidth: 44 },
            { header: "Documents", align: "right" },
            { header: "Share" },
          ],
          sorted.map(([coll, count]) => {
            const ratio = totalDocs > 0 ? count / totalDocs : 0;
            const pct = `${(ratio * 100).toFixed(1)}%`;
            return [coll, count.toLocaleString(), `${renderBar(ratio, 12)} ${style.dim(pct)}`];
          }),
          { indent: 1 },
        );
        sections.push(
          renderPanel(tbl.split("\n"), {
            title: "Collections & Document Counts",
            subtitle: `${entries.length} collections · ${totalDocs.toLocaleString()} docs`,
          }),
        );
      }

      console.log(`\n${sections.join("\n\n")}`);
    } catch (error) {
      logger.error({ err: error }, "Failed to describe backup");
      process.exit(1);
    }
  }
}
