import { formatDate, formatDuration, formatBytes } from "../format.js";
import { describeBackup } from "../restore.js";
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
      } else if (manifest) {
        console.log("\nBackup Metadata Details");
        console.log("=======================");
        console.log(`Key:          ${manifest.key}`);
        console.log(`Engine:       ${(manifest.engine || "logical").toUpperCase()}`);
        console.log(`Database:     ${manifest.database}`);
        console.log(`Created At:   ${formatDate(manifest.createdAt)}`);
        console.log(`Completed At: ${formatDate(manifest.completedAt)}`);
        console.log(`Duration:     ${formatDuration(manifest.durationMs)}`);
        console.log(`Archive Size: ${formatBytes(manifest.size)} (${manifest.size} bytes)`);
        console.log(`SHA-256:      ${manifest.sha256}`);
        console.log(`Compression:  ${manifest.compression}`);
        if (manifest.encryption) {
          console.log(
            `Encryption:   ${manifest.encryption.algorithm} (Key ID: ${manifest.encryption.keyId})`,
          );
        } else {
          console.log("Encryption:   None (Unencrypted)");
        }
        if (manifest.oplogWindow) {
          console.log(
            `Oplog Window: ${manifest.oplogWindow.startTs} -> ${manifest.oplogWindow.endTs}`,
          );
        }
        if (manifest.physical) {
          console.log(
            `Physical:     ${manifest.physical.cursorType} (${manifest.physical.filesCount} files, dbPath: ${manifest.physical.dbPath})`,
          );
        }
        if (manifest.sharding) {
          console.log(
            `Sharding:     ${Object.keys(manifest.sharding.shards).length} shards (clusterTime: ${manifest.sharding.clusterTime})`,
          );
        }
        if (manifest.excludedCollections && manifest.excludedCollections.length > 0) {
          console.log(`Excluded:     ${manifest.excludedCollections.join(", ")}`);
        }
        if (manifest.collections && Object.keys(manifest.collections).length > 0) {
          console.log("\nCollections & Document Counts:");
          console.log("------------------------------");
          for (const [coll, count] of Object.entries(manifest.collections)) {
            console.log(`  • ${coll.padEnd(35)} : ${count.toLocaleString()} docs`);
          }
        }
      } else {
        console.log(`Backup found: ${key} (No .manifest.json available)`);
      }
    } catch (error) {
      logger.error({ err: error }, "Failed to describe backup");
      process.exit(1);
    }
  }
}
