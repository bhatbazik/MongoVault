import { EngineRegistry } from "../engines.js";
import { formatBytes } from "../format.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class RestoreCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;

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
        oplogReplay: values["oplog-replay"],
        oplogLimit: values["oplog-limit"],
        pitr: values.pitr,
        parallelCollections: values.parallel ? Number(values.parallel) : undefined,
      })) as Record<string, unknown>;

      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        if (engine.name === "physical") {
          console.log("\nPhysical Restore Completed Successfully");
          console.log(`Key:        ${result.key}`);
          console.log(`Target:     ${result.targetDbPath}`);
          console.log(
            `Files:      ${result.restoredFiles} restored (${formatBytes(result.totalBytes as number)})`,
          );
        } else if (engine.name === "snapshot") {
          console.log("\nCloud Snapshot Restored Successfully");
          console.log(`Snapshot ID:  ${result.snapshotId}`);
          console.log(`Volume ID:    ${result.volumeId}`);
          console.log(`New Volume:   ${result.newVolumeId || "n/a"}`);
        } else if (engine.name === "sharded") {
          console.log("\nSharded Cluster Restored Successfully");
          console.log(`Key:          ${result.key}`);
          console.log(`Cluster Time: ${result.clusterTime}`);
          console.log(`Shards:       ${(result.restoredShards as string[]).join(", ")}`);
        } else {
          console.log("\nRestore Completed Successfully");
          console.log(`Key:        ${result.key}`);
          console.log(`Target DB:  ${result.target}`);
          console.log(`Restored:   ${result.restored} documents`);
          if (result.pitrTarget) {
            console.log(`PITR Limit: ${result.pitrTarget}`);
            console.log(
              `Slices:     ${result.replayedSlices} replayed (${result.replayedEntries} entries)`,
            );
          }
        }
      }
    } catch (error) {
      logger.error({ err: error }, "Restore execution failed");
      process.exit(1);
    }
  }
}
