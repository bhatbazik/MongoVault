import { prune } from "../retention.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class PruneCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    try {
      const result = await prune({ config, storage, logger }, { dryRun: values["dry-run"] });
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        console.log(`Retention prune completed (${values["dry-run"] ? "dry-run" : "applied"}):`);
        console.log(`Kept:    ${result.keep.length}`);
        console.log(`Removed: ${result.remove.length}`);
        for (const item of result.remove) {
          console.log(`  - ${item.key}`);
        }
      }
    } catch (error) {
      logger.error({ err: error }, "Prune failed");
      process.exit(1);
    }
  }
}
