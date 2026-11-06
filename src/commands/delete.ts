import { deleteBackup } from "../retention.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class DeleteCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    if (!values.key) {
      console.error("Error: --key <string> is required for delete command");
      process.exit(1);
    }
    try {
      const result = await deleteBackup(
        { config, storage, logger },
        { key: values.key, force: values.force, dryRun: values["dry-run"] },
      );
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        console.log(result.message);
      }
    } catch (error) {
      logger.error({ err: error }, "Delete failed");
      process.exit(1);
    }
  }
}
