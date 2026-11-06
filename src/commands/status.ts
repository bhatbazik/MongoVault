import { formatStatus, getStatus } from "../status.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class StatusCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    try {
      const status = await getStatus({ config, storage, logger });
      if (values.json) {
        console.log(JSON.stringify(status, null, 2));
      } else {
        console.log("\n" + formatStatus(status));
      }
    } catch (error) {
      logger.error({ err: error }, "Failed to retrieve system status");
      process.exit(1);
    }
  }
}
