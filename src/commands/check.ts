import { formatPreflightCheck, runPreflightCheck } from "../check.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class CheckCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    try {
      const result = await runPreflightCheck({ config, storage });
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        console.log("\n" + formatPreflightCheck(result));
      }
      if (!result.allPassed) {
        process.exit(1);
      }
    } catch (error) {
      logger.error({ err: error }, "Preflight check failed");
      process.exit(1);
    }
  }
}
