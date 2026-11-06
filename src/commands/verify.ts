import { formatBytes } from "../format.js";
import { verifyBackup } from "../restore.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class VerifyCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    try {
      const result = await verifyBackup({ config, storage, logger }, values.key || "latest");
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        console.log("\nBackup Verification Passed");
        console.log(`Key:        ${result.key}`);
        console.log(`Size:       ${formatBytes(result.size)}`);
        console.log(`SHA-256:    ${result.sha256}`);
        console.log(`Encrypted:  ${result.encrypted}`);
      }
    } catch (error) {
      logger.error({ err: error }, "Verification failed");
      process.exit(1);
    }
  }
}
