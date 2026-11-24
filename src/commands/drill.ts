import { runDrill } from "../drill.js";
import { formatDuration } from "../format.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class DrillCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    try {
      const result = await runDrill(
        { config, storage, logger },
        values.pitr ? { pitr: values.pitr } : undefined,
      );
      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        console.log("\nRestore Drill Passed Successfully");
        console.log(`Key:         ${result.key}`);
        if (result.pitrTarget) {
          console.log(`PITR Target: ${result.pitrTarget} (${result.replayedSlices ?? 0} slices)`);
        }
        console.log(`Collections: ${result.collections}`);
        console.log(`Documents:   ${result.documents}`);
        console.log(`Indexes:     ${result.indexesVerified} verified`);
        console.log(`Validators:  ${result.validatorsVerified} verified`);
        console.log(`Views:       ${result.viewsVerified} verified`);
        console.log(`Read/Write:  ${result.readWriteVerified ? "verified" : "skipped"}`);
        console.log(`Duration:    ${formatDuration(result.durationMs)}`);
      }
    } catch (error) {
      logger.error({ err: error }, "Restore drill failed");
      process.exit(1);
    }
  }
}
