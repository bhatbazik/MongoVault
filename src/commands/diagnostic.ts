import fs from "node:fs/promises";
import { generateDiagnosticBundle } from "../diagnostic.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class DiagnosticCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    try {
      const bundle = await generateDiagnosticBundle({ config, storage });
      const jsonOutput = JSON.stringify(bundle, null, 2);
      if (values.output) {
        const tmpPath = `${values.output}.tmp-${process.pid}-${Date.now()}`;
        await fs.writeFile(tmpPath, jsonOutput, "utf8");
        await fs.rename(tmpPath, values.output);
        console.log(`Diagnostic bundle written to ${values.output}`);
      } else {
        console.log(jsonOutput);
      }
    } catch (error) {
      logger.error({ err: error }, "Failed to generate diagnostic bundle");
      process.exit(1);
    }
  }
}
