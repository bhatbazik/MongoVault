import { runDrill } from "../drill.js";
import { formatDuration } from "../format.js";
import { abortOnTerminationSignal } from "./command-lock.js";
import {
  badge,
  createSpinner,
  renderBanner,
  renderKeyValue,
  renderPanel,
  type KeyValueRow,
} from "../tui.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class DrillCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    const spinner = createSpinner(
      values.pitr
        ? `Running PITR restore drill (${values.pitr})`
        : "Running automated restore drill",
      { enabled: !values.json && Boolean(process.stderr.isTTY) },
    );

    const controller = new AbortController();
    const disposeSignals = abortOnTerminationSignal(controller);
    let failed = false;
    try {
      const result = await runDrill(
        { config, storage, logger, signal: controller.signal },
        values.pitr ? { pitr: values.pitr } : undefined,
      );
      spinner.succeed("Restore drill passed");

      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      const rows: KeyValueRow[] = [
        { label: "Key", value: result.key },
        ...(result.pitrTarget
          ? [
              {
                label: "PITR Target",
                value: `${result.pitrTarget} (${result.replayedSlices ?? 0} slices)`,
                tone: "info" as const,
              },
            ]
          : []),
        { label: "Collections", value: String(result.collections) },
        { label: "Documents", value: String(result.documents) },
        { label: "Indexes", value: badge(`${result.indexesVerified ?? 0} verified`, "success") },
        {
          label: "Validators",
          value: badge(`${result.validatorsVerified ?? 0} verified`, "success"),
        },
        { label: "Views", value: badge(`${result.viewsVerified ?? 0} verified`, "success") },
        {
          label: "Read/Write",
          value: result.readWriteVerified
            ? badge("verified", "success")
            : badge("skipped", "muted"),
        },
        { label: "Duration", value: formatDuration(result.durationMs) },
      ];
      console.log(
        `\n${renderBanner("Restore Drill")}\n\n${renderPanel(renderKeyValue(rows, { indent: 1 }), {
          title: "Restore Drill Passed Successfully",
          tone: "success",
        })}`,
      );
    } catch (error) {
      spinner.fail("Restore drill failed");
      logger.error({ err: error }, "Restore drill failed");
      failed = true;
    } finally {
      disposeSignals();
    }
    if (failed) process.exit(1);
  }
}
