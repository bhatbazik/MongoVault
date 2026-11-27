import { formatBytes } from "../format.js";
import { verifyBackup } from "../restore.js";
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

export class VerifyCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    const spinner = createSpinner(`Verifying archive integrity (${values.key || "latest"})`, {
      enabled: !values.json && Boolean(process.stderr.isTTY),
    });

    const controller = new AbortController();
    const disposeSignals = abortOnTerminationSignal(controller);
    let failed = false;
    try {
      const result = await verifyBackup(
        { config, storage, logger, signal: controller.signal },
        values.key || "latest",
      );
      spinner.succeed("Archive checksum & decryption verified");

      if (values.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      const rows: KeyValueRow[] = [
        { label: "Key", value: result.key },
        { label: "Size", value: formatBytes(result.size) },
        { label: "SHA-256", value: result.sha256 },
        {
          label: "Encrypted",
          value: result.encrypted ? badge("AES-256-GCM verified", "success") : "false",
        },
      ];
      console.log(
        `\n${renderBanner("Verify")}\n\n${renderPanel(renderKeyValue(rows, { indent: 1 }), {
          title: "Backup Verification Passed",
          tone: "success",
        })}`,
      );
    } catch (error) {
      spinner.fail("Verification failed");
      logger.error({ err: error }, "Verification failed");
      failed = true;
    } finally {
      disposeSignals();
    }
    if (failed) process.exit(1);
  }
}
