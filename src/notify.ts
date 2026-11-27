import nodemailer, { type Transporter } from "nodemailer";
import { formatBytes, formatDuration } from "./format.js";
import type { BackupManifest } from "./types/catalog.js";
import type { AppConfig, SmtpConfig } from "./types/config.js";
import type { Logger } from "./types/logger.js";
import type { EmailOptions, NotificationFailureContext, Notifier } from "./types/notify.js";

const SMTP_TIMEOUT_MS = 15_000;

function sanitizeSecrets(text: string): string {
  return text.replace(/mongodb(?:\+srv)?:\/\/([^:@\s]+):([^@\s]+)@/gi, "mongodb://$1:***@");
}

/**
 * Formats notification email subjects and body summaries.
 */
export class NotificationMessageBuilder {
  constructor(private readonly defaultDatabase: string) {}

  buildSuccessTitle(database?: string): string {
    return `MongoDB Backup Successful: ${database ?? this.defaultDatabase}`;
  }

  buildSuccessSummary(event: string, data: Partial<BackupManifest>): string {
    return [
      `Event: ${event}`,
      `Database: ${data.database ?? this.defaultDatabase}`,
      `Archive: ${data.key ?? "N/A"}`,
      `Size: ${data.size !== undefined ? formatBytes(data.size) : "N/A"}`,
      `Duration: ${data.durationMs !== undefined ? formatDuration(data.durationMs) : "N/A"}`,
      `Collections: ${Object.keys(data.collections || {}).length}`,
      `SHA-256: ${data.sha256 ?? "N/A"}`,
      `Encrypted: ${Boolean(data.encryption)}`,
    ].join("\n");
  }

  buildFailureTitle(database?: string): string {
    return `MongoDB Backup Failed: ${database || this.defaultDatabase}`;
  }

  buildFailureSummary(event: string, error: Error, database?: string): string {
    return [
      `Event: ${event}`,
      `Database: ${database || this.defaultDatabase}`,
      `Error: ${sanitizeSecrets(error.message)}`,
      error.stack ? `\nStack:\n${sanitizeSecrets(error.stack)}` : "",
    ].join("\n");
  }
}

/**
 * Email notifier using Nodemailer SMTP transport.
 */
export class EmailNotifier implements Notifier {
  private readonly mailer: Transporter | null = null;
  private readonly emailConfig: SmtpConfig | null;
  private readonly notifyOnSuccess: boolean;
  private readonly messageBuilder: NotificationMessageBuilder;

  constructor(
    config: AppConfig,
    private readonly logger?: Logger,
  ) {
    this.emailConfig = config.notifications.email;
    this.notifyOnSuccess = config.notifications.onSuccess;
    this.messageBuilder = new NotificationMessageBuilder(config.mongo.database);

    if (this.emailConfig) {
      this.mailer = nodemailer.createTransport({
        host: this.emailConfig.host,
        port: this.emailConfig.port,
        secure: this.emailConfig.secure,
        connectionTimeout: SMTP_TIMEOUT_MS,
        greetingTimeout: SMTP_TIMEOUT_MS,
        socketTimeout: SMTP_TIMEOUT_MS * 2,
        auth:
          this.emailConfig.user && this.emailConfig.password
            ? { user: this.emailConfig.user, pass: this.emailConfig.password }
            : undefined,
      });
    }
  }

  async sendEmail({ subject, text }: EmailOptions): Promise<void> {
    if (!this.mailer || !this.emailConfig) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.mailer.sendMail({
          from: this.emailConfig.from,
          to: this.emailConfig.to.join(", "),
          subject,
          text,
        }),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`SMTP sendMail timed out after ${SMTP_TIMEOUT_MS * 2}ms`)),
            SMTP_TIMEOUT_MS * 2,
          );
          timer.unref?.();
        }),
      ]);
    } catch (error) {
      this.logger?.error({ err: error }, "Failed to send email notification");
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  close(): void {
    this.mailer?.close?.();
  }

  async notifySuccess(
    event: string,
    data: BackupManifest | Record<string, unknown>,
  ): Promise<void> {
    if (!this.notifyOnSuccess) return;
    const manifest = data as Partial<BackupManifest>;
    const subject = this.messageBuilder.buildSuccessTitle(manifest.database);
    const text = this.messageBuilder.buildSuccessSummary(event, manifest);
    await this.sendEmail({ subject, text });
  }

  async notifyFailure(
    event: string,
    error: Error,
    context: NotificationFailureContext = {},
  ): Promise<void> {
    const subject = this.messageBuilder.buildFailureTitle(context.database);
    const text = this.messageBuilder.buildFailureSummary(event, error, context.database);
    await this.sendEmail({ subject, text });
  }
}

/**
 * Factory function creating a Nodemailer email Notifier configured from AppConfig.
 */
export function createNotifier(config: AppConfig, logger: Logger): Notifier {
  return new EmailNotifier(config, logger);
}
