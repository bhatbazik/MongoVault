import nodemailer, { type Transporter } from "nodemailer";
import { formatBytes, formatDuration } from "./format.js";
import type { BackupManifest } from "./types/catalog.js";
import type { AppConfig, SmtpConfig } from "./types/config.js";
import type { Logger } from "./types/logger.js";
import type { EmailOptions, NotificationFailureContext, Notifier } from "./types/notify.js";

/**
 * Strategy interface for notification delivery channels.
 */
export interface NotificationChannel {
  readonly name: string;
  notifySuccess(event: string, data: BackupManifest | Record<string, unknown>): Promise<void>;
  notifyFailure(event: string, error: Error, context?: NotificationFailureContext): Promise<void>;
}

/**
 * Encapsulates notification message formatting and payload generation (SRP).
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
      `Error: ${error.message}`,
      error.stack ? `\nStack:\n${error.stack}` : "",
    ].join("\n");
  }
}

/**
 * Channel for Dead Man's Snitch / Heartbeat ping endpoints.
 */
export class HeartbeatChannel implements NotificationChannel {
  readonly name = "heartbeat";

  constructor(
    private readonly heartbeatUrl?: string | null,
    private readonly drillHeartbeatUrl?: string | null,
    private readonly logger?: Logger,
  ) {}

  private resolveUrl(event: string): string | null {
    if (event === "drill") {
      return this.drillHeartbeatUrl || this.heartbeatUrl || null;
    }
    return this.heartbeatUrl || null;
  }

  async notifySuccess(
    event: string,
    _data?: BackupManifest | Record<string, unknown>,
  ): Promise<void> {
    const url = this.resolveUrl(event);
    if (!url) return;
    try {
      await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "text/plain" },
        body: `${event} succeeded`,
      });
    } catch (error) {
      this.logger?.error({ err: error, url }, "Failed to ping heartbeat endpoint");
    }
  }

  async notifyFailure(
    event: string,
    error: Error,
    _context?: NotificationFailureContext,
  ): Promise<void> {
    const url = this.resolveUrl(event);
    if (!url) return;
    try {
      const target = `${url.replace(/\/+$/, "")}/fail`;
      await fetch(target, {
        method: "POST",
        headers: { "Content-Type": "text/plain" },
        body: error.message,
      });
    } catch (err) {
      this.logger?.error({ err, url }, "Failed to ping heartbeat endpoint");
    }
  }
}

/**
 * Channel for SMTP email delivery via nodemailer.
 */
export class EmailNotificationChannel implements NotificationChannel {
  readonly name = "email";
  private readonly mailer: Transporter | null = null;

  constructor(
    private readonly emailConfig: SmtpConfig | null,
    private readonly messageBuilder: NotificationMessageBuilder,
    private readonly logger?: Logger,
  ) {
    if (emailConfig) {
      this.mailer = nodemailer.createTransport({
        host: emailConfig.host,
        port: emailConfig.port,
        secure: emailConfig.secure,
        auth:
          emailConfig.user && emailConfig.password
            ? { user: emailConfig.user, pass: emailConfig.password }
            : undefined,
      });
    }
  }

  async sendEmail({ subject, text }: EmailOptions): Promise<void> {
    if (!this.mailer || !this.emailConfig) return;
    try {
      await this.mailer.sendMail({
        from: this.emailConfig.from,
        to: this.emailConfig.to.join(", "),
        subject,
        text,
      });
    } catch (error) {
      this.logger?.error({ err: error }, "Failed to send email notification");
    }
  }

  async notifySuccess(
    event: string,
    data: BackupManifest | Record<string, unknown>,
  ): Promise<void> {
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
 * Channel for Slack incoming webhook notifications.
 */
export class SlackNotificationChannel implements NotificationChannel {
  readonly name = "slack";

  constructor(
    private readonly webhookUrl: string,
    private readonly messageBuilder: NotificationMessageBuilder,
    private readonly logger?: Logger,
  ) {}

  private async postJson(payload: unknown): Promise<void> {
    try {
      const response = await fetch(this.webhookUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        this.logger?.warn(
          { status: response.status, url: this.webhookUrl },
          "Slack webhook returned non-2xx status",
        );
      }
    } catch (error) {
      this.logger?.error(
        { err: error, url: this.webhookUrl },
        "Failed to deliver Slack notification",
      );
    }
  }

  async notifySuccess(
    event: string,
    data: BackupManifest | Record<string, unknown>,
  ): Promise<void> {
    const manifest = data as Partial<BackupManifest>;
    const title = this.messageBuilder.buildSuccessTitle(manifest.database);
    const summary = this.messageBuilder.buildSuccessSummary(event, manifest);
    await this.postJson({ text: `*${title}*\n\`\`\`${summary}\`\`\`` });
  }

  async notifyFailure(
    event: string,
    error: Error,
    context: NotificationFailureContext = {},
  ): Promise<void> {
    const title = this.messageBuilder.buildFailureTitle(context.database);
    const summary = this.messageBuilder.buildFailureSummary(event, error, context.database);
    await this.postJson({ text: `*${title}*\n\`\`\`${summary}\`\`\`` });
  }
}

/**
 * Channel for generic HTTP webhook notifications.
 */
export class WebhookNotificationChannel implements NotificationChannel {
  readonly name = "webhook";

  constructor(
    private readonly webhookUrl: string,
    private readonly logger?: Logger,
  ) {}

  private async postJson(payload: unknown): Promise<void> {
    try {
      const response = await fetch(this.webhookUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        this.logger?.warn(
          { status: response.status, url: this.webhookUrl },
          "Webhook notification returned non-2xx status",
        );
      }
    } catch (error) {
      this.logger?.error(
        { err: error, url: this.webhookUrl },
        "Failed to deliver webhook notification",
      );
    }
  }

  async notifySuccess(
    event: string,
    data: BackupManifest | Record<string, unknown>,
  ): Promise<void> {
    await this.postJson({ event, status: "success", data });
  }

  async notifyFailure(
    event: string,
    error: Error,
    context: NotificationFailureContext = {},
  ): Promise<void> {
    await this.postJson({
      event,
      status: "failure",
      error: error.message,
      context,
    });
  }
}

/**
 * Composite notifier that coordinates dispatch to all configured channels (Composite Pattern).
 */
export class CompositeNotifier implements Notifier {
  private readonly channels: NotificationChannel[] = [];
  private readonly heartbeatChannel: HeartbeatChannel | null = null;
  private readonly notifyOnSuccess: boolean;

  constructor(config: AppConfig, logger: Logger) {
    const { notifications, mongo } = config;
    this.notifyOnSuccess = notifications.onSuccess;
    const messageBuilder = new NotificationMessageBuilder(mongo.database);

    // Heartbeat channel operates independently of onSuccess (uptime monitoring)
    if (notifications.heartbeatUrl || notifications.drillHeartbeatUrl) {
      this.heartbeatChannel = new HeartbeatChannel(
        notifications.heartbeatUrl,
        notifications.drillHeartbeatUrl,
        logger,
      );
    }

    if (notifications.email) {
      this.channels.push(new EmailNotificationChannel(notifications.email, messageBuilder, logger));
    }

    if (notifications.slackWebhookUrl) {
      this.channels.push(
        new SlackNotificationChannel(notifications.slackWebhookUrl, messageBuilder, logger),
      );
    }

    if (notifications.webhookUrl) {
      this.channels.push(new WebhookNotificationChannel(notifications.webhookUrl, logger));
    }
  }

  async notifySuccess(
    event: string,
    data: BackupManifest | Record<string, unknown>,
  ): Promise<void> {
    // Always ping heartbeat if configured
    if (this.heartbeatChannel) {
      await this.heartbeatChannel.notifySuccess(event, data);
    }

    if (!this.notifyOnSuccess) return;

    await Promise.allSettled(this.channels.map((channel) => channel.notifySuccess(event, data)));
  }

  async notifyFailure(
    event: string,
    error: Error,
    context: NotificationFailureContext = {},
  ): Promise<void> {
    if (this.heartbeatChannel) {
      await this.heartbeatChannel.notifyFailure(event, error, context);
    }

    await Promise.allSettled(
      this.channels.map((channel) => channel.notifyFailure(event, error, context)),
    );
  }
}

/**
 * Factory function creating a Notifier configured from AppConfig.
 */
export function createNotifier(config: AppConfig, logger: Logger): Notifier {
  return new CompositeNotifier(config, logger);
}
