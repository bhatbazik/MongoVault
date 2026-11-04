import nodemailer from "nodemailer";
import { formatBytes, formatDuration } from "./format.js";
import type { BackupManifest } from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type { Logger } from "./types/logger.js";
import type { EmailOptions, NotificationFailureContext, Notifier } from "./types/notify.js";

export function createNotifier(config: AppConfig, logger: Logger): Notifier {
  const { notifications } = config;

  const mailer = notifications.email
    ? nodemailer.createTransport({
        host: notifications.email.host,
        port: notifications.email.port,
        secure: notifications.email.secure,
        auth:
          notifications.email.user && notifications.email.password
            ? {
                user: notifications.email.user,
                pass: notifications.email.password,
              }
            : undefined,
      })
    : null;

  async function sendEmail({ subject, text }: EmailOptions): Promise<void> {
    if (!mailer || !notifications.email) return;
    try {
      await mailer.sendMail({
        from: notifications.email.from,
        to: notifications.email.to.join(", "),
        subject,
        text,
      });
    } catch (error) {
      logger.error({ err: error }, "Failed to send email notification");
    }
  }

  async function postJson(url: string, payload: unknown): Promise<void> {
    if (!url) return;
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        logger.warn(
          { status: response.status, url },
          "Webhook notification returned non-2xx status",
        );
      }
    } catch (error) {
      logger.error({ err: error, url }, "Failed to deliver webhook notification");
    }
  }

  async function pingHeartbeat(ok: boolean, message?: string, event?: string): Promise<void> {
    const url =
      event === "drill"
        ? notifications.drillHeartbeatUrl || notifications.heartbeatUrl
        : notifications.heartbeatUrl;
    if (!url) return;
    try {
      const target = ok ? url : `${url.replace(/\/+$/, "")}/fail`;
      await fetch(target, {
        method: "POST",
        headers: { "Content-Type": "text/plain" },
        body: message ?? (ok ? "ok" : "failed"),
      });
    } catch (error) {
      logger.error({ err: error }, "Failed to ping heartbeat endpoint");
    }
  }

  return {
    async notifySuccess(
      event: string,
      data: BackupManifest | Record<string, unknown>,
    ): Promise<void> {
      await pingHeartbeat(true, `${event} succeeded`, event);

      if (!notifications.onSuccess) return;

      const d = data as Partial<BackupManifest>;
      const title = `MongoDB Backup Successful: ${d.database ?? config.mongo.database}`;
      const summary = [
        `Event: ${event}`,
        `Database: ${d.database ?? config.mongo.database}`,
        `Archive: ${d.key ?? "N/A"}`,
        `Size: ${d.size !== undefined ? formatBytes(d.size) : "N/A"}`,
        `Duration: ${d.durationMs !== undefined ? formatDuration(d.durationMs) : "N/A"}`,
        `Collections: ${Object.keys(d.collections || {}).length}`,
        `SHA-256: ${d.sha256 ?? "N/A"}`,
        `Encrypted: ${Boolean(d.encryption)}`,
      ].join("\n");

      await Promise.allSettled([
        sendEmail({ subject: title, text: summary }),
        notifications.slackWebhookUrl
          ? postJson(notifications.slackWebhookUrl, {
              text: `*${title}*\n\`\`\`${summary}\`\`\``,
            })
          : Promise.resolve(),
        notifications.webhookUrl
          ? postJson(notifications.webhookUrl, {
              event,
              status: "success",
              data,
            })
          : Promise.resolve(),
      ]);
    },

    async notifyFailure(
      event: string,
      error: Error,
      context: NotificationFailureContext = {},
    ): Promise<void> {
      await pingHeartbeat(false, error.message, event);

      const title = `MongoDB Backup Failed: ${context.database || config.mongo.database}`;
      const summary = [
        `Event: ${event}`,
        `Database: ${context.database || config.mongo.database}`,
        `Error: ${error.message}`,
        error.stack ? `\nStack:\n${error.stack}` : "",
      ].join("\n");

      await Promise.allSettled([
        sendEmail({ subject: title, text: summary }),
        notifications.slackWebhookUrl
          ? postJson(notifications.slackWebhookUrl, {
              text: `*${title}*\n\`\`\`${summary}\`\`\``,
            })
          : Promise.resolve(),
        notifications.webhookUrl
          ? postJson(notifications.webhookUrl, {
              event,
              status: "failure",
              error: error.message,
              context,
            })
          : Promise.resolve(),
      ]);
    },
  };
}
