import { beforeEach, describe, expect, it, vi } from "vitest";

const sendMailMock = vi.fn(async () => {});
vi.mock("nodemailer", () => ({
  default: {
    createTransport: vi.fn(() => ({
      sendMail: sendMailMock,
    })),
  },
}));

import { createNotifier, EmailNotifier, NotificationMessageBuilder } from "../src/notify.js";

function noopLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
}

describe("Nodemailer Email Notifications (notify.ts)", () => {
  beforeEach(() => {
    sendMailMock.mockClear();
  });

  it("NotificationMessageBuilder formats success and failure messages", () => {
    const builder = new NotificationMessageBuilder("mydb");
    expect(builder.buildSuccessTitle()).toContain("mydb");
    expect(builder.buildSuccessTitle("other")).toContain("other");
    const summary = builder.buildSuccessSummary("backup", {
      key: "k.gz",
      size: 2048,
      durationMs: 3000,
      collections: { users: 10 },
      sha256: "abc",
      encryption: { algo: "aes" },
    });
    expect(summary).toContain("Event: backup");
    expect(summary).toContain("Archive: k.gz");
    expect(summary).toContain("Encrypted: true");

    expect(builder.buildFailureTitle()).toContain("mydb");
    const failSummary = builder.buildFailureSummary("backup", new Error("disk full"));
    expect(failSummary).toContain("Error: disk full");
  });

  it("EmailNotifier sends via nodemailer, respects onSuccess, and logs on SMTP error", async () => {
    const logger = noopLogger();
    const baseConfig = {
      mongo: { database: "app" },
      notifications: {
        onSuccess: true,
        email: {
          host: "smtp.test",
          port: 587,
          secure: false,
          user: "u",
          password: "p",
          from: "a@test.com",
          to: ["b@test.com", "c@test.com"],
        },
      },
    };

    const notifier = createNotifier(baseConfig, logger);
    expect(notifier).toBeInstanceOf(EmailNotifier);

    await notifier.notifySuccess("backup", { database: "app", key: "k1.gz" });
    await notifier.notifyFailure("backup", new Error("boom"), { database: "app" });
    expect(sendMailMock).toHaveBeenCalledTimes(2);

    sendMailMock.mockRejectedValueOnce(new Error("smtp error"));
    await notifier.notifySuccess("backup", { database: "app" });
    expect(logger.error).toHaveBeenCalled();

    // When onSuccess is false, success notifications are skipped while failures still send
    const failureOnly = createNotifier(
      { ...baseConfig, notifications: { ...baseConfig.notifications, onSuccess: false } },
      logger,
    );
    await failureOnly.notifySuccess("backup", { database: "app" });
    expect(sendMailMock).toHaveBeenCalledTimes(3);
    await failureOnly.notifyFailure("backup", new Error("crash"));
    expect(sendMailMock).toHaveBeenCalledTimes(4);

    // When email is null, sendEmail is a clean no-op
    const disabled = createNotifier(
      { mongo: { database: "app" }, notifications: { onSuccess: true, email: null } },
      logger,
    );
    await disabled.notifySuccess("backup", { database: "app" });
    await disabled.notifyFailure("backup", new Error("ignored"));
    expect(sendMailMock).toHaveBeenCalledTimes(4);
  });
});
