import os from "node:os";
import path from "node:path";
import cron from "node-cron";
import { z } from "zod";
import { parseKey } from "./crypto.js";

const list = z
  .string()
  .default("")
  .transform((value) =>
    value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean),
  );

const flag = (fallback) =>
  z
    .enum(["true", "false", "1", "0"])
    .default(fallback ? "true" : "false")
    .transform((value) => value === "true" || value === "1");

const count = z.coerce.number().int().nonnegative().optional();

const cronExpression = z
  .string()
  .refine((value) => cron.validate(value), { message: "Invalid cron expression" });

const timezone = z.string().refine(
  (value) => {
    try {
      new Intl.DateTimeFormat("en-US", { timeZone: value });
      return true;
    } catch {
      return false;
    }
  },
  { message: "Unknown time zone" },
);

const schema = z
  .object({
    MONGODB_URI: z.string().min(1),
    MONGODB_DATABASE: z.string().default("*"),
    MONGODB_EXCLUDE_COLLECTIONS: list,
    MONGODB_OPLOG: flag(false),
    MONGODB_PARALLEL_COLLECTIONS: z.coerce.number().int().min(1).max(32).default(2),

    S3_BUCKET: z.string().min(1),
    S3_ACCESS_KEY_ID: z.string().min(1),
    S3_SECRET_ACCESS_KEY: z.string().min(1),
    S3_ENDPOINT: z.string().url().optional(),
    S3_REGION: z.string().optional(),
    S3_FORCE_PATH_STYLE: flag(false),
    S3_PREFIX: z.string().default("mongodb"),
    S3_PART_SIZE_MB: z.coerce.number().int().min(5).max(512).default(32),
    S3_QUEUE_SIZE: z.coerce.number().int().min(1).max(16).default(2),
    R2_ACCOUNT_ID: z.string().optional(),

    BACKUP_CRON: cronExpression.default("0 2 * * *"),
    BACKUP_TIMEZONE: timezone.default("UTC"),
    BACKUP_TIMEOUT_MINUTES: z.coerce.number().positive().default(180),
    BACKUP_ON_START: flag(false),
    WORK_DIR: z.string().default(path.join(os.tmpdir(), "mongodb-backup")),

    ENCRYPTION_KEY: z.string().optional(),

    RETENTION_KEEP_LAST: count,
    RETENTION_KEEP_DAILY: count,
    RETENTION_KEEP_WEEKLY: count,
    RETENTION_KEEP_MONTHLY: count,

    DRILL_MONGODB_URI: z.string().optional(),
    DRILL_CRON: cronExpression.optional(),

    SMTP_HOST: z.string().optional(),
    SMTP_PORT: z.coerce.number().int().positive().default(587),
    SMTP_SECURE: flag(false),
    SMTP_USER: z.string().optional(),
    SMTP_PASSWORD: z.string().optional(),
    SMTP_FROM: z.string().optional(),
    SMTP_TO: list,

    SLACK_WEBHOOK_URL: z.string().url().optional(),
    WEBHOOK_URL: z.string().url().optional(),
    HEARTBEAT_URL: z.string().url().optional(),
    NOTIFY_ON_SUCCESS: flag(true),

    HTTP_PORT: z.coerce.number().int().min(0).max(65535).default(8080),
    LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),
  })
  .superRefine((env, ctx) => {
    if (env.SMTP_HOST && (!env.SMTP_FROM || env.SMTP_TO.length === 0)) {
      ctx.addIssue({
        code: "custom",
        path: ["SMTP_HOST"],
        message: "SMTP_FROM and SMTP_TO are required when SMTP_HOST is set",
      });
    }
    if (env.DRILL_CRON && !env.DRILL_MONGODB_URI) {
      ctx.addIssue({
        code: "custom",
        path: ["DRILL_CRON"],
        message: "DRILL_MONGODB_URI is required when DRILL_CRON is set",
      });
    }
    if (env.ENCRYPTION_KEY) {
      try {
        parseKey(env.ENCRYPTION_KEY);
      } catch (error) {
        ctx.addIssue({ code: "custom", path: ["ENCRYPTION_KEY"], message: error.message });
      }
    }
  });

export class ConfigError extends Error {
  constructor(issues) {
    const details = issues.map((issue) => `  ${issue.path.join(".")}: ${issue.message}`);
    super(`Invalid configuration:\n${details.join("\n")}`);
    this.name = "ConfigError";
    this.issues = issues;
  }
}

export function loadConfig(env = process.env) {
  const present = Object.fromEntries(
    Object.entries(env).filter(([, value]) => value !== undefined && value !== ""),
  );
  const result = schema.safeParse(present);
  if (!result.success) {
    throw new ConfigError(result.error.issues);
  }
  return toConfig(result.data);
}

function toConfig(env) {
  const retention = {
    keepLast: env.RETENTION_KEEP_LAST ?? 0,
    keepDaily: env.RETENTION_KEEP_DAILY ?? 0,
    keepWeekly: env.RETENTION_KEEP_WEEKLY ?? 0,
    keepMonthly: env.RETENTION_KEEP_MONTHLY ?? 0,
  };
  const retentionEnabled = Object.values(retention).some((value) => value > 0);

  let endpoint = env.S3_ENDPOINT;
  if (!endpoint && env.R2_ACCOUNT_ID) {
    endpoint = `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
  }

  return {
    mongo: {
      uri: env.MONGODB_URI,
      database: env.MONGODB_DATABASE,
      excludeCollections: env.MONGODB_EXCLUDE_COLLECTIONS,
      oplog: env.MONGODB_OPLOG,
      parallelCollections: env.MONGODB_PARALLEL_COLLECTIONS,
    },
    storage: {
      bucket: env.S3_BUCKET,
      endpoint,
      region: env.S3_REGION ?? (env.R2_ACCOUNT_ID ? "auto" : (process.env.AWS_REGION || "us-east-1")),
      forcePathStyle: env.S3_FORCE_PATH_STYLE,
      accessKeyId: env.S3_ACCESS_KEY_ID,
      secretAccessKey: env.S3_SECRET_ACCESS_KEY,
      prefix: env.S3_PREFIX.replace(/^\/+|\/+$/g, ""),
      partSizeMb: env.S3_PART_SIZE_MB,
      queueSize: env.S3_QUEUE_SIZE,
    },
    schedule: {
      cron: env.BACKUP_CRON,
      timezone: env.BACKUP_TIMEZONE,
      runOnStart: env.BACKUP_ON_START,
    },
    timeoutMs: env.BACKUP_TIMEOUT_MINUTES * 60_000,
    workDir: env.WORK_DIR,
    encryptionKey: env.ENCRYPTION_KEY ? parseKey(env.ENCRYPTION_KEY) : null,
    retention: retentionEnabled ? retention : null,
    drill: env.DRILL_MONGODB_URI ? { uri: env.DRILL_MONGODB_URI, cron: env.DRILL_CRON ?? null } : null,
    notifications: {
      onSuccess: env.NOTIFY_ON_SUCCESS,
      email: env.SMTP_HOST
        ? {
            host: env.SMTP_HOST,
            port: env.SMTP_PORT,
            secure: env.SMTP_SECURE,
            user: env.SMTP_USER,
            password: env.SMTP_PASSWORD,
            from: env.SMTP_FROM,
            to: env.SMTP_TO,
          }
        : null,
      slackWebhookUrl: env.SLACK_WEBHOOK_URL ?? null,
      webhookUrl: env.WEBHOOK_URL ?? null,
      heartbeatUrl: env.HEARTBEAT_URL ?? null,
    },
    http: { port: env.HTTP_PORT },
    logLevel: env.LOG_LEVEL,
  };
}
