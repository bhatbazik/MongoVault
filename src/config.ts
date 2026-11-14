import os from "node:os";
import path from "node:path";
import cron from "node-cron";
import { z } from "zod";
import { isClusterScope } from "./catalog.js";
import { parseKey, parseKeyRing } from "./crypto.js";
import type { AppConfig, ConfigIssue } from "./types/config.js";

const list = z
  .string()
  .default("")
  .transform((value) =>
    value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean),
  );

const flag = (fallback: boolean) =>
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
    MONGODB_READ_PREFERENCE: z
      .enum(["primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"])
      .default("secondaryPreferred"),

    STORAGE_TYPE: z.enum(["s3", "filesystem"]).default("s3"),
    STORAGE_PATH: z.string().optional(),
    S3_BUCKET: z.string().default("mongodb-backups"),
    S3_ACCESS_KEY_ID: z.string().default(""),
    S3_SECRET_ACCESS_KEY: z.string().default(""),
    S3_ENDPOINT: z.string().url().optional(),
    S3_REGION: z.string().optional(),
    S3_FORCE_PATH_STYLE: flag(false),
    S3_PREFIX: z.string().default("mongodb"),
    S3_PART_SIZE_MB: z.coerce.number().int().min(5).max(512).default(32),
    S3_QUEUE_SIZE: z.coerce.number().int().min(1).max(16).default(2),

    S3_OBJECT_LOCK_ENABLED: flag(false),
    S3_OBJECT_LOCK_MODE: z.enum(["GOVERNANCE", "COMPLIANCE"]).default("GOVERNANCE"),
    S3_OBJECT_LOCK_RETENTION_DAYS: z.coerce.number().int().positive().default(30),
    S3_OBJECT_LOCK_LEGAL_HOLD: flag(false),

    BACKUP_ENGINE: z.enum(["logical", "physical"]).default("logical"),
    PHYSICAL_DB_PATH: z.string().optional(),

    SHARDING_ENABLED: flag(false),
    SHARDING_CONFIGSVR_URI: z.string().optional(),

    BACKUP_CRON: cronExpression.default("0 2 * * *"),
    BACKUP_TIMEZONE: timezone.default("UTC"),
    BACKUP_TIMEOUT_MINUTES: z.coerce.number().positive().default(180),
    BACKUP_ON_START: flag(false),
    WORK_DIR: z.string().default(path.join(os.tmpdir(), "mongodb-backup")),

    ENCRYPTION_KEY: z.string().optional(),
    ENCRYPTION_KEY_RING: z.string().optional(),

    PITR_ENABLED: flag(false),
    PITR_FLUSH_INTERVAL_SECONDS: z.coerce.number().int().min(5).max(3600).default(60),
    PITR_MAX_SLICE_MB: z.coerce.number().int().min(1).max(256).default(16),

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
    DRILL_HEARTBEAT_URL: z.string().url().optional(),
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
        ctx.addIssue({
          code: "custom",
          path: ["ENCRYPTION_KEY"],
          message: (error as Error).message,
        });
      }
    }
    if (env.ENCRYPTION_KEY_RING) {
      try {
        parseKeyRing(env.ENCRYPTION_KEY_RING);
      } catch (error) {
        ctx.addIssue({
          code: "custom",
          path: ["ENCRYPTION_KEY_RING"],
          message: (error as Error).message,
        });
      }
    }
    if (env.MONGODB_OPLOG && !isClusterScope(env.MONGODB_DATABASE)) {
      ctx.addIssue({
        code: "custom",
        path: ["MONGODB_OPLOG"],
        message:
          "MONGODB_OPLOG can only be used with full cluster backup (MONGODB_DATABASE: * or all)",
      });
    }
    if (env.PITR_ENABLED && !isClusterScope(env.MONGODB_DATABASE)) {
      ctx.addIssue({
        code: "custom",
        path: ["PITR_ENABLED"],
        message:
          "PITR_ENABLED can only be used with full cluster backup (MONGODB_DATABASE: * or all)",
      });
    }
    if (env.MONGODB_EXCLUDE_COLLECTIONS.length > 0 && isClusterScope(env.MONGODB_DATABASE)) {
      ctx.addIssue({
        code: "custom",
        path: ["MONGODB_EXCLUDE_COLLECTIONS"],
        message:
          "MONGODB_EXCLUDE_COLLECTIONS cannot be used with full cluster backup (MONGODB_DATABASE: * or all); mongodump requires --db when --excludeCollection is specified",
      });
    }
    if (env.STORAGE_TYPE === "s3") {
      if (!env.S3_BUCKET || env.S3_BUCKET.trim() === "") {
        ctx.addIssue({
          code: "custom",
          path: ["S3_BUCKET"],
          message: "S3_BUCKET is required when STORAGE_TYPE is s3",
        });
      }
      if (!env.S3_ACCESS_KEY_ID || env.S3_ACCESS_KEY_ID.trim() === "") {
        ctx.addIssue({
          code: "custom",
          path: ["S3_ACCESS_KEY_ID"],
          message: "S3_ACCESS_KEY_ID is required when STORAGE_TYPE is s3",
        });
      }
      if (!env.S3_SECRET_ACCESS_KEY || env.S3_SECRET_ACCESS_KEY.trim() === "") {
        ctx.addIssue({
          code: "custom",
          path: ["S3_SECRET_ACCESS_KEY"],
          message: "S3_SECRET_ACCESS_KEY is required when STORAGE_TYPE is s3",
        });
      }
    } else if (env.STORAGE_TYPE === "filesystem") {
      if (!env.STORAGE_PATH || env.STORAGE_PATH.trim() === "") {
        ctx.addIssue({
          code: "custom",
          path: ["STORAGE_PATH"],
          message: "STORAGE_PATH is required when STORAGE_TYPE is filesystem",
        });
      }
    }
  });

export class ConfigError extends Error {
  issues: ConfigIssue[];

  constructor(issues: Array<{ path: PropertyKey[]; message: string; code?: string }>) {
    const details = issues.map((issue) => `  ${issue.path.join(".")}: ${issue.message}`);
    super(`Invalid configuration:\n${details.join("\n")}`);
    this.name = "ConfigError";
    this.issues = issues.map((i) => ({
      code: i.code ?? "custom",
      path: i.path.map((p) => (typeof p === "symbol" ? p.toString() : p)),
      message: i.message,
    }));
  }
}

export function loadConfig(env: Record<string, string | undefined> = process.env): AppConfig {
  const present = Object.fromEntries(
    Object.entries(env).filter(([, value]) => value !== undefined && value !== ""),
  );
  const result = schema.safeParse(present);
  if (!result.success) {
    throw new ConfigError(result.error.issues);
  }
  return toConfig(result.data);
}

function toConfig(env: z.infer<typeof schema>): AppConfig {
  const retention = {
    keepLast: env.RETENTION_KEEP_LAST ?? 0,
    keepDaily: env.RETENTION_KEEP_DAILY ?? 0,
    keepWeekly: env.RETENTION_KEEP_WEEKLY ?? 0,
    keepMonthly: env.RETENTION_KEEP_MONTHLY ?? 0,
  };
  const retentionEnabled = Object.values(retention).some((value) => value > 0);

  return {
    engine: env.BACKUP_ENGINE,
    mongo: {
      uri: env.MONGODB_URI,
      database: env.MONGODB_DATABASE,
      excludeCollections: env.MONGODB_EXCLUDE_COLLECTIONS,
      oplog: env.MONGODB_OPLOG || env.PITR_ENABLED,
      parallelCollections: env.MONGODB_PARALLEL_COLLECTIONS,
      readPreference: env.MONGODB_READ_PREFERENCE,
    },
    pitr: {
      enabled: env.PITR_ENABLED,
      flushIntervalSeconds: env.PITR_FLUSH_INTERVAL_SECONDS,
      maxSliceMb: env.PITR_MAX_SLICE_MB,
    },
    storage: {
      type: env.STORAGE_TYPE,
      path: env.STORAGE_PATH,
      bucket:
        env.STORAGE_TYPE === "filesystem" ? (env.STORAGE_PATH ?? "filesystem") : env.S3_BUCKET,
      endpoint: env.S3_ENDPOINT,
      region: env.S3_REGION ?? (process.env.AWS_REGION || "ap-south-1"),
      forcePathStyle: env.S3_FORCE_PATH_STYLE,
      accessKeyId: env.S3_ACCESS_KEY_ID,
      secretAccessKey: env.S3_SECRET_ACCESS_KEY,
      prefix: env.S3_PREFIX.replace(/^\/+|\/+$/g, ""),
      partSizeMb: env.S3_PART_SIZE_MB,
      queueSize: env.S3_QUEUE_SIZE,
      objectLock: env.S3_OBJECT_LOCK_ENABLED
        ? {
            enabled: true,
            mode: env.S3_OBJECT_LOCK_MODE,
            retentionDays: env.S3_OBJECT_LOCK_RETENTION_DAYS,
            legalHold: env.S3_OBJECT_LOCK_LEGAL_HOLD,
          }
        : undefined,
    },
    schedule: {
      cron: env.BACKUP_CRON,
      timezone: env.BACKUP_TIMEZONE,
      runOnStart: env.BACKUP_ON_START,
    },
    timeoutMs: env.BACKUP_TIMEOUT_MINUTES * 60_000,
    workDir: env.WORK_DIR,
    encryptionKey: env.ENCRYPTION_KEY ? parseKey(env.ENCRYPTION_KEY) : null,
    encryptionKeyRing: deduplicateKeys([
      ...(env.ENCRYPTION_KEY ? [parseKey(env.ENCRYPTION_KEY)] : []),
      ...(env.ENCRYPTION_KEY_RING ? parseKeyRing(env.ENCRYPTION_KEY_RING) : []),
    ]),
    retention: retentionEnabled ? retention : null,
    drill: env.DRILL_MONGODB_URI
      ? { uri: env.DRILL_MONGODB_URI, cron: env.DRILL_CRON ?? null }
      : null,
    physical: env.PHYSICAL_DB_PATH ? { dbPath: env.PHYSICAL_DB_PATH } : null,
    sharding: env.SHARDING_ENABLED
      ? {
          enabled: true,
          configsvrUri: env.SHARDING_CONFIGSVR_URI,
        }
      : null,
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
      drillHeartbeatUrl: env.DRILL_HEARTBEAT_URL ?? null,
    },
    http: { port: env.HTTP_PORT },
    logLevel: env.LOG_LEVEL,
  };
}

function deduplicateKeys(keys: Buffer[]): Buffer[] {
  const seen = new Set<string>();
  return keys.filter((key) => {
    const hex = key.toString("hex");
    if (seen.has(hex)) return false;
    seen.add(hex);
    return true;
  });
}
