import type { LogLevel } from "./logger.js";
import type { ObjectLockOptions } from "./storage.js";
import type { BackupEngine } from "./catalog.js";

export type MongoReadPreference =
  "primary" | "primaryPreferred" | "secondary" | "secondaryPreferred" | "nearest";

export interface MongoConfig {
  uri: string;
  database: string;
  excludeCollections: string[];
  oplog: boolean;
  parallelCollections: number;
  readPreference?: MongoReadPreference;
}

export interface PitrConfig {
  enabled: boolean;
  flushIntervalSeconds: number;
  maxSliceMb: number;
}

export interface StorageConfig {
  type: "s3" | "filesystem";
  path?: string;
  bucket: string;
  endpoint?: string;
  region: string;
  forcePathStyle: boolean;
  accessKeyId: string;
  secretAccessKey: string;
  prefix: string;
  partSizeMb: number;
  queueSize: number;
  objectLock?: ObjectLockOptions;
}

export interface ScheduleConfig {
  cron: string;
  timezone: string;
  runOnStart: boolean;
}

export interface RetentionPolicy {
  keepLast?: number;
  keepDaily?: number;
  keepWeekly?: number;
  keepMonthly?: number;
}

export interface DrillConfig {
  uri: string;
  cron: string | null;
}

export interface PhysicalConfig {
  dbPath?: string;
  encryptionKeyFile?: string;
  directoryPerDb?: boolean;
}

export interface ShardingConfig {
  enabled: boolean;
  configsvrUri?: string;
}

export interface SmtpConfig {
  host: string;
  port: number;
  secure: boolean;
  user?: string;
  password?: string;
  from?: string;
  to: string[];
}

export interface NotificationsConfig {
  onSuccess: boolean;
  email: SmtpConfig | null;
  slackWebhookUrl: string | null;
  webhookUrl: string | null;
  heartbeatUrl: string | null;
  drillHeartbeatUrl: string | null;
}

export interface HttpConfig {
  port: number;
}

export interface AppConfig {
  engine?: BackupEngine;
  mongo: MongoConfig;
  pitr: PitrConfig;
  storage: StorageConfig;
  schedule: ScheduleConfig;
  timeoutMs: number;
  workDir: string;
  encryptionKey: Buffer | null;
  encryptionKeyRing: Buffer[];
  retention: RetentionPolicy | null;
  drill: DrillConfig | null;
  physical?: PhysicalConfig | null;
  sharding?: ShardingConfig | null;
  notifications: NotificationsConfig;
  http: HttpConfig;
  logLevel: LogLevel;
}

export interface ConfigIssue {
  code: string;
  path: (string | number)[];
  message: string;
}
