import type { LogLevel } from "./logger.js";

export interface MongoConfig {
  uri: string;
  database: string;
  excludeCollections: string[];
  oplog: boolean;
  parallelCollections: number;
}

export interface PitrConfig {
  enabled: boolean;
  flushIntervalSeconds: number;
  maxSliceMb: number;
}

export interface StorageConfig {
  bucket: string;
  endpoint?: string;
  region: string;
  forcePathStyle: boolean;
  accessKeyId: string;
  secretAccessKey: string;
  prefix: string;
  partSizeMb: number;
  queueSize: number;
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
  notifications: NotificationsConfig;
  http: HttpConfig;
  logLevel: LogLevel;
}

export interface ConfigIssue {
  code: string;
  path: (string | number)[];
  message: string;
}
