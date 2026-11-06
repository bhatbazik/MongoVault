import type { AppConfig } from "../types/config.js";
import type { Logger } from "../types/logger.js";
import type { MetricsRegistry } from "../types/metrics.js";
import type { Notifier } from "../types/notify.js";
import type { StorageClient } from "../types/storage.js";

export interface CliParsedValues {
  key?: string;
  engine?: string;
  "db-path"?: string;
  "volume-id"?: string;
  sharding?: boolean;
  uri?: string;
  database?: string;
  "ns-include"?: string;
  "ns-exclude"?: string;
  "read-preference"?: string;
  "older-than"?: string;
  output?: string;
  force?: boolean;
  drop?: boolean;
  oplog?: boolean;
  "oplog-replay"?: boolean;
  "oplog-limit"?: string;
  pitr?: string;
  parallel?: string;
  "dry-run"?: boolean;
  json?: boolean;
  help?: boolean;
  [key: string]: unknown;
}

export interface CliCommandContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  notifier: Notifier;
  metrics: MetricsRegistry;
  values: CliParsedValues;
}

export interface CommandHandler {
  execute(ctx: CliCommandContext): Promise<void>;
}
