import type { BackupSummary } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { OplogSlice } from "./oplog.js";
import type { StorageClient } from "./storage.js";

export type RetentionTarget = BackupSummary | OplogSlice;

export interface RetentionPlan {
  keep: BackupSummary[];
  remove: BackupSummary[];
}

export interface PruneContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
}

export interface PruneOptions {
  dryRun?: boolean;
  now?: number;
}

export interface PruneResult {
  keep: BackupSummary[];
  remove: RetentionTarget[];
}
