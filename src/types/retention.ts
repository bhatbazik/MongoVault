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

export interface DeleteBackupOptions {
  key: string;
  force?: boolean;
  dryRun?: boolean;
}

export interface DeleteBackupResult {
  key: string;
  deleted: boolean;
  message?: string;
}

export interface PruneOplogOptions {
  olderThan: string;
  dryRun?: boolean;
}

export interface PruneOplogResult {
  cutoffTs: string;
  deletedSlices: string[];
}
