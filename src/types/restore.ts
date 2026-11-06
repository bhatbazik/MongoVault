import type { BackupManifest } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { StorageClient } from "./storage.js";
import type { RestoreTools } from "./tools.js";

export interface RestoreContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  tools?: RestoreTools;
}

export interface RestoreOptions {
  ref?: string;
  uri: string;
  database?: string;
  nsInclude?: string;
  nsExclude?: string;
  drop?: boolean;
  preVerify?: boolean;
  oplogReplay?: boolean;
  oplogLimit?: string;
  pitr?: string;
  parallelCollections?: number;
}

export interface RestoreResult {
  key: string;
  source: string;
  target: string;
  restored: number;
  manifest: BackupManifest | null;
  pitrTarget?: string;
  replayedSlices?: number;
  replayedEntries?: number;
}

export interface VerifyBackupResult {
  key: string;
  size: number;
  sha256: string;
  encrypted: boolean;
}

export interface DescribeBackupResult {
  key: string;
  manifest: BackupManifest | null;
  checksum: string | null;
}
