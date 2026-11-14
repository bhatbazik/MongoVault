import type { MongoClient } from "mongodb";
import type { BackupContext } from "./backup.js";
import type { BackupManifest, ShardedClusterMeta } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { RestoreContext, RestoreOptions, RestoreResult } from "./restore.js";
import type { StorageClient } from "./storage.js";

export interface ShardInfo {
  id: string;
  host: string;
  tags?: string[];
}

export interface ConfigServerInfo {
  host: string;
  setName?: string;
}

export interface ClusterTopology {
  isSharded: boolean;
  mongosHost?: string;
  configsvr?: ConfigServerInfo;
  shards: ShardInfo[];
  balancerEnabled: boolean;
  balancerRunning: boolean;
}

export interface ShardedBackupOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  createMongoClient?: (uri: string) => MongoClient;
  backupRunner?: (options: BackupContext) => Promise<BackupManifest>;
}

export interface ShardedBackupResult {
  manifest: BackupManifest;
  clusterMeta: ShardedClusterMeta;
  durationMs: number;
}

export interface ShardedRestoreOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  ref?: string;
  pitr?: string;
  signal?: AbortSignal;
  drop?: boolean;
  createMongoClient?: (uri: string) => MongoClient;
  restoreRunner?: (ctx: RestoreContext, opts: RestoreOptions) => Promise<RestoreResult>;
}

export interface ShardedRestoreResult {
  key: string;
  clusterTime: string;
  restoredShards: string[];
  configsvrRestored: boolean;
  pitrTarget?: string;
  replayedSlices?: number;
}
