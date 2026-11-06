import type { BackupManifest, ShardedClusterMeta } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
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
  signal?: AbortSignal;
  drop?: boolean;
}

export interface ShardedRestoreResult {
  key: string;
  clusterTime: string;
  restoredShards: string[];
  configsvrRestored: boolean;
}
