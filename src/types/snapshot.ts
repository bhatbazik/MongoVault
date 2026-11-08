import type { BackupManifest, CloudSnapshotMeta } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { StorageClient } from "./storage.js";
import type { MongoClient } from "mongodb";

export type SnapshotProviderType = "aws-ebs" | "custom";

export interface SnapshotDriverOptions {
  provider: SnapshotProviderType;
  volumeId?: string;
  region?: string;
  commandHook?: string;
}

export interface CloudSnapshotDriver {
  createSnapshot(params: {
    volumeId: string;
    description: string;
    tags?: Record<string, string>;
    signal?: AbortSignal;
  }): Promise<{ snapshotId: string; status: string }>;

  getSnapshotStatus(snapshotId: string): Promise<{ status: string; progress?: string }>;

  restoreSnapshot?(params: {
    snapshotId: string;
    targetZone?: string;
    signal?: AbortSignal;
  }): Promise<{ newVolumeId: string }>;
}

export interface SnapshotBackupOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  driver?: CloudSnapshotDriver;
  volumeId?: string;
  signal?: AbortSignal;
  createMongoClient?: (uri: string) => MongoClient;
}

export interface SnapshotBackupResult {
  manifest: BackupManifest;
  snapshot: CloudSnapshotMeta;
}

export interface SnapshotRestoreOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  ref?: string;
  driver?: CloudSnapshotDriver;
  targetZone?: string;
  signal?: AbortSignal;
}

export interface SnapshotRestoreResult {
  snapshotId: string;
  volumeId: string;
  newVolumeId?: string;
  restoredAt: string;
}
