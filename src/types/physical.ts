import type { BackupManifest } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { StorageClient } from "./storage.js";
import type { MongoClient } from "mongodb";

export interface BackupCursorMetadata {
  backupId: string;
  dbpath: string;
  oplogStart?: { ts: { t: number; i: number }; t: number };
  oplogEnd?: { ts: { t: number; i: number }; t: number };
  checkpointTimestamp?: { t: number; i: number };
}

export interface BackupCursorFile {
  filename: string;
  fileLen?: number;
  fileSize?: number;
}

export interface PhysicalBackupOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  dbPath?: string;
  fsyncLockFallback?: boolean;
  createMongoClient?: (uri: string) => MongoClient;
}

export interface PhysicalBackupResult {
  manifest: BackupManifest;
  filesCount: number;
  totalBytes: number;
}

export interface PhysicalRestoreOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  targetDbPath: string;
  ref?: string;
  signal?: AbortSignal;
}

export interface PhysicalRestoreResult {
  key: string;
  restoredFiles: number;
  totalBytes: number;
  targetDbPath: string;
}
