import type { MongoClient } from "mongodb";
import type { BackupManifest } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { OplogApplierStats, PitrPlan } from "./oplog.js";
import type { StorageClient } from "./storage.js";

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

export interface PhysicalRecoveryContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  targetDbPath: string;
  manifest: BackupManifest | null;
  pitrPlan: PitrPlan | null;
  signal?: AbortSignal;
}

export interface PhysicalRestoreOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  targetDbPath: string;
  ref?: string;
  pitr?: string;
  signal?: AbortSignal;
  /**
   * Test-only hook to bypass booting an ephemeral `mongod` for WiredTiger/RS recovery.
   * Defaults to `false` (full multi-step recovery always runs in production).
   */
  skipRecovery?: boolean;
  /**
   * Optional dependency-injected recovery coordinator for testing.
   */
  recoveryRunner?: (
    ctx: PhysicalRecoveryContext,
  ) => Promise<{ replayedEntries: number; oplogStats?: OplogApplierStats }>;
}

export interface PhysicalRestoreResult {
  key: string;
  restoredFiles: number;
  totalBytes: number;
  targetDbPath: string;
  pitrTarget?: string;
  replayedSlices?: number;
  replayedEntries?: number;
  oplogStats?: OplogApplierStats;
}
