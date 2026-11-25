import type { EncryptionEnvelope } from "./crypto.js";

export interface BackupScope {
  prefix: string;
  database: string;
}

export interface ArchiveKeyOptions {
  encrypted?: boolean;
  suffix?: string;
}

export interface BackupSummary {
  key: string;
  size: number;
  createdAt: Date;
  lastModified?: Date;
  encrypted: boolean;
  complete: boolean;
}

export interface ManifestOplogWindow {
  previousEndTs?: string | null;
  startTs: string;
  endTs: string;
  coverageStartTs?: string;
  coverageEndTs?: string;
  entries?: number;
  gapDetected?: boolean;
}

export type BackupEngine = "logical" | "physical";

export interface PhysicalBackupFile {
  name: string;
  size: number;
}

export interface PhysicalStorageOptions {
  directoryPerDB?: boolean;
  directoryForIndexes?: boolean;
  journalCompressor?: string;
  blockCompressor?: string;
}

export interface PhysicalBackupMeta {
  dbPath?: string;
  cursorType?: "$backupCursor" | "fsyncLock";
  mongoVersion?: string;
  storageOptions?: PhysicalStorageOptions;
  filesCount?: number;
  files: PhysicalBackupFile[];
  checkpointTimestamp?: string;
  oplogStart?: string;
  oplogEnd?: string;
}

export interface ShardedClusterShardBackup {
  shardId: string;
  host: string;
  backupKey: string;
  manifestKey: string;
  bridgingSliceKey?: string;
  bridgingSliceKeys?: string[];
  status: "success" | "failed";
  size?: number;
}

export interface ShardedClusterMeta {
  clusterTime: string;
  configsvr: {
    backupKey: string;
    manifestKey: string;
    host?: string;
    bridgingSliceKey?: string;
    bridgingSliceKeys?: string[];
  };
  shards: Record<string, ShardedClusterShardBackup>;
}

export interface ManifestSignature {
  algorithm: "HMAC-SHA256";
  keyId: string;
  hmac: string;
}

export interface SchemaIndexSpec {
  name: string;
  key: Record<string, unknown>;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;
  partialFilterExpression?: Record<string, unknown>;
  hidden?: boolean;
}

export interface SchemaCollectionOptions {
  validator?: Record<string, unknown>;
  validationLevel?: string;
  validationAction?: string;
  capped?: boolean;
  size?: number;
  max?: number;
  timeseries?: Record<string, unknown>;
}

export interface SchemaCollectionMeta {
  indexes: SchemaIndexSpec[];
  options?: SchemaCollectionOptions;
}

export interface SchemaViewMeta {
  name: string;
  viewOn: string;
  pipeline: Record<string, unknown>[];
}

export interface BackupSchemaMetadata {
  collections: Record<string, SchemaCollectionMeta>;
  views: SchemaViewMeta[];
}

export interface BackupManifest {
  version: number;
  type?: string;
  engine?: BackupEngine;
  key: string;
  database: string;
  createdAt: string;
  completedAt: string;
  durationMs: number;
  size: number;
  sha256: string;
  compression: string;
  encryption: EncryptionEnvelope | null;
  signature?: ManifestSignature | null;
  oplog: boolean;
  oplogWindow?: ManifestOplogWindow | null;
  excludedCollections: string[];
  collections: Record<string, number>;
  schema?: BackupSchemaMetadata;
  physical?: PhysicalBackupMeta;
  sharding?: ShardedClusterMeta;
  objectLock?: {
    mode: "GOVERNANCE" | "COMPLIANCE";
    retainUntil: string;
    legalHold?: boolean;
  };
}
