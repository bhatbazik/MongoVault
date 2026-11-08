import type { EncryptionEnvelope } from "./crypto.js";

export interface BackupScope {
  prefix: string;
  database: string;
}

export interface ArchiveKeyOptions {
  encrypted?: boolean;
}

export interface BackupSummary {
  key: string;
  size: number;
  createdAt: Date;
  encrypted: boolean;
  complete: boolean;
}

export interface ManifestOplogWindow {
  previousEndTs?: string | null;
  startTs: string;
  endTs: string;
  entries?: number;
  gapDetected?: boolean;
}

export type BackupEngine = "logical" | "physical" | "snapshot";

export interface PhysicalBackupFile {
  name: string;
  size: number;
}

export interface PhysicalBackupMeta {
  dbPath?: string;
  cursorType?: "$backupCursor" | "fsyncLock";
  filesCount?: number;
  files: PhysicalBackupFile[];
  checkpointTimestamp?: string;
  oplogStart?: string;
  oplogEnd?: string;
}

export interface CloudSnapshotMeta {
  provider: "aws-ebs" | "custom";
  snapshotId: string;
  volumeId: string;
  region?: string;
  clusterTime?: string;
}

export interface ShardedClusterShardBackup {
  shardId: string;
  host: string;
  backupKey: string;
  manifestKey: string;
  status: "success" | "failed";
  size?: number;
}

export interface ShardedClusterMeta {
  clusterTime: string;
  configsvr: {
    backupKey: string;
    manifestKey: string;
    host?: string;
  };
  shards: Record<string, ShardedClusterShardBackup>;
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
  oplog: boolean;
  oplogWindow?: ManifestOplogWindow | null;
  excludedCollections: string[];
  collections: Record<string, number>;
  physical?: PhysicalBackupMeta;
  snapshot?: CloudSnapshotMeta;
  sharding?: ShardedClusterMeta;
  objectLock?: {
    mode: "GOVERNANCE" | "COMPLIANCE";
    retainUntil: string;
    legalHold?: boolean;
  };
}
