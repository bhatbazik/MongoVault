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

export interface BackupManifest {
  version: number;
  type?: string;
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
}
