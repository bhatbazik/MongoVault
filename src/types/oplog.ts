import type { MongoClient, Timestamp } from "mongodb";
import type { BackupManifest, BackupScope, BackupSummary } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { Notifier } from "./notify.js";
import type { StorageClient } from "./storage.js";

export interface OplogTimestamp {
  t: number;
  i: number;
}

export interface BsonTimestampLike {
  getHighBits?: () => number;
  getLowBits?: () => number;
  t?: number;
  i?: number;
}

export interface OplogBounds {
  oldestTs: OplogTimestamp | null;
  newestTs: OplogTimestamp | null;
}

export interface OplogSliceKeyTimestamps {
  startTs: OplogTimestamp;
  endTs: OplogTimestamp;
  encrypted: boolean;
}

export interface OplogSlice {
  key: string;
  size: number;
  lastModified?: Date;
  startTs: OplogTimestamp;
  endTs: OplogTimestamp;
  encrypted: boolean;
  complete: boolean;
  manifest?: BackupManifest | null;
}

export interface UploadOplogSliceOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  rawDocs: Buffer[];
  startTs: OplogTimestamp;
  endTs: OplogTimestamp;
  previousEndTs?: OplogTimestamp | null;
  gapDetected?: boolean;
  signal?: AbortSignal;
}

export interface CaptureOplogSliceOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  sinceTs?: string | OplogTimestamp;
  createMongoClient?: (uri: string) => MongoClient;
}

export interface PitrPlan {
  baseBackup: BackupSummary;
  baseManifest: BackupManifest | null;
  baseEndTs: OplogTimestamp;
  slices: OplogSlice[];
  lastCoveredTs: OplogTimestamp;
  targetTs: OplogTimestamp;
}

export interface ExtractOplogSliceResult {
  bytes: number;
  sha256: string;
  manifest: BackupManifest | null;
}

export interface StartOplogArchiverOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  notifier?: Notifier;
  signal?: AbortSignal;
  createMongoClient?: (uri: string) => MongoClient;
}

export interface OplogArchiver {
  start: () => void;
  stop: () => Promise<void>;
}
