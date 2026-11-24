import type { MongoClient } from "mongodb";
import type { BackupManifest, BackupSummary } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { MetricsRegistry } from "./metrics.js";
import type { Notifier } from "./notify.js";
import type { StorageClient } from "./storage.js";

export interface OplogTimestamp {
  t: number;
  i: number;
}

export interface OplogSkipBreakdown {
  beforeWindow: number;
  noop: number;
  filtered: number;
  indexLifecycle: number;
  benignReplayRace: number;
  unrecognized: number;
}

export interface OplogApplierStats {
  appliedEntries: number;
  skippedEntries: number;
  skipBreakdown: OplogSkipBreakdown;
  indexesBuilt: number;
  lastAppliedTs: OplogTimestamp | null;
  uncommittedTxns: number;
  partialTxns: number;
  unknownCommands: string[];
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
  coverageStartTs?: OplogTimestamp | null;
  coverageEndTs?: OplogTimestamp | null;
  previousEndTs?: OplogTimestamp | null;
  gapDetected?: boolean;
  keyPrefix?: string;
  signal?: AbortSignal;
}

export interface CaptureOplogSliceOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  sinceTs?: string | OplogTimestamp;
  untilTs?: string | OplogTimestamp;
  /** Optional custom storage prefix (e.g. backup-scoped bridging slices outside `_pitr/`). */
  keyPrefix?: string;
  /** If false, does not read or update `_pitr/state.json` (used for backup-scoped bridging slices). */
  updateState?: boolean;
  /** Optional callback invoked when the slice upper bound is capped by an active/prepared transaction. */
  onCappedByActiveTxn?: (activeTxStartTs: OplogTimestamp) => void;
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
  metrics?: MetricsRegistry;
  signal?: AbortSignal;
  createMongoClient?: (uri: string) => MongoClient;
}

export interface OplogArchiver {
  start: () => void;
  stop: () => Promise<void>;
}
