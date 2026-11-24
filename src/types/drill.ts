import type { MongoClient } from "mongodb";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { OplogApplierStats } from "./oplog.js";
import type { StorageClient } from "./storage.js";
import type { RestoreTools } from "./tools.js";

export interface DrillMismatch {
  collection: string;
  expected: number;
  actual: number | "missing";
}

export interface DrillSchemaMismatch {
  namespace: string;
  kind: "index" | "validator" | "options" | "view" | "read_write";
  detail: string;
}

export interface DrillVariance {
  collection: string;
  expected: number;
  actual: number;
  diff: number;
}

export interface DrillOptions {
  /**
   * Optional PITR drill target: `"latest"` drills the latest archived PITR slice timestamp,
   * or pass an explicit ISO-8601 / `<seconds>:<ordinal>` timestamp.
   */
  pitr?: string;
}

export interface DrillResult {
  key: string;
  database: string;
  collections: number;
  documents: number;
  indexesVerified: number;
  validatorsVerified: number;
  viewsVerified: number;
  readWriteVerified: boolean;
  oplogVariances: DrillVariance[];
  pitrTarget?: string;
  replayedSlices?: number;
  oplogStats?: OplogApplierStats;
  durationMs: number;
}

export interface DrillContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  tools?: RestoreTools;
  createMongoClient?: (uri: string) => MongoClient;
}
