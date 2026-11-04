import type { MongoClient } from "mongodb";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { StorageClient } from "./storage.js";

export interface DrillMismatch {
  collection: string;
  expected: number;
  actual: number | "missing";
}

export interface DrillVariance {
  collection: string;
  expected: number;
  actual: number;
  diff: number;
}

export interface DrillResult {
  key: string;
  database: string;
  collections: number;
  documents: number;
  oplogVariances: DrillVariance[];
  durationMs: number;
}

export interface DrillContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  createMongoClient?: (uri: string) => MongoClient;
}
