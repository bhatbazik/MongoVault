import type { ChildProcess } from "node:child_process";
import type { Readable, Writable } from "node:stream";
import type { Logger } from "./logger.js";
import type { OplogBounds } from "./oplog.js";

export interface DumpLineResult {
  database: string;
  collection: string;
  documents: number;
}

export interface RestoreLineResult {
  restored: number;
  failed: number;
}

export interface RunToolOptions {
  stdio: (string | "ignore" | "pipe" | "inherit")[];
  signal?: AbortSignal;
  timeoutMs?: number;
  onLine?: (line: string) => void;
}

export interface RunToolResult {
  child: ChildProcess;
  done: Promise<void>;
}

export interface MongodumpOptions {
  uri: string;
  database: string;
  excludeCollections?: string[];
  parallelCollections?: number;
  oplog?: boolean;
  readPreference?: string;
  workDir: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  logger?: Logger;
  command?: string;
}

export interface MongodumpResult {
  stdout: Readable;
  done: Promise<{ collections: Record<string, number> }>;
}

export interface MongorestoreOptions {
  uri: string;
  workDir: string;
  nsInclude?: string;
  nsExclude?: string;
  nsFrom?: string;
  nsTo?: string;
  drop?: boolean;
  oplogReplay?: boolean;
  oplogLimit?: string;
  parallelCollections?: number;
  signal?: AbortSignal;
  timeoutMs?: number;
  logger?: Logger;
  command?: string;
}

export interface MongorestoreResult {
  stdin: Writable;
  done: Promise<RestoreLineResult>;
}

export interface ReplayOplogFileOptions {
  uri: string;
  workDir: string;
  oplogFile: string;
  oplogLimit?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  logger?: Logger;
  command?: string;
}

export interface BackupTools {
  mongodump: (options: MongodumpOptions) => Promise<MongodumpResult>;
  getOplogBounds?: (uri: string) => Promise<OplogBounds | null>;
}

export interface RestoreTools {
  mongorestore?: (options: MongorestoreOptions) => Promise<MongorestoreResult>;
  replayOplogFile?: (options: ReplayOplogFileOptions) => Promise<void>;
}

export interface ToolVersionInfo {
  available: boolean;
  version?: string;
  error?: string;
}

export interface DatabaseToolsStatus {
  mongodump: ToolVersionInfo;
  mongorestore: ToolVersionInfo;
}
