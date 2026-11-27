import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { createGunzip, createGzip } from "node:zlib";
import { MongoClient, MongoCursorExhaustedError, ObjectId, Timestamp } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import {
  MANIFEST_VERSION,
  archiveKey,
  databasePrefix,
  isClusterScope,
  listBackups,
  readManifest,
  resolveKeyRing,
  scopeOf,
  writeManifest,
} from "./catalog.js";
import { createDecryptor, createEncryptor, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { formatBytes, formatDuration } from "./format.js";
import { MongoOplogTargetClient, OplogApplier } from "./oplog-applier.js";
import {
  compareTimestamps,
  formatOplogTimestamp,
  fromBsonTimestamp,
  markPitrRestore,
  parseOplogTimestamp,
  resolvePitrPlan,
  streamOplogSlice,
} from "./oplog.js";
import { IntegrityError, verifyArchiveObject } from "./restore.js";
import { runStreamPipeline } from "./stream.js";
import {
  createTarHeader,
  createTarPackStream,
  extractTarStream,
  generateTarStream,
  verifyTarStream,
} from "./tar.js";
import { withUriReadPreference } from "./tools.js";
import type {
  BackupManifest,
  PhysicalBackupMeta,
  PhysicalStorageOptions,
} from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type { OplogApplierStats, OplogTimestamp } from "./types/oplog.js";
import type {
  BackupCursorFile,
  BackupCursorMetadata,
  PhysicalBackupOptions,
  PhysicalBackupResult,
  PhysicalRecoveryContext,
  PhysicalRestoreOptions,
  PhysicalRestoreResult,
} from "./types/physical.js";

// Re-export tar utilities for backwards compatibility
export {
  createTarHeader,
  createTarPackStream,
  generateTarStream,
  extractTarStream,
  verifyTarStream,
};

const CURSOR_CREATE_RETRIES = 10;
const OPEN_CONFLICT_WITH_CHECKPOINT_ERR_CODE = 50915;
const OPLOG_ROLLED_OVER_ERR_CODE = 50917;
const DISK_HEADROOM_MULTIPLIER = 1.1;
const DEFAULT_FSYNC_LOCK_TIMEOUT_MS = 5 * 60_000;
const FSYNC_UNLOCK_MAX_ATTEMPTS = 3;
const FSYNC_UNLOCK_RETRY_DELAY_MS = 250;

/**
 * Critical error thrown when MongoVault cannot confirm that a MongoDB `fsyncLock`
 * was released. Never swallowed so operators and alert pipelines are immediately
 * notified that the node may still be write-locked.
 */
export class FsyncUnlockError extends Error {
  readonly unlockError: unknown;

  constructor(message: string, options: { unlockError: unknown; cause?: unknown }) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = "FsyncUnlockError";
    this.unlockError = options.unlockError;
  }
}

function makeAbortError(reason?: unknown): Error {
  if (reason instanceof Error) {
    if (reason.name === "AbortError") return reason;
    const err = new Error(reason.message, { cause: reason });
    err.name = "AbortError";
    return err;
  }
  const err = new Error(reason ? String(reason) : "The operation was aborted");
  err.name = "AbortError";
  return err;
}

async function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    throw makeAbortError(signal.reason);
  }
  if (ms <= 0) return;
  await new Promise<void>((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer);
      reject(makeAbortError(signal?.reason));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function isNotLockedError(err: unknown): boolean {
  const e = err as { code?: number; message?: string } | null;
  if (!e) return false;
  if (e.code === 211) return true;
  return /not locked/i.test(String(e.message ?? ""));
}

async function releaseFsyncLockWithRetry({
  client,
  uri,
  createMongoClient,
  logger,
  readPreference,
  maxAttempts = FSYNC_UNLOCK_MAX_ATTEMPTS,
  baseDelayMs = FSYNC_UNLOCK_RETRY_DELAY_MS,
}: {
  client: MongoClient;
  uri: string;
  createMongoClient: (u: string) => MongoClient;
  logger: PhysicalBackupOptions["logger"];
  readPreference?: AppConfig["mongo"]["readPreference"];
  maxAttempts?: number;
  baseDelayMs?: number;
}): Promise<void> {
  let lastError: unknown = null;
  const cmdOpts = readPreference ? { readPreference } : undefined;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    let tempClient: MongoClient | null = null;
    try {
      const targetClient = attempt === 1 || !uri ? client : (tempClient = createMongoClient(uri));
      if (tempClient) {
        await tempClient.connect();
      }
      const unlockRes = await targetClient.db("admin").command({ fsyncUnlock: 1 }, cmdOpts);
      if (!unlockRes || unlockRes.ok === 1 || unlockRes.lockCount === 0) {
        if (attempt > 1) {
          logger.warn({ attempt }, "Released MongoDB fsyncLock via fresh recovery connection");
        } else {
          logger.info("Released MongoDB fsyncLock");
        }
        return;
      }
      lastError = new Error(`Unexpected fsyncUnlock response: ${JSON.stringify(unlockRes)}`);
    } catch (err) {
      if (isNotLockedError(err)) {
        logger.info("MongoDB reports node is already unlocked (fsyncUnlock not required)");
        return;
      }
      lastError = err;
      logger.error(
        { err, attempt, maxAttempts },
        "Failed attempt to release MongoDB fsyncLock; will retry on fresh connection",
      );
    } finally {
      if (tempClient) {
        await tempClient.close().catch(() => {});
      }
    }

    if (attempt < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, baseDelayMs * attempt));
    }
  }

  const detail = lastError instanceof Error ? lastError.message : String(lastError);
  throw new FsyncUnlockError(
    `CRITICAL: Failed to release MongoDB fsyncLock after ${maxAttempts} attempt(s) (${detail}). Node may remain write-locked! Run \`db.adminCommand({ fsyncUnlock: 1 })\` manually on the affected node immediately.`,
    { unlockError: lastError },
  );
}

function uriAlreadyPinnedTo(uri: string, hostPort: string): boolean {
  const hashIdx = uri.indexOf("#");
  const base = hashIdx === -1 ? uri : uri.slice(0, hashIdx);
  const queryIdx = base.indexOf("?");
  if (queryIdx === -1) return false;
  const params = new URLSearchParams(base.slice(queryIdx + 1));
  if (params.get("directConnection") !== "true") return false;
  const beforeQuery = base.slice(0, queryIdx);
  const schemeIdx = beforeQuery.indexOf("://");
  const afterScheme = schemeIdx === -1 ? beforeQuery : beforeQuery.slice(schemeIdx + 3);
  const slashIdx = afterScheme.indexOf("/");
  const authority = slashIdx === -1 ? afterScheme : afterScheme.slice(0, slashIdx);
  const atIdx = authority.lastIndexOf("@");
  const hostsPart = atIdx === -1 ? authority : authority.slice(atIdx + 1);
  return hostsPart.toLowerCase() === hostPort.toLowerCase();
}

function buildDirectNodeUri(originalUri: string, hostPort: string): string {
  const hashIdx = originalUri.indexOf("#");
  const base = hashIdx === -1 ? originalUri : originalUri.slice(0, hashIdx);
  const fragment = hashIdx === -1 ? "" : originalUri.slice(hashIdx);
  const queryIdx = base.indexOf("?");
  const beforeQuery = queryIdx === -1 ? base : base.slice(0, queryIdx);
  const queryStr = queryIdx === -1 ? "" : base.slice(queryIdx + 1);

  const schemeMatch = /^(mongodb(?:\+srv)?):\/\//i.exec(beforeQuery);
  const afterScheme = schemeMatch ? beforeQuery.slice(schemeMatch[0].length) : beforeQuery;
  const slashIdx = afterScheme.indexOf("/");
  const authority = slashIdx === -1 ? afterScheme : afterScheme.slice(0, slashIdx);
  const dbPathPart = slashIdx === -1 ? "/" : afterScheme.slice(slashIdx);
  const atIdx = authority.lastIndexOf("@");
  const userInfo = atIdx === -1 ? "" : authority.slice(0, atIdx + 1);

  const params = new URLSearchParams(queryStr);
  params.delete("replicaSet");
  params.delete("readPreference");
  if (
    schemeMatch?.[1]?.toLowerCase() === "mongodb+srv" &&
    !Array.from(params.keys()).some((k) => k.toLowerCase() === "tls" || k.toLowerCase() === "ssl")
  ) {
    params.set("tls", "true");
  }
  params.set("directConnection", "true");
  const newQuery = params.toString();

  return `mongodb://${userInfo}${hostPort}${dbPathPart}${newQuery ? `?${newQuery}` : ""}${fragment}`;
}

/**
 * Safely extracts `--dbpath` from MongoDB `getCmdLineOpts` output, avoiding the
 * `argv.indexOf("--dbpath") === -1 => argv[0]` bug.
 */
export function extractDbPathFromCmdOpts(
  cmdOpts: Record<string, unknown> | null,
): string | undefined {
  if (!cmdOpts) return undefined;
  const parsed = cmdOpts.parsed as { storage?: { dbPath?: string } } | undefined;
  if (parsed?.storage?.dbPath && typeof parsed.storage.dbPath === "string") {
    return parsed.storage.dbPath;
  }
  const argv = cmdOpts.argv;
  if (Array.isArray(argv)) {
    for (let i = 0; i < argv.length; i++) {
      const arg = String(argv[i]);
      if (arg.startsWith("--dbpath=")) {
        const value = arg.slice("--dbpath=".length);
        if (value) return value;
      } else if (arg === "--dbpath" && i + 1 < argv.length) {
        const next = String(argv[i + 1]);
        if (next && !next.startsWith("-")) return next;
      }
    }
  }
  return undefined;
}

function extractStorageOptionsFromCmdOpts(
  cmdOpts: Record<string, unknown> | null,
): PhysicalStorageOptions | undefined {
  if (!cmdOpts) return undefined;
  const parsed = cmdOpts.parsed as
    | {
        storage?: {
          directoryPerDB?: boolean;
          wiredTiger?: {
            engineConfig?: {
              directoryForIndexes?: boolean;
              journalCompressor?: string;
            };
            collectionConfig?: {
              blockCompressor?: string;
            };
          };
        };
      }
    | undefined;
  const storage = parsed?.storage;
  if (!storage) return undefined;
  const opts: PhysicalStorageOptions = {};
  if (typeof storage.directoryPerDB === "boolean") opts.directoryPerDB = storage.directoryPerDB;
  if (typeof storage.wiredTiger?.engineConfig?.directoryForIndexes === "boolean") {
    opts.directoryForIndexes = storage.wiredTiger.engineConfig.directoryForIndexes;
  }
  if (typeof storage.wiredTiger?.engineConfig?.journalCompressor === "string") {
    opts.journalCompressor = storage.wiredTiger.engineConfig.journalCompressor;
  }
  if (typeof storage.wiredTiger?.collectionConfig?.blockCompressor === "string") {
    opts.blockCompressor = storage.wiredTiger.collectionConfig.blockCompressor;
  }
  return Object.keys(opts).length > 0 ? opts : undefined;
}

function tryRealpathSync(p: string): string | null {
  try {
    return fsSync.realpathSync(p);
  } catch {
    return null;
  }
}

function resolveCanonicalPosix(p: string): string {
  const real = tryRealpathSync(p);
  if (real) {
    return path.posix.normalize(real.replace(/\\/g, "/")).replace(/\/+$/, "");
  }
  const dir = path.dirname(p);
  const base = path.basename(p);
  const realDir = tryRealpathSync(dir);
  if (realDir) {
    return path.posix.normalize(`${realDir.replace(/\\/g, "/")}/${base}`).replace(/\/+$/, "");
  }
  return path.posix.normalize(p.replace(/\\/g, "/")).replace(/\/+$/, "");
}

/**
 * Strips dbPath prefix from a file path returned by $backupCursor / $backupCursorExtend
 * and normalizes it to a clean relative POSIX path. Canonicalizes symlinks and rejects
 * absolute paths outside the configured dbPath.
 */
export function trimDbPathPrefix(filename: string, dbPath: string, realDbPath?: string): string {
  const normFile = path.posix.normalize(filename.replace(/\\/g, "/"));
  if (!normFile.startsWith("/")) {
    if (
      !normFile ||
      normFile === "." ||
      normFile === ".." ||
      normFile.startsWith("../") ||
      /^[A-Za-z]:/.test(normFile)
    ) {
      throw new Error(`Invalid relative backup cursor file path "${filename}"`);
    }
    return normFile;
  }

  const prefixes = new Set<string>();
  const addPrefix = (p: string | null | undefined) => {
    if (!p) return;
    const norm = path.posix.normalize(p.replace(/\\/g, "/")).replace(/\/+$/, "");
    if (norm) prefixes.add(norm);
  };
  addPrefix(dbPath);
  addPrefix(realDbPath);
  addPrefix(tryRealpathSync(dbPath));

  const fileCandidates = new Set<string>([normFile]);
  fileCandidates.add(resolveCanonicalPosix(filename));

  for (const candidate of fileCandidates) {
    for (const prefix of prefixes) {
      if (candidate.startsWith(`${prefix}/`)) {
        const rel = path.posix.normalize(candidate.slice(prefix.length + 1));
        if (rel && rel !== "." && rel !== ".." && !rel.startsWith("../")) {
          return rel;
        }
      }
    }
  }

  throw new Error(`Backup cursor file "${filename}" is outside configured dbPath "${dbPath}"`);
}

/**
 * Creates $backupCursor with retries on WiredTiger checkpoint conflict.
 * Triggers the initial cursor batch inside the retry loop because the MongoDB
 * Node.js driver only sends the aggregate command when the cursor is first read.
 */
export async function createBackupCursorWithRetries(
  admin: ReturnType<MongoClient["db"]>,
  retries = CURSOR_CREATE_RETRIES,
  signal?: AbortSignal,
): Promise<ReturnType<ReturnType<MongoClient["db"]>["aggregate"]>> {
  for (let i = 0; i < retries; i++) {
    if (signal?.aborted) {
      throw makeAbortError(signal.reason);
    }
    let cursor: ReturnType<ReturnType<MongoClient["db"]>["aggregate"]> | null = null;
    try {
      cursor = admin.aggregate([{ $backupCursor: {} }]);
      const c = cursor as unknown as { next?: () => Promise<Record<string, unknown> | null> };
      if (typeof c.next === "function") {
        const firstDoc = await c.next();
        (cursor as unknown as { __firstDoc?: Record<string, unknown> | null }).__firstDoc =
          firstDoc;
      }
      return cursor;
    } catch (err: unknown) {
      if (cursor) {
        await cursor.close().catch(() => {});
      }
      if (signal?.aborted || (err instanceof Error && err.name === "AbortError")) {
        throw err;
      }
      const error = err as { code?: number; message?: string };
      const code = error?.code;
      const isRetriable =
        code === OPEN_CONFLICT_WITH_CHECKPOINT_ERR_CODE ||
        code === OPLOG_ROLLED_OVER_ERR_CODE ||
        String(error?.message || "").includes("checkpoint");

      if (isRetriable && i < retries - 1) {
        await abortableSleep((i + 1) * 1000, signal);
        continue;
      }
      throw err;
    }
  }
  throw new Error("Tries limit exceeded establishing $backupCursor");
}

export async function probePhysicalCapability(
  client: MongoClient,
  options: {
    probeFsyncLock?: boolean;
    uri?: string;
    createMongoClient?: (u: string) => MongoClient;
  } = {},
): Promise<{ supportsBackupCursor: boolean; supportsFsyncLock: boolean; dbPath?: string }> {
  const admin = client.db("admin");
  let supportsBackupCursor = false;
  let supportsFsyncLock = false;
  let dbPath: string | undefined = undefined;

  // 1. Try $backupCursor (read-only, non-blocking)
  let cursor: ReturnType<ReturnType<MongoClient["db"]>["aggregate"]> | null = null;
  try {
    cursor = admin.aggregate([{ $backupCursor: {} }]);
    const doc = await cursor.next();
    if (doc) {
      supportsBackupCursor = true;
      if (doc.metadata && typeof doc.metadata.dbpath === "string") {
        dbPath = doc.metadata.dbpath;
      }
    }
  } catch {
    supportsBackupCursor = false;
  } finally {
    if (cursor) {
      await cursor.close().catch(() => {});
    }
  }

  // 2. Discover dbPath from getCmdLineOpts if not already known
  if (!dbPath) {
    try {
      const cmdOpts = (await admin.command({ getCmdLineOpts: 1 })) as Record<string, unknown>;
      dbPath = extractDbPathFromCmdOpts(cmdOpts);
    } catch {
      // Ignore
    }
  }

  // 3. Never acquire a live write lock during read-only capability probing unless explicitly opted in.
  if (options.probeFsyncLock && !supportsBackupCursor) {
    let probeLocked = false;
    try {
      const res = await admin.command({ fsync: 1, lock: true });
      if (res && (res.ok === 1 || res.lock === true)) {
        supportsFsyncLock = true;
        probeLocked = true;
      }
    } catch {
      supportsFsyncLock = false;
    }
    if (probeLocked) {
      await releaseFsyncLockWithRetry({
        client,
        uri: options.uri ?? "",
        createMongoClient: options.createMongoClient ?? ((u) => new MongoClient(u)),
        logger: {
          info: () => {},
          warn: () => {},
          error: () => {},
          debug: () => {},
        } as unknown as PhysicalBackupOptions["logger"],
      });
    }
  }

  return { supportsBackupCursor, supportsFsyncLock, dbPath };
}

/**
 * Scans a WiredTiger dbPath directory for physical data files, excluding volatile/lock/staging
 * entries and traversing symlinks safely with a realpath cycle guard.
 */
export async function scanWiredTigerFiles(
  dbPath: string,
): Promise<Array<{ name: string; fullPath: string; size: number }>> {
  const results: Array<{ name: string; fullPath: string; size: number }> = [];
  const visitedDirs = new Set<string>();
  const rootRealPath = await fs.realpath(dbPath).catch(() => path.resolve(dbPath));
  visitedDirs.add(rootRealPath);

  async function walk(dir: string, relativePrefix: string = ""): Promise<void> {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      // Skip volatile FTDC, lock files, temp/staging dirs, and sockets
      if (
        entry.name === "mongod.lock" ||
        entry.name === "WiredTiger.lock" ||
        entry.name === "ongoingBackup.lock" ||
        entry.name === "diagnostic.data" ||
        entry.name === "_tmp" ||
        entry.name.startsWith(".mongovault-") ||
        entry.name.endsWith(".sock")
      ) {
        continue;
      }

      const fullPath = path.join(dir, entry.name);
      const relName = path.join(relativePrefix, entry.name).replace(/\\/g, "/");

      if (entry.isDirectory()) {
        const realDir = await fs.realpath(fullPath).catch(() => path.resolve(fullPath));
        if (visitedDirs.has(realDir)) continue;
        visitedDirs.add(realDir);
        await walk(fullPath, relName);
      } else if (entry.isSymbolicLink()) {
        let stat;
        try {
          stat = await fs.stat(fullPath);
        } catch {
          continue; // Skip dangling symlinks
        }
        if (stat.isDirectory()) {
          const realDir = await fs.realpath(fullPath).catch(() => path.resolve(fullPath));
          if (visitedDirs.has(realDir)) continue;
          visitedDirs.add(realDir);
          await walk(fullPath, relName);
        } else if (stat.isFile()) {
          results.push({ name: relName, fullPath, size: stat.size });
        }
      } else if (entry.isFile()) {
        const stat = await fs.stat(fullPath);
        results.push({ name: relName, fullPath, size: stat.size });
      }
    }
  }

  await walk(dbPath);
  return results;
}

async function drainCursorDocs(
  cursor: unknown,
  options: { finite?: boolean } = {},
): Promise<Record<string, unknown>[]> {
  const docs: Record<string, unknown>[] = [];
  if (!cursor || typeof cursor !== "object") return docs;
  const c = cursor as {
    closed?: boolean;
    __firstDoc?: Record<string, unknown> | null;
    tryNext?: () => Promise<Record<string, unknown> | null>;
    toArray?: () => Promise<Record<string, unknown>[]>;
    hasNext?: () => Promise<boolean>;
    next?: () => Promise<Record<string, unknown> | null>;
    [Symbol.asyncIterator]?: () => AsyncIterator<Record<string, unknown>>;
  };

  if (c.__firstDoc !== undefined) {
    if (c.__firstDoc) {
      docs.push(c.__firstDoc);
    }
    delete c.__firstDoc;
    if (docs.length === 0) return docs;
  }

  if (options.finite) {
    if (typeof c[Symbol.asyncIterator] === "function") {
      for await (const doc of c as AsyncIterable<Record<string, unknown>>) {
        if (!doc) break;
        docs.push(doc);
      }
    } else if (typeof c.toArray === "function") {
      const arr = await c.toArray();
      if (Array.isArray(arr)) docs.push(...arr);
    } else if (typeof c.hasNext === "function" && typeof c.next === "function") {
      while (await c.hasNext()) {
        const doc = await c.next();
        if (!doc) break;
        docs.push(doc);
      }
    } else if (typeof c.next === "function") {
      while (true) {
        const doc = await c.next();
        if (!doc) break;
        docs.push(doc);
      }
    }
    return docs;
  }

  if (typeof c.tryNext === "function") {
    while (!c.closed) {
      try {
        const doc = await c.tryNext();
        if (!doc) break;
        docs.push(doc);
      } catch (err) {
        if (
          err instanceof MongoCursorExhaustedError ||
          (err as { name?: string })?.name === "MongoCursorExhaustedError"
        ) {
          break;
        }
        throw err;
      }
    }
  } else if (typeof c.hasNext === "function" && typeof c.next === "function") {
    while (!c.closed && (await c.hasNext())) {
      const doc = await c.next();
      if (!doc) break;
      docs.push(doc);
    }
  } else if (typeof c[Symbol.asyncIterator] === "function") {
    for await (const doc of c as AsyncIterable<Record<string, unknown>>) {
      if (!doc) break;
      docs.push(doc);
    }
  } else if (typeof c.next === "function") {
    while (!c.closed) {
      const doc = await c.next();
      if (!doc) break;
      docs.push(doc);
    }
  }
  return docs;
}

async function getMajorityCommittedTs(client: MongoClient): Promise<OplogTimestamp | null> {
  try {
    const status = (await client.db("admin").command({ replSetGetStatus: 1 })) as {
      optimes?: {
        lastCommittedOpTime?: { ts?: unknown };
        appliedOpTime?: { ts?: unknown };
      };
    };
    const committed = fromBsonTimestamp(status?.optimes?.lastCommittedOpTime?.ts);
    const applied = fromBsonTimestamp(status?.optimes?.appliedOpTime?.ts);
    if (committed && applied) {
      return compareTimestamps(committed, applied) <= 0 ? committed : applied;
    }
    return committed ?? applied;
  } catch {
    return null;
  }
}

function isBackupCursorUnsupportedError(err: unknown): boolean {
  const e = err as { code?: unknown; message?: string } | null;
  if (!e) return false;
  if (typeof e.code === "number") {
    return e.code === 59 || e.code === 40324 || e.code === 115;
  }
  const msg = String(e.message ?? "");
  if (/not authorized|unauthorized|auth|checkpoint|conflict|econn|etimedout|network/i.test(msg)) {
    return false;
  }
  return /\$backupcursor unsupported|command \$backupcursor not found|unrecognized pipeline stage|no cursor/i.test(
    msg,
  );
}

async function assertSameDbPath(
  explicitPath: string,
  reportedPath: string,
  sourceLabel: string,
): Promise<void> {
  const canonExplicit = await fs.realpath(explicitPath).catch(() => path.resolve(explicitPath));
  const canonReported = await fs.realpath(reportedPath).catch(() => path.resolve(reportedPath));
  if (canonExplicit !== canonReported) {
    throw new Error(
      `Physical backup dbPath mismatch: configured dbPath "${explicitPath}" (${canonExplicit}) does not match ${sourceLabel} dbPath "${reportedPath}" (${canonReported}). MongoVault must run co-located on the target mongod host.`,
    );
  }
}

/**
 * Executes a Physical Hot Backup of MongoDB WiredTiger files straight to S3/Storage.
 */
export async function runPhysicalBackup({
  config,
  storage,
  logger,
  signal,
  dbPath: overrideDbPath,
  fsyncLockFallback,
  allowPrimaryFsyncLock,
  fsyncLockTimeoutMs,
  createMongoClient = (u) => new MongoClient(u),
}: PhysicalBackupOptions): Promise<PhysicalBackupResult> {
  const startedAt = new Date();
  const encrypted = Boolean(config.encryptionKey);
  if (config.requireEncryption && !encrypted) {
    throw new Error(
      "Refusing to create unencrypted physical backup: REQUIRE_ENCRYPTION is enabled but ENCRYPTION_KEY is not configured",
    );
  }
  if (!isClusterScope(config.mongo.database)) {
    throw new Error(
      `Physical backup captures the entire MongoDB instance and requires cluster scope (database: "*" or "all"), got "${config.mongo.database}"`,
    );
  }
  const effectiveFsyncFallback =
    fsyncLockFallback ?? Boolean(config.physical?.allowFsyncLock ?? false);
  const effectiveAllowPrimaryFsyncLock =
    allowPrimaryFsyncLock ?? Boolean(config.physical?.allowPrimaryFsyncLock ?? false);
  const effectiveFsyncLockTimeoutMs =
    fsyncLockTimeoutMs ?? config.physical?.fsyncLockTimeoutMs ?? DEFAULT_FSYNC_LOCK_TIMEOUT_MS;

  const scope = scopeOf(config);
  const key = archiveKey(scope, startedAt, { encrypted }).replace(
    /\.archive\.gz(\.enc)?$/,
    ".physical.tar.gz$1",
  );
  const log = logger.child({ key, engine: "physical" });
  const controller = linkedController(signal);

  if (!encrypted) {
    log.warn(
      "ENCRYPTION_KEY is not configured; physical backup archive and manifest will be stored unencrypted and unsigned",
    );
  }
  log.info({ uri: config.mongo.uri }, "Initiating physical hot backup");

  const discoveryUri = withUriReadPreference(config.mongo.uri, config.mongo.readPreference);
  const readPrefOpt = config.mongo.readPreference
    ? { readPreference: config.mongo.readPreference }
    : undefined;
  let targetUri = discoveryUri;
  let client = createMongoClient(discoveryUri);
  let pinned = false;
  let locked = false;
  let cursor: ReturnType<ReturnType<MongoClient["db"]>["aggregate"]> | null = null;
  let keepAliveTimer: NodeJS.Timeout | null = null;
  let fsyncWatchdogTimer: NodeJS.Timeout | null = null;
  let watchdogError: Error | null = null;
  let uploadCompleted = false;
  let backupError: unknown = null;
  let unlockFailure: FsyncUnlockError | null = null;

  let signalReceived: NodeJS.Signals | null = null;
  const onProcessSignal = (sig: NodeJS.Signals) => {
    if (signalReceived) return;
    signalReceived = sig;
    log.error(
      { signal: sig },
      "Received process signal while fsyncLock is held; aborting physical backup to release write lock",
    );
    controller.abort(new Error(`Physical backup interrupted by ${sig} while fsyncLock was held`));
  };
  const onSigint = () => onProcessSignal("SIGINT");
  const onSigterm = () => onProcessSignal("SIGTERM");
  let signalHandlersAttached = false;
  const attachSignalHandlers = () => {
    if (signalHandlersAttached) return;
    process.on("SIGINT", onSigint);
    process.on("SIGTERM", onSigterm);
    signalHandlersAttached = true;
  };
  const cleanupSignalHandlers = () => {
    if (!signalHandlersAttached) return;
    process.removeListener("SIGINT", onSigint);
    process.removeListener("SIGTERM", onSigterm);
    signalHandlersAttached = false;
  };

  try {
    await client.connect();
    let admin = client.db("admin");

    // Discover topology and pin all physical backup commands to a single node
    let helloError: unknown = null;
    let hello = (await admin.command({ hello: 1 }, readPrefOpt).catch((err) => {
      helloError = err;
      return null;
    })) as {
      ok?: number;
      setName?: string;
      isWritablePrimary?: boolean;
      hosts?: string[];
      me?: string;
      errmsg?: string;
    } | null;

    if (
      hello?.setName &&
      typeof hello.me === "string" &&
      hello.me.trim().length > 0 &&
      !uriAlreadyPinnedTo(discoveryUri, hello.me.trim())
    ) {
      const pinnedUri = buildDirectNodeUri(config.mongo.uri, hello.me.trim());
      await client.close().catch(() => {});
      client = createMongoClient(pinnedUri);
      targetUri = pinnedUri;
      pinned = true;
      await client.connect();
      admin = client.db("admin");
      helloError = null;
      hello = (await admin.command({ hello: 1 }).catch((err) => {
        helloError = err;
        return null;
      })) as typeof hello;
    }

    const explicitDbPath = overrideDbPath || config.physical?.dbPath;
    let resolvedDbPath = explicitDbPath;

    // Gather MongoDB version & WiredTiger storage options (non-fatal)
    const [buildInfo, cmdOpts] = await Promise.all([
      admin.command({ buildInfo: 1 }).catch(() => null) as Promise<{ version?: string } | null>,
      admin.command({ getCmdLineOpts: 1 }).catch(() => null) as Promise<Record<
        string,
        unknown
      > | null>,
    ]);
    const mongoVersion = typeof buildInfo?.version === "string" ? buildInfo.version : undefined;
    const storageOptions = extractStorageOptionsFromCmdOpts(cmdOpts);
    const cmdOptsDbPath = extractDbPathFromCmdOpts(cmdOpts);

    if (explicitDbPath && cmdOptsDbPath) {
      await assertSameDbPath(explicitDbPath, cmdOptsDbPath, "getCmdLineOpts");
    }

    let cursorMeta: BackupCursorMetadata | null = null;
    const dataFiles: Array<{ name: string; fullPath: string; size: number }> = [];
    const journalFiles: Array<{ name: string; fullPath: string; size: number }> = [];
    let filesToArchive: Array<{ name: string; fullPath: string; size: number }> = [];

    // 1. Open $backupCursor directly (single open — never open twice)
    try {
      cursor = await createBackupCursorWithRetries(admin, CURSOR_CREATE_RETRIES, controller.signal);
      const initialDocs = await drainCursorDocs(cursor);
      for (const doc of initialDocs) {
        if (doc.metadata) {
          cursorMeta = doc.metadata as BackupCursorMetadata;
          if (explicitDbPath && typeof cursorMeta.dbpath === "string" && cursorMeta.dbpath) {
            await assertSameDbPath(explicitDbPath, cursorMeta.dbpath, "$backupCursor metadata");
          }
          if (!resolvedDbPath && typeof cursorMeta.dbpath === "string") {
            resolvedDbPath = cursorMeta.dbpath;
          }
        }
      }

      if (!resolvedDbPath) {
        resolvedDbPath = cmdOptsDbPath;
      }

      if (resolvedDbPath) {
        const realResolvedDbPath = await fs.realpath(resolvedDbPath).catch(() => resolvedDbPath);
        for (const doc of initialDocs) {
          const fileDoc = doc as unknown as BackupCursorFile;
          if (fileDoc?.filename) {
            const relName = trimDbPathPrefix(fileDoc.filename, resolvedDbPath, realResolvedDbPath);
            const fullPath = path.join(resolvedDbPath, relName);
            let size = fileDoc.fileLen || fileDoc.fileSize || fileDoc.length || 0;
            if (!size) {
              try {
                size = (await fs.stat(fullPath)).size;
              } catch {}
            }
            const targetList = relName.startsWith("journal/") ? journalFiles : dataFiles;
            if (!targetList.some((f) => f.name === relName)) {
              targetList.push({ name: relName, fullPath, size });
            }
          }
        }

        // Include storage.bson in dataFiles
        const storageBsonPath = path.join(resolvedDbPath, "storage.bson");
        try {
          const stat = await fs.stat(storageBsonPath);
          if (!dataFiles.some((f) => f.name === "storage.bson")) {
            dataFiles.push({
              name: "storage.bson",
              fullPath: storageBsonPath,
              size: stat.size,
            });
          }
        } catch {}
      }

      // Keep cursor alive during long uploads; if keep-alive fails, abort backup immediately
      // so we never upload torn/unpinned WiredTiger files.
      let keepAliveInFlight = false;
      keepAliveTimer = setInterval(() => {
        if (
          uploadCompleted ||
          controller.signal.aborted ||
          keepAliveInFlight ||
          !cursor ||
          (cursor as { closed?: boolean }).closed
        ) {
          return;
        }
        const c = cursor as {
          tryNext?: () => Promise<unknown>;
          hasNext?: () => Promise<boolean>;
        };
        const ping =
          typeof c.tryNext === "function"
            ? c.tryNext()
            : typeof c.hasNext === "function"
              ? c.hasNext()
              : null;
        if (!ping) return;
        keepAliveInFlight = true;
        ping
          .catch((err) => {
            if (uploadCompleted || controller.signal.aborted) return;
            const msg = err instanceof Error ? err.message : String(err);
            log.error({ err }, "$backupCursor keep-alive failed; aborting physical backup");
            controller.abort(new Error(`$backupCursor keep-alive failed: ${msg}`));
          })
          .finally(() => {
            keepAliveInFlight = false;
          });
      }, 45_000);
      keepAliveTimer.unref();
    } catch (cursorErr) {
      cursorMeta = null;
      if (cursor) {
        await cursor.close().catch(() => {});
        cursor = null;
      }
      if (!isBackupCursorUnsupportedError(cursorErr)) {
        throw cursorErr;
      }
      if (!effectiveFsyncFallback) {
        throw new Error(
          "Target MongoDB does not support $backupCursor and fsyncLock fallback is disabled (set PHYSICAL_ALLOW_FSYNC_LOCK=true or fsyncLockFallback: true to explicitly opt in to write-blocking fsyncLock backups)",
          { cause: cursorErr },
        );
      }
      log.warn({ err: cursorErr }, "Could not establish $backupCursor; falling back to fsyncLock");
    }

    if (!resolvedDbPath) {
      resolvedDbPath = cmdOptsDbPath;
    }

    if (!resolvedDbPath) {
      throw new Error(
        "Could not detect MongoDB dbPath for physical backup. Please specify PHYSICAL_DB_PATH in configuration.",
      );
    }

    const activeDbPath = resolvedDbPath;
    const realActiveDbPath = await fs.realpath(activeDbPath).catch(() => activeDbPath);

    // Fallback to fsyncLock ONLY when explicitly opted in and $backupCursor is unavailable
    let fsyncOplogTs: string | undefined;
    if (!cursorMeta && dataFiles.length === 0) {
      if (cursor) {
        await cursor.close().catch(() => {});
        cursor = null;
      }
      if (keepAliveTimer) {
        clearInterval(keepAliveTimer);
        keepAliveTimer = null;
      }
      if (!effectiveFsyncFallback) {
        throw new Error(
          "Target MongoDB does not support $backupCursor and fsyncLock fallback is disabled (set PHYSICAL_ALLOW_FSYNC_LOCK=true or fsyncLockFallback: true to explicitly opt in to write-blocking fsyncLock backups)",
        );
      }

      if (!effectiveAllowPrimaryFsyncLock && (helloError !== null || !hello || hello.ok === 0)) {
        const detail =
          helloError instanceof Error
            ? helloError.message
            : (hello?.errmsg ?? (helloError ? String(helloError) : "empty or ok=0 hello response"));
        throw new Error(
          `Refusing fsyncLock fallback: failed to verify replica set role via hello (${detail})`,
          helloError ? { cause: helloError } : undefined,
        );
      }

      if (
        hello?.setName &&
        hello?.isWritablePrimary &&
        Array.isArray(hello?.hosts) &&
        hello.hosts.length > 1
      ) {
        if (!effectiveAllowPrimaryFsyncLock) {
          throw new Error(
            `Refusing to acquire write-blocking fsyncLock on multi-node replica set PRIMARY (${hello.setName}). Target a secondary node (MONGODB_READ_PREFERENCE=secondary) or explicitly set PHYSICAL_ALLOW_PRIMARY_FSYNC_LOCK=true if you accept write downtime and failover lock risks.`,
          );
        }
        log.warn(
          { setName: hello.setName, hosts: hello.hosts },
          "Running opt-in fsyncLock physical backup on a multi-node replica set PRIMARY (blocks all cluster writes)",
        );
      }

      log.info("Acquiring fsyncLock on MongoDB cluster...");
      await admin.command(
        { fsync: 1, lock: true },
        !pinned && readPrefOpt ? readPrefOpt : undefined,
      );
      locked = true;
      attachSignalHandlers();

      // Start watchdog timer so fsyncLock is never held indefinitely if upload stalls
      if (effectiveFsyncLockTimeoutMs > 0) {
        fsyncWatchdogTimer = setTimeout(() => {
          if (!locked || uploadCompleted) return;
          const msg = `Physical backup fsyncLock watchdog triggered after ${effectiveFsyncLockTimeoutMs}ms; aborting backup to release write lock`;
          log.error({ timeoutMs: effectiveFsyncLockTimeoutMs }, msg);
          watchdogError = new Error(msg);
          controller.abort(watchdogError);
        }, effectiveFsyncLockTimeoutMs);
        fsyncWatchdogTimer.unref();
      }

      // Warn if any in-progress/prepared multi-document transactions exist under fsyncLock
      try {
        const activeTxns = await client
          .db("config")
          .collection("transactions")
          .countDocuments({ state: { $in: ["inProgress", "prepared"] } });
        if (activeTxns > 0) {
          log.warn(
            { activeTxns },
            "Detected in-progress/prepared distributed transactions during fsyncLock backup; those transactions will be rolled back on restore",
          );
        }
      } catch {
        // Standalone or no config.transactions
      }

      // Capture exact oplog position while write-locked so fsyncLock backups support PITR
      try {
        const newestDoc = await client
          .db("local")
          .collection("oplog.rs")
          .find({}, { projection: { ts: 1 } })
          .sort({ $natural: -1 })
          .limit(1)
          .next();
        const ts = fromBsonTimestamp(newestDoc?.ts);
        if (ts) {
          fsyncOplogTs = formatOplogTimestamp(ts);
        }
      } catch {
        // Standalone instance without oplog.rs
      }

      const scanned = await scanWiredTigerFiles(activeDbPath);
      for (const f of scanned) {
        if (f.name.startsWith("journal/")) {
          journalFiles.push(f);
        } else {
          dataFiles.push(f);
        }
      }
      log.info(
        { filesCount: scanned.length, fsyncOplogTs },
        "Scanned WiredTiger data files under lock",
      );
    }

    if (dataFiles.length === 0 && journalFiles.length === 0) {
      throw new Error(`No WiredTiger files found under dbPath: ${activeDbPath}`);
    }

    // Stream .wt data files first, then call $backupCursorExtend
    // to extend the pinned journal set up to majority committed opTime, and stream
    // ONLY the cursor-pinned journal files (never blind fs.readdir when extended).
    let extendedToTs: OplogTimestamp | null = null;
    const collectJournalFilesAfterDataCopy = async () => {
      if (cursorMeta) {
        const cursorEndTs =
          fromBsonTimestamp(cursorMeta.oplogEnd?.ts) ??
          fromBsonTimestamp(cursorMeta.checkpointTimestamp);
        const majorityTs = await getMajorityCommittedTs(client);
        let extendTargetTs = cursorEndTs;
        if (majorityTs && cursorEndTs && compareTimestamps(majorityTs, cursorEndTs) > 0) {
          extendTargetTs = majorityTs;
        } else if (majorityTs && !extendTargetTs) {
          extendTargetTs = majorityTs;
        }

        if (cursorMeta.backupId && extendTargetTs) {
          try {
            const bsonTarget = new Timestamp({ t: extendTargetTs.t, i: extendTargetTs.i });
            const extCursor = admin.aggregate([
              {
                $backupCursorExtend: {
                  backupId: cursorMeta.backupId,
                  timestamp: bsonTarget,
                },
              },
            ]);
            const extDocs = await drainCursorDocs(extCursor, { finite: true });
            for (const doc of extDocs) {
              const jDoc = doc as unknown as BackupCursorFile;
              if (jDoc?.filename) {
                const relName = trimDbPathPrefix(jDoc.filename, activeDbPath, realActiveDbPath);
                const fullPath = path.join(activeDbPath, relName);
                let size = jDoc.fileLen || jDoc.fileSize || jDoc.length || 0;
                if (!size) {
                  try {
                    size = (await fs.stat(fullPath)).size;
                  } catch {}
                }
                if (
                  !journalFiles.some((f) => f.name === relName) &&
                  !dataFiles.some((f) => f.name === relName)
                ) {
                  journalFiles.push({ name: relName, fullPath, size });
                }
              }
            }
            await extCursor.close().catch(() => {});
            extendedToTs = extendTargetTs;
          } catch (extErr) {
            const msg = extErr instanceof Error ? extErr.message : String(extErr);
            throw new Error(
              `$backupCursorExtend failed; refusing to produce physical backup with incomplete journal coverage: ${msg}`,
              { cause: extErr },
            );
          }
        } else if (!cursorMeta.oplogEnd?.ts) {
          extendedToTs = fromBsonTimestamp(cursorMeta.checkpointTimestamp);
        }
      }
      filesToArchive = [...dataFiles, ...journalFiles];
    };

    const updateEntrySize = (entry: { size: number }, actualSize: number) => {
      entry.size = actualSize;
    };

    async function* generateOrderedPhysicalTar(): AsyncGenerator<Buffer, void, unknown> {
      // Phase 1: Stream all .wt data files first (without EOF marker)
      yield* generateTarStream(dataFiles, controller.signal, {
        includeEof: false,
        onEntrySize: updateEntrySize,
      });
      // Phase 2: Extend backup cursor and stream pinned journal files last (with EOF marker)
      await collectJournalFilesAfterDataCopy();
      yield* generateTarStream(journalFiles, controller.signal, {
        includeEof: true,
        onEntrySize: updateEntrySize,
      });
    }

    const initialRawBytes = [...dataFiles, ...journalFiles].reduce((acc, f) => acc + f.size, 0);
    log.info(
      { dataFilesCount: dataFiles.length, initialRawBytes: formatBytes(initialRawBytes) },
      "Starting streaming physical pack and upload (data files -> journal files)",
    );

    const gzip = createGzip();
    const encryptor =
      encrypted && config.encryptionKey ? createEncryptor(config.encryptionKey) : null;
    const digest = new DigestStream();
    const body = new PassThrough({ highWaterMark: 1024 * 1024 });

    const tarSource = Readable.from(generateOrderedPhysicalTar());
    const stages = [tarSource, gzip, encryptor?.stream, digest, body];

    await settleAll(controller, [
      storage.upload(key, body, {
        contentType: encrypted ? "application/octet-stream" : "application/gzip",
        expectedSizeBytes: initialRawBytes > 0 ? initialRawBytes : undefined,
        signal: controller.signal,
        onProgress: ({ loaded }) =>
          log.debug({ uploadedBytes: loaded }, "Physical upload progress"),
      }),
      runStreamPipeline(stages, { signal: controller.signal }),
    ]);

    uploadCompleted = true;

    if (typeof storage.head === "function") {
      const stored = await storage.head(key);
      if (stored && stored.size !== digest.bytes) {
        throw new Error(
          `Physical backup upload verification failed: expected ${digest.bytes} bytes, found ${stored.size}`,
        );
      }
    }

    // Unlock MongoDB write lock if acquired and verify unlock succeeded BEFORE finalizing manifest.
    // Never swallow unlock failures: if the node remains write-locked, propagate FsyncUnlockError.
    if (locked) {
      if (fsyncWatchdogTimer) {
        clearTimeout(fsyncWatchdogTimer);
        fsyncWatchdogTimer = null;
      }
      await releaseFsyncLockWithRetry({
        client,
        uri: targetUri,
        createMongoClient,
        logger: log,
        readPreference: !pinned ? config.mongo.readPreference : undefined,
      });
      locked = false;
      cleanupSignalHandlers();
    }

    const completedAt = new Date();
    const durationMs = completedAt.getTime() - startedAt.getTime();

    const physicalMeta: PhysicalBackupMeta = {
      dbPath: resolvedDbPath,
      cursorType: cursorMeta ? "$backupCursor" : "fsyncLock",
      ...(mongoVersion ? { mongoVersion } : {}),
      ...(storageOptions ? { storageOptions } : {}),
      filesCount: filesToArchive.length,
      files: filesToArchive.map((f) => ({ name: f.name, size: f.size })),
      checkpointTimestamp: cursorMeta?.checkpointTimestamp
        ? formatOplogTimestamp(cursorMeta.checkpointTimestamp)
        : undefined,
      oplogStart: cursorMeta?.oplogStart?.ts
        ? formatOplogTimestamp(cursorMeta.oplogStart.ts)
        : undefined,
      oplogEnd: extendedToTs
        ? formatOplogTimestamp(extendedToTs)
        : cursorMeta?.oplogEnd?.ts
          ? formatOplogTimestamp(cursorMeta.oplogEnd.ts)
          : undefined,
    };

    const effectiveStartTs =
      physicalMeta.oplogStart || physicalMeta.checkpointTimestamp || fsyncOplogTs;
    const effectiveEndTs =
      (extendedToTs ? formatOplogTimestamp(extendedToTs) : undefined) ||
      physicalMeta.oplogEnd ||
      physicalMeta.checkpointTimestamp ||
      fsyncOplogTs;

    const manifest: BackupManifest = {
      version: MANIFEST_VERSION,
      type: "physical-hot-backup",
      engine: "physical",
      key,
      database: config.mongo.database,
      createdAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs,
      size: digest.bytes,
      sha256: digest.digest(),
      compression: "gzip",
      encryption: encryptor?.envelope() ?? null,
      oplog: Boolean(effectiveEndTs),
      ...(effectiveStartTs && effectiveEndTs
        ? {
            oplogWindow: {
              startTs: effectiveStartTs,
              endTs: effectiveEndTs,
            },
          }
        : {}),
      excludedCollections: [],
      collections: {},
      physical: physicalMeta,
    };

    await writeManifest(storage, manifest, config.encryptionKey);

    log.info(
      {
        size: formatBytes(manifest.size),
        duration: formatDuration(durationMs),
        files: filesToArchive.length,
      },
      "Physical hot backup completed successfully",
    );

    return { manifest, filesCount: filesToArchive.length, totalBytes: digest.bytes };
  } catch (error) {
    backupError = watchdogError ?? error;
    if (typeof storage.remove === "function") {
      await storage.remove(key).catch(() => {});
    }
  } finally {
    uploadCompleted = true;
    if (keepAliveTimer) clearInterval(keepAliveTimer);
    if (fsyncWatchdogTimer) clearTimeout(fsyncWatchdogTimer);
    if (cursor) {
      await cursor.close().catch(() => {});
    }
    if (locked && !(backupError instanceof FsyncUnlockError)) {
      try {
        await releaseFsyncLockWithRetry({
          client,
          uri: targetUri,
          createMongoClient,
          logger: log,
          readPreference: !pinned ? config.mongo.readPreference : undefined,
        });
        locked = false;
      } catch (err) {
        unlockFailure =
          err instanceof FsyncUnlockError
            ? new FsyncUnlockError(err.message, {
                unlockError: err.unlockError,
                cause: backupError ?? err.cause,
              })
            : new FsyncUnlockError(
                `CRITICAL: Failed to release MongoDB fsyncLock during cleanup (${err instanceof Error ? err.message : String(err)}). Run \`db.adminCommand({ fsyncUnlock: 1 })\` manually.`,
                { unlockError: err, cause: backupError },
              );
      }
    }
    cleanupSignalHandlers();
    await client.close().catch(() => {});
    controller.dispose();
  }

  if (unlockFailure) {
    throw unlockFailure;
  }
  throw backupError;
}

async function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close((err) => (err ? reject(err) : resolve(port)));
    });
    srv.on("error", reject);
  });
}

async function probeLocalMongodVersion(): Promise<string | null> {
  return new Promise((resolve) => {
    const child = spawn("mongod", ["--version"], { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    child.stdout?.on("data", (d) => {
      out += String(d);
    });
    child.once("error", () => resolve(null));
    child.once("close", (code) => {
      if (code !== 0) return resolve(null);
      const match = /db version\s+v?([\d.]+)/i.exec(out);
      resolve(match ? match[1]! : null);
    });
  });
}

function assertCompatibleMongoVersion(backupVersion: string, localVersion: string): void {
  const bParts = backupVersion.split(".").map((n) => parseInt(n, 10));
  const lParts = localVersion.split(".").map((n) => parseInt(n, 10));
  if (bParts[0] !== lParts[0] || bParts[1] !== lParts[1]) {
    throw new Error(
      `MongoDB major.minor version mismatch for physical restore: backup was taken on ${backupVersion}, local mongod is ${localVersion}`,
    );
  }
}

async function computeDirectoryBytes(dir: string): Promise<number> {
  let total = 0;
  try {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        total += await computeDirectoryBytes(fullPath);
      } else if (entry.isFile()) {
        const stat = await fs.stat(fullPath).catch(() => null);
        if (stat) total += stat.size;
      }
    }
  } catch {
    return 0;
  }
  return total;
}

async function waitForMongodReady(
  uri: string,
  timeoutMs = 60_000,
  hasExited?: () => boolean,
  logPath?: string,
  signal?: AbortSignal,
): Promise<MongoClient> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown = null;
  while (Date.now() < deadline) {
    if (signal?.aborted) {
      throw makeAbortError(signal.reason);
    }
    if (hasExited?.()) {
      let logTail = "";
      if (logPath) {
        try {
          const raw = await fs.readFile(logPath, "utf8");
          logTail = raw.trim().split("\n").slice(-40).join("\n");
        } catch {}
      }
      throw new Error(
        `Temporary mongod exited unexpectedly before becoming ready${logTail ? `:\n${logTail}` : ""}`,
      );
    }
    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 2000 });
    try {
      const connectAndPing = (async () => {
        await client.connect();
        await client.db("admin").command({ ping: 1 });
      })();
      if (signal) {
        await new Promise<void>((resolve, reject) => {
          const onAbort = () => reject(makeAbortError(signal.reason));
          if (signal.aborted) {
            reject(makeAbortError(signal.reason));
            return;
          }
          signal.addEventListener("abort", onAbort, { once: true });
          connectAndPing.then(
            () => {
              signal.removeEventListener("abort", onAbort);
              resolve();
            },
            (err) => {
              signal.removeEventListener("abort", onAbort);
              reject(err);
            },
          );
        });
      } else {
        await connectAndPing;
      }
      return client;
    } catch (err) {
      await client.close().catch(() => {});
      if (signal?.aborted || (err instanceof Error && err.name === "AbortError")) {
        throw err;
      }
      lastErr = err;
      await abortableSleep(300, signal);
    }
  }
  throw new Error(
    `Temporary mongod did not become ready within ${timeoutMs}ms: ${lastErr instanceof Error ? lastErr.message : String(lastErr)}`,
  );
}

export async function waitForLockFileCleared(
  dbPath: string,
  timeoutMs = 15_000,
  signal?: AbortSignal,
): Promise<void> {
  const lockPath = path.join(dbPath, "mongod.lock");
  const deadline = Date.now() + timeoutMs;
  do {
    if (signal?.aborted) {
      throw makeAbortError(signal.reason);
    }
    try {
      const stat = await fs.stat(lockPath);
      if (stat.size === 0) return;
    } catch (err: unknown) {
      const code = (err as { code?: string })?.code;
      if (code === "ENOENT") return;
      throw err;
    }
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    await abortableSleep(Math.min(200, remaining), signal);
  } while (Date.now() < deadline);

  throw new Error(`Timed out after ${timeoutMs}ms waiting for mongod.lock to clear in ${dbPath}`);
}

/**
 * Coordinates multi-step WiredTiger & Replica Set physical recovery:
 * 1. Prepare `local.replset.minvalid` (`Timestamp(0, 1)`), clear `local.replset.election`,
 *    and set `local.replset.oplogTruncateAfterPoint` (for `$backupCursor` snapshots).
 * 2. Boot with `recoverFromOplogAsStandalone=true` + `takeUnstableCheckpointOnShutdown=true` so WiredTiger
 *    replays the pinned journal + oplog to the exact consistent point.
 * 3. Replay PITR oplog slices via streaming `OplogApplier` + `buildPendingIndexes({ standalone: true })`.
 * 4. Clean stale RS/cluster state (`local.system.replset`, `config.cache.*`, `config.system.sessions`,
 *    `config.transactions`, `config.rangeDeletions`), `fsync`, and gracefully shut down (`force: false`).
 *    Note: Because `local.system.replset` is cleared, multi-node replica sets must be restarted with
 *    `--replSet`, re-initialized via `rs.initiate()`, and secondaries resynced (or restored onto each node).
 */
export class PhysicalRecoveryCoordinator {
  private async flushCheckpoint(client: MongoClient, step: string): Promise<void> {
    const res = await client.db("admin").command({ fsync: 1 });
    if (!res || res.ok !== 1) {
      throw new Error(`WiredTiger fsync checkpoint failed during ${step}: ${JSON.stringify(res)}`);
    }
  }

  async run(
    ctx: PhysicalRecoveryContext,
  ): Promise<{ replayedEntries: number; oplogStats?: OplogApplierStats }> {
    const { config, storage, logger, targetDbPath, manifest, pitrPlan, signal } = ctx;
    if (signal?.aborted) {
      throw makeAbortError(signal.reason);
    }

    if (config.physical?.encryptionKeyFile) {
      try {
        await fs.access(config.physical.encryptionKeyFile);
      } catch (err) {
        throw new Error(
          `Configured physical encryptionKeyFile is missing or unreadable: ${config.physical.encryptionKeyFile}`,
          { cause: err },
        );
      }
    }

    const isBackupCursor = manifest?.physical?.cursorType === "$backupCursor";
    const restoreToStr =
      manifest?.oplogWindow?.endTs ??
      manifest?.physical?.oplogEnd ??
      manifest?.physical?.checkpointTimestamp;
    const restoreToTs = restoreToStr ? parseOplogTimestamp(restoreToStr) : null;

    const baseArgs = this.buildBaseMongodArgs(config, manifest, targetDbPath);
    let replayedEntries = 0;
    let oplogStats: OplogApplierStats | undefined;

    if (isBackupCursor && restoreToTs) {
      // Step 1: Prepare recovery markers (minvalid + oplogTruncateAfterPoint + clear stale election)
      logger.info(
        { restoreToTs: formatOplogTimestamp(restoreToTs) },
        "Physical recovery step 1/3: writing WiredTiger recovery markers",
      );
      await this.runWithEphemeralMongod(
        config.workDir,
        targetDbPath,
        baseArgs,
        async (client) => {
          const localDb = client.db("local");
          await localDb
            .collection("system.replset")
            .deleteMany({})
            .catch(() => {});
          await localDb
            .collection("replset.minvalid")
            .deleteMany({})
            .catch(() => {});
          await localDb
            .collection("replset.oplogTruncateAfterPoint")
            .deleteMany({})
            .catch(() => {});
          await localDb
            .collection("replset.election")
            .deleteMany({})
            .catch(() => {});

          const bsonTarget = new Timestamp({ t: restoreToTs.t, i: restoreToTs.i });
          await localDb.collection("replset.minvalid").insertOne({
            _id: new ObjectId(),
            t: -1,
            ts: new Timestamp({ t: 0, i: 1 }),
          });
          await localDb.collection("replset.oplogTruncateAfterPoint").insertOne({
            _id: "oplogTruncateAfterPoint" as unknown as ObjectId,
            oplogTruncateAfterPoint: bsonTarget,
          });
          await this.flushCheckpoint(client, "step 1/3 (recovery markers)");
        },
        signal,
      );

      // Step 2: Recover from oplog as standalone (replays journal + oplog to restoreToTs)
      logger.info(
        { restoreToTs: formatOplogTimestamp(restoreToTs) },
        "Physical recovery step 2/3: running recoverFromOplogAsStandalone",
      );
      await this.runWithEphemeralMongod(
        config.workDir,
        targetDbPath,
        [
          ...baseArgs,
          "--setParameter=recoverFromOplogAsStandalone=true",
          "--setParameter=takeUnstableCheckpointOnShutdown=true",
        ],
        async (client) => {
          await this.flushCheckpoint(client, "step 2/3 (recoverFromOplogAsStandalone)");
        },
        signal,
      );
    }

    // Step 3 (+ 4): Replay PITR slices (if any) via streaming OplogApplier, then reset RS state
    const hasPitrSlices = Boolean(pitrPlan && pitrPlan.slices.length > 0);
    const step3Args = hasPitrSlices
      ? [...baseArgs, "--setParameter=takeUnstableCheckpointOnShutdown=true"]
      : baseArgs;
    logger.info(
      { slices: pitrPlan?.slices.length ?? 0 },
      pitrPlan
        ? "Physical recovery final step: replaying PITR oplog slices and resetting replica set state"
        : "Physical recovery final step: resetting replica set state and flushing clean checkpoint",
    );
    await this.runWithEphemeralMongod(
      config.workDir,
      targetDbPath,
      step3Args,
      async (client) => {
        if (pitrPlan && pitrPlan.slices.length > 0) {
          const applier = new OplogApplier({
            target: new MongoOplogTargetClient(client),
            workDir: config.workDir,
            logger,
            startExclusiveTs: pitrPlan.baseEndTs,
            endTs: pitrPlan.targetTs,
            backupType: "physical",
            unsafe: true,
          });

          try {
            for (const slice of pitrPlan.slices) {
              await streamOplogSlice({ config, storage, signal }, slice.key, (stream) =>
                applier.applyStream(stream, signal),
              );
            }
            await applier.buildPendingIndexes({ standalone: true });
            applier.assertComplete();
            oplogStats = applier.getStats();
            replayedEntries = oplogStats.appliedEntries;
          } finally {
            await applier.dispose();
          }
        }

        await this.resetReplicaSetState(client);
        await this.flushCheckpoint(client, "final recovery step");
      },
      signal,
    );

    return { replayedEntries, ...(oplogStats ? { oplogStats } : {}) };
  }

  private buildBaseMongodArgs(
    config: AppConfig,
    manifest: BackupManifest | null,
    targetDbPath: string,
  ): string[] {
    const storageOpts = manifest?.physical?.storageOptions;
    const args = [
      `--dbpath=${targetDbPath}`,
      "--bind_ip=127.0.0.1",
      "--setParameter=ttlMonitorEnabled=false",
      "--setParameter=disableLogicalSessionCacheRefresh=true",
      "--setParameter=skipShardingConfigurationChecks=true",
    ];
    if (storageOpts?.directoryPerDB || config.physical?.directoryPerDb) {
      args.push("--directoryperdb");
    }
    if (storageOpts?.directoryForIndexes) {
      args.push("--wiredTigerDirectoryForIndexes");
    }
    if (storageOpts?.journalCompressor) {
      args.push(`--wiredTigerJournalCompressor=${storageOpts.journalCompressor}`);
    }
    if (storageOpts?.blockCompressor) {
      args.push(`--wiredTigerCollectionBlockCompressor=${storageOpts.blockCompressor}`);
    }
    if (config.physical?.encryptionKeyFile) {
      args.push("--enableEncryption", `--encryptionKeyFile=${config.physical.encryptionKeyFile}`);
    }
    return args;
  }

  /**
   * Clears stale replica set and sharding metadata so the restored node can start standalone
   * or be cleanly re-initialized as a replica set via `rs.initiate()` (multi-node replica sets
   * must re-add/resync secondaries after recovery).
   */
  private async resetReplicaSetState(client: MongoClient): Promise<void> {
    const localDb = client.db("local");
    const configDb = client.db("config");

    // Note: MongoDB forbids dropping `local.system.replset` directly, so clear it via `deleteMany({})`
    await Promise.all([
      localDb
        .collection("system.replset")
        .deleteMany({})
        .catch(() => {}),
      localDb
        .collection("replset.minvalid")
        .deleteMany({})
        .catch(() => {}),
      localDb
        .collection("replset.oplogTruncateAfterPoint")
        .deleteMany({})
        .catch(() => {}),
      localDb
        .collection("replset.election")
        .deleteMany({})
        .catch(() => {}),
      configDb
        .collection("transactions")
        .drop()
        .catch(() => {}),
      configDb
        .collection("rangeDeletions")
        .drop()
        .catch(() => {}),
    ]);

    // Drop config.cache.* metadata collections to ensure clean cluster state
    try {
      const cacheColls = await configDb
        .listCollections({ name: /^cache\./ }, { nameOnly: true })
        .toArray();
      for (const coll of cacheColls) {
        await configDb
          .collection(coll.name)
          .drop()
          .catch(() => {});
      }
    } catch {
      // Ignore if config db has no cache collections
    }

    // Retry dropping config.system.sessions (session reaper may briefly hold a lock)
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        await configDb.collection("system.sessions").drop();
        break;
      } catch (err: unknown) {
        const code = (err as { code?: number })?.code;
        if (code === 26) break; // NamespaceNotFound
        if (attempt < 4) {
          await new Promise((r) => setTimeout(r, 500));
        }
      }
    }
  }

  private async runWithEphemeralMongod(
    workDir: string,
    targetDbPath: string,
    baseArgs: string[],
    fn: (client: MongoClient) => Promise<void>,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) {
      throw makeAbortError(signal.reason);
    }
    await fs.mkdir(workDir, { recursive: true });
    const port = await findFreePort();
    const ephemeralUri = `mongodb://127.0.0.1:${port}`;
    const mongodLog = path.join(workDir, `.pitr-mongod-${randomUUID()}.log`);

    const proc = spawn("mongod", [...baseArgs, `--port=${port}`, `--logpath=${mongodLog}`], {
      stdio: "ignore",
    });

    const procExit = new Promise<void>((resolve) => {
      proc.once("close", () => resolve());
      proc.once("error", () => resolve());
    });

    let tmpClient: MongoClient | null = null;
    let abortedBySignal = false;
    const onAbort = () => {
      abortedBySignal = true;
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill("SIGTERM");
      }
      if (tmpClient) {
        void tmpClient.close().catch(() => {});
      }
    };
    signal?.addEventListener("abort", onAbort, { once: true });

    try {
      tmpClient = await waitForMongodReady(
        ephemeralUri,
        60_000,
        () => proc.exitCode !== null || proc.signalCode !== null,
        mongodLog,
        signal,
      );

      await fn(tmpClient);

      if (abortedBySignal || signal?.aborted) {
        throw makeAbortError(signal?.reason);
      }

      // Graceful shutdown (force: false) so WiredTiger writes a clean stable checkpoint.
      // MongoDB drops the connection immediately upon shutdown, so an expected socket-close error is normal;
      // cleanliness is verified by requiring the process to exit cleanly with code 0.
      await tmpClient
        .db("admin")
        .command({ shutdown: 1, force: false, timeoutSecs: 30 })
        .catch(() => {});
      await tmpClient.close().catch(() => {});
      tmpClient = null;
      await Promise.race([procExit, new Promise((r) => setTimeout(r, 35_000))]);
      if (proc.exitCode !== 0 || proc.signalCode !== null) {
        throw new Error(
          `Temporary recovery mongod did not shut down cleanly (exitCode=${String(proc.exitCode)}, signal=${String(proc.signalCode)}); WiredTiger checkpoint may be inconsistent`,
        );
      }
      await waitForLockFileCleared(targetDbPath, 15_000, signal);
    } finally {
      signal?.removeEventListener("abort", onAbort);
      if (tmpClient) {
        await tmpClient
          .db("admin")
          .command({ shutdown: 1, force: true })
          .catch(() => {});
        await tmpClient.close().catch(() => {});
      }
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill("SIGTERM");
        await Promise.race([procExit, new Promise((r) => setTimeout(r, 10_000))]);
        if (proc.exitCode === null && proc.signalCode === null) {
          proc.kill("SIGKILL");
        }
      }
      await fs.rm(mongodLog, { force: true }).catch(() => {});
    }
  }
}

async function assertTargetDbPathUnlocked(targetDbPath: string): Promise<void> {
  try {
    const lockStat = await fs.stat(path.join(targetDbPath, "mongod.lock"));
    if (lockStat.size > 0) {
      throw new Error(
        `Target dbPath ${targetDbPath} appears to be in use by a running mongod process (non-empty mongod.lock). Stop mongod before physical restore.`,
      );
    }
  } catch (err: unknown) {
    const code = (err as { code?: string })?.code;
    if (code === "ENOENT" || code === "ENOTDIR") {
      return;
    }
    throw err;
  }
}

function assertPhysicalManifestIntegrity(
  config: AppConfig,
  backupKey: string,
  manifestData: BackupManifest | null,
): asserts manifestData is BackupManifest {
  if (!manifestData) {
    throw new IntegrityError(
      `Missing backup manifest for physical backup ${backupKey}; refusing unverified restore`,
    );
  }
  if (manifestData.engine !== "physical") {
    throw new IntegrityError(
      `Backup ${backupKey} has engine "${String(manifestData.engine)}", expected "physical"`,
    );
  }
  if (!manifestData.sha256) {
    throw new IntegrityError(
      `Physical backup manifest for ${backupKey} is missing required sha256 digest`,
    );
  }
  if (config.requireEncryption && !manifestData.encryption) {
    throw new IntegrityError(
      `REQUIRE_ENCRYPTION is enabled, but physical backup ${backupKey} lacks an encrypted manifest`,
    );
  }

  const keyRing = resolveKeyRing(config);
  const isSigned = Boolean(manifestData.signature?.hmac || manifestData.encryption?.manifestHmac);
  const mustVerify =
    isSigned ||
    Boolean(manifestData.encryption) ||
    keyRing.length > 0 ||
    Boolean(config.requireEncryption);

  if (mustVerify) {
    if ((isSigned || manifestData.encryption || config.requireEncryption) && keyRing.length === 0) {
      throw new IntegrityError(
        `Physical backup manifest for ${backupKey} requires HMAC verification, but ENCRYPTION_KEY / key ring is not configured`,
      );
    }
    try {
      verifyManifest(keyRing, manifestData, {
        requireSigned: Boolean(config.requireEncryption || manifestData.encryption),
      });
    } catch (err) {
      throw new IntegrityError(err instanceof Error ? err.message : String(err));
    }
  }
}

async function verifyPhysicalArchiveZeroDisk(
  storage: PhysicalRestoreOptions["storage"],
  backupKey: string,
  manifestData: BackupManifest | null,
  keyRing: Buffer[],
  signal?: AbortSignal,
): Promise<void> {
  const digest = new DigestStream();
  const decryptStage = manifestData?.encryption
    ? [createDecryptor(keyRing, manifestData.encryption)]
    : [];
  const gunzip = createGunzip();
  const verifyPass = new PassThrough();
  const downloadStream = await storage.download(backupKey, { signal });
  const controller = linkedController(signal);
  try {
    await settleAll(controller, [
      runStreamPipeline([downloadStream, digest, ...decryptStage, gunzip, verifyPass], {
        signal: controller.signal,
      }),
      verifyTarStream(verifyPass, controller.signal),
    ]);
  } finally {
    controller.dispose();
  }
  const actualSha256 = digest.digest();
  if (manifestData?.sha256 && manifestData.sha256 !== actualSha256) {
    throw new Error(
      `Pre-verification checksum mismatch for ${backupKey}: expected ${manifestData.sha256}, got ${actualSha256}`,
    );
  }
  if (manifestData?.size !== undefined && manifestData.size !== digest.bytes) {
    throw new Error(
      `Pre-verification size mismatch for ${backupKey}: expected ${manifestData.size}, got ${digest.bytes}`,
    );
  }
}

/**
 * Restores a physical WiredTiger hot backup directly into a target dbPath,
 * with small-disk awareness (opt-in 1x-disk direct mode after zero-disk pre-verification),
 * multi-step WiredTiger/RS recovery with rollback retention, and optional streaming PITR oplog replay.
 */
export async function restorePhysicalBackup({
  config,
  storage,
  logger,
  targetDbPath,
  ref = "latest",
  pitr,
  signal,
  allowDirectMode = false,
  skipRecovery = false,
  recoveryRunner,
}: PhysicalRestoreOptions): Promise<PhysicalRestoreResult> {
  const log = logger.child({ targetDbPath });
  log.info({ ref, pitr }, "Starting physical backup restore");

  // Live mongod guard (read-only) before resolving manifests or modifying disk
  await assertTargetDbPathUnlocked(targetDbPath);

  const pitrPlan = pitr
    ? await resolvePitrPlan(storage, scopeOf(config), pitr, { engine: "physical" })
    : null;

  let resolvedRef: string;
  if (pitrPlan) {
    resolvedRef = pitrPlan.baseBackup.key;
  } else if (ref === "latest" && typeof storage.list === "function") {
    const scope = scopeOf(config);
    const latestPhysical = (await listBackups(storage, scope)).find(
      (b) => b.complete && /\.physical\.tar\.gz(\.enc)?$/.test(b.key),
    );
    if (!latestPhysical) {
      throw new Error(`No complete physical backups found under ${databasePrefix(scope)}`);
    }
    resolvedRef = latestPhysical.key;
  } else {
    resolvedRef = ref;
  }
  const manifestData = pitrPlan?.baseManifest ?? (await readManifest(storage, resolvedRef));
  const backupKey = manifestData?.key || resolvedRef;

  assertPhysicalManifestIntegrity(config, backupKey, manifestData);
  const keyRing = resolveKeyRing(config);

  // Pre-verify all planned PITR oplog slices BEFORE extracting or modifying targetDbPath
  if (pitrPlan && pitrPlan.slices.length > 0 && typeof storage.download === "function") {
    log.info(
      { slicesCount: pitrPlan.slices.length },
      "Pre-verifying all planned physical PITR oplog slices before extraction",
    );
    for (const slice of pitrPlan.slices) {
      await verifyArchiveObject({ config, storage, logger: log, signal }, slice.key);
    }
  }

  await fs.mkdir(targetDbPath, { recursive: true });
  await assertTargetDbPathUnlocked(targetDbPath);

  // MongoDB version compatibility check when recovery will run
  if (!skipRecovery && !recoveryRunner && manifestData.physical?.mongoVersion) {
    const localVersion = await probeLocalMongodVersion();
    if (localVersion) {
      assertCompatibleMongoVersion(manifestData.physical.mongoVersion, localVersion);
    }
  }

  // Disk space strategy for small-storage instances (e.g. 20-30 GB disks):
  // - >= 1.1x free: safe staged extraction + full-directory rollback swap
  // - < 1.1x free but free + existing >= 1.1x: requires explicit allowDirectMode opt-in,
  //   zero-disk TAR/SHA pre-verify first, then re-check mongod.lock, clear old files & 1x extract
  // - < 1.1x even after clearing: fail fast before touching disk
  const requiredBytes =
    manifestData.physical?.files?.reduce((sum, f) => sum + (f.size || 0), 0) ?? 0;
  let useDirectMode = false;
  if (requiredBytes > 0 && typeof fs.statfs === "function") {
    let stats;
    try {
      stats = await fs.statfs(targetDbPath);
    } catch (err: unknown) {
      const code = (err as { code?: string })?.code;
      if (code === "ENOSYS" || code === "ENOTSUP") {
        log.warn(
          { err, targetDbPath },
          "fs.statfs unsupported on target filesystem; skipping pre-flight space check",
        );
      } else {
        throw err;
      }
    }
    if (stats) {
      const freeBytes = Number(stats.bavail) * Number(stats.bsize);
      const needed = Math.ceil(requiredBytes * DISK_HEADROOM_MULTIPLIER);
      if (freeBytes < needed) {
        const existingBytes = await computeDirectoryBytes(targetDbPath);
        if (freeBytes + existingBytes >= needed) {
          if (!allowDirectMode) {
            throw new Error(
              `Insufficient free space on ${targetDbPath} for safe 2x staged physical restore (need ${formatBytes(needed)}, available ${formatBytes(freeBytes)}, reclaimable ${formatBytes(existingBytes)}). Destructive 1x direct mode requires explicit allowDirectMode: true opt-in.`,
            );
          }
          useDirectMode = true;
          log.warn(
            {
              freeBytes: formatBytes(freeBytes),
              existingBytes: formatBytes(existingBytes),
              requiredBytes: formatBytes(requiredBytes),
            },
            "Insufficient free space for 2x staged physical restore; switching to pre-verified 1x-disk direct extraction mode",
          );
        } else {
          throw new Error(
            `Insufficient disk space on ${targetDbPath}: need ${formatBytes(needed)}, available ${formatBytes(freeBytes)} (reclaimable ${formatBytes(existingBytes)})`,
          );
        }
      }
    }
  }

  const controller = linkedController(signal);
  let extractedCount: number;
  let totalBytes: number;
  let retainedRollbackDir: string | null = null;
  let retainedRollbackName: string | null = null;

  const extractToDir = async (
    destDir: string,
  ): Promise<{ extractedCount: number; totalBytes: number }> => {
    const digest = new DigestStream();
    const decryptStage = manifestData.encryption
      ? [createDecryptor(keyRing, manifestData.encryption)]
      : [];
    const gunzip = createGunzip();

    const downloadStream = await storage.download(backupKey, { signal: controller.signal });
    const unpackPass = new PassThrough();
    const stages = [downloadStream, digest, ...decryptStage, gunzip, unpackPass];

    const [, extractResult] = await settleAll(controller, [
      runStreamPipeline(stages, { signal: controller.signal }),
      extractTarStream(unpackPass, destDir, controller.signal),
    ]);

    const actualSha256 = digest.digest();
    if (manifestData.sha256 && manifestData.sha256 !== actualSha256) {
      throw new Error(`Checksum mismatch: expected ${manifestData.sha256}, got ${actualSha256}`);
    }
    if (manifestData.size !== undefined && manifestData.size !== digest.bytes) {
      throw new Error(
        `Size mismatch for ${backupKey}: expected ${manifestData.size}, got ${digest.bytes}`,
      );
    }
    return extractResult;
  };

  try {
    if (useDirectMode) {
      // Zero-disk streaming pre-verification (including TAR framing) before clearing existing dbPath
      log.info({ backupKey }, "Pre-verifying physical backup archive before clearing targetDbPath");
      await verifyPhysicalArchiveZeroDisk(
        storage,
        backupKey,
        manifestData,
        keyRing,
        controller.signal,
      );

      // Re-check mongod.lock right before destructive 1x wipe
      await assertTargetDbPathUnlocked(targetDbPath);

      const existingEntries = await fs.readdir(targetDbPath);
      for (const name of existingEntries) {
        await fs.rm(path.join(targetDbPath, name), { recursive: true, force: true });
      }

      const result = await extractToDir(targetDbPath);
      extractedCount = result.extractedCount;
      totalBytes = result.totalBytes;

      // Remove ONLY lock markers — preserve WiredTiger.backup for WT $backupCursor recovery!
      await fs.rm(path.join(targetDbPath, "mongod.lock"), { force: true }).catch(() => {});
      await fs.rm(path.join(targetDbPath, "WiredTiger.lock"), { force: true }).catch(() => {});
      await fs.rm(path.join(targetDbPath, "ongoingBackup.lock"), { force: true }).catch(() => {});
    } else {
      const stagingName = `.mongovault-staging-${randomUUID()}`;
      const rollbackName = `.mongovault-rollback-${randomUUID()}`;
      const stagingDir = path.join(targetDbPath, stagingName);
      const rollbackDir = path.join(targetDbPath, rollbackName);
      let rollbackRetained = false;
      await fs.mkdir(stagingDir, { recursive: true });

      try {
        const result = await extractToDir(stagingDir);
        extractedCount = result.extractedCount;
        totalBytes = result.totalBytes;

        // Remove ONLY stale lock markers — preserve WiredTiger.backup for WT recovery!
        await fs.rm(path.join(stagingDir, "mongod.lock"), { force: true }).catch(() => {});
        await fs.rm(path.join(stagingDir, "WiredTiger.lock"), { force: true }).catch(() => {});
        await fs.rm(path.join(stagingDir, "ongoingBackup.lock"), { force: true }).catch(() => {});

        // Re-verify mongod.lock immediately before moving existing dbPath into rollbackDir
        await assertTargetDbPathUnlocked(targetDbPath);

        // Move ALL existing entries (handles directoryPerDB & directoryForIndexes subdirs too)
        // into rollbackDir, then promote staged entries atomically with rollback on any rename failure.
        await fs.mkdir(rollbackDir, { recursive: true });
        const movedToRollback: string[] = [];
        const promoted: string[] = [];
        try {
          const existingEntries = await fs.readdir(targetDbPath);
          for (const name of existingEntries) {
            if (name === stagingName || name === rollbackName) continue;
            await fs.rename(path.join(targetDbPath, name), path.join(rollbackDir, name));
            movedToRollback.push(name);
          }

          const stagedEntries = await fs.readdir(stagingDir);
          for (const name of stagedEntries) {
            await fs.rename(path.join(stagingDir, name), path.join(targetDbPath, name));
            promoted.push(name);
          }
        } catch (swapError) {
          for (const name of promoted) {
            await fs
              .rm(path.join(targetDbPath, name), { recursive: true, force: true })
              .catch(() => {});
          }
          let allSwapRestored = true;
          for (const name of movedToRollback) {
            try {
              await fs.rename(path.join(rollbackDir, name), path.join(targetDbPath, name));
            } catch (rbErr) {
              allSwapRestored = false;
              log.error(
                { err: rbErr, entry: name, rollbackDir },
                "Failed to restore entry from rollback directory after swap failure",
              );
            }
          }
          if (!allSwapRestored) {
            rollbackRetained = true;
            log.error(
              { rollbackDir },
              "Rollback directory retained for manual rescue because some entries could not be restored",
            );
          }
          throw swapError;
        }

        // Retain rollbackDir until PhysicalRecoveryCoordinator finishes cleanly
        retainedRollbackDir = rollbackDir;
        retainedRollbackName = rollbackName;
        rollbackRetained = true;
      } finally {
        await fs.rm(stagingDir, { recursive: true, force: true }).catch(() => {});
        if (!rollbackRetained) {
          await fs.rm(rollbackDir, { recursive: true, force: true }).catch(() => {});
        }
      }
    }
  } finally {
    controller.dispose();
  }

  log.info(
    {
      restoredFiles: extractedCount,
      totalBytes: formatBytes(totalBytes),
      targetDbPath,
    },
    "Physical restore extracted successfully",
  );

  // Run multi-step WiredTiger recovery + optional PITR replay + RS state cleanup.
  // If recovery fails, restore original targetDbPath contents from retainedRollbackDir before rethrowing.
  let replayedEntries = 0;
  let oplogStats: OplogApplierStats | undefined;
  if (!skipRecovery) {
    try {
      const recoveryCtx: PhysicalRecoveryContext = {
        config,
        storage,
        logger: log,
        targetDbPath,
        manifest: manifestData,
        pitrPlan,
        signal,
      };
      const runner =
        recoveryRunner ??
        ((c: PhysicalRecoveryContext) => new PhysicalRecoveryCoordinator().run(c));
      const recResult = await runner(recoveryCtx);
      replayedEntries = recResult.replayedEntries;
      oplogStats = recResult.oplogStats;
    } catch (recErr) {
      if (retainedRollbackDir && retainedRollbackName) {
        let allRestored = true;
        try {
          const currentEntries = await fs.readdir(targetDbPath);
          for (const name of currentEntries) {
            if (name === retainedRollbackName) continue;
            await fs
              .rm(path.join(targetDbPath, name), { recursive: true, force: true })
              .catch(() => {});
          }
          const savedEntries = await fs.readdir(retainedRollbackDir);
          for (const name of savedEntries) {
            try {
              await fs.rename(path.join(retainedRollbackDir, name), path.join(targetDbPath, name));
            } catch (restoreErr) {
              allRestored = false;
              log.error(
                { err: restoreErr, entry: name, rollbackDir: retainedRollbackDir },
                "Failed to restore entry from rollback directory after recovery failure",
              );
            }
          }
        } catch (rbErr) {
          allRestored = false;
          log.error(
            { err: rbErr, rollbackDir: retainedRollbackDir },
            "Failed during rollback after physical recovery failure",
          );
        }
        if (allRestored) {
          await fs.rm(retainedRollbackDir, { recursive: true, force: true }).catch(() => {});
        } else {
          log.error(
            { rollbackDir: retainedRollbackDir },
            "Rollback directory retained for manual rescue because some entries could not be restored",
          );
        }
      }
      throw recErr;
    }
  }

  if (retainedRollbackDir) {
    await fs.rm(retainedRollbackDir, { recursive: true, force: true }).catch(() => {});
  }

  if (typeof storage.putJson === "function") {
    const restoreScope = {
      prefix: config.storage.prefix,
      database: manifestData?.database ?? config.mongo.database,
    };
    await markPitrRestore(storage, restoreScope).catch((err) => {
      log.warn({ err }, "Failed to write PITR restore marker");
    });
  }

  if (!pitrPlan) {
    return {
      key: backupKey,
      restoredFiles: extractedCount,
      totalBytes,
      targetDbPath,
      ...(oplogStats ? { oplogStats } : {}),
    };
  }

  return {
    key: backupKey,
    restoredFiles: extractedCount,
    totalBytes,
    targetDbPath,
    pitrTarget: formatOplogTimestamp(pitrPlan.targetTs),
    replayedSlices: pitrPlan.slices.length,
    replayedEntries,
    ...(oplogStats ? { oplogStats } : {}),
  };
}
