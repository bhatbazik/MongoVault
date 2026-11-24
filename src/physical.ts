import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import { PassThrough, Readable, Writable } from "node:stream";
import { createGunzip, createGzip } from "node:zlib";
import { MongoClient, ObjectId, Timestamp } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import {
  MANIFEST_VERSION,
  archiveKey,
  readManifest,
  resolveBackup,
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
  parseOplogTimestamp,
  resolvePitrPlan,
  streamOplogSlice,
} from "./oplog.js";
import { runStreamPipeline } from "./stream.js";
import {
  createTarHeader,
  createTarPackStream,
  extractTarStream,
  generateTarStream,
} from "./tar.js";
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
export { createTarHeader, createTarPackStream, generateTarStream, extractTarStream };

const CURSOR_CREATE_RETRIES = 10;
const OPEN_CONFLICT_WITH_CHECKPOINT_ERR_CODE = 50915;
const OPLOG_ROLLED_OVER_ERR_CODE = 50917;
const DISK_HEADROOM_MULTIPLIER = 1.1;

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

/**
 * Strips dbPath prefix from a file path returned by $backupCursor / $backupCursorExtend
 * and normalizes it to a clean relative POSIX path.
 */
export function trimDbPathPrefix(filename: string, dbPath: string): string {
  const normFile = filename.replace(/\\/g, "/");
  const normDbPath = dbPath.replace(/\\/g, "/").replace(/\/+$/, "");
  const stripped = normFile.startsWith(`${normDbPath}/`)
    ? normFile.slice(normDbPath.length + 1)
    : normFile.replace(/^\/+/, "");
  return path.posix.normalize(stripped).replace(/^(\.\.\/)+/, "");
}

/**
 * Creates $backupCursor with retries on WiredTiger checkpoint conflict.
 * Triggers the initial cursor batch inside the retry loop because the MongoDB
 * Node.js driver only sends the aggregate command when the cursor is first read.
 */
export async function createBackupCursorWithRetries(
  admin: ReturnType<MongoClient["db"]>,
  retries = CURSOR_CREATE_RETRIES,
): Promise<ReturnType<ReturnType<MongoClient["db"]>["aggregate"]>> {
  for (let i = 0; i < retries; i++) {
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
      const error = err as { code?: number; message?: string };
      const code = error?.code;
      const isRetriable =
        code === OPEN_CONFLICT_WITH_CHECKPOINT_ERR_CODE ||
        code === OPLOG_ROLLED_OVER_ERR_CODE ||
        String(error?.message || "").includes("checkpoint");

      if (isRetriable && i < retries - 1) {
        await new Promise((resolve) => setTimeout(resolve, (i + 1) * 1000));
        continue;
      }
      throw err;
    }
  }
  throw new Error("Tries limit exceeded establishing $backupCursor");
}

export async function probePhysicalCapability(
  client: MongoClient,
): Promise<{ supportsBackupCursor: boolean; supportsFsyncLock: boolean; dbPath?: string }> {
  const admin = client.db("admin");
  let supportsBackupCursor = false;
  let supportsFsyncLock = false;
  let dbPath: string | undefined = undefined;

  // 1. Try $backupCursor
  try {
    const cursor = admin.aggregate([{ $backupCursor: {} }]);
    const doc = await cursor.next();
    if (doc) {
      supportsBackupCursor = true;
      if (doc.metadata && typeof doc.metadata.dbpath === "string") {
        dbPath = doc.metadata.dbpath;
      }
    }
    await cursor.close();
  } catch {
    supportsBackupCursor = false;
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

  // 3. Check fsyncLock capability
  try {
    const res = await admin.command({ fsync: 1, lock: true });
    if (res && (res.ok === 1 || res.lock === true)) {
      supportsFsyncLock = true;
      await admin.command({ fsyncUnlock: 1 }).catch(() => {});
    }
  } catch {
    supportsFsyncLock = false;
  }

  return { supportsBackupCursor, supportsFsyncLock, dbPath };
}

/**
 * Scans a WiredTiger dbPath directory for physical data files.
 */
export async function scanWiredTigerFiles(
  dbPath: string,
): Promise<Array<{ name: string; fullPath: string; size: number }>> {
  const results: Array<{ name: string; fullPath: string; size: number }> = [];

  async function walk(dir: string, relativePrefix: string = ""): Promise<void> {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      const relName = path.join(relativePrefix, entry.name).replace(/\\/g, "/");

      // Skip temporary lock files and sockets
      if (entry.name === "mongod.lock" || entry.name.endsWith(".sock")) {
        continue;
      }

      if (entry.isDirectory()) {
        await walk(fullPath, relName);
      } else if (entry.isFile()) {
        const stat = await fs.stat(fullPath);
        results.push({ name: relName, fullPath, size: stat.size });
      }
    }
  }

  await walk(dbPath);
  return results;
}

async function drainCursorDocs(cursor: unknown): Promise<Record<string, unknown>[]> {
  const docs: Record<string, unknown>[] = [];
  if (!cursor || typeof cursor !== "object") return docs;
  const c = cursor as {
    __firstDoc?: Record<string, unknown> | null;
    tryNext?: () => Promise<Record<string, unknown> | null>;
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

  if (typeof c.tryNext === "function") {
    while (true) {
      const doc = await c.tryNext();
      if (!doc) break;
      docs.push(doc);
    }
  } else if (typeof c.hasNext === "function" && typeof c.next === "function") {
    while (await c.hasNext()) {
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
    while (true) {
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

/**
 * Executes a Physical Hot Backup of MongoDB WiredTiger files straight to S3/Storage.
 */
export async function runPhysicalBackup({
  config,
  storage,
  logger,
  signal,
  dbPath: overrideDbPath,
  fsyncLockFallback = true,
  createMongoClient = (u) => new MongoClient(u),
}: PhysicalBackupOptions): Promise<PhysicalBackupResult> {
  const startedAt = new Date();
  const encrypted = Boolean(config.encryptionKey);
  const scope = scopeOf(config);
  const key = archiveKey(scope, startedAt, { encrypted }).replace(
    /\.archive\.gz(\.enc)?$/,
    ".physical.tar.gz$1",
  );
  const log = logger.child({ key, engine: "physical" });
  const controller = linkedController(signal);

  log.info({ uri: config.mongo.uri }, "Initiating physical hot backup");

  const client = createMongoClient(config.mongo.uri);
  let locked = false;
  let cursor: ReturnType<ReturnType<MongoClient["db"]>["aggregate"]> | null = null;
  let keepAliveTimer: NodeJS.Timeout | null = null;
  let uploadCompleted = false;

  try {
    await client.connect();
    const admin = client.db("admin");
    let resolvedDbPath = overrideDbPath || config.physical?.dbPath;

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

    let cursorMeta: BackupCursorMetadata | null = null;
    const dataFiles: Array<{ name: string; fullPath: string; size: number }> = [];
    const journalFiles: Array<{ name: string; fullPath: string; size: number }> = [];
    let filesToArchive: Array<{ name: string; fullPath: string; size: number }> = [];

    // 1. Open $backupCursor directly (single open — never open twice)
    try {
      cursor = await createBackupCursorWithRetries(admin, CURSOR_CREATE_RETRIES);
      const initialDocs = await drainCursorDocs(cursor);
      for (const doc of initialDocs) {
        if (doc.metadata) {
          cursorMeta = doc.metadata as BackupCursorMetadata;
          if (!resolvedDbPath && typeof cursorMeta.dbpath === "string") {
            resolvedDbPath = cursorMeta.dbpath;
          }
        }
      }

      if (!resolvedDbPath) {
        resolvedDbPath = extractDbPathFromCmdOpts(cmdOpts);
      }

      if (resolvedDbPath) {
        for (const doc of initialDocs) {
          const fileDoc = doc as unknown as BackupCursorFile;
          if (fileDoc?.filename) {
            const relName = trimDbPathPrefix(fileDoc.filename, resolvedDbPath);
            const fullPath = path.join(resolvedDbPath, relName);
            let size = fileDoc.fileLen || fileDoc.fileSize || 0;
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
      keepAliveTimer = setInterval(() => {
        if (uploadCompleted || !cursor || (cursor as { closed?: boolean }).closed) return;
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
        ping.catch((err) => {
          if (uploadCompleted) return;
          const msg = err instanceof Error ? err.message : String(err);
          log.error({ err }, "$backupCursor keep-alive failed; aborting physical backup");
          controller.abort(new Error(`$backupCursor keep-alive failed: ${msg}`));
        });
      }, 45_000);
      keepAliveTimer.unref();
    } catch (cursorErr) {
      cursorMeta = null;
      if (cursor) {
        await cursor.close().catch(() => {});
        cursor = null;
      }
      log.warn({ err: cursorErr }, "Could not establish $backupCursor; falling back to fsyncLock");
    }

    if (!resolvedDbPath) {
      resolvedDbPath = extractDbPathFromCmdOpts(cmdOpts);
    }

    if (!resolvedDbPath) {
      throw new Error(
        "Could not detect MongoDB dbPath for physical backup. Please specify PHYSICAL_DB_PATH in configuration.",
      );
    }

    const activeDbPath = resolvedDbPath;

    // Fallback to fsyncLock if $backupCursor returned empty or is not supported
    let fsyncOplogTs: string | undefined;
    if (!cursorMeta && dataFiles.length === 0) {
      if (!fsyncLockFallback) {
        throw new Error(
          "Target MongoDB does not support $backupCursor and fsyncLock fallback is disabled",
        );
      }

      const hello = await admin.command({ hello: 1 }).catch(() => null);
      if (
        hello?.setName &&
        hello?.isWritablePrimary &&
        Array.isArray(hello?.hosts) &&
        hello.hosts.length > 1
      ) {
        log.warn(
          { setName: hello.setName, hosts: hello.hosts },
          "Running fsyncLock physical backup on a multi-node replica set PRIMARY blocks all cluster writes. Configure a secondary readPreference for zero-downtime backups.",
        );
      }

      log.info("Acquiring fsyncLock on MongoDB cluster...");
      await admin.command({ fsync: 1, lock: true });
      locked = true;

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
            const extDocs = await drainCursorDocs(extCursor);
            for (const doc of extDocs) {
              const jDoc = doc as unknown as BackupCursorFile;
              if (jDoc?.filename) {
                const relName = trimDbPathPrefix(jDoc.filename, activeDbPath);
                const fullPath = path.join(activeDbPath, relName);
                let size = jDoc.fileLen || jDoc.fileSize || 0;
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

    async function* generateOrderedPhysicalTar(): AsyncGenerator<Buffer, void, unknown> {
      // Phase 1: Stream all .wt data files first (without EOF marker)
      yield* generateTarStream(dataFiles, controller.signal, { includeEof: false });
      // Phase 2: Extend backup cursor and stream pinned journal files last (with EOF marker)
      await collectJournalFilesAfterDataCopy();
      yield* generateTarStream(journalFiles, controller.signal, { includeEof: true });
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

    // Unlock MongoDB write lock if acquired and verify unlock succeeded
    if (locked) {
      const unlockRes = await client
        .db("admin")
        .command({ fsyncUnlock: 1 })
        .catch((err) => {
          log.error({ err }, "CRITICAL: Failed to release MongoDB fsyncLock");
          return null;
        });
      if (unlockRes && unlockRes.ok !== 1 && unlockRes.lockCount !== 0) {
        log.warn({ unlockRes }, "Unexpected response from fsyncUnlock");
      }
      locked = false;
      log.info("Released MongoDB fsyncLock");
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
    if (typeof storage.remove === "function") {
      await storage.remove(key).catch(() => {});
    }
    throw error;
  } finally {
    uploadCompleted = true;
    if (keepAliveTimer) clearInterval(keepAliveTimer);
    if (cursor) {
      await cursor.close().catch(() => {});
    }
    if (locked) {
      await client
        .db("admin")
        .command({ fsyncUnlock: 1 })
        .catch(() => {});
    }
    await client.close().catch(() => {});
    controller.dispose();
  }
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
): Promise<MongoClient> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown = null;
  while (Date.now() < deadline) {
    if (hasExited?.()) {
      let logTail = "";
      if (logPath) {
        try {
          const raw = await fs.readFile(logPath, "utf8");
          logTail = raw.trim().split("\n").slice(-15).join("\n");
        } catch {}
      }
      throw new Error(
        `Temporary mongod exited unexpectedly before becoming ready${logTail ? `:\n${logTail}` : ""}`,
      );
    }
    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 2000 });
    try {
      await client.connect();
      await client.db("admin").command({ ping: 1 });
      return client;
    } catch (err) {
      lastErr = err;
      await client.close().catch(() => {});
      await new Promise((r) => setTimeout(r, 300));
    }
  }
  throw new Error(
    `Temporary mongod did not become ready within ${timeoutMs}ms: ${lastErr instanceof Error ? lastErr.message : String(lastErr)}`,
  );
}

async function waitForLockFileCleared(dbPath: string, timeoutMs = 15_000): Promise<void> {
  const lockPath = path.join(dbPath, "mongod.lock");
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const stat = await fs.stat(lockPath);
      if (stat.size === 0) return;
    } catch {
      return;
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

/**
 * Coordinates multi-step WiredTiger & Replica Set physical recovery:
 * 1. Prepare `local.replset.minvalid` & `local.replset.oplogTruncateAfterPoint` (for `$backupCursor` snapshots).
 * 2. Boot with `recoverFromOplogAsStandalone=true` + `takeUnstableCheckpointOnShutdown=true` so WiredTiger
 *    replays the pinned journal + oplog to the exact consistent point.
 * 3. Replay PITR oplog slices via streaming `OplogApplier` + `buildPendingIndexes({ standalone: true })`.
 * 4. Clean stale RS/cluster state (`local.system.replset`, `config.cache.*`, `config.system.sessions`,
 *    `config.transactions`, `config.rangeDeletions`), `fsync`, and gracefully shut down (`force: false`).
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
      // Step 1: Prepare recovery markers (minvalid + oplogTruncateAfterPoint)
      logger.info(
        { restoreToTs: formatOplogTimestamp(restoreToTs) },
        "Physical recovery step 1/3: writing WiredTiger recovery markers",
      );
      await this.runWithEphemeralMongod(config.workDir, targetDbPath, baseArgs, async (client) => {
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

        const bsonTarget = new Timestamp({ t: restoreToTs.t, i: restoreToTs.i });
        await localDb.collection("replset.minvalid").insertOne({
          _id: new ObjectId(),
          t: -1,
          ts: bsonTarget,
          oplogDeleteFromPoint: new Timestamp({ t: 0, i: 0 }),
        });
        await localDb.collection("replset.oplogTruncateAfterPoint").insertOne({
          _id: "oplogTruncateAfterPoint" as unknown as ObjectId,
          oplogTruncateAfterPoint: bsonTarget,
        });
        await this.flushCheckpoint(client, "step 1/3 (recovery markers)");
      });

      // Step 2: Recover from oplog as standalone (replays journal + oplog to restoreToTs)
      logger.info(
        { restoreToTs: formatOplogTimestamp(restoreToTs) },
        "Physical recovery step 2/3: running recoverFromOplogAsStandalone",
      );
      await this.runWithEphemeralMongod(
        config.workDir,
        targetDbPath,
        [...baseArgs, "--setParameter=recoverFromOplogAsStandalone=true"],
        async (client) => {
          await this.flushCheckpoint(client, "step 2/3 (recoverFromOplogAsStandalone)");
        },
      );
    }

    // Step 3 (+ 4): Replay PITR slices (if any) via streaming OplogApplier, then reset RS state
    logger.info(
      { slices: pitrPlan?.slices.length ?? 0 },
      pitrPlan
        ? "Physical recovery final step: replaying PITR oplog slices and resetting replica set state"
        : "Physical recovery final step: resetting replica set state and flushing clean checkpoint",
    );
    await this.runWithEphemeralMongod(config.workDir, targetDbPath, baseArgs, async (client) => {
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
    });

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
      "--setParameter=takeUnstableCheckpointOnShutdown=true",
    ];
    if (storageOpts?.directoryPerDB || config.physical?.directoryPerDb) {
      args.push("--directoryperdb");
    }
    if (storageOpts?.directoryForIndexes) {
      args.push("--wiredTigerDirectoryForIndexes");
    }
    if (config.physical?.encryptionKeyFile) {
      args.push("--enableEncryption", `--encryptionKeyFile=${config.physical.encryptionKeyFile}`);
    }
    return args;
  }

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
  ): Promise<void> {
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
    try {
      tmpClient = await waitForMongodReady(
        ephemeralUri,
        60_000,
        () => proc.exitCode !== null || proc.signalCode !== null,
        mongodLog,
      );

      await fn(tmpClient);

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
      await waitForLockFileCleared(targetDbPath);
    } finally {
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
  const discard = new Writable({ write: (_c, _e, cb) => cb() });
  const downloadStream = await storage.download(backupKey, { signal });
  await runStreamPipeline([downloadStream, digest, ...decryptStage, gunzip, discard], { signal });
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
 * with small-disk awareness (1x-disk direct mode after zero-disk pre-verification),
 * multi-step WiredTiger/RS recovery, and optional streaming PITR oplog replay.
 */
export async function restorePhysicalBackup({
  config,
  storage,
  logger,
  targetDbPath,
  ref = "latest",
  pitr,
  signal,
  skipRecovery = false,
  recoveryRunner,
}: PhysicalRestoreOptions): Promise<PhysicalRestoreResult> {
  const log = logger.child({ targetDbPath });
  log.info({ ref, pitr }, "Starting physical backup restore");

  const pitrPlan = pitr
    ? await resolvePitrPlan(storage, scopeOf(config), pitr, { engine: "physical" })
    : null;

  const resolvedRef = pitrPlan
    ? pitrPlan.baseBackup.key
    : ref === "latest" && typeof storage.list === "function"
      ? await resolveBackup(storage, scopeOf(config), ref)
      : ref;
  const manifestData = pitrPlan?.baseManifest ?? (await readManifest(storage, resolvedRef));
  const backupKey = manifestData?.key || resolvedRef;

  const keyRing = resolveKeyRing(config);
  const isSigned = Boolean(manifestData?.signature?.hmac || manifestData?.encryption?.manifestHmac);
  if (isSigned && manifestData) {
    if (keyRing.length === 0) {
      throw new Error(
        `Physical backup manifest for ${backupKey} is signed with HMAC, but ENCRYPTION_KEY / key ring is not configured`,
      );
    }
    verifyManifest(keyRing, manifestData);
  } else if (manifestData?.encryption) {
    if (keyRing.length === 0) {
      throw new Error(`Physical backup ${backupKey} is encrypted, but ENCRYPTION_KEY is not set`);
    }
  }

  await fs.mkdir(targetDbPath, { recursive: true });

  // Live mongod guard: refuse to overwrite a dbPath currently held by a running mongod
  try {
    const lockStat = await fs.stat(path.join(targetDbPath, "mongod.lock"));
    if (lockStat.size > 0) {
      throw new Error(
        `Target dbPath ${targetDbPath} appears to be in use by a running mongod process (non-empty mongod.lock). Stop mongod before physical restore.`,
      );
    }
  } catch (err) {
    if (err instanceof Error && err.message.includes("running mongod")) throw err;
  }

  // MongoDB version compatibility check when recovery will run
  if (!skipRecovery && !recoveryRunner && manifestData?.physical?.mongoVersion) {
    const localVersion = await probeLocalMongodVersion();
    if (localVersion) {
      assertCompatibleMongoVersion(manifestData.physical.mongoVersion, localVersion);
    }
  }

  // Disk space strategy for small-storage instances (e.g. 20-30 GB disks):
  // - >= 1.1x free: safe staged extraction + full-directory rollback swap
  // - < 1.1x free but free + existing >= 1.1x: zero-disk pre-verify first, then clear old files & direct 1x-disk extract
  // - < 1.1x even after clearing: fail fast before touching disk
  const requiredBytes =
    manifestData?.physical?.files?.reduce((sum, f) => sum + (f.size || 0), 0) ?? 0;
  let useDirectMode = false;
  if (requiredBytes > 0 && typeof fs.statfs === "function") {
    try {
      const stats = await fs.statfs(targetDbPath);
      const freeBytes = Number(stats.bavail) * Number(stats.bsize);
      const needed = Math.ceil(requiredBytes * DISK_HEADROOM_MULTIPLIER);
      if (freeBytes < needed) {
        const existingBytes = await computeDirectoryBytes(targetDbPath);
        if (freeBytes + existingBytes >= needed) {
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
    } catch (err) {
      if (err instanceof Error && err.message.startsWith("Insufficient disk space")) {
        throw err;
      }
    }
  }

  const controller = linkedController(signal);
  let extractedCount: number;
  let totalBytes: number;

  const extractToDir = async (
    destDir: string,
  ): Promise<{ extractedCount: number; totalBytes: number }> => {
    const digest = new DigestStream();
    const decryptStage = manifestData?.encryption
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
    if (manifestData?.sha256 && manifestData.sha256 !== actualSha256) {
      throw new Error(`Checksum mismatch: expected ${manifestData.sha256}, got ${actualSha256}`);
    }
    if (manifestData?.size !== undefined && manifestData.size !== digest.bytes) {
      throw new Error(
        `Size mismatch for ${backupKey}: expected ${manifestData.size}, got ${digest.bytes}`,
      );
    }
    return extractResult;
  };

  try {
    if (useDirectMode) {
      // Zero-disk streaming pre-verification before clearing existing dbPath
      log.info({ backupKey }, "Pre-verifying physical backup archive before clearing targetDbPath");
      await verifyPhysicalArchiveZeroDisk(
        storage,
        backupKey,
        manifestData,
        keyRing,
        controller.signal,
      );

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
      await fs.mkdir(stagingDir, { recursive: true });

      try {
        const result = await extractToDir(stagingDir);
        extractedCount = result.extractedCount;
        totalBytes = result.totalBytes;

        // Remove ONLY stale lock markers — preserve WiredTiger.backup for WT recovery!
        await fs.rm(path.join(stagingDir, "mongod.lock"), { force: true }).catch(() => {});
        await fs.rm(path.join(stagingDir, "WiredTiger.lock"), { force: true }).catch(() => {});
        await fs.rm(path.join(stagingDir, "ongoingBackup.lock"), { force: true }).catch(() => {});

        // Move ALL existing entries (handles directoryPerDB & directoryForIndexes subdirs too)
        // into rollbackDir, then promote staged entries atomically with rollback on failure.
        await fs.mkdir(rollbackDir, { recursive: true });
        const movedToRollback: string[] = [];
        const existingEntries = await fs.readdir(targetDbPath);
        for (const name of existingEntries) {
          if (name === stagingName || name === rollbackName) continue;
          await fs.rename(path.join(targetDbPath, name), path.join(rollbackDir, name));
          movedToRollback.push(name);
        }

        const stagedEntries = await fs.readdir(stagingDir);
        const promoted: string[] = [];
        try {
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
          for (const name of movedToRollback) {
            await fs
              .rename(path.join(rollbackDir, name), path.join(targetDbPath, name))
              .catch(() => {});
          }
          throw swapError;
        }
      } finally {
        await fs.rm(stagingDir, { recursive: true, force: true }).catch(() => {});
        await fs.rm(rollbackDir, { recursive: true, force: true }).catch(() => {});
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

  // Run multi-step WiredTiger recovery + optional PITR replay + RS state cleanup
  let replayedEntries = 0;
  let oplogStats: OplogApplierStats | undefined;
  if (!skipRecovery) {
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
      recoveryRunner ?? ((c: PhysicalRecoveryContext) => new PhysicalRecoveryCoordinator().run(c));
    const recResult = await runner(recoveryCtx);
    replayedEntries = recResult.replayedEntries;
    oplogStats = recResult.oplogStats;
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
