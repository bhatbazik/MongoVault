import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { createGunzip, createGzip } from "node:zlib";
import { MongoClient } from "mongodb";
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
import {
  downloadAndExtractOplogSlice,
  filterOplogSliceFile,
  formatOplogTimestamp,
  fromBsonTimestamp,
  incrementOplogTimestamp,
  resolvePitrPlan,
} from "./oplog.js";
import { runStreamPipeline } from "./stream.js";
import {
  createTarHeader,
  createTarPackStream,
  extractTarStream,
  generateTarStream,
} from "./tar.js";
import { replayOplogFile } from "./tools.js";
import type { BackupManifest, PhysicalBackupMeta } from "./types/catalog.js";
import type {
  BackupCursorFile,
  BackupCursorMetadata,
  PhysicalBackupOptions,
  PhysicalBackupResult,
  PhysicalRestoreOptions,
  PhysicalRestoreResult,
} from "./types/physical.js";

// Re-export tar utilities for backwards compatibility
export { createTarHeader, createTarPackStream, generateTarStream, extractTarStream };

const CURSOR_CREATE_RETRIES = 10;
const OPEN_CONFLICT_WITH_CHECKPOINT_ERR_CODE = 50915;
const OPLOG_ROLLED_OVER_ERR_CODE = 50917;

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
      const cmdOpts = await admin.command({ getCmdLineOpts: 1 });
      dbPath =
        cmdOpts?.parsed?.storage?.dbPath || cmdOpts?.argv?.[cmdOpts.argv.indexOf("--dbpath") + 1];
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
        // Include journal, diagnostic.data
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
    // If firstDoc was null, the cursor is already exhausted.
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

  try {
    await client.connect();
    const admin = client.db("admin");
    let resolvedDbPath = overrideDbPath || config.physical?.dbPath;

    let cursorMeta: BackupCursorMetadata | null = null;
    const dataFiles: Array<{ name: string; fullPath: string; size: number }> = [];
    const journalFiles: Array<{ name: string; fullPath: string; size: number }> = [];
    let filesToArchive: Array<{ name: string; fullPath: string; size: number }> = [];

    // 1. Open $backupCursor directly (single open — never open twice via probePhysicalCapability)
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
        try {
          const cmdOpts = await admin.command({ getCmdLineOpts: 1 });
          resolvedDbPath =
            cmdOpts?.parsed?.storage?.dbPath ||
            cmdOpts?.argv?.[cmdOpts.argv.indexOf("--dbpath") + 1];
        } catch {}
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

      // Keep cursor alive during long uploads using non-blocking tryNext poll
      keepAliveTimer = setInterval(async () => {
        try {
          if (cursor && !(cursor as { closed?: boolean }).closed) {
            const c = cursor as {
              tryNext?: () => Promise<unknown>;
              hasNext?: () => Promise<boolean>;
            };
            if (typeof c.tryNext === "function") {
              await c.tryNext().catch(() => {});
            } else if (typeof c.hasNext === "function") {
              await c.hasNext().catch(() => {});
            }
          }
        } catch {}
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
      try {
        const cmdOpts = await admin.command({ getCmdLineOpts: 1 });
        resolvedDbPath =
          cmdOpts?.parsed?.storage?.dbPath || cmdOpts?.argv?.[cmdOpts.argv.indexOf("--dbpath") + 1];
      } catch {}
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

    // Helper to collect extended journal files AFTER all .wt data files have been copied
    // (Percona PBM pattern: data files first, then $backupCursorExtend & journal files last).
    let extendedToOplogEnd = false;
    const collectJournalFilesAfterDataCopy = async () => {
      if (cursorMeta) {
        const extendTargetTs = cursorMeta.oplogEnd?.ts || cursorMeta.checkpointTimestamp;
        if (cursorMeta.backupId && extendTargetTs) {
          try {
            const extCursor = admin.aggregate([
              {
                $backupCursorExtend: {
                  backupId: cursorMeta.backupId,
                  timestamp: extendTargetTs,
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
            extendedToOplogEnd = true;
          } catch (extErr) {
            log.warn(
              { err: extErr },
              "$backupCursorExtend failed; anchoring physical backup oplogWindow to checkpointTimestamp instead of oplogEnd",
            );
          }
        } else if (!cursorMeta.oplogEnd?.ts) {
          extendedToOplogEnd = true;
        }

        // Also scan journal directory for any WiredTigerLog* files created during data copy
        const journalDir = path.join(activeDbPath, "journal");
        try {
          const jEntries = await fs.readdir(journalDir, { withFileTypes: true });
          for (const jEntry of jEntries) {
            if (jEntry.isFile() && jEntry.name.startsWith("WiredTigerLog")) {
              const relName = `journal/${jEntry.name}`;
              if (!journalFiles.some((f) => f.name === relName)) {
                const fullJPath = path.join(journalDir, jEntry.name);
                const stat = await fs.stat(fullJPath);
                journalFiles.push({ name: relName, fullPath: fullJPath, size: stat.size });
              }
            }
          }
        } catch {}
      }
      filesToArchive = [...dataFiles, ...journalFiles];
    };

    async function* generateOrderedPhysicalTar(): AsyncGenerator<Buffer, void, unknown> {
      // Phase 1: Stream all .wt data files first (without EOF marker)
      yield* generateTarStream(dataFiles, controller.signal, { includeEof: false });
      // Phase 2: Extend backup cursor and stream journal files last (with EOF marker)
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
        signal: controller.signal,
        onProgress: ({ loaded }) =>
          log.debug({ uploadedBytes: loaded }, "Physical upload progress"),
      }),
      runStreamPipeline(stages, { signal: controller.signal }),
    ]);

    if (typeof storage.head === "function") {
      const stored = await storage.head(key);
      if (stored && stored.size !== digest.bytes) {
        throw new Error(
          `Physical backup upload verification failed: expected ${digest.bytes} bytes, found ${stored.size}`,
        );
      }
    }

    // Unlock MongoDB write lock if acquired
    if (locked) {
      await client
        .db("admin")
        .command({ fsyncUnlock: 1 })
        .catch(() => {});
      locked = false;
      log.info("Released MongoDB fsyncLock");
    }

    const completedAt = new Date();
    const durationMs = completedAt.getTime() - startedAt.getTime();

    const physicalMeta: PhysicalBackupMeta = {
      dbPath: resolvedDbPath,
      cursorType: cursorMeta ? "$backupCursor" : "fsyncLock",
      filesCount: filesToArchive.length,
      files: filesToArchive.map((f) => ({ name: f.name, size: f.size })),
      checkpointTimestamp: cursorMeta?.checkpointTimestamp
        ? formatOplogTimestamp(cursorMeta.checkpointTimestamp)
        : undefined,
      oplogStart: cursorMeta?.oplogStart?.ts
        ? formatOplogTimestamp(cursorMeta.oplogStart.ts)
        : undefined,
      oplogEnd: cursorMeta?.oplogEnd?.ts ? formatOplogTimestamp(cursorMeta.oplogEnd.ts) : undefined,
    };

    const effectiveStartTs =
      physicalMeta.oplogStart || physicalMeta.checkpointTimestamp || fsyncOplogTs;
    const effectiveEndTs =
      (extendedToOplogEnd ? physicalMeta.oplogEnd : undefined) ||
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

async function waitForMongodReady(
  uri: string,
  timeoutMs = 30_000,
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
          logTail = raw.trim().split("\n").slice(-10).join("\n");
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

/**
 * Restores a physical WiredTiger hot backup directly into a target dbPath,
 * with optional Point-in-Time Recovery (PITR) oplog replay via an ephemeral local mongod.
 */
export async function restorePhysicalBackup({
  config,
  storage,
  logger,
  targetDbPath,
  ref = "latest",
  pitr,
  signal,
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

  // Clean existing WiredTiger data and journal files in targetDbPath before extracting
  // so no leftover orphan .wt or journal files from a prior database corrupt recovery.
  try {
    const existingEntries = await fs.readdir(targetDbPath, { withFileTypes: true });
    for (const entry of existingEntries) {
      if (
        entry.name.endsWith(".wt") ||
        entry.name.endsWith(".turtle") ||
        entry.name.startsWith("WiredTiger") ||
        entry.name === "storage.bson" ||
        entry.name === "journal" ||
        entry.name === "mongod.lock" ||
        entry.name === "ongoingBackup.lock"
      ) {
        await fs
          .rm(path.join(targetDbPath, entry.name), { recursive: true, force: true })
          .catch(() => {});
      }
    }
  } catch {
    // targetDbPath may not exist yet
  }

  const digest = new DigestStream();
  const decryptStage = manifestData?.encryption
    ? [createDecryptor(keyRing, manifestData.encryption)]
    : [];
  const gunzip = createGunzip();

  const downloadStream = await storage.download(backupKey, { signal });
  const unpackPass = new PassThrough();

  const stages = [downloadStream, digest, ...decryptStage, gunzip, unpackPass];

  const [_, { extractedCount, totalBytes }] = await Promise.all([
    runStreamPipeline(stages, { signal }),
    extractTarStream(unpackPass, targetDbPath, signal),
  ]);

  if (manifestData?.sha256 && manifestData.sha256 !== digest.digest()) {
    throw new Error(`Checksum mismatch: expected ${manifestData.sha256}, got ${digest.digest()}`);
  }
  if (manifestData?.size !== undefined && manifestData.size !== digest.bytes) {
    throw new Error(
      `Size mismatch for ${backupKey}: expected ${manifestData.size}, got ${digest.bytes}`,
    );
  }

  // Remove any stale lock/backup marker files in target dbPath
  await fs.rm(path.join(targetDbPath, "mongod.lock"), { force: true }).catch(() => {});
  await fs.rm(path.join(targetDbPath, "WiredTiger.lock"), { force: true }).catch(() => {});
  await fs.rm(path.join(targetDbPath, "WiredTiger.backup"), { force: true }).catch(() => {});
  await fs.rm(path.join(targetDbPath, "ongoingBackup.lock"), { force: true }).catch(() => {});

  log.info(
    {
      restoredFiles: extractedCount,
      totalBytes: formatBytes(totalBytes),
      targetDbPath,
    },
    "Physical restore extracted successfully",
  );

  if (!pitrPlan) {
    return {
      key: backupKey,
      restoredFiles: extractedCount,
      totalBytes,
      targetDbPath,
    };
  }

  // Stage 2 (PITR): Boot ephemeral standalone mongod on targetDbPath, replay oplog slices, and shut down cleanly
  const port = await findFreePort();
  const ephemeralUri = `mongodb://127.0.0.1:${port}`;
  const targetLimit = formatOplogTimestamp(pitrPlan.targetTs);
  const exclusiveLimit = formatOplogTimestamp(incrementOplogTimestamp(pitrPlan.targetTs));
  const mongodLog = path.join(config.workDir, `.pitr-mongod-${randomUUID()}.log`);

  await fs.mkdir(config.workDir, { recursive: true });
  log.info(
    { port, slices: pitrPlan.slices.length, targetLimit },
    "Starting temporary mongod for physical PITR oplog replay",
  );

  const recoveryArgs = [
    `--dbpath=${targetDbPath}`,
    `--port=${port}`,
    "--bind_ip=127.0.0.1",
    `--logpath=${mongodLog}`,
    "--setParameter=ttlMonitorEnabled=false",
    "--setParameter=disableLogicalSessionCacheRefresh=true",
    "--setParameter=takeUnstableCheckpointOnShutdown=true",
  ];
  if (config.physical?.directoryPerDb) {
    recoveryArgs.push("--directoryperdb");
  }
  if (config.physical?.encryptionKeyFile) {
    recoveryArgs.push(
      "--enableEncryption",
      `--encryptionKeyFile=${config.physical.encryptionKeyFile}`,
    );
  }

  const proc = spawn("mongod", recoveryArgs, { stdio: "ignore" });

  const procExit = new Promise<void>((resolve) => {
    proc.once("close", () => resolve());
    proc.once("error", () => resolve());
  });

  let tmpClient: MongoClient | null = null;
  let replayedEntries = 0;

  try {
    tmpClient = await waitForMongodReady(
      ephemeralUri,
      30_000,
      () => proc.exitCode !== null || proc.signalCode !== null,
      mongodLog,
    );
    // Clear stale replica-set coordination state and sessions on recovery instance
    // so it starts cleanly as a standalone or newly initialized replica set.
    const localDb = tmpClient.db("local");
    await Promise.all([
      localDb
        .collection("system.replset")
        .drop()
        .catch(() => {}),
      localDb
        .collection("replset.minvalid")
        .drop()
        .catch(() => {}),
      localDb
        .collection("replset.oplogTruncateAfterPoint")
        .drop()
        .catch(() => {}),
      localDb
        .collection("replset.election")
        .drop()
        .catch(() => {}),
      tmpClient
        .db("config")
        .collection("system.sessions")
        .drop()
        .catch(() => {}),
    ]);

    for (const slice of pitrPlan.slices) {
      const rawSlice = path.join(config.workDir, `.phys-pitr-${randomUUID()}.oplog.bson`);
      const filteredSlice = path.join(config.workDir, `.phys-pitr-f-${randomUUID()}.oplog.bson`);
      try {
        await downloadAndExtractOplogSlice({ config, storage, signal }, slice.key, rawSlice);
        const { keptEntries } = await filterOplogSliceFile(rawSlice, filteredSlice, {
          targetTs: pitrPlan.targetTs,
        });
        if (keptEntries === 0) continue;

        await replayOplogFile({
          uri: ephemeralUri,
          workDir: config.workDir,
          oplogFile: filteredSlice,
          oplogLimit: exclusiveLimit,
          timeoutMs: config.timeoutMs,
          signal,
          logger: log,
        });
        replayedEntries += keptEntries;
      } finally {
        await Promise.allSettled([
          fs.rm(rawSlice, { force: true }),
          fs.rm(filteredSlice, { force: true }),
        ]);
      }
    }

    await tmpClient
      .db("admin")
      .command({ fsync: 1 })
      .catch(() => {});
    await tmpClient
      .db("admin")
      .command({ shutdown: 1, force: true })
      .catch(() => {});
    await tmpClient.close().catch(() => {});
    tmpClient = null;
    await procExit;
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
      await Promise.race([procExit, new Promise((r) => setTimeout(r, 5_000))]);
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill("SIGKILL");
      }
    }
    await fs.rm(mongodLog, { force: true }).catch(() => {});
  }

  return {
    key: backupKey,
    restoredFiles: extractedCount,
    totalBytes,
    targetDbPath,
    pitrTarget: targetLimit,
    replayedSlices: pitrPlan.slices.length,
    replayedEntries,
  };
}
