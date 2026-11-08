import fs from "node:fs/promises";
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
import { formatOplogTimestamp } from "./oplog.js";
import { runStreamPipeline } from "./stream.js";
import {
  createTarHeader,
  createTarPackStream,
  extractTarStream,
  generateTarStream,
} from "./tar.js";
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
 */
export async function createBackupCursorWithRetries(
  admin: ReturnType<MongoClient["db"]>,
  retries = CURSOR_CREATE_RETRIES,
): Promise<ReturnType<ReturnType<MongoClient["db"]>["aggregate"]>> {
  for (let i = 0; i < retries; i++) {
    try {
      const cursor = admin.aggregate([{ $backupCursor: {} }]);
      return cursor;
    } catch (err: unknown) {
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
    const probe = await probePhysicalCapability(client);
    const resolvedDbPath = overrideDbPath || config.physical?.dbPath || probe.dbPath;

    if (!resolvedDbPath) {
      throw new Error(
        "Could not detect MongoDB dbPath for physical backup. Please specify PHYSICAL_DB_PATH in configuration.",
      );
    }

    log.info(
      {
        resolvedDbPath,
        supportsBackupCursor: probe.supportsBackupCursor,
        supportsFsyncLock: probe.supportsFsyncLock,
      },
      "Physical storage environment detected",
    );

    let cursorMeta: BackupCursorMetadata | null = null;
    let filesToArchive: Array<{ name: string; fullPath: string; size: number }> = [];

    if (probe.supportsBackupCursor) {
      const admin = client.db("admin");
      try {
        cursor = await createBackupCursorWithRetries(admin, CURSOR_CREATE_RETRIES);
        const firstDoc = await cursor.next();
        if (firstDoc?.metadata) {
          cursorMeta = firstDoc.metadata as BackupCursorMetadata;
        }

        while (await cursor.hasNext()) {
          const doc = (await cursor.next()) as BackupCursorFile | null;
          if (doc?.filename) {
            const relName = trimDbPathPrefix(doc.filename, resolvedDbPath);
            const fullPath = path.join(resolvedDbPath, relName);
            const size = doc.fileLen || doc.fileSize || 0;
            if (!filesToArchive.some((f) => f.name === relName)) {
              filesToArchive.push({ name: relName, fullPath, size });
            }
          }
        }

        // Request extended journal files via $backupCursorExtend if supported.
        const extendTargetTs = cursorMeta?.oplogEnd?.ts || cursorMeta?.checkpointTimestamp;
        if (cursorMeta?.backupId && extendTargetTs) {
          try {
            const extCursor = admin.aggregate([
              {
                $backupCursorExtend: {
                  backupId: cursorMeta.backupId,
                  timestamp: extendTargetTs,
                },
              },
            ]);
            while (await extCursor.hasNext()) {
              const jDoc = (await extCursor.next()) as BackupCursorFile | null;
              if (jDoc?.filename) {
                const relName = trimDbPathPrefix(jDoc.filename, resolvedDbPath);
                const fullPath = path.join(resolvedDbPath, relName);
                const size = jDoc.fileLen || jDoc.fileSize || 0;
                if (!filesToArchive.some((f) => f.name === relName)) {
                  filesToArchive.push({ name: relName, fullPath, size });
                }
              }
            }
            await extCursor.close().catch(() => {});
          } catch {
            // $backupCursorExtend is optional if server does not support it
          }
        }

        // Include storage.bson
        const storageBsonPath = path.join(resolvedDbPath, "storage.bson");
        try {
          const stat = await fs.stat(storageBsonPath);
          if (!filesToArchive.some((f) => f.name === "storage.bson")) {
            filesToArchive.push({
              name: "storage.bson",
              fullPath: storageBsonPath,
              size: stat.size,
            });
          }
        } catch {}

        // Include active journal logs
        const journalDir = path.join(resolvedDbPath, "journal");
        try {
          const jEntries = await fs.readdir(journalDir, { withFileTypes: true });
          for (const jEntry of jEntries) {
            if (jEntry.isFile() && jEntry.name.startsWith("WiredTigerLog")) {
              const relName = `journal/${jEntry.name}`;
              if (!filesToArchive.some((f) => f.name === relName)) {
                const fullJPath = path.join(journalDir, jEntry.name);
                const stat = await fs.stat(fullJPath);
                filesToArchive.push({ name: relName, fullPath: fullJPath, size: stat.size });
              }
            }
          }
        } catch {}

        // Keep cursor alive during long uploads
        keepAliveTimer = setInterval(async () => {
          try {
            if (cursor && !cursor.closed) {
              await cursor.hasNext().catch(() => {});
            }
          } catch {}
        }, 45_000);
        keepAliveTimer.unref();
      } catch (cursorErr) {
        log.warn(
          { err: cursorErr },
          "Could not establish $backupCursor; falling back to fsyncLock",
        );
      }
    }

    // Fallback to fsyncLock if $backupCursor returned empty or is not supported
    if (filesToArchive.length === 0) {
      if (!fsyncLockFallback && !probe.supportsBackupCursor) {
        throw new Error(
          "Target MongoDB does not support $backupCursor and fsyncLock fallback is disabled",
        );
      }

      log.info("Acquiring fsyncLock on MongoDB cluster...");
      await client.db("admin").command({ fsync: 1, lock: true });
      locked = true;

      filesToArchive = await scanWiredTigerFiles(resolvedDbPath);
      log.info({ filesCount: filesToArchive.length }, "Scanned WiredTiger data files under lock");
    }

    if (filesToArchive.length === 0) {
      throw new Error(`No WiredTiger files found under dbPath: ${resolvedDbPath}`);
    }

    const totalRawBytes = filesToArchive.reduce((acc, f) => acc + f.size, 0);
    log.info(
      { filesCount: filesToArchive.length, totalRawBytes: formatBytes(totalRawBytes) },
      "Starting streaming physical pack and upload",
    );

    const gzip = createGzip();
    const encryptor =
      encrypted && config.encryptionKey ? createEncryptor(config.encryptionKey) : null;
    const digest = new DigestStream();
    const body = new PassThrough({ highWaterMark: 1024 * 1024 });

    const tarSource = Readable.from(generateTarStream(filesToArchive, controller.signal));
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

    const effectiveStartTs = physicalMeta.oplogStart || physicalMeta.checkpointTimestamp;
    const effectiveEndTs = physicalMeta.oplogEnd || physicalMeta.checkpointTimestamp;

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

/**
 * Restores a physical WiredTiger hot backup directly into a target dbPath.
 */
export async function restorePhysicalBackup({
  config,
  storage,
  logger,
  targetDbPath,
  ref = "latest",
  signal,
}: PhysicalRestoreOptions): Promise<PhysicalRestoreResult> {
  const log = logger.child({ targetDbPath });
  log.info({ ref }, "Starting physical backup restore");

  const resolvedRef =
    ref === "latest" && typeof storage.list === "function"
      ? await resolveBackup(storage, scopeOf(config), ref)
      : ref;
  const manifestData = await readManifest(storage, resolvedRef);
  const backupKey = manifestData?.key || resolvedRef;

  const keyRing = resolveKeyRing(config);
  if (manifestData?.encryption) {
    if (keyRing.length === 0) {
      throw new Error(`Physical backup ${backupKey} is encrypted, but ENCRYPTION_KEY is not set`);
    }
    verifyManifest(keyRing, manifestData);
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

  // Remove any stale lock files in target dbPath (mongod.lock, WiredTiger.lock)
  await fs.rm(path.join(targetDbPath, "mongod.lock"), { force: true }).catch(() => {});
  await fs.rm(path.join(targetDbPath, "WiredTiger.lock"), { force: true }).catch(() => {});

  log.info(
    {
      restoredFiles: extractedCount,
      totalBytes: formatBytes(totalBytes),
      targetDbPath,
    },
    "Physical restore extracted successfully",
  );

  return {
    key: backupKey,
    restoredFiles: extractedCount,
    totalBytes,
    targetDbPath,
  };
}
