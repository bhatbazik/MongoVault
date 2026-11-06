import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip, createGzip } from "node:zlib";
import { MongoClient } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import { MANIFEST_VERSION, archiveKey, manifestKey, scopeOf } from "./catalog.js";
import { createDecryptor, createEncryptor, signManifest, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { formatBytes, formatDuration } from "./format.js";
import { formatOplogTimestamp } from "./oplog.js";
import type { BackupManifest, PhysicalBackupMeta } from "./types/catalog.js";
import type {
  BackupCursorFile,
  BackupCursorMetadata,
  PhysicalBackupOptions,
  PhysicalBackupResult,
  PhysicalRestoreOptions,
  PhysicalRestoreResult,
} from "./types/physical.js";

const CURSOR_CREATE_RETRIES = 10;
const OPEN_CONFLICT_WITH_CHECKPOINT_ERR_CODE = 50915;
const OPLOG_ROLLED_OVER_ERR_CODE = 50917;

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

export function createTarHeader(name: string, size: number, mtime: number = Date.now()): Buffer {
  const buf = Buffer.alloc(512, 0);

  // name: 0..100
  const normName = name.replace(/\\/g, "/").slice(0, 100);
  buf.write(normName, 0, "utf8");

  // mode: 100..108
  buf.write("0000644\0", 100, "ascii");
  // uid: 108..116
  buf.write("0000000\0", 108, "ascii");
  // gid: 116..124
  buf.write("0000000\0", 116, "ascii");

  // size: 124..136 (11 octal digits + null)
  const sizeOctal = size.toString(8).padStart(11, "0") + "\0";
  buf.write(sizeOctal, 124, "ascii");

  // mtime: 136..148
  const mtimeOctal =
    Math.floor(mtime / 1000)
      .toString(8)
      .padStart(11, "0") + "\0";
  buf.write(mtimeOctal, 136, "ascii");

  // typeflag: 156
  buf.write("0", 156, "ascii");

  // magic: 257..263
  buf.write("ustar\0", 257, "ascii");
  buf.write("00", 263, "ascii");

  // Compute checksum over 512 bytes with chksum field treated as 8 spaces (ASCII 32)
  buf.fill(32, 148, 156);
  let checksum = 0;
  for (let i = 0; i < 512; i++) {
    checksum += buf[i]!;
  }
  const chksumOctal = checksum.toString(8).padStart(6, "0") + "\0 ";
  buf.write(chksumOctal, 148, "ascii");

  return buf;
}

export function createTarPackStream(
  files: Array<{ name: string; fullPath: string; size: number }>,
  signal?: AbortSignal,
): Readable {
  return Readable.from(generateTarStream(files, signal));
}

export async function* generateTarStream(
  files: Array<{ name: string; fullPath: string; size: number }>,
  signal?: AbortSignal,
): AsyncGenerator<Buffer, void, unknown> {
  for (const file of files) {
    if (signal?.aborted) return;
    let stat;
    try {
      stat = await fs.stat(file.fullPath);
    } catch {
      continue;
    }
    const header = createTarHeader(file.name, stat.size, stat.mtimeMs);
    yield header;

    // Stream file contents in 64KB chunks
    const stream = fsSync.createReadStream(file.fullPath, { highWaterMark: 64 * 1024 });
    for await (const chunk of stream) {
      if (signal?.aborted) {
        stream.destroy();
        return;
      }
      yield Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    }

    // Pad file data to 512-byte block boundary
    const remainder = stat.size % 512;
    if (remainder > 0) {
      yield Buffer.alloc(512 - remainder, 0);
    }
  }

  // End of archive: two 512-byte zero blocks
  yield Buffer.alloc(1024, 0);
}

export async function extractTarStream(
  stream: Readable,
  targetDir: string,
  signal?: AbortSignal,
): Promise<{ extractedCount: number; totalBytes: number }> {
  await fs.mkdir(targetDir, { recursive: true });

  let buffer = Buffer.alloc(0);
  let currentHeader: { name: string; size: number } | null = null;
  let remainingBytes = 0;
  let remainingPad = 0;
  let currentFileHandle: fs.FileHandle | null = null;
  let extractedCount = 0;
  let totalBytes = 0;

  for await (const chunk of stream) {
    if (signal?.aborted) break;
    buffer = Buffer.concat([buffer, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);

    while (buffer.length > 0) {
      if (remainingPad > 0) {
        const toDiscard = Math.min(buffer.length, remainingPad);
        buffer = buffer.subarray(toDiscard);
        remainingPad -= toDiscard;
        if (remainingPad > 0) break;
      }

      if (!currentHeader) {
        if (buffer.length < 512) break; // Need full header

        const headerBuf = buffer.subarray(0, 512);
        buffer = buffer.subarray(512);

        // Check for 512-byte zero block (end of tar)
        const isZero = headerBuf.every((b) => b === 0);
        if (isZero) continue;

        const rawName = headerBuf.toString("utf8", 0, 100).replace(/\0.*$/, "").trim();
        if (!rawName) continue;

        // Prevent path traversal
        const safeName = path.normalize(rawName).replace(/^(\.\.[/\\])+/, "");
        const rawSize = headerBuf.toString("ascii", 124, 136).replace(/\0.*$/, "").trim();
        const size = parseInt(rawSize, 8) || 0;

        currentHeader = { name: safeName, size };
        remainingBytes = size;

        const fullPath = path.join(targetDir, safeName);
        await fs.mkdir(path.dirname(fullPath), { recursive: true });
        currentFileHandle = await fs.open(fullPath, "w", 0o600);
      }

      if (currentHeader && currentFileHandle) {
        if (remainingBytes > 0) {
          const toWrite = Math.min(buffer.length, remainingBytes);
          const slice = buffer.subarray(0, toWrite);
          await currentFileHandle.writeFile(slice);
          buffer = buffer.subarray(toWrite);
          remainingBytes -= toWrite;
          totalBytes += toWrite;
        }

        if (remainingBytes === 0) {
          await currentFileHandle.close();
          currentFileHandle = null;
          extractedCount++;

          const remainder = currentHeader.size % 512;
          remainingPad = remainder > 0 ? 512 - remainder : 0;
          currentHeader = null;
        } else {
          break; // Need more data for current file
        }
      }
    }
  }

  if (currentFileHandle) {
    await currentFileHandle.close().catch(() => {});
  }

  return { extractedCount, totalBytes };
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
            const fullPath = path.join(resolvedDbPath, doc.filename);
            const size = doc.fileLen || doc.fileSize || 0;
            filesToArchive.push({ name: doc.filename, fullPath, size });
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
    const stages = [tarSource, gzip, encryptor?.stream, digest, body].filter(Boolean);
    const runPipeline = pipeline as unknown as (...args: unknown[]) => Promise<void>;

    await settleAll(controller, [
      storage.upload(key, body, {
        contentType: encrypted ? "application/octet-stream" : "application/gzip",
        signal: controller.signal,
        onProgress: ({ loaded }) =>
          log.debug({ uploadedBytes: loaded }, "Physical upload progress"),
      }),
      runPipeline(...stages, { signal: controller.signal }),
    ]);

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
      files: filesToArchive.map((f) => ({ name: f.name, size: f.size })),
      checkpointTimestamp: cursorMeta?.checkpointTimestamp
        ? formatOplogTimestamp(cursorMeta.checkpointTimestamp)
        : undefined,
      oplogStart: cursorMeta?.oplogStart?.ts
        ? formatOplogTimestamp(cursorMeta.oplogStart.ts)
        : undefined,
      oplogEnd: cursorMeta?.oplogEnd?.ts ? formatOplogTimestamp(cursorMeta.oplogEnd.ts) : undefined,
    };

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
      oplog: Boolean(cursorMeta?.oplogEnd),
      excludedCollections: [],
      collections: {},
      physical: physicalMeta,
    };

    if (manifest.encryption && config.encryptionKey) {
      manifest.encryption.manifestHmac = signManifest(config.encryptionKey, manifest);
    }

    await storage.putJson(manifestKey(key), manifest);

    log.info(
      {
        size: formatBytes(manifest.size),
        duration: formatDuration(durationMs),
        files: filesToArchive.length,
      },
      "Physical hot backup completed successfully",
    );

    return { manifest, filesCount: filesToArchive.length, totalBytes: digest.bytes };
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

  const manifest = await storage.getJson<BackupManifest>(manifestKey(ref));
  const backupKey = manifest?.data?.key || ref;
  const manifestData = manifest?.data;

  const keyRing = config.encryptionKeyRing || (config.encryptionKey ? [config.encryptionKey] : []);
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
  const runPipeline = pipeline as unknown as (...args: unknown[]) => Promise<void>;

  const [_, { extractedCount, totalBytes }] = await Promise.all([
    runPipeline(...stages, { signal }),
    extractTarStream(unpackPass, targetDbPath, signal),
  ]);

  if (manifestData?.sha256 && manifestData.sha256 !== digest.digest()) {
    throw new Error(`Checksum mismatch: expected ${manifestData.sha256}, got ${digest.digest()}`);
  }

  // Remove any stale lock file in target dbPath
  await fs.rm(path.join(targetDbPath, "mongod.lock"), { force: true }).catch(() => {});

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
