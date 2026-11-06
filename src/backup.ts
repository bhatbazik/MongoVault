import { PassThrough } from "node:stream";
import { pipeline } from "node:stream/promises";
import { linkedController, settleAll } from "./abort.js";
import { MANIFEST_VERSION, archiveKey, manifestKey, scopeOf } from "./catalog.js";
import { createEncryptor, signManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { formatOplogTimestamp, getOplogBounds } from "./oplog.js";
import { mongodump } from "./tools.js";
import type { BackupContext } from "./types/backup.js";
import type { BackupManifest } from "./types/catalog.js";
import type { BackupTools } from "./types/tools.js";

export async function runBackup({
  config,
  storage,
  logger,
  signal,
  tools,
}: BackupContext): Promise<BackupManifest> {
  const effectiveTools: BackupTools = tools ?? { mongodump, getOplogBounds };
  const startedAt = new Date();
  const encrypted = Boolean(config.encryptionKey);
  const key = archiveKey(scopeOf(config), startedAt, { encrypted });
  const log = logger.child({ key });
  const controller = linkedController(signal);

  log.info({ database: config.mongo.database, encrypted }, "Backup started");

  try {
    const preOplog =
      config.mongo.oplog && effectiveTools.getOplogBounds
        ? await effectiveTools.getOplogBounds(config.mongo.uri).catch(() => null)
        : null;

    const dump = await effectiveTools.mongodump({
      uri: config.mongo.uri,
      database: config.mongo.database,
      excludeCollections: config.mongo.excludeCollections,
      parallelCollections: config.mongo.parallelCollections,
      oplog: config.mongo.oplog,
      workDir: config.workDir,
      timeoutMs: config.timeoutMs,
      signal: controller.signal,
      logger: log,
    });

    const encryptor =
      encrypted && config.encryptionKey ? createEncryptor(config.encryptionKey) : null;
    const digest = new DigestStream();
    const body = new PassThrough({ highWaterMark: 1024 * 1024 });
    const stages = [dump.stdout, encryptor?.stream, digest, body].filter(Boolean);
    const runPipeline = pipeline as unknown as (...args: unknown[]) => Promise<void>;

    // Query oplog boundary immediately when mongodump finishes collection dumping,
    // rather than waiting for S3 upload to complete, preventing a timeline gap.
    const dumpTask = dump.done.then(async (res) => {
      const dumpEndOplog =
        config.mongo.oplog && effectiveTools.getOplogBounds
          ? await effectiveTools.getOplogBounds(config.mongo.uri).catch(() => null)
          : null;
      return { collections: res.collections, dumpEndOplog };
    });

    const [{ collections, dumpEndOplog }] = await settleAll(controller, [
      dumpTask,
      storage.upload(key, body, {
        contentType: encrypted ? "application/octet-stream" : "application/gzip",
        signal: controller.signal,
        onProgress: ({ loaded }) => log.debug({ uploadedBytes: loaded }, "Upload progress"),
      }),
      runPipeline(...stages, { signal: controller.signal }),
    ]);

    if (Object.keys(collections).length === 0) {
      log.warn(
        { database: config.mongo.database },
        "mongodump completed with 0 collections (empty database or collections had 0 documents)",
      );
    }

    const stored = await storage.head(key);
    if (stored?.size !== digest.bytes) {
      throw new Error(
        `Upload verification failed: expected ${digest.bytes} bytes, found ${stored?.size ?? "no object"}`,
      );
    }

    const postOplog = dumpEndOplog;
    const completedAt = new Date();
    const oplogEndTs = postOplog?.newestTs ?? preOplog?.newestTs ?? null;
    const oplogStartTs = preOplog?.newestTs ?? oplogEndTs;
    const oplogWindow = oplogEndTs
      ? {
          startTs: formatOplogTimestamp(oplogStartTs!),
          endTs: formatOplogTimestamp(oplogEndTs),
        }
      : null;

    const manifest: BackupManifest = {
      version: MANIFEST_VERSION,
      key,
      database: config.mongo.database,
      createdAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs: completedAt.getTime() - startedAt.getTime(),
      size: digest.bytes,
      sha256: digest.digest(),
      compression: "gzip",
      encryption: encryptor?.envelope() ?? null,
      excludedCollections: config.mongo.excludeCollections,
      oplog: Boolean(config.mongo.oplog),
      ...(oplogWindow ? { oplogWindow } : {}),
      collections,
    };
    if (manifest.encryption && config.encryptionKey) {
      manifest.encryption.manifestHmac = signManifest(config.encryptionKey, manifest);
    }
    await storage.putJson(manifestKey(key), manifest);

    log.info(
      {
        size: manifest.size,
        durationMs: manifest.durationMs,
        collections: Object.keys(collections).length,
      },
      "Backup completed",
    );
    return manifest;
  } catch (error) {
    await storage.remove(key).catch((cleanupError) => {
      log.warn({ err: cleanupError }, "Could not remove partial upload");
    });
    throw error;
  } finally {
    controller.dispose();
  }
}
