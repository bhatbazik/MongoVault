import { PassThrough } from "node:stream";
import { pipeline } from "node:stream/promises";
import { linkedController, settleAll } from "./abort.js";
import { MANIFEST_VERSION, archiveKey, manifestKey, scopeOf } from "./catalog.js";
import { createEncryptor } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { mongodump } from "./tools.js";

export async function runBackup({ config, storage, logger, signal, tools = { mongodump } }) {
  const startedAt = new Date();
  const encrypted = Boolean(config.encryptionKey);
  const key = archiveKey(scopeOf(config), startedAt, { encrypted });
  const log = logger.child({ key });
  const controller = linkedController(signal);

  log.info({ database: config.mongo.database, encrypted }, "Backup started");

  try {
    const dump = await tools.mongodump({
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

    const encryptor = encrypted ? createEncryptor(config.encryptionKey) : null;
    const digest = new DigestStream();
    const body = new PassThrough({ highWaterMark: 1024 * 1024 });
    const stages = [dump.stdout, encryptor?.stream, digest, body].filter(Boolean);

    const [{ collections }] = await settleAll(controller, [
      dump.done,
      storage.upload(key, body, {
        contentType: encrypted ? "application/octet-stream" : "application/gzip",
        signal: controller.signal,
        onProgress: ({ loaded }) => log.debug({ uploadedBytes: loaded }, "Upload progress"),
      }),
      pipeline(...stages, { signal: controller.signal }),
    ]);

    if (Object.keys(collections).length === 0) {
      throw new Error(
        `mongodump did not dump any collections. Check that database "${config.mongo.database}" exists.`,
      );
    }

    const stored = await storage.head(key);
    if (stored?.size !== digest.bytes) {
      throw new Error(
        `Upload verification failed: expected ${digest.bytes} bytes, found ${stored?.size ?? "no object"}`,
      );
    }

    const completedAt = new Date();
    const manifest = {
      version: MANIFEST_VERSION,
      key,
      database: config.mongo.database,
      createdAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs: completedAt - startedAt,
      size: digest.bytes,
      sha256: digest.digest(),
      compression: "gzip",
      encryption: encryptor?.envelope() ?? null,
      excludedCollections: config.mongo.excludeCollections,
      oplog: Boolean(config.mongo.oplog),
      collections,
    };
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
