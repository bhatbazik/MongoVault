import { Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip } from "node:zlib";
import { linkedController, settleAll } from "./abort.js";
import { isClusterScope, readManifest, resolveBackup, scopeOf } from "./catalog.js";
import { createDecryptor, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { mongorestore } from "./tools.js";

export class IntegrityError extends Error {
  constructor(message) {
    super(message);
    this.name = "IntegrityError";
  }
}

async function describeBackup({ config, storage }, ref) {
  const key = await resolveBackup(storage, scopeOf(config), ref);
  const manifest = await readManifest(storage, key);
  const object = manifest ? null : await storage.head(key);
  if (!manifest && !object) {
    throw new Error(`Backup not found: ${key}`);
  }
  if (manifest?.key && manifest.key !== key) {
    throw new IntegrityError(
      `Manifest key mismatch for ${key}: manifest belongs to ${manifest.key}`,
    );
  }
  const checksum = manifest?.sha256 ?? object.metadata.sha256 ?? null;

  const keyRing = config.encryptionKeyRing || (config.encryptionKey ? [config.encryptionKey] : []);
  if (manifest?.encryption) {
    if (keyRing.length === 0) {
      throw new Error(`${key} is encrypted, but ENCRYPTION_KEY is not set`);
    }
    try {
      verifyManifest(keyRing, manifest);
    } catch (error) {
      throw new IntegrityError(error.message);
    }
  }
  return { key, manifest, checksum };
}

function decryptStage(config, manifest) {
  const keyRing = config.encryptionKeyRing || (config.encryptionKey ? [config.encryptionKey] : []);
  return manifest?.encryption ? [createDecryptor(keyRing, manifest.encryption)] : [];
}

function assertChecksum(key, expected, actual) {
  if (expected && expected !== actual) {
    throw new IntegrityError(`Checksum mismatch for ${key}: expected ${expected}, got ${actual}`);
  }
}

export async function verifyBackup(ctx, ref = "latest") {
  const { config, storage, logger, signal } = ctx;
  const { key, manifest, checksum } = await describeBackup(ctx, ref);
  if (!checksum) {
    throw new IntegrityError(`${key} has no manifest or checksum to verify against`);
  }

  logger.info({ key }, "Verifying backup");
  const digest = new DigestStream();
  const discard = new Writable({ write: (_chunk, _encoding, callback) => callback() });

  try {
    await pipeline(
      await storage.download(key, { signal }),
      digest,
      ...decryptStage(config, manifest),
      createGunzip(),
      discard,
      { signal },
    );
  } catch (error) {
    if (error.name === "AbortError") throw error;
    throw new IntegrityError(`${key} could not be read back: ${error.message}`);
  }

  const sha256 = digest.digest();
  assertChecksum(key, checksum, sha256);
  if (manifest && manifest.size !== digest.bytes) {
    throw new IntegrityError(`Size mismatch for ${key}: expected ${manifest.size}, got ${digest.bytes}`);
  }

  logger.info({ key, sha256 }, "Backup verified");
  return { key, size: digest.bytes, sha256, encrypted: Boolean(manifest?.encryption) };
}

export async function restoreBackup(
  ctx,
  {
    ref = "latest",
    uri,
    database,
    drop = false,
    preVerify = drop,
    oplogReplay,
    oplogLimit,
    parallelCollections,
  },
) {
  const { config, storage, logger, signal, tools = { mongorestore } } = ctx;
  const { key, manifest, checksum } = await describeBackup(ctx, ref);
  const log = logger.child({ key });
  const source = manifest?.database ?? config.mongo.database;
  const target = database ?? source;
  const isCluster = isClusterScope(source);
  const isFilteredCluster = isCluster && !isClusterScope(target);
  const isRenamedDb = !isCluster && target !== source;
  const hasNamespaceFilter = isFilteredCluster || isRenamedDb;

  if (hasNamespaceFilter && (oplogReplay === true || Boolean(oplogLimit))) {
    throw new Error(
      "Cannot use --oplog-replay or --oplog-limit when filtering or renaming target database",
    );
  }

  const shouldReplayOplog = hasNamespaceFilter
    ? false
    : (oplogReplay ?? (Boolean(oplogLimit) || Boolean(manifest?.oplog)));

  if (!checksum) log.warn("Backup has no checksum; restoring without an integrity check");

  if (preVerify) {
    log.info({ ref, drop }, "Pre-verifying backup archive integrity and checksum before dropping collections");
    await verifyBackup(ctx, ref);
  }

  const controller = linkedController(signal);
  try {
    const restore = await tools.mongorestore({
      uri,
      workDir: config.workDir,
      nsInclude: isFilteredCluster ? `${target}.*` : (isRenamedDb ? `${source}.*` : undefined),
      nsFrom: isRenamedDb ? `${source}.*` : undefined,
      nsTo: isRenamedDb ? `${target}.*` : undefined,
      drop,
      oplogReplay: shouldReplayOplog,
      oplogLimit,
      parallelCollections: parallelCollections ?? config.mongo.parallelCollections,
      timeoutMs: config.timeoutMs,
      signal: controller.signal,
      logger: log,
    });

    log.info({ source, target, drop }, "Streaming restore pipeline from storage (zero-disk)");
    const digest = new DigestStream();
    const downloadStream = await storage.download(key, { signal: controller.signal });

    const [summary] = await settleAll(controller, [
      restore.done,
      pipeline(
        downloadStream,
        digest,
        ...decryptStage(config, manifest),
        restore.stdin,
        { signal: controller.signal },
      ),
    ]);

    assertChecksum(key, checksum, digest.digest());
    if (manifest && manifest.size !== digest.bytes) {
      throw new IntegrityError(
        `Size mismatch for ${key}: expected ${manifest.size}, got ${digest.bytes}`,
      );
    }

    log.info({ restored: summary.restored, target }, "Restore completed");
    return { key, source, target, restored: summary.restored, manifest };
  } finally {
    controller.dispose();
  }
}
