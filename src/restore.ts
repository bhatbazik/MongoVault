import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { type Transform, Writable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip } from "node:zlib";
import { linkedController, settleAll } from "./abort.js";
import { isClusterScope, readManifest, resolveBackup, scopeOf } from "./catalog.js";
import { createDecryptor, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { downloadAndExtractOplogSlice, formatOplogTimestamp, resolvePitrPlan } from "./oplog.js";
import { mongorestore, replayOplogFile } from "./tools.js";
import type { BackupManifest } from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type {
  DescribeBackupResult,
  RestoreContext,
  RestoreOptions,
  RestoreResult,
  VerifyBackupResult,
} from "./types/restore.js";

export class IntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IntegrityError";
  }
}

export async function describeBackup(
  { config, storage }: RestoreContext,
  ref: string,
): Promise<DescribeBackupResult> {
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
  const checksum = manifest?.sha256 ?? object?.metadata?.sha256 ?? null;

  const keyRing = config.encryptionKeyRing || (config.encryptionKey ? [config.encryptionKey] : []);
  if (manifest?.encryption) {
    if (keyRing.length === 0) {
      throw new Error(`${key} is encrypted, but ENCRYPTION_KEY is not set`);
    }
    try {
      verifyManifest(keyRing, manifest);
    } catch (error) {
      throw new IntegrityError((error as Error).message);
    }
  }
  return { key, manifest, checksum };
}

function decryptStage(config: AppConfig, manifest: BackupManifest | null): Transform[] {
  const keyRing = config.encryptionKeyRing || (config.encryptionKey ? [config.encryptionKey] : []);
  return manifest?.encryption ? [createDecryptor(keyRing, manifest.encryption)] : [];
}

function assertChecksum(key: string, expected: string | null, actual: string): void {
  if (expected && expected !== actual) {
    throw new IntegrityError(`Checksum mismatch for ${key}: expected ${expected}, got ${actual}`);
  }
}

export async function verifyBackup(
  ctx: RestoreContext,
  ref: string = "latest",
): Promise<VerifyBackupResult> {
  const { config, storage, logger, signal } = ctx;
  const { key, manifest, checksum } = await describeBackup(ctx, ref);
  if (!checksum) {
    throw new IntegrityError(`${key} has no manifest or checksum to verify against`);
  }

  logger.info({ key }, "Verifying backup");
  const digest = new DigestStream();
  const discard = new Writable({ write: (_chunk, _encoding, callback) => callback() });

  try {
    const stages = [
      await storage.download(key, { signal }),
      digest,
      ...decryptStage(config, manifest),
      createGunzip(),
      discard,
    ];
    const runPipeline = pipeline as unknown as (...args: unknown[]) => Promise<void>;
    await runPipeline(...stages, { signal });
  } catch (error: unknown) {
    const err = error as { name?: string; message?: string };
    if (err.name === "AbortError") throw error;
    throw new IntegrityError(`${key} could not be read back: ${err.message ?? String(error)}`);
  }

  const sha256 = digest.digest();
  assertChecksum(key, checksum, sha256);
  if (manifest && manifest.size !== digest.bytes) {
    throw new IntegrityError(
      `Size mismatch for ${key}: expected ${manifest.size}, got ${digest.bytes}`,
    );
  }

  logger.info({ key, sha256 }, "Backup verified");
  return { key, size: digest.bytes, sha256, encrypted: Boolean(manifest?.encryption) };
}

export async function restoreBackup(
  ctx: RestoreContext,
  {
    ref = "latest",
    uri,
    database,
    nsInclude,
    nsExclude,
    drop = false,
    preVerify = drop,
    oplogReplay,
    oplogLimit,
    pitr,
    parallelCollections,
  }: RestoreOptions,
): Promise<RestoreResult> {
  if (pitr) {
    return restorePointInTime(ctx, {
      pitr,
      uri,
      database,
      nsInclude,
      nsExclude,
      drop,
      preVerify,
      parallelCollections,
    });
  }

  const { config, storage, logger, signal, tools = { mongorestore } } = ctx;
  const { key, manifest, checksum } = await describeBackup(ctx, ref);
  const log = logger.child({ key });
  const source = manifest?.database ?? config.mongo.database;
  const target = database ?? source;
  const isCluster = isClusterScope(source);
  const isFilteredCluster = isCluster && !isClusterScope(target);
  const isRenamedDb = !isCluster && target !== source;
  const hasNamespaceFilter =
    isFilteredCluster || isRenamedDb || Boolean(nsInclude) || Boolean(nsExclude);

  if (hasNamespaceFilter && (oplogReplay === true || Boolean(oplogLimit))) {
    throw new Error(
      "Cannot use --oplog-replay or --oplog-limit when filtering namespaces or renaming target database",
    );
  }

  const shouldReplayOplog = hasNamespaceFilter
    ? false
    : (oplogReplay ?? (Boolean(oplogLimit) || Boolean(manifest?.oplog)));

  if (!checksum) log.warn("Backup has no checksum; restoring without an integrity check");

  if (preVerify) {
    log.info(
      { ref, drop },
      "Pre-verifying backup archive integrity and checksum before dropping collections",
    );
    await verifyBackup(ctx, ref);
  }

  const effectiveNsInclude =
    nsInclude ?? (isFilteredCluster ? `${target}.*` : isRenamedDb ? `${source}.*` : undefined);

  const controller = linkedController(signal);
  try {
    const effectiveMongorestore = tools.mongorestore ?? mongorestore;
    const restore = await effectiveMongorestore({
      uri,
      workDir: config.workDir,
      nsInclude: effectiveNsInclude,
      nsExclude,
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
    const stages = [downloadStream, digest, ...decryptStage(config, manifest), restore.stdin];

    const [summary] = await settleAll(controller, [
      restore.done,
      (pipeline as unknown as (...args: unknown[]) => Promise<void>)(...stages, {
        signal: controller.signal,
      }),
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

export async function restorePointInTime(
  ctx: RestoreContext,
  { pitr, uri, database, drop = false, preVerify = drop, parallelCollections }: RestoreOptions,
): Promise<RestoreResult> {
  const { config, storage, logger, signal, tools } = ctx;
  const effectiveReplay = tools?.replayOplogFile ?? replayOplogFile;
  const plan = await resolvePitrPlan(storage, scopeOf(config), pitr!);
  const targetLimit = formatOplogTimestamp(plan.targetTs);

  logger.info(
    {
      baseKey: plan.baseBackup.key,
      baseEndTs: formatOplogTimestamp(plan.baseEndTs),
      slicesCount: plan.slices.length,
      targetTs: targetLimit,
    },
    "Resolved Point-in-Time Recovery (PITR) plan",
  );

  // Stage 1: Restore base backup
  const baseResult = await restoreBackup(ctx, {
    ref: plan.baseBackup.key,
    uri,
    database,
    drop,
    preVerify,
    oplogReplay: Boolean(plan.baseManifest?.oplog),
    oplogLimit: plan.slices.length === 0 && plan.baseManifest?.oplog ? targetLimit : undefined,
    parallelCollections,
  });

  // Stage 2: Sequentially download, verify, decrypt, and replay continuous oplog slices up to targetLimit
  const controller = linkedController(signal);
  let replayedEntries = 0;

  try {
    for (const slice of plan.slices) {
      const sliceFile = path.join(config.workDir, `.pitr-slice-${randomUUID()}.oplog.bson`);
      try {
        const extracted = await downloadAndExtractOplogSlice(
          { config, storage, signal: controller.signal },
          slice.key,
          sliceFile,
        );
        await effectiveReplay({
          uri,
          workDir: config.workDir,
          oplogFile: sliceFile,
          oplogLimit: targetLimit,
          timeoutMs: config.timeoutMs,
          signal: controller.signal,
          logger,
        });
        replayedEntries += extracted.manifest?.oplogWindow?.entries ?? 0;
        logger.info(
          {
            sliceKey: slice.key,
            startTs: formatOplogTimestamp(slice.startTs),
            endTs: formatOplogTimestamp(slice.endTs),
            oplogLimit: targetLimit,
          },
          "Replayed PITR oplog slice",
        );
      } finally {
        await fs.rm(sliceFile, { force: true }).catch(() => {});
      }
    }

    return {
      ...baseResult,
      pitrTarget: targetLimit,
      replayedSlices: plan.slices.length,
      replayedEntries,
    };
  } finally {
    controller.dispose();
  }
}
