import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { type Transform, Writable } from "node:stream";
import { createGunzip } from "node:zlib";
import { MongoClient } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import { isClusterScope, readManifest, resolveBackup, resolveKeyRing, scopeOf } from "./catalog.js";
import { createDecryptor, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { MongoOplogTargetClient, OplogApplier } from "./oplog-applier.js";
import {
  downloadAndExtractOplogSlice,
  filterOplogSliceFile,
  formatOplogTimestamp,
  incrementOplogTimestamp,
  resolvePitrPlan,
  streamOplogSlice,
} from "./oplog.js";
import { runStreamPipeline } from "./stream.js";
import { mongorestore } from "./tools.js";
import type { BackupManifest, ShardedClusterMeta } from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type {
  DescribeBackupResult,
  RestoreContext,
  RestoreOptions,
  RestoreResult,
  VerifyBackupResult,
} from "./types/restore.js";

/**
 * System namespaces excluded during full-cluster logical restores
 * so target cluster metadata, sessions, active transaction tables, and FCV are never clobbered.
 */
export const CLUSTER_SYSTEM_NS_EXCLUDE = [
  "admin.system.version",
  "config.system.sessions",
  "config.transactions",
  "config.rangeDeletions",
  "config.shards",
  "config.version",
  "config.locks",
  "config.lockpings",
  "config.mongos",
  "config.cache.*",
];

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

  const keyRing = resolveKeyRing(config);
  const isSigned = Boolean(manifest?.signature?.hmac || manifest?.encryption?.manifestHmac);

  if (isSigned && manifest) {
    if (keyRing.length === 0) {
      throw new Error(
        `Backup manifest for ${key} is signed with HMAC, but ENCRYPTION_KEY / key ring is not configured`,
      );
    }
    try {
      verifyManifest(keyRing, manifest);
    } catch (error) {
      throw new IntegrityError((error as Error).message);
    }
  } else if (manifest?.encryption) {
    if (keyRing.length === 0) {
      throw new Error(`${key} is encrypted, but ENCRYPTION_KEY is not set`);
    }
  }
  return { key, manifest, checksum };
}

function decryptStage(config: AppConfig, manifest: BackupManifest | null): Transform[] {
  const keyRing = resolveKeyRing(config);
  return manifest?.encryption ? [createDecryptor(keyRing, manifest.encryption)] : [];
}

function assertChecksum(key: string, expected: string | null, actual: string): void {
  if (expected && expected !== actual) {
    throw new IntegrityError(`Checksum mismatch for ${key}: expected ${expected}, got ${actual}`);
  }
}

async function verifyGzipArchiveObject(
  ctx: RestoreContext,
  key: string,
): Promise<VerifyBackupResult> {
  const { config, storage, logger, signal } = ctx;
  const manifest = await readManifest(storage, key);
  const object = manifest ? null : await storage.head(key);
  if (!manifest && !object) {
    throw new IntegrityError(`Backup not found: ${key}`);
  }
  if (manifest?.key && manifest.key !== key) {
    throw new IntegrityError(
      `Manifest key mismatch for ${key}: manifest belongs to ${manifest.key}`,
    );
  }
  const keyRing = resolveKeyRing(config);
  const isSigned = Boolean(manifest?.signature?.hmac || manifest?.encryption?.manifestHmac);
  if (isSigned && manifest) {
    if (keyRing.length === 0) {
      throw new IntegrityError(
        `Backup manifest for ${key} is signed with HMAC, but ENCRYPTION_KEY is not configured`,
      );
    }
    try {
      verifyManifest(keyRing, manifest);
    } catch (error) {
      throw new IntegrityError((error as Error).message);
    }
  } else if (manifest?.encryption && keyRing.length === 0) {
    throw new IntegrityError(`${key} is encrypted, but ENCRYPTION_KEY is not set`);
  }

  const checksum = manifest?.sha256 ?? object?.metadata?.sha256 ?? null;
  if (!checksum) {
    throw new IntegrityError(`${key} has no manifest or checksum to verify against`);
  }

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
    await runStreamPipeline(stages, { signal });
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

  logger.debug({ key, sha256 }, "Archive object verified");
  return { key, size: digest.bytes, sha256, encrypted: Boolean(manifest?.encryption) };
}

async function verifyShardedClusterBackup(
  ctx: RestoreContext,
  key: string,
  manifest: BackupManifest | null,
  checksum: string,
): Promise<VerifyBackupResult> {
  const { config, storage, logger, signal } = ctx;
  const digest = new DigestStream();
  const chunks: Buffer[] = [];
  const collector = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      callback();
    },
  });

  try {
    const stages = [
      await storage.download(key, { signal }),
      digest,
      ...decryptStage(config, manifest),
      collector,
    ];
    await runStreamPipeline(stages, { signal });
  } catch (error: unknown) {
    const err = error as { name?: string; message?: string };
    if (err.name === "AbortError") throw error;
    throw new IntegrityError(`${key} could not be read back: ${err.message ?? String(error)}`);
  }

  const sha256 = digest.digest();
  assertChecksum(key, checksum, sha256);

  let clusterMeta: ShardedClusterMeta;
  try {
    clusterMeta = JSON.parse(Buffer.concat(chunks).toString("utf8")) as ShardedClusterMeta;
  } catch (error) {
    throw new IntegrityError(
      `Invalid sharded cluster metadata JSON in ${key}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  // Recursively verify configsvr backup, every shard backup, and all bridging slices
  const keysToVerify = new Set<string>();
  if (clusterMeta.configsvr?.backupKey) {
    keysToVerify.add(clusterMeta.configsvr.backupKey);
  }
  if (clusterMeta.configsvr?.bridgingSliceKey) {
    keysToVerify.add(clusterMeta.configsvr.bridgingSliceKey);
  }
  for (const sKey of clusterMeta.configsvr?.bridgingSliceKeys ?? []) {
    keysToVerify.add(sKey);
  }
  for (const shard of Object.values(clusterMeta.shards ?? {})) {
    if (shard.backupKey) keysToVerify.add(shard.backupKey);
    if (shard.bridgingSliceKey) keysToVerify.add(shard.bridgingSliceKey);
    for (const sKey of shard.bridgingSliceKeys ?? []) {
      keysToVerify.add(sKey);
    }
  }

  logger.info(
    { key, childObjects: keysToVerify.size },
    "Verifying sharded cluster child archives and bridging slices",
  );
  for (const childKey of keysToVerify) {
    await verifyGzipArchiveObject(ctx, childKey);
  }

  logger.info(
    { key, sha256, verifiedChildren: keysToVerify.size },
    "Sharded cluster backup verified",
  );
  return { key, size: digest.bytes, sha256, encrypted: Boolean(manifest?.encryption) };
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

  if (manifest?.type === "sharded-cluster-backup" || /\.cluster(\.enc)?$/.test(key)) {
    logger.info({ key }, "Verifying sharded cluster backup");
    return verifyShardedClusterBackup(ctx, key, manifest, checksum);
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
    await runStreamPipeline(stages, { signal });
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
  // mongorestore rejects --nsExclude when --oplogReplay is enabled ("cannot use --oplogReplay with excludes specified").
  // Pass systemNsExclude whenever oplogReplay is disabled; when oplogReplay is active, mongorestore handles internal collections natively.
  const systemNsExclude =
    isCluster && !hasNamespaceFilter && drop && !shouldReplayOplog
      ? CLUSTER_SYSTEM_NS_EXCLUDE
      : undefined;

  const controller = linkedController(signal);
  try {
    const effectiveMongorestore = tools.mongorestore ?? mongorestore;
    const restore = await effectiveMongorestore({
      uri,
      workDir: config.workDir,
      nsInclude: effectiveNsInclude,
      nsExclude,
      systemNsExclude,
      nsFrom: isRenamedDb ? `${source}.*` : undefined,
      nsTo: isRenamedDb ? `${target}.*` : undefined,
      drop,
      preserveUUID: drop && !isRenamedDb,
      bypassDocumentValidation: true,
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
      runStreamPipeline(stages, { signal: controller.signal }),
    ]);

    assertChecksum(key, checksum, digest.digest());
    if (manifest && manifest.size !== digest.bytes) {
      throw new IntegrityError(
        `Size mismatch for ${key}: expected ${manifest.size}, got ${digest.bytes}`,
      );
    }

    log.info({ restored: summary.restored, target }, "Restore completed");
    return { key, source, target, restored: summary.restored, manifest };
  } catch (error) {
    if (drop) {
      log.error(
        { err: error, target },
        "CRITICAL: Destructive restore with --drop failed mid-operation. Target collections may be in a partially dropped or torn state. Immediate operator intervention, staging restore, or re-restore required.",
      );
    }
    throw error;
  } finally {
    controller.dispose();
  }
}

export async function restorePointInTime(
  ctx: RestoreContext,
  {
    pitr,
    uri,
    database,
    nsInclude,
    nsExclude,
    drop = false,
    preVerify = drop,
    parallelCollections,
  }: RestoreOptions,
): Promise<RestoreResult> {
  const { config, storage, logger, signal, tools } = ctx;
  const initialIsFiltered =
    (Boolean(database) &&
      !isClusterScope(database) &&
      (isClusterScope(config.mongo.database) || database !== config.mongo.database)) ||
    Boolean(nsInclude) ||
    Boolean(nsExclude);

  let plan = await resolvePitrPlan(storage, scopeOf(config), pitr!, {
    engine: "logical",
    includeBaseWindow: initialIsFiltered,
  });

  const source = plan.baseManifest?.database ?? config.mongo.database;
  const target = database ?? source;
  const isCluster = isClusterScope(source);
  const isFilteredCluster = isCluster && !isClusterScope(target);
  const isRenamedDb = !isCluster && target !== source;
  const hasNamespaceFilter =
    isFilteredCluster || isRenamedDb || Boolean(nsInclude) || Boolean(nsExclude);

  if (hasNamespaceFilter !== initialIsFiltered) {
    plan = await resolvePitrPlan(storage, scopeOf(config), pitr!, {
      engine: "logical",
      includeBaseWindow: hasNamespaceFilter,
    });
  }

  const targetLimit = formatOplogTimestamp(plan.targetTs);
  const exclusiveLimit = formatOplogTimestamp(incrementOplogTimestamp(plan.targetTs));

  if (hasNamespaceFilter && plan.baseManifest?.oplog && plan.slices.length === 0) {
    throw new Error(
      `Cannot perform namespace-filtered PITR restore from ${plan.baseBackup.key}: --oplogReplay on the base archive is disabled for filtered restores and no external oplog slices cover the base dump window`,
    );
  }

  logger.info(
    {
      baseKey: plan.baseBackup.key,
      baseEndTs: formatOplogTimestamp(plan.baseEndTs),
      slicesCount: plan.slices.length,
      targetTs: targetLimit,
    },
    "Resolved Point-in-Time Recovery (PITR) plan",
  );

  const replayedBaseOplog = Boolean(plan.baseManifest?.oplog) && !hasNamespaceFilter;

  // Stage 1: Restore base backup
  const baseResult = await restoreBackup(ctx, {
    ref: plan.baseBackup.key,
    uri,
    database,
    nsInclude,
    nsExclude,
    drop,
    preVerify,
    oplogReplay: replayedBaseOplog,
    oplogLimit: plan.slices.length === 0 && replayedBaseOplog ? exclusiveLimit : undefined,
    parallelCollections,
  });

  // Stage 2: Replay continuous oplog slices up to plan.targetTs
  const controller = linkedController(signal);
  let replayedEntries = 0;
  let oplogStats: RestoreResult["oplogStats"];

  try {
    if (tools?.replayOplogFile) {
      // Injectable legacy replay hook (primarily used by unit tests)
      for (const slice of plan.slices) {
        const sliceFile = path.join(config.workDir, `.pitr-slice-${randomUUID()}.oplog.bson`);
        const filteredFile = path.join(config.workDir, `.pitr-filtered-${randomUUID()}.oplog.bson`);
        try {
          await downloadAndExtractOplogSlice(
            { config, storage, signal: controller.signal },
            slice.key,
            sliceFile,
          );
          const { keptEntries } = await filterOplogSliceFile(sliceFile, filteredFile, {
            startExclusiveTs: replayedBaseOplog ? plan.baseEndTs : undefined,
            targetTs: plan.targetTs,
            database: isFilteredCluster ? target : undefined,
            nsInclude,
            nsExclude,
          });
          if (keptEntries === 0) {
            logger.debug({ sliceKey: slice.key }, "Skipping oplog slice with 0 matching entries");
            continue;
          }
          await tools.replayOplogFile({
            uri,
            workDir: config.workDir,
            oplogFile: filteredFile,
            oplogLimit: exclusiveLimit,
            timeoutMs: config.timeoutMs,
            signal: controller.signal,
            logger,
          });
          replayedEntries += keptEntries;
          logger.info(
            {
              sliceKey: slice.key,
              startTs: formatOplogTimestamp(slice.startTs),
              endTs: formatOplogTimestamp(slice.endTs),
              keptEntries,
              oplogLimit: targetLimit,
            },
            "Replayed PITR oplog slice",
          );
        } finally {
          await Promise.allSettled([
            fs.rm(sliceFile, { force: true }),
            fs.rm(filteredFile, { force: true }),
          ]);
        }
      }
    } else if (plan.slices.length > 0) {
      // Production streaming OplogApplier (0 temp disk files, cross-slice txns, index DDL, rename cloning)
      const client = new MongoClient(uri);
      await client.connect();
      const applier = new OplogApplier({
        target: new MongoOplogTargetClient(client),
        workDir: config.workDir,
        logger,
        startExclusiveTs: replayedBaseOplog ? plan.baseEndTs : undefined,
        endTs: plan.targetTs,
        filter: {
          database: isFilteredCluster ? target : undefined,
          nsInclude,
          nsExclude,
          clone: isRenamedDb ? { fromDb: source, toDb: target } : undefined,
        },
        backupType: "logical",
      });

      try {
        for (const slice of plan.slices) {
          const prevApplied = applier.getStats().appliedEntries;
          await streamOplogSlice(
            { config, storage, signal: controller.signal },
            slice.key,
            (stream) => applier.applyStream(stream, controller.signal),
          );
          const appliedInSlice = applier.getStats().appliedEntries - prevApplied;
          logger.info(
            {
              sliceKey: slice.key,
              startTs: formatOplogTimestamp(slice.startTs),
              endTs: formatOplogTimestamp(slice.endTs),
              appliedEntries: appliedInSlice,
              oplogLimit: targetLimit,
            },
            "Replayed PITR oplog slice",
          );
        }
        await applier.buildPendingIndexes();
        applier.assertComplete();
        oplogStats = applier.getStats();
        replayedEntries = oplogStats.appliedEntries;
        logger.info(
          {
            appliedEntries: oplogStats.appliedEntries,
            skippedEntries: oplogStats.skippedEntries,
            skipBreakdown: oplogStats.skipBreakdown,
            indexesBuilt: oplogStats.indexesBuilt,
            uncommittedTxns: oplogStats.uncommittedTxns,
            partialTxns: oplogStats.partialTxns,
          },
          "Completed PITR oplog replay",
        );
      } finally {
        await applier.dispose();
        await client.close().catch(() => {});
      }
    }

    return {
      ...baseResult,
      pitrTarget: targetLimit,
      replayedSlices: plan.slices.length,
      replayedEntries,
      ...(oplogStats ? { oplogStats } : {}),
    };
  } finally {
    controller.dispose();
  }
}
