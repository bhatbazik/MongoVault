import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { type Transform, Writable } from "node:stream";
import { createGunzip, createGzip } from "node:zlib";
import { MongoClient } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import { ArchiveNamespaceFilter } from "./archive-stream.js";
import { isClusterScope, readManifest, resolveBackup, resolveKeyRing, scopeOf } from "./catalog.js";
import { createDecryptor, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { MongoOplogTargetClient, OplogApplier, matchesNamespacePattern } from "./oplog-applier.js";
import {
  downloadAndExtractOplogSlice,
  filterOplogSliceFile,
  formatOplogTimestamp,
  incrementOplogTimestamp,
  parseOplogTimestamp,
  resolvePitrPlan,
  streamOplogSlice,
} from "./oplog.js";
import { runStreamPipeline } from "./stream.js";
import { mongorestore, validateRestoreNamespaceOptions } from "./tools.js";
import type { BackupManifest, ShardedClusterMeta } from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type { OplogTimestamp } from "./types/oplog.js";
import type {
  DescribeBackupResult,
  RestoreContext,
  RestoreOptions,
  RestoreResult,
  VerifyBackupResult,
} from "./types/restore.js";

/**
 * System namespaces excluded during full-cluster logical restores
 * so target cluster metadata, sessions, active transaction tables, pre-images,
 * shard/balancer history, internal signing keys, and FCV are never clobbered.
 *
 * Note on `admin.system.users` and `admin.system.roles` (R1 / PBM parity):
 * `mongorestore` skips `admin.system.users` and `admin.system.roles` by default unless
 * `--restoreDbUsersAndRoles` is passed (which MongoVault never passes), and `local.*`
 * is skipped natively, so no user/role temporary-collection swap is performed.
 */
export const CLUSTER_SYSTEM_NS_EXCLUDE = [
  "admin.system.version",
  "admin.system.keys",
  "config.system.sessions",
  "config.system.preimages",
  "config.transactions",
  "config.rangeDeletions",
  "config.shards",
  "config.version",
  "config.locks",
  "config.lockpings",
  "config.mongos",
  "config.actionlog",
  "config.changelog",
  "config.migrations",
  "config.cache.*",
  "config.shard.catalog.*",
];

const PROTECTED_SYSTEM_DBS = new Set(["admin", "local", "config"]);

export class IntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IntegrityError";
  }
}

function assertManifestIntegrity(
  config: AppConfig,
  key: string,
  manifest: BackupManifest | null,
): void {
  if (config.requireEncryption && (!manifest || !manifest.encryption)) {
    throw new IntegrityError(
      `REQUIRE_ENCRYPTION is enabled, but backup ${key} lacks an encrypted manifest`,
    );
  }
  if (!manifest) return;

  const keyRing = resolveKeyRing(config);
  const isSigned = Boolean(manifest.signature?.hmac || manifest.encryption?.manifestHmac);
  const mustVerify =
    isSigned ||
    Boolean(manifest.encryption) ||
    keyRing.length > 0 ||
    Boolean(config.requireEncryption);

  if (mustVerify) {
    if ((isSigned || manifest.encryption || config.requireEncryption) && keyRing.length === 0) {
      throw new IntegrityError(
        `Backup manifest for ${key} requires HMAC verification, but ENCRYPTION_KEY / key ring is not configured`,
      );
    }
    try {
      verifyManifest(keyRing, manifest, {
        requireSigned: Boolean(config.requireEncryption || manifest.encryption),
      });
    } catch (error) {
      throw new IntegrityError(error instanceof Error ? error.message : String(error));
    }
  }
}

function decrementOplogTimestamp(ts: OplogTimestamp): OplogTimestamp | null {
  const t = ts.t >>> 0;
  const i = ts.i >>> 0;
  if (i > 0) {
    return { t, i: i - 1 };
  }
  if (t > 0) {
    return { t: t - 1, i: 0xffffffff };
  }
  return null;
}

function expandTimeseriesNamespacePatterns(pattern?: string): string | undefined {
  if (!pattern) return undefined;
  const parts = pattern
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
  if (parts.length === 0) return undefined;
  const out = new Set<string>();
  for (const part of parts) {
    out.add(part);
    const dotIdx = part.indexOf(".");
    if (dotIdx > 0 && dotIdx < part.length - 1) {
      const db = part.slice(0, dotIdx);
      const coll = part.slice(dotIdx + 1);
      if (coll !== "*" && !coll.startsWith("system.") && !coll.startsWith("$")) {
        out.add(`${db}.system.buckets.${coll}`);
      }
    }
  }
  return [...out].join(",");
}

function abortErrorFromSignal(signal?: AbortSignal, fallbackMessage = "Operation aborted"): Error {
  const reason = signal?.reason;
  if (reason instanceof Error) return reason;
  const err = new Error(reason ? String(reason) : fallbackMessage);
  err.name = "AbortError";
  return err;
}

async function connectMongoWithSignal(client: MongoClient, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    void client.close().catch(() => {});
    throw abortErrorFromSignal(signal);
  }
  if (!signal) {
    await client.connect();
    return;
  }
  let onAbort: (() => void) | undefined;
  try {
    await Promise.race([
      client.connect(),
      new Promise<never>((_resolve, reject) => {
        onAbort = () => {
          void client.close().catch(() => {});
          reject(abortErrorFromSignal(signal));
        };
        signal.addEventListener("abort", onAbort, { once: true });
      }),
    ]);
  } finally {
    if (onAbort) {
      signal.removeEventListener("abort", onAbort);
    }
  }
}

function ignoreNamespaceNotFound(err: unknown): void {
  if (!err || typeof err !== "object") throw err;
  const code = (err as { code?: unknown }).code;
  const msg = String((err as { message?: unknown }).message ?? "");
  if (code === 26 || /ns not found/i.test(msg)) {
    return;
  }
  throw err;
}

async function dropTargetNamespacesBeforePitr(
  client: MongoClient,
  opts: {
    source: string;
    target: string;
    isRenamedDb: boolean;
    nsInclude?: string;
    nsExclude?: string;
  },
): Promise<void> {
  const { source, target, isRenamedDb, nsInclude, nsExclude } = opts;
  const targetIsCluster = isClusterScope(target);

  if (nsInclude === undefined && nsExclude === undefined) {
    if (targetIsCluster) {
      const res = (await client.db("admin").command({ listDatabases: 1, nameOnly: true })) as {
        databases?: Array<{ name?: string }>;
      };
      for (const entry of res.databases ?? []) {
        const dbName = entry.name;
        if (!dbName || PROTECTED_SYSTEM_DBS.has(dbName)) continue;
        await client.db(dbName).dropDatabase();
      }
      return;
    }
    if (!PROTECTED_SYSTEM_DBS.has(target)) {
      await client.db(target).dropDatabase();
    }
    return;
  }

  const expandedInclude = expandTimeseriesNamespacePatterns(nsInclude);
  const expandedExclude = expandTimeseriesNamespacePatterns(nsExclude);
  if (nsInclude !== undefined && !expandedInclude) {
    throw new Error("nsInclude must contain at least one non-empty namespace pattern");
  }
  if (nsExclude !== undefined && !expandedExclude) {
    throw new Error("nsExclude must contain at least one non-empty namespace pattern");
  }

  let candidateDbs: string[];
  if (!targetIsCluster) {
    candidateDbs = PROTECTED_SYSTEM_DBS.has(target) ? [] : [target];
  } else {
    const res = (await client.db("admin").command({ listDatabases: 1, nameOnly: true })) as {
      databases?: Array<{ name?: string }>;
    };
    candidateDbs = (res.databases ?? [])
      .map((d) => d.name)
      .filter((n): n is string => Boolean(n) && !PROTECTED_SYSTEM_DBS.has(n!));
  }

  for (const dbName of candidateDbs) {
    if (PROTECTED_SYSTEM_DBS.has(dbName)) continue;
    const matchDb = isRenamedDb ? source : dbName;
    if (
      !expandedExclude &&
      (!expandedInclude || matchesNamespacePattern(`${matchDb}.*`, expandedInclude))
    ) {
      await client.db(dbName).dropDatabase();
      continue;
    }

    const db = client.db(dbName);
    const colls = await db.listCollections({}, { nameOnly: true }).toArray();
    for (const coll of colls) {
      const collName = coll.name;
      if (!collName) continue;
      if (
        collName.startsWith("system.") &&
        collName !== "system.views" &&
        !collName.startsWith("system.buckets.")
      ) {
        continue;
      }
      const matchNs = `${matchDb}.${collName}`;
      if (expandedInclude && !matchesNamespacePattern(matchNs, expandedInclude)) {
        continue;
      }
      if (expandedExclude && matchesNamespacePattern(matchNs, expandedExclude)) {
        continue;
      }
      await db.collection(collName).drop().catch(ignoreNamespaceNotFound);
    }
  }
}

export async function describeBackup(
  { config, storage }: RestoreContext,
  ref: string,
): Promise<DescribeBackupResult> {
  const key = await resolveBackup(storage, scopeOf(config), ref);
  const manifest = await readManifest(storage, key);
  const hasHead = typeof storage.head === "function";
  const object = hasHead ? await storage.head(key) : null;
  if (!manifest && !object) {
    throw new Error(`Backup not found: ${key}`);
  }
  if (manifest && hasHead && !object) {
    throw new IntegrityError(`Backup archive object not found for ${key}`);
  }
  if (manifest?.key && manifest.key !== key) {
    throw new IntegrityError(
      `Manifest key mismatch for ${key}: manifest belongs to ${manifest.key}`,
    );
  }
  const checksum = manifest?.sha256 ?? object?.metadata?.sha256 ?? null;

  assertManifestIntegrity(config, key, manifest);
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

export async function verifyArchiveObject(
  ctx: RestoreContext,
  key: string,
): Promise<VerifyBackupResult> {
  const { config, storage, logger, signal } = ctx;
  const manifest = await readManifest(storage, key);
  const hasHead = typeof storage.head === "function";
  const object = hasHead ? await storage.head(key) : null;
  if (!manifest && !object) {
    throw new IntegrityError(`Backup not found: ${key}`);
  }
  if (manifest && hasHead && !object) {
    throw new IntegrityError(`Backup archive object not found for ${key}`);
  }
  if (manifest?.key && manifest.key !== key) {
    throw new IntegrityError(
      `Manifest key mismatch for ${key}: manifest belongs to ${manifest.key}`,
    );
  }
  assertManifestIntegrity(config, key, manifest);

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
  const verifiedSizes = new Map<string, number>();
  for (const childKey of keysToVerify) {
    const childResult = await verifyArchiveObject(ctx, childKey);
    verifiedSizes.set(childKey, childResult.size);
  }

  if (manifest) {
    let childBackupSizeSum = 0;
    if (clusterMeta.configsvr?.backupKey) {
      childBackupSizeSum += verifiedSizes.get(clusterMeta.configsvr.backupKey) ?? 0;
    }
    for (const shard of Object.values(clusterMeta.shards ?? {})) {
      if (shard.backupKey) {
        childBackupSizeSum += verifiedSizes.get(shard.backupKey) ?? 0;
      }
    }
    if (manifest.size !== digest.bytes && manifest.size !== childBackupSizeSum) {
      throw new IntegrityError(
        `Size mismatch for ${key}: expected ${manifest.size}, got ${digest.bytes} (or child backup sum ${childBackupSizeSum})`,
      );
    }
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
  options: RestoreOptions,
): Promise<RestoreResult> {
  const {
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
  } = options;
  const { nsFrom, nsTo } = options as RestoreOptions & { nsFrom?: string; nsTo?: string };
  validateRestoreNamespaceOptions({ nsInclude, nsExclude, nsFrom, nsTo });

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

  if (!checksum) {
    if (drop) {
      throw new IntegrityError(
        `Refusing destructive restore (--drop) for ${key}: backup has no manifest or checksum to verify against`,
      );
    }
    log.warn("Backup has no checksum; restoring without an integrity check");
  }

  if (preVerify) {
    log.info(
      { key, drop },
      "Pre-verifying backup archive integrity and checksum before dropping collections",
    );
    await verifyBackup(ctx, key);
  }

  const effectiveNsInclude =
    nsInclude ?? (isFilteredCluster ? `${target}.*` : isRenamedDb ? `${source}.*` : undefined);
  // mongorestore rejects --nsExclude when --oplogReplay is enabled ("cannot use --oplogReplay with excludes specified").
  // When oplogReplay is active on full-cluster restores, strip CLUSTER_SYSTEM_NS_EXCLUDE via
  // ArchiveNamespaceFilter before mongorestore; when oplogReplay is disabled, pass systemNsExclude.
  const systemNsExclude = isCluster && !shouldReplayOplog ? CLUSTER_SYSTEM_NS_EXCLUDE : undefined;

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
    const archiveFilterStages: Transform[] =
      isCluster && shouldReplayOplog
        ? [
            createGunzip(),
            new ArchiveNamespaceFilter(CLUSTER_SYSTEM_NS_EXCLUDE, { passthroughNonArchive: true }),
            createGzip(),
          ]
        : [];
    const stages = [
      downloadStream,
      digest,
      ...decryptStage(config, manifest),
      ...archiveFilterStages,
      restore.stdin,
    ];

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
  options: RestoreOptions,
): Promise<RestoreResult> {
  const {
    pitr,
    uri,
    database,
    nsInclude,
    nsExclude,
    drop = false,
    preVerify = drop,
    parallelCollections,
  } = options;
  const { nsFrom, nsTo } = options as RestoreOptions & { nsFrom?: string; nsTo?: string };
  validateRestoreNamespaceOptions({ nsInclude, nsExclude, nsFrom, nsTo });

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

  // Pre-verify ALL planned PITR oplog slices AND the base backup BEFORE any destructive --drop
  // so a corrupted, missing, or undecryptable artifact never leaves the target database torn.
  if (preVerify) {
    if (plan.slices.length > 0 && typeof storage.download === "function") {
      logger.info(
        { slicesCount: plan.slices.length, drop },
        "Pre-verifying all planned PITR oplog slices before base restore",
      );
      for (const slice of plan.slices) {
        await verifyArchiveObject(ctx, slice.key);
      }
    }
    if (typeof storage.download === "function") {
      await verifyBackup(ctx, plan.baseBackup.key);
    }
  } else if (drop) {
    const baseDesc = await describeBackup(ctx, plan.baseBackup.key);
    if (!baseDesc.checksum) {
      throw new IntegrityError(
        `Refusing destructive PITR restore (--drop) for ${plan.baseBackup.key}: backup has no manifest or checksum to verify against`,
      );
    }
  }

  // Drop target non-system namespaces/databases created after the base snapshot before Stage 1
  // so post-backup collections do not survive PITR with --drop.
  if (drop && (!tools?.mongorestore || ctx.createMongoClient)) {
    const dropController = linkedController(signal);
    const dropClient = (ctx.createMongoClient ?? ((u) => new MongoClient(u)))(uri);
    try {
      await connectMongoWithSignal(dropClient, dropController.signal);
      await dropTargetNamespacesBeforePitr(dropClient, {
        source,
        target,
        isRenamedDb,
        nsInclude,
        nsExclude,
      });
    } finally {
      dropController.dispose();
      await dropClient.close().catch(() => {});
    }
  }

  // Stage 1: Restore base backup (already pre-verified above when preVerify is true)
  const baseResult = await restoreBackup(ctx, {
    ref: plan.baseBackup.key,
    uri,
    database,
    nsInclude,
    nsExclude,
    drop,
    preVerify: false,
    oplogReplay: replayedBaseOplog,
    oplogLimit: replayedBaseOplog ? exclusiveLimit : undefined,
    parallelCollections,
  });

  // Stage 2: Replay continuous oplog slices up to plan.targetTs
  const baseStartTs = plan.baseManifest?.oplogWindow?.startTs
    ? parseOplogTimestamp(plan.baseManifest.oplogWindow.startTs)
    : null;
  const stage2StartTs = !replayedBaseOplog && baseStartTs ? baseStartTs : undefined;
  const stage2StartExclusiveTs = replayedBaseOplog
    ? plan.baseEndTs
    : baseStartTs
      ? (decrementOplogTimestamp(baseStartTs) ?? undefined)
      : plan.baseEndTs;

  const expandedNsInclude = expandTimeseriesNamespacePatterns(nsInclude);
  const expandedNsExclude = expandTimeseriesNamespacePatterns(nsExclude);

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
            startTs: stage2StartTs,
            startExclusiveTs: stage2StartExclusiveTs,
            targetTs: plan.targetTs,
            database: isFilteredCluster ? target : undefined,
            nsInclude: expandedNsInclude,
            nsExclude: expandedNsExclude,
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
      const client = (ctx.createMongoClient ?? ((u) => new MongoClient(u)))(uri);
      let applier: OplogApplier | null = null;

      try {
        await connectMongoWithSignal(client, controller.signal);
        applier = new OplogApplier({
          target: new MongoOplogTargetClient(client),
          workDir: config.workDir,
          logger,
          startTs: stage2StartTs,
          startExclusiveTs: stage2StartExclusiveTs,
          endTs: plan.targetTs,
          filter: {
            database: isFilteredCluster ? target : undefined,
            nsInclude: expandedNsInclude,
            nsExclude: expandedNsExclude,
            clone: isRenamedDb ? { fromDb: source, toDb: target } : undefined,
          },
          backupType: "logical",
        });

        for (const slice of plan.slices) {
          const prevApplied = applier.getStats().appliedEntries;
          await streamOplogSlice(
            { config, storage, signal: controller.signal },
            slice.key,
            (stream) => applier!.applyStream(stream, controller.signal),
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
        await applier?.dispose();
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
