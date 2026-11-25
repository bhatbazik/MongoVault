import path from "node:path";
import {
  databasePrefix,
  listBackups,
  manifestKey,
  readManifest,
  resolveBackup,
  scopeOf,
} from "./catalog.js";
import { isStale, readLock } from "./lock.js";
import {
  compareTimestamps,
  formatOplogTimestamp,
  listOplogSlices,
  parseOplogTimestamp,
  parsePitrTarget,
  pitrPrefix,
} from "./oplog.js";
import { isLockActive, type LockRecord } from "./storage.js";
import type { BackupManifest, BackupScope, BackupSummary } from "./types/catalog.js";
import type { DateParts } from "./types/common.js";
import type { RetentionPolicy } from "./types/config.js";
import type { OplogSlice, OplogTimestamp } from "./types/oplog.js";
import type {
  DeleteBackupOptions,
  DeleteBackupResult,
  PruneContext,
  PruneOplogOptions,
  PruneOplogResult,
  PruneOptions,
  PruneResult,
  RetentionPlan,
  RetentionTarget,
} from "./types/retention.js";
import type { StorageClient } from "./types/storage.js";

const ORPHAN_AGE_MS = 60 * 60 * 1000;

const PERIODS: Record<string, (parts: DateParts) => string> = {
  keepDaily: ({ year, month, day }) => `${year}-${month}-${day}`,
  keepWeekly: isoWeek,
  keepMonthly: ({ year, month }) => `${year}-${month}`,
};

interface BatchErrorItem {
  key?: string;
  code?: string;
  message?: string;
  name?: string;
  Code?: string;
}

function isSingleObjectLockError(item: BatchErrorItem, objectLockEnabled: boolean): boolean {
  const code = item.code || item.Code || item.name || "";
  const message = item.message || "";
  const str = `${code} ${message}`.toLowerCase();
  if (
    str.includes("object lock") ||
    str.includes("worm") ||
    str.includes("legal hold") ||
    str.includes("retention period") ||
    str.includes("immutable")
  ) {
    return true;
  }
  if (objectLockEnabled && (code === "AccessDenied" || /access\s*denied/i.test(message))) {
    return true;
  }
  return false;
}

function isObjectLockError(error: unknown, configObjectLockEnabled = false): boolean {
  const err = error as {
    name?: string;
    message?: string;
    Code?: string;
    code?: string;
    objectLockEnabled?: boolean;
    errors?: BatchErrorItem[];
    batchErrors?: BatchErrorItem[];
  };
  const effectiveObjectLock = Boolean(configObjectLockEnabled || err?.objectLockEnabled);
  const items = Array.isArray(err?.errors)
    ? err.errors
    : Array.isArray(err?.batchErrors)
      ? err.batchErrors
      : null;
  if (items && items.length > 0) {
    return items.every((item) => isSingleObjectLockError(item, effectiveObjectLock));
  }
  return isSingleObjectLockError(
    { name: err?.name, message: err?.message, Code: err?.Code, code: err?.code },
    effectiveObjectLock,
  );
}

async function deleteKeysFromStorage(storage: StorageClient, keys: string[]): Promise<void> {
  const unique = [...new Set(keys)];
  if (unique.length === 0) return;
  if (typeof storage.removeMany === "function") {
    await storage.removeMany(unique);
  } else {
    for (const key of unique) {
      await storage.remove(key);
    }
  }
}

/**
 * Collects storage keys owned by a backup in dependency-safe deletion stages.
 * For sharded `.cluster` backups, child archives/bridging slices and child manifests
 * are deleted BEFORE the `.cluster` archive and `.cluster.manifest.json` (which is always last).
 */
async function collectCascadeDeleteStages(
  storage: StorageClient,
  key: string,
  preloadedManifest?: BackupManifest | null,
): Promise<string[][]> {
  const isClusterBackup =
    preloadedManifest?.type === "sharded-cluster-backup" || /\.cluster(\.enc)?$/.test(key);
  if (!isClusterBackup) {
    return [[manifestKey(key), key]];
  }

  const manifest =
    preloadedManifest ??
    (typeof storage.getJson === "function"
      ? await readManifest(storage, key).catch(() => null)
      : null);
  const sharding = manifest?.sharding;
  if (!sharding) {
    return [[key], [manifestKey(key)]];
  }

  const childDataKeys: string[] = [];
  const childManifestKeys: string[] = [];
  const addChild = (childKey?: string) => {
    if (!childKey) return;
    childDataKeys.push(childKey);
    childManifestKeys.push(manifestKey(childKey));
  };

  addChild(sharding.configsvr?.backupKey);
  addChild(sharding.configsvr?.bridgingSliceKey);
  for (const bKey of sharding.configsvr?.bridgingSliceKeys ?? []) {
    addChild(bKey);
  }

  for (const shard of Object.values(sharding.shards ?? {})) {
    addChild(shard.backupKey);
    addChild(shard.bridgingSliceKey);
    for (const bKey of shard.bridgingSliceKeys ?? []) {
      addChild(bKey);
    }
  }

  const rootManifest = manifestKey(key);
  const stages: string[][] = [childDataKeys, childManifestKeys, [key], [rootManifest]]
    .map((stage) => [...new Set(stage)])
    .filter((stage) => stage.length > 0);
  return stages;
}

async function discoverShardedPitrScopes(
  storage: StorageClient,
  basePrefix: string,
): Promise<BackupScope[]> {
  const cfgPrefix = basePrefix ? `${basePrefix}/configsvr` : "configsvr";
  const scopes: BackupScope[] = [{ prefix: cfgPrefix, database: "*" }];
  const shardRoot = basePrefix ? `${basePrefix}/shards/` : "shards/";
  const shardIds = new Set<string>();
  try {
    for await (const obj of storage.list(shardRoot)) {
      const rel = obj.key.slice(shardRoot.length);
      const shardId = rel.split("/")[0];
      if (shardId) shardIds.add(shardId);
    }
  } catch {
    // Ignore listing errors
  }
  for (const shardId of [...shardIds].sort()) {
    const sPrefix = basePrefix ? `${basePrefix}/shards/${shardId}` : `shards/${shardId}`;
    scopes.push({ prefix: sPrefix, database: "*" });
  }
  return scopes;
}

async function isLocalLockActive(lockFile: string, staleAfterMs: number): Promise<boolean> {
  const owner = await readLock(lockFile);
  if (!owner) return false;
  return !(await isStale(lockFile, staleAfterMs).catch(() => false));
}

async function isDistributedLockActive(
  storage: StorageClient,
  keys: string[],
  now: number,
): Promise<boolean> {
  if (typeof storage.getJson !== "function") return false;
  const realNow = Date.now();
  for (const key of keys) {
    try {
      const res = await storage.getJson<LockRecord>(key);
      if (res?.data && (isLockActive(res.data, realNow) || isLockActive(res.data, now))) {
        return true;
      }
    } catch {
      // Fail-safe: on transient read/parse failure, treat lock as active so orphans aren't deleted prematurely
      return true;
    }
  }
  return false;
}

async function isBackupLockActive(
  config: PruneContext["config"],
  storage: StorageClient,
  scope: BackupScope,
  now: number,
): Promise<boolean> {
  if (config.workDir) {
    if (await isLocalLockActive(path.join(config.workDir, ".backup.lock"), ORPHAN_AGE_MS)) {
      return true;
    }
  }
  const distKeys = [`${databasePrefix(scope)}.locks/backup.lock.json`];
  if (config.storage.prefix) {
    distKeys.push(`${config.storage.prefix}/locks/backup.lock`);
  }
  return isDistributedLockActive(storage, distKeys, now);
}

async function isOplogLockActive(
  config: PruneContext["config"],
  storage: StorageClient,
  scope: BackupScope,
  sliceScopes: BackupScope[],
  now: number,
): Promise<boolean> {
  if (config.workDir) {
    if (await isLocalLockActive(path.join(config.workDir, ".oplog.lock"), ORPHAN_AGE_MS)) {
      return true;
    }
  }
  const distKeys = [
    ...new Set([scope, ...sliceScopes].map((s) => `${pitrPrefix(s)}.locks/oplog.lock.json`)),
  ];
  return isDistributedLockActive(storage, distKeys, now);
}

type BackupLineage = "logical" | "physical" | "sharded";

function getBackupLineage(key: string, manifest: BackupManifest | null): BackupLineage {
  if (manifest?.type === "sharded-cluster-backup" || /\.cluster(\.enc)?$/.test(key)) {
    return "sharded";
  }
  if (manifest?.engine === "physical" || /\.physical\.tar\.gz(\.enc)?$/.test(key)) {
    return "physical";
  }
  return "logical";
}

async function getManifestCached(
  storage: StorageClient,
  key: string,
  cache: Map<string, BackupManifest | null>,
): Promise<BackupManifest | null> {
  if (cache.has(key)) {
    return cache.get(key) ?? null;
  }
  if (typeof storage.getJson !== "function") {
    cache.set(key, null);
    return null;
  }
  const m = await readManifest(storage, key).catch(() => null);
  cache.set(key, m);
  return m;
}

function safeParseTs(tsStr: string | undefined | null): OplogTimestamp | null {
  if (!tsStr) return null;
  try {
    return parseOplogTimestamp(tsStr);
  } catch {
    return null;
  }
}

function fallbackTimestampFromDate(dateLike: Date | string): OplogTimestamp {
  const ms = new Date(dateLike).getTime();
  const sec = Number.isNaN(ms) ? 0 : Math.floor(ms / 1000) >>> 0;
  return { t: sec, i: 0 };
}

interface ResolvedBackupScopeInfo {
  key: string;
  startTs: OplogTimestamp;
  endTs: OplogTimestamp;
  oplogEnabled: boolean;
  lineage: BackupLineage;
}

async function resolveBackupForScope(
  storage: StorageClient,
  backup: BackupSummary,
  sScope: BackupScope,
  basePrefix: string,
  cache: Map<string, BackupManifest | null>,
): Promise<ResolvedBackupScopeInfo> {
  const manifest = await getManifestCached(storage, backup.key, cache);
  const lineage = getBackupLineage(backup.key, manifest);
  let targetManifest = manifest;

  if (manifest?.sharding) {
    const cfgPrefix = databasePrefix({
      prefix: basePrefix ? `${basePrefix}/configsvr` : "configsvr",
      database: "*",
    });
    const scopePfx = databasePrefix(sScope);
    if (scopePfx === cfgPrefix && manifest.sharding.configsvr?.backupKey) {
      targetManifest =
        (await getManifestCached(storage, manifest.sharding.configsvr.backupKey, cache)) ??
        manifest;
    } else {
      for (const [shardId, shard] of Object.entries(manifest.sharding.shards ?? {})) {
        const shardScopePfx = databasePrefix({
          prefix: basePrefix ? `${basePrefix}/shards/${shardId}` : `shards/${shardId}`,
          database: "*",
        });
        if (scopePfx === shardScopePfx && shard.backupKey) {
          targetManifest = (await getManifestCached(storage, shard.backupKey, cache)) ?? manifest;
          break;
        }
      }
    }
  }

  const oplogEnabled = targetManifest ? targetManifest.oplog !== false : true;
  const fallbackTs = fallbackTimestampFromDate(
    targetManifest?.completedAt || targetManifest?.createdAt || backup.createdAt,
  );
  const parsedEnd =
    safeParseTs(targetManifest?.oplogWindow?.endTs) ??
    safeParseTs(manifest?.sharding?.clusterTime) ??
    fallbackTs;
  const parsedStart =
    safeParseTs(targetManifest?.oplogWindow?.startTs) ??
    safeParseTs(manifest?.sharding?.clusterTime) ??
    fallbackTimestampFromDate(targetManifest?.createdAt || backup.createdAt);

  return {
    key: backup.key,
    startTs: parsedStart,
    endTs: parsedEnd,
    oplogEnabled,
    lineage,
  };
}

async function findContiguousSegments(
  slices: OplogSlice[],
  storage: StorageClient,
  cache: Map<string, BackupManifest | null>,
): Promise<
  Array<{
    startTs: OplogTimestamp;
    endTs: OplogTimestamp;
    gapBoundaryTs: OplogTimestamp | null;
  }>
> {
  if (slices.length === 0) return [];
  const firstSlice = slices[0]!;
  const firstManifest =
    firstSlice.manifest ?? (await getManifestCached(storage, firstSlice.key, cache));
  const firstGapBoundary =
    firstManifest?.oplogWindow?.gapDetected === true && firstManifest.oplogWindow.previousEndTs
      ? safeParseTs(firstManifest.oplogWindow.previousEndTs)
      : null;
  const segments: Array<{
    startTs: OplogTimestamp;
    endTs: OplogTimestamp;
    gapBoundaryTs: OplogTimestamp | null;
  }> = [
    {
      startTs: firstSlice.startTs,
      endTs: firstSlice.endTs,
      gapBoundaryTs: firstGapBoundary,
    },
  ];

  for (let i = 1; i < slices.length; i++) {
    const prev = slices[i - 1]!;
    const curr = slices[i]!;
    const currManifest = curr.manifest ?? (await getManifestCached(storage, curr.key, cache));
    let isGap = false;
    if (currManifest?.oplogWindow?.gapDetected) {
      isGap = true;
    } else if (currManifest?.oplogWindow?.previousEndTs) {
      const prevEnd = safeParseTs(currManifest.oplogWindow.previousEndTs);
      if (prevEnd && compareTimestamps(prevEnd, prev.endTs) !== 0) {
        isGap = true;
      }
    }

    if (isGap) {
      segments.push({ startTs: curr.startTs, endTs: curr.endTs, gapBoundaryTs: prev.endTs });
    } else {
      segments[segments.length - 1]!.endTs = curr.endTs;
    }
  }

  return segments;
}

function selectAnchorKeyForSegment(
  eligibleDesc: ResolvedBackupScopeInfo[],
  segStart: OplogTimestamp,
  segEnd: OplogTimestamp,
): string | undefined {
  if (eligibleDesc.length === 0) return undefined;
  // 1. Newest backup whose endTs <= segStart
  for (const ab of eligibleDesc) {
    if (compareTimestamps(ab.endTs, segStart) <= 0) {
      return ab.key;
    }
  }
  // 2. Else newest backup whose startTs <= segStart (base snapshot window spans segStart)
  for (const ab of eligibleDesc) {
    if (
      compareTimestamps(ab.startTs, segStart) <= 0 &&
      compareTimestamps(ab.startTs, segEnd) <= 0
    ) {
      return ab.key;
    }
  }
  // 3. Else oldest backup whose startTs <= segEnd (never newest!)
  for (let i = eligibleDesc.length - 1; i >= 0; i--) {
    const ab = eligibleDesc[i]!;
    if (compareTimestamps(ab.startTs, segEnd) <= 0) {
      return ab.key;
    }
  }
  return undefined;
}

export function planRetention(
  backups: BackupSummary[],
  policy: RetentionPolicy,
  timeZone?: string,
): RetentionPlan {
  const sorted = [...backups].sort(
    (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
  );
  const keep = new Set<string>(
    sorted.slice(0, Math.max(policy.keepLast ?? 0, 1)).map((b) => b.key),
  );

  for (const [rule, period] of Object.entries(PERIODS)) {
    const limit = (policy as Record<string, number | undefined>)[rule] ?? 0;
    const seen = new Set<string>();
    for (const backup of sorted) {
      if (seen.size >= limit) break;
      const bucket = period(zonedDate(backup.createdAt, timeZone));
      if (!seen.has(bucket)) {
        seen.add(bucket);
        keep.add(backup.key);
      }
    }
  }

  return {
    keep: sorted.filter((backup) => keep.has(backup.key)),
    remove: sorted.filter((backup) => !keep.has(backup.key)),
  };
}

export async function prune(
  { config, storage, logger }: PruneContext,
  { dryRun = false, now = Date.now() }: PruneOptions = {},
): Promise<PruneResult> {
  if (!config.retention) {
    throw new Error("No retention policy configured. Set one of the RETENTION_KEEP_* variables.");
  }

  const scope = scopeOf(config);
  const backups = await listBackups(storage, scope);
  const completeBackups = backups
    .filter((backup) => backup.complete)
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
  const plan = planRetention(completeBackups, config.retention, config.schedule.timezone);
  const keepSet = new Set<string>(plan.keep.map((b) => b.key));

  const backupLockActive = await isBackupLockActive(config, storage, scope, now);
  const orphans = backupLockActive
    ? []
    : backups.filter((b) => {
        if (b.complete) return false;
        const createdMs = new Date(b.createdAt).getTime();
        const refMs =
          !Number.isNaN(createdMs) && createdMs > 0
            ? createdMs
            : new Date(b.lastModified ?? b.createdAt).getTime();
        return now - refMs > ORPHAN_AGE_MS;
      });

  const sliceScopes = config.sharding?.enabled
    ? await discoverShardedPitrScopes(storage, config.storage.prefix)
    : [scope];
  const oplogLockActive = await isOplogLockActive(config, storage, scope, sliceScopes, now);

  const manifestCache = new Map<string, BackupManifest | null>();
  const slicesByScope = new Map<string, OplogSlice[]>();
  for (const sScope of sliceScopes) {
    slicesByScope.set(databasePrefix(sScope), await listOplogSlices(storage, sScope));
  }

  const computeCutoffsForKept = async (
    keptBackups: BackupSummary[],
  ): Promise<{ defaultCutoff: number | null; scopeCutoffs: Map<string, number> }> => {
    const scopeCutoffs = new Map<string, number>();
    const oldestKept = keptBackups.length > 0 ? keptBackups[keptBackups.length - 1]! : null;
    if (!oldestKept) {
      return { defaultCutoff: null, scopeCutoffs };
    }

    const createdEpoch = Math.floor(new Date(oldestKept.createdAt).getTime() / 1000);
    let defaultCutoff = Number.isNaN(createdEpoch) ? 0 : createdEpoch;

    const oldestManifest = await getManifestCached(storage, oldestKept.key, manifestCache);
    if (oldestManifest?.sharding) {
      const clusterTs = safeParseTs(oldestManifest.sharding.clusterTime);
      const componentEpochs: number[] = [];

      if (oldestManifest.sharding.configsvr?.backupKey) {
        const cfgScopePfx = databasePrefix({
          prefix: config.storage.prefix ? `${config.storage.prefix}/configsvr` : "configsvr",
          database: "*",
        });
        const cfgM = await getManifestCached(
          storage,
          oldestManifest.sharding.configsvr.backupKey,
          manifestCache,
        );
        const cfgStart =
          safeParseTs(cfgM?.oplogWindow?.startTs) ??
          safeParseTs(cfgM?.oplogWindow?.endTs) ??
          clusterTs;
        if (cfgStart) {
          scopeCutoffs.set(cfgScopePfx, cfgStart.t);
          componentEpochs.push(cfgStart.t);
        }
      }

      for (const [shardId, shard] of Object.entries(oldestManifest.sharding.shards ?? {})) {
        const shardScopePfx = databasePrefix({
          prefix: config.storage.prefix
            ? `${config.storage.prefix}/shards/${shardId}`
            : `shards/${shardId}`,
          database: "*",
        });
        const shardM = shard.backupKey
          ? await getManifestCached(storage, shard.backupKey, manifestCache)
          : null;
        const shardStart =
          safeParseTs(shardM?.oplogWindow?.startTs) ??
          safeParseTs(shardM?.oplogWindow?.endTs) ??
          clusterTs;
        if (shardStart) {
          scopeCutoffs.set(shardScopePfx, shardStart.t);
          componentEpochs.push(shardStart.t);
        }
      }

      if (componentEpochs.length > 0) {
        defaultCutoff = Math.min(...componentEpochs, defaultCutoff);
      } else if (clusterTs && clusterTs.t < defaultCutoff) {
        defaultCutoff = clusterTs.t;
      }
    } else {
      const startTs = safeParseTs(oldestManifest?.oplogWindow?.startTs);
      if (startTs && startTs.t < defaultCutoff) {
        defaultCutoff = startTs.t;
      }
    }

    return { defaultCutoff, scopeCutoffs };
  };

  // Promote any PITR anchor backups from `remove` -> `keep` if retained slices depend on them
  for (let pass = 0; pass <= completeBackups.length; pass++) {
    const keptBackups = completeBackups.filter((b) => keepSet.has(b.key));
    const { defaultCutoff, scopeCutoffs } = await computeCutoffsForKept(keptBackups);
    let promotedAny = false;

    for (const sScope of sliceScopes) {
      const sScopePfx = databasePrefix(sScope);
      const cutoff = scopeCutoffs.get(sScopePfx) ?? defaultCutoff;
      const allSlices = slicesByScope.get(sScopePfx) ?? [];
      const retainedCompleteSlices = allSlices.filter(
        (slice) => slice.complete && (cutoff === null || slice.endTs.t >= cutoff),
      );
      if (retainedCompleteSlices.length === 0) continue;

      const resolvedAll = (
        await Promise.all(
          completeBackups.map((b) =>
            resolveBackupForScope(storage, b, sScope, config.storage.prefix, manifestCache),
          ),
        )
      ).filter((info) => info.oplogEnabled);
      if (resolvedAll.length === 0) continue;

      const resolvedKept = resolvedAll.filter((info) => keepSet.has(info.key));
      const segments = await findContiguousSegments(retainedCompleteSlices, storage, manifestCache);

      for (let segIdx = 0; segIdx < segments.length; segIdx++) {
        const seg = segments[segIdx]!;
        const prevSeg = segIdx > 0 ? segments[segIdx - 1]! : null;
        const lowerBoundTs = prevSeg ? prevSeg.endTs : seg.gapBoundaryTs;
        const canAnchorSegment = (b: ResolvedBackupScopeInfo): boolean =>
          compareTimestamps(b.startTs, seg.endTs) <= 0 &&
          (!lowerBoundTs || compareTimestamps(b.endTs, lowerBoundTs) > 0);

        const hasKeptAnchor = resolvedKept.some(canAnchorSegment);
        if (!hasKeptAnchor) {
          const resolvedCandidates = resolvedAll.filter(canAnchorSegment);
          const anchorKey = selectAnchorKeyForSegment(resolvedCandidates, seg.startTs, seg.endTs);
          if (anchorKey && !keepSet.has(anchorKey)) {
            keepSet.add(anchorKey);
            promotedAny = true;
          }
        }
      }
    }

    if (!promotedAny) break;
  }

  const keep = completeBackups.filter((b) => keepSet.has(b.key));
  const remove = completeBackups.filter((b) => !keepSet.has(b.key));
  const { defaultCutoff, scopeCutoffs } = await computeCutoffsForKept(keep);

  const expiredSlices: RetentionTarget[] = [];
  for (const sScope of sliceScopes) {
    const sScopePfx = databasePrefix(sScope);
    const cutoff = scopeCutoffs.get(sScopePfx) ?? defaultCutoff;
    const slices = slicesByScope.get(sScopePfx) ?? [];
    for (const slice of slices) {
      if (!slice.complete) {
        if (!oplogLockActive && slice.lastModified !== undefined) {
          const modMs = new Date(slice.lastModified).getTime();
          if (!Number.isNaN(modMs) && now - modMs > ORPHAN_AGE_MS) {
            expiredSlices.push(slice);
          }
        }
      } else if (cutoff !== null && slice.endTs.t < cutoff) {
        expiredSlices.push(slice);
      }
    }
  }

  const targets: RetentionTarget[] = [...remove, ...orphans, ...expiredSlices];

  if (!dryRun && targets.length > 0) {
    const unshardedKeys: string[] = [];
    const shardedTargets: RetentionTarget[] = [];
    for (const target of targets) {
      const stages = await collectCascadeDeleteStages(storage, target.key);
      if (stages.length === 1) {
        unshardedKeys.push(...stages[0]!);
      } else {
        shardedTargets.push(target);
      }
      logger.debug({ key: target.key, stages: stages.length }, "Marked backup for deletion");
    }

    if (unshardedKeys.length > 0) {
      try {
        await deleteKeysFromStorage(storage, unshardedKeys);
      } catch (err) {
        if (isObjectLockError(err, config.storage.objectLock?.enabled)) {
          logger.warn(
            { err },
            "Some retention targets could not be deleted because they are protected under S3 WORM Object Lock",
          );
        } else {
          throw err;
        }
      }
    }

    for (const shardedTarget of shardedTargets) {
      const stages = await collectCascadeDeleteStages(storage, shardedTarget.key);
      try {
        for (const stage of stages) {
          await deleteKeysFromStorage(storage, stage);
        }
      } catch (err) {
        if (isObjectLockError(err, config.storage.objectLock?.enabled)) {
          logger.warn(
            { err, key: shardedTarget.key },
            "Some sharded retention targets could not be deleted because they are protected under S3 WORM Object Lock",
          );
        } else {
          throw err;
        }
      }
    }
  }

  logger.info({ kept: keep.length, removed: targets.length, dryRun }, "Retention policy applied");
  return { keep, remove: targets };
}

export async function deleteBackup(
  { config, storage, logger }: PruneContext,
  { key, force = false, dryRun = false }: DeleteBackupOptions,
): Promise<DeleteBackupResult> {
  const scope = scopeOf(config);
  const resolvedKey = await resolveBackup(storage, scope, key);
  const manifest = await readManifest(storage, resolvedKey);
  if (!manifest) {
    const head = await storage.head(resolvedKey);
    if (!head) {
      throw new Error(`Backup not found: ${key}`);
    }
  }

  const isSharded = Boolean(
    config.sharding?.enabled ||
    manifest?.type === "sharded-cluster-backup" ||
    /\.cluster(\.enc)?$/.test(resolvedKey),
  );

  // Prevent deleting any base backup that is the sole anchor of any contiguous PITR slice segment
  if (!force && manifest?.oplog !== false) {
    const rawScopes = isSharded
      ? [scope, ...(await discoverShardedPitrScopes(storage, config.storage.prefix))]
      : [scope];
    const seenScopePrefixes = new Set<string>();
    const checkScopes: BackupScope[] = [];
    for (const s of rawScopes) {
      const pfx = databasePrefix(s);
      if (!seenScopePrefixes.has(pfx)) {
        seenScopePrefixes.add(pfx);
        checkScopes.push(s);
      }
    }

    const targetLineage = getBackupLineage(resolvedKey, manifest);
    const manifestCache = new Map<string, BackupManifest | null>([[resolvedKey, manifest]]);
    const completeBackups = (await listBackups(storage, scope)).filter((b) => b.complete);

    for (const sScope of checkScopes) {
      const slices = (await listOplogSlices(storage, sScope)).filter((s) => s.complete);
      if (slices.length === 0) continue;

      const resolvedBackups = (
        await Promise.all(
          completeBackups.map((b) =>
            resolveBackupForScope(storage, b, sScope, config.storage.prefix, manifestCache),
          ),
        )
      ).filter((info) => info.oplogEnabled && info.lineage === targetLineage);

      if (resolvedBackups.length === 0) continue;
      if (resolvedBackups.length === 1 && resolvedBackups[0]?.key === resolvedKey) {
        throw new Error(
          `Cannot delete backup "${resolvedKey}": it is an active base snapshot anchoring PITR continuous oplog slices. Deleting it would invalidate point-in-time recovery. Pass --force to override.`,
        );
      }

      const segments = await findContiguousSegments(slices, storage, manifestCache);
      for (const seg of segments) {
        const anchorKey = selectAnchorKeyForSegment(resolvedBackups, seg.startTs, seg.endTs);
        if (anchorKey === resolvedKey) {
          throw new Error(
            `Cannot delete backup "${resolvedKey}": it is an active base snapshot anchoring PITR continuous oplog slices. Deleting it would invalidate point-in-time recovery. Pass --force to override.`,
          );
        }
      }
    }
  }

  if (!dryRun) {
    const stages = await collectCascadeDeleteStages(storage, resolvedKey, manifest);
    try {
      for (const stage of stages) {
        await deleteKeysFromStorage(storage, stage);
      }
    } catch (err) {
      if (isObjectLockError(err, config.storage.objectLock?.enabled)) {
        throw new Error(
          `Cannot delete backup "${resolvedKey}": It is protected by active S3 WORM Compliance Object Lock and cannot be deleted until retention expires.`,
          { cause: err },
        );
      }
      throw err;
    }
  }

  logger.info({ key: resolvedKey, dryRun, force }, "Backup deleted");
  return {
    key: resolvedKey,
    deleted: !dryRun,
    message: dryRun ? `[Dry Run] Would delete ${resolvedKey}` : `Deleted ${resolvedKey}`,
  };
}

export async function pruneOplogSlices(
  { config, storage, logger }: PruneContext,
  { olderThan, dryRun = false }: PruneOplogOptions,
): Promise<PruneOplogResult> {
  const cutoff = parsePitrTarget(olderThan);
  const scopes = config.sharding?.enabled
    ? await discoverShardedPitrScopes(storage, config.storage.prefix)
    : [scopeOf(config)];

  const deletedKeys: string[] = [];
  const keysToDelete: string[] = [];

  for (const scope of scopes) {
    const slices = await listOplogSlices(storage, scope);
    const toDelete = slices.filter((s) => s.endTs.t < cutoff.t);
    for (const slice of toDelete) {
      deletedKeys.push(slice.key);
      keysToDelete.push(manifestKey(slice.key), slice.key);
    }
  }

  if (!dryRun && keysToDelete.length > 0) {
    try {
      await deleteKeysFromStorage(storage, keysToDelete);
    } catch (err) {
      if (isObjectLockError(err, config.storage.objectLock?.enabled)) {
        logger.warn(
          { err },
          "Some PITR oplog slices could not be deleted because they are protected under S3 WORM Object Lock",
        );
      } else {
        throw err;
      }
    }
  }

  logger.info(
    { cutoff: formatOplogTimestamp(cutoff), count: deletedKeys.length, dryRun },
    "Pruned expired PITR oplog slices",
  );

  return {
    cutoffTs: formatOplogTimestamp(cutoff),
    deletedSlices: deletedKeys,
  };
}

function zonedDate(date: Date, timeZone?: string): DateParts {
  const safeDate = Number.isNaN(date.getTime()) ? new Date(0) : date;
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(safeDate);
  const value = (type: string) => parts.find((part) => part.type === type)!.value;
  return { year: value("year"), month: value("month"), day: value("day") };
}

function isoWeek({ year, month, day }: DateParts): string {
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  const weekday = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - weekday);
  const weekYear = date.getUTCFullYear();
  const week = Math.ceil(((date.getTime() - Date.UTC(weekYear, 0, 1)) / 86_400_000 + 1) / 7);
  return `${weekYear}-W${String(week).padStart(2, "0")}`;
}
