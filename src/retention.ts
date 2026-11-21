import { listBackups, manifestKey, readManifest, resolveBackup, scopeOf } from "./catalog.js";
import {
  compareTimestamps,
  formatOplogTimestamp,
  listOplogSlices,
  parseOplogTimestamp,
  parsePitrTarget,
} from "./oplog.js";
import type { BackupManifest, BackupScope, BackupSummary } from "./types/catalog.js";
import type { DateParts } from "./types/common.js";
import type { RetentionPolicy } from "./types/config.js";
import type { OplogTimestamp } from "./types/oplog.js";
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

function isObjectLockError(error: unknown): boolean {
  const err = error as { name?: string; message?: string; Code?: string };
  const str = `${err?.name || ""} ${err?.message || ""} ${err?.Code || ""}`.toLowerCase();
  return (
    str.includes("object lock") ||
    str.includes("worm") ||
    str.includes("legal hold") ||
    str.includes("retention period") ||
    str.includes("immutable")
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
 * Collects all storage keys owned by a backup (manifest + archive, plus child configsvr,
 * shard archives, manifests, and bridging slices when deleting a sharded `.cluster` backup).
 */
async function collectCascadeDeleteKeys(
  storage: StorageClient,
  key: string,
  preloadedManifest?: BackupManifest | null,
): Promise<string[]> {
  const keys = new Set<string>([manifestKey(key), key]);

  const isClusterBackup =
    preloadedManifest?.type === "sharded-cluster-backup" || /\.cluster(\.enc)?$/.test(key);
  if (!isClusterBackup) {
    return [...keys];
  }

  const manifest =
    preloadedManifest ??
    (typeof storage.getJson === "function"
      ? await readManifest(storage, key).catch(() => null)
      : null);
  const sharding = manifest?.sharding;
  if (!sharding) {
    return [...keys];
  }

  const addChild = (childKey?: string) => {
    if (!childKey) return;
    keys.add(manifestKey(childKey));
    keys.add(childKey);
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

  return [...keys];
}

async function discoverShardedPitrScopes(
  storage: StorageClient,
  basePrefix: string,
): Promise<BackupScope[]> {
  const scopes: BackupScope[] = [{ prefix: `${basePrefix}/configsvr`, database: "*" }];
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
    scopes.push({ prefix: `${basePrefix}/shards/${shardId}`, database: "*" });
  }
  return scopes;
}

export function planRetention(
  backups: BackupSummary[],
  policy: RetentionPolicy,
  timeZone?: string,
): RetentionPlan {
  const sorted = [...backups].sort(
    (a, b) => (b.createdAt as unknown as number) - (a.createdAt as unknown as number),
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
  const { keep, remove } = planRetention(
    backups.filter((backup) => backup.complete),
    config.retention,
    config.schedule.timezone,
  );
  const orphans = backups.filter(
    (b) => !b.complete && now - (b.createdAt as unknown as number) > ORPHAN_AGE_MS,
  );

  // Prune PITR oplog slices older than the oldest retained base backup's oplog start boundary
  const oldestKeptBackup = keep.length > 0 ? keep[keep.length - 1] : null;
  let oldestKeptEpochSec = oldestKeptBackup
    ? Math.floor(new Date(oldestKeptBackup.createdAt).getTime() / 1000)
    : null;
  if (oldestKeptBackup && typeof storage.getJson === "function") {
    const oldestManifest = await readManifest(storage, oldestKeptBackup.key).catch(() => null);
    const startTsStr =
      oldestManifest?.oplogWindow?.startTs ?? oldestManifest?.sharding?.clusterTime;
    if (startTsStr) {
      try {
        const parsedStart = parseOplogTimestamp(startTsStr);
        if (parsedStart && (oldestKeptEpochSec === null || parsedStart.t < oldestKeptEpochSec)) {
          oldestKeptEpochSec = parsedStart.t;
        }
      } catch {}
    }
  }

  const sliceScopes = config.sharding?.enabled
    ? await discoverShardedPitrScopes(storage, config.storage.prefix)
    : [scope];
  const expiredSlices: RetentionTarget[] = [];
  for (const sScope of sliceScopes) {
    const slices = await listOplogSlices(storage, sScope);
    for (const slice of slices) {
      if (!slice.complete) {
        const sliceAgeMs =
          now -
          (slice.lastModified ? new Date(slice.lastModified).getTime() : slice.endTs.t * 1000);
        if (sliceAgeMs > ORPHAN_AGE_MS) {
          expiredSlices.push(slice);
        }
      } else if (oldestKeptEpochSec !== null && slice.endTs.t < oldestKeptEpochSec) {
        expiredSlices.push(slice);
      }
    }
  }

  const targets: RetentionTarget[] = [...remove, ...orphans, ...expiredSlices];

  if (!dryRun && targets.length > 0) {
    const keysToDelete: string[] = [];
    for (const target of targets) {
      const cascadeKeys = await collectCascadeDeleteKeys(storage, target.key);
      keysToDelete.push(...cascadeKeys);
      logger.debug(
        { key: target.key, cascadeCount: cascadeKeys.length },
        "Marked backup for deletion",
      );
    }

    try {
      await deleteKeysFromStorage(storage, keysToDelete);
    } catch (err) {
      if (isObjectLockError(err)) {
        logger.warn(
          { err },
          "Some retention targets could not be deleted because they are protected under S3 WORM Object Lock",
        );
      } else {
        throw err;
      }
    }
  }

  logger.info({ kept: keep.length, removed: targets.length, dryRun }, "Retention policy applied");
  return { keep, remove: targets };
}

async function resolveBackupAnchorTs(
  storage: StorageClient,
  backup: BackupSummary,
): Promise<OplogTimestamp> {
  if (typeof storage.getJson === "function") {
    const m = await readManifest(storage, backup.key).catch(() => null);
    const tsStr = m?.oplogWindow?.endTs ?? m?.sharding?.clusterTime;
    if (tsStr) {
      const parsed = parseOplogTimestamp(tsStr);
      if (parsed) return parsed;
    }
  }
  return { t: Math.floor(new Date(backup.createdAt).getTime() / 1000) >>> 0, i: 0 };
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

  // Prevent deleting any base backup that is the sole anchor of any contiguous PITR slice segment
  if (!force) {
    const slices = (await listOplogSlices(storage, scope)).filter((s) => s.complete);
    if (slices.length > 0) {
      const backups = (await listBackups(storage, scope)).filter((b) => b.complete);
      if (backups.length === 1 && backups[0]?.key === resolvedKey) {
        throw new Error(
          `Cannot delete backup "${resolvedKey}": it is an active base snapshot anchoring PITR continuous oplog slices. Deleting it would invalidate point-in-time recovery. Pass --force to override.`,
        );
      }

      // Resolve exact anchor timestamp (manifest.oplogWindow.endTs) for each backup (newest first)
      const anchoredBackups = await Promise.all(
        backups.map(async (b) => ({
          key: b.key,
          anchorTs: await resolveBackupAnchorTs(storage, b),
        })),
      );

      // Identify the start of every contiguous PITR slice segment
      const segmentStarts: OplogTimestamp[] = [slices[0]!.startTs];
      for (let i = 1; i < slices.length; i++) {
        const prev = slices[i - 1]!;
        const curr = slices[i]!;
        const currManifest =
          curr.manifest ??
          (typeof storage.getJson === "function"
            ? await readManifest(storage, curr.key).catch(() => null)
            : null);
        if (currManifest?.oplogWindow?.gapDetected) {
          segmentStarts.push(curr.startTs);
        } else if (currManifest?.oplogWindow?.previousEndTs) {
          const prevEnd = parseOplogTimestamp(currManifest.oplogWindow.previousEndTs);
          if (prevEnd && compareTimestamps(prevEnd, prev.endTs) !== 0) {
            segmentStarts.push(curr.startTs);
          }
        }
      }

      for (const segStart of segmentStarts) {
        // Find the newest backup whose anchorTs <= segStart (or fallback to newest backup)
        let anchorKey: string | undefined = anchoredBackups[0]?.key;
        for (const ab of anchoredBackups) {
          if (compareTimestamps(ab.anchorTs, segStart) <= 0) {
            anchorKey = ab.key;
            break;
          }
        }
        if (anchorKey === resolvedKey) {
          throw new Error(
            `Cannot delete backup "${resolvedKey}": it is an active base snapshot anchoring PITR continuous oplog slices. Deleting it would invalidate point-in-time recovery. Pass --force to override.`,
          );
        }
      }
    }
  }

  if (!dryRun) {
    const keysToDelete = await collectCascadeDeleteKeys(storage, resolvedKey, manifest);
    try {
      await deleteKeysFromStorage(storage, keysToDelete);
    } catch (err) {
      if (isObjectLockError(err)) {
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
      if (isObjectLockError(err)) {
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
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
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
