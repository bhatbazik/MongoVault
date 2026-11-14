import { listBackups, manifestKey, readManifest, resolveBackup, scopeOf } from "./catalog.js";
import {
  formatOplogTimestamp,
  listOplogSlices,
  parseOplogTimestamp,
  parsePitrTarget,
} from "./oplog.js";
import type { BackupSummary } from "./types/catalog.js";
import type { DateParts } from "./types/common.js";
import type { RetentionPolicy } from "./types/config.js";
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
  if (keys.length === 0) return;
  if (typeof storage.removeMany === "function") {
    await storage.removeMany(keys);
  } else {
    for (const key of keys) {
      await storage.remove(key);
    }
  }
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
    const startTsStr = oldestManifest?.oplogWindow?.startTs;
    if (startTsStr) {
      try {
        const parsedStart = parseOplogTimestamp(startTsStr);
        if (parsedStart && (oldestKeptEpochSec === null || parsedStart.t < oldestKeptEpochSec)) {
          oldestKeptEpochSec = parsedStart.t;
        }
      } catch {}
    }
  }
  const slices = await listOplogSlices(storage, scope);
  const expiredSlices = slices.filter((slice) => {
    if (!slice.complete) {
      const sliceAgeMs =
        now - (slice.lastModified ? new Date(slice.lastModified).getTime() : slice.endTs.t * 1000);
      return sliceAgeMs > ORPHAN_AGE_MS;
    }
    return oldestKeptEpochSec !== null && slice.endTs.t < oldestKeptEpochSec;
  });

  const targets: RetentionTarget[] = [...remove, ...orphans, ...expiredSlices];

  if (!dryRun && targets.length > 0) {
    const keysToDelete: string[] = [];
    for (const backup of targets) {
      keysToDelete.push(manifestKey(backup.key), backup.key);
      logger.debug({ key: backup.key }, "Marked backup for deletion");
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

  // Prevent deleting the active base backup that anchors PITR slices unless --force is passed
  if (!force) {
    const slices = (await listOplogSlices(storage, scope)).filter((s) => s.complete);
    if (slices.length > 0) {
      const backups = (await listBackups(storage, scope)).filter((b) => b.complete);
      if (backups.length > 0 && backups[0]?.key === resolvedKey) {
        throw new Error(
          `Cannot delete backup "${resolvedKey}": it is the active base snapshot anchoring PITR continuous oplog slices. Deleting it would invalidate point-in-time recovery. Pass --force to override.`,
        );
      }
    }
  }

  if (!dryRun) {
    const keysToDelete = [manifestKey(resolvedKey), resolvedKey];
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
  const scope = scopeOf(config);
  const cutoff = parsePitrTarget(olderThan);
  const slices = await listOplogSlices(storage, scope);
  const toDelete = slices.filter((s) => s.endTs.t < cutoff.t);

  const keysToDelete: string[] = [];
  for (const slice of toDelete) {
    keysToDelete.push(manifestKey(slice.key), slice.key);
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
    { cutoff: formatOplogTimestamp(cutoff), count: toDelete.length, dryRun },
    "Pruned expired PITR oplog slices",
  );

  return {
    cutoffTs: formatOplogTimestamp(cutoff),
    deletedSlices: toDelete.map((s) => s.key),
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
