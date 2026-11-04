import { listBackups, manifestKey, scopeOf } from "./catalog.js";
import { listOplogSlices } from "./oplog.js";
import type { BackupSummary } from "./types/catalog.js";
import type { DateParts } from "./types/common.js";
import type { RetentionPolicy } from "./types/config.js";
import type {
  PruneContext,
  PruneOptions,
  PruneResult,
  RetentionPlan,
  RetentionTarget,
} from "./types/retention.js";

const ORPHAN_AGE_MS = 60 * 60 * 1000;

const PERIODS: Record<string, (parts: DateParts) => string> = {
  keepDaily: ({ year, month, day }) => `${year}-${month}-${day}`,
  keepWeekly: isoWeek,
  keepMonthly: ({ year, month }) => `${year}-${month}`,
};

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

  // Prune PITR oplog slices older than the oldest retained base backup
  const oldestKeptBackup = keep.length > 0 ? keep[keep.length - 1] : null;
  const oldestKeptEpochSec = oldestKeptBackup
    ? Math.floor(new Date(oldestKeptBackup.createdAt).getTime() / 1000)
    : null;
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

    if (typeof storage.removeMany === "function") {
      await storage.removeMany(keysToDelete);
    } else {
      for (const key of keysToDelete) {
        await storage.remove(key);
      }
    }
  }

  logger.info({ kept: keep.length, removed: targets.length, dryRun }, "Retention policy applied");
  return { keep, remove: targets };
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
