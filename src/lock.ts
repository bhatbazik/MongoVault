import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type {
  AcquireDistributedLockOptions,
  AcquireHybridLockOptions,
  AcquireLockOptions,
  LockPayload,
  ReleaseLockFn,
} from "./types/lock.js";
import type { StorageClient } from "./types/storage.js";

const UNREADABLE_GRACE_MS = 5_000;

export async function acquireLock(
  file: string,
  {
    staleAfterMs,
    heartbeatIntervalMs = Math.min(10_000, Math.max(1_000, Math.floor(staleAfterMs / 3))),
    logger,
    onLost,
  }: AcquireLockOptions,
): Promise<ReleaseLockFn | null> {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const token = randomUUID();

  const createPayload = (): LockPayload => ({
    pid: process.pid,
    token,
    acquiredAt: new Date().toISOString(),
    lastHeartbeatAt: new Date().toISOString(),
  });

  const setupHeartbeat = (): NodeJS.Timeout => {
    let lost = false;
    const timer = setInterval(async () => {
      if (lost) return;
      try {
        const owner = await readLock(file);
        if (!owner || owner.token !== token) {
          lost = true;
          clearInterval(timer);
          const err = new Error(
            `Local lock lost on "${file}": lock file was modified or removed by another process`,
          );
          logger?.error({ file }, err.message);
          onLost?.(err);
          return;
        }
        const now = new Date();
        await fs.utimes(file, now, now);
      } catch (err: unknown) {
        logger?.warn({ err, file }, "Failed to update local lock heartbeat timestamp");
      }
    }, heartbeatIntervalMs);
    timer.unref();
    return timer;
  };

  try {
    await fs.writeFile(file, JSON.stringify(createPayload()), { flag: "wx" });
    const timer = setupHeartbeat();
    return async () => {
      clearInterval(timer);
      await release(file, token);
    };
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }

  if (!(await isStale(file, staleAfterMs))) {
    return null;
  }

  // Atomic stale lock takeover using atomic rename to prevent TOCTOU race
  const tombstone = `${file}.stale.${randomUUID()}`;
  try {
    await fs.rename(file, tombstone);
    await fs.rm(tombstone, { force: true });
  } catch {
    // Another concurrent process already claimed or removed the stale lock
    return null;
  }

  // Now safely re-attempt acquisition
  try {
    await fs.writeFile(file, JSON.stringify(createPayload()), { flag: "wx" });
    const timer = setupHeartbeat();
    return async () => {
      clearInterval(timer);
      await release(file, token);
    };
  } catch {
    return null;
  }
}

export async function readLock(file: string): Promise<LockPayload | null> {
  try {
    return JSON.parse(await fs.readFile(file, "utf8")) as LockPayload;
  } catch {
    return null;
  }
}

async function release(file: string, token: string): Promise<void> {
  const owner = await readLock(file);
  if (owner?.token === token) {
    await fs.rm(file, { force: true });
  }
}

export async function isStale(file: string, staleAfterMs: number): Promise<boolean> {
  let stat;
  try {
    stat = await fs.stat(file);
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return true;
    throw error;
  }

  const age = Date.now() - stat.mtimeMs;
  const owner = await readLock(file);
  if (!owner) {
    return age > UNREADABLE_GRACE_MS;
  }

  // First priority: check owner process liveness.
  // If the owner process is not alive on this host, the lock is stale immediately.
  if (!isAlive(owner.pid)) {
    return true;
  }

  // If the owner process is alive, the lock is only stale if its heartbeat lease has expired.
  return age > staleAfterMs;
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: unknown) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

export async function acquireDistributedLock(
  storage?: StorageClient,
  lockKey?: string,
  {
    holder = `${os.hostname()}:${process.pid}`,
    ttlMs = 90_000,
    heartbeatIntervalMs = 30_000,
    logger,
    onLost,
  }: AcquireDistributedLockOptions = {},
): Promise<ReleaseLockFn | null> {
  if (!storage || !lockKey || typeof storage.acquireLock !== "function") {
    return null;
  }

  const token = await storage.acquireLock(lockKey, { holder, ttlMs });
  if (!token) return null;

  let lastSuccessfulHeartbeat = Date.now();
  let lost = false;

  const timer = setInterval(async () => {
    if (lost) return;
    try {
      const refreshed = await storage.refreshLock!(lockKey, token, ttlMs);
      if (!refreshed) {
        lost = true;
        clearInterval(timer);
        const err = new Error(
          `Distributed lock lease lost on "${lockKey}": lease renewal was rejected (lease held by another owner or expired)`,
        );
        logger?.error({ lockKey }, err.message);
        onLost?.(err);
        return;
      }
      lastSuccessfulHeartbeat = Date.now();
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      logger?.warn(
        { err: error.message, lockKey },
        "Failed to refresh distributed S3 lock heartbeat",
      );
      if (Date.now() - lastSuccessfulHeartbeat > ttlMs) {
        lost = true;
        clearInterval(timer);
        const expiredErr = new Error(
          `Distributed lock lease expired on "${lockKey}" after ${ttlMs}ms without successful renewal`,
        );
        logger?.error({ lockKey }, expiredErr.message);
        onLost?.(expiredErr);
      }
    }
  }, heartbeatIntervalMs);
  timer.unref();

  return async function release() {
    clearInterval(timer);
    try {
      await storage.releaseLock!(lockKey, token);
    } catch (err: unknown) {
      logger?.warn(
        { err: err instanceof Error ? err.message : String(err) },
        "Failed to release distributed S3 lock",
      );
    }
  };
}

export async function acquireHybridLock({
  file,
  storage,
  lockKey,
  staleAfterMs = 180_000,
  distributedTtlMs = 90_000,
  heartbeatIntervalMs = 30_000,
  holder,
  logger,
  onLost,
}: AcquireHybridLockOptions): Promise<ReleaseLockFn | null> {
  const localRelease = await acquireLock(file, {
    staleAfterMs,
    heartbeatIntervalMs: Math.min(
      heartbeatIntervalMs,
      Math.max(1_000, Math.floor(staleAfterMs / 3)),
    ),
    logger,
    onLost,
  });
  if (!localRelease) {
    return null;
  }

  if (storage && lockKey && typeof storage.acquireLock === "function") {
    try {
      const distRelease = await acquireDistributedLock(storage, lockKey, {
        holder,
        ttlMs: distributedTtlMs,
        heartbeatIntervalMs,
        logger,
        onLost,
      });

      if (!distRelease) {
        await localRelease();
        return null;
      }

      return async function releaseAll() {
        await distRelease().catch(() => {});
        await localRelease().catch(() => {});
      };
    } catch (err) {
      await localRelease().catch(() => {});
      throw err;
    }
  }

  return localRelease;
}
