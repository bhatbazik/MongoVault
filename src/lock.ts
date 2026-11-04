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

export async function acquireLock(file: string, { staleAfterMs }: AcquireLockOptions): Promise<ReleaseLockFn | null> {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const token = randomUUID();

  try {
    await fs.writeFile(
      file,
      JSON.stringify({ pid: process.pid, token, acquiredAt: new Date().toISOString() }),
      { flag: "wx" },
    );
    return () => release(file, token);
  } catch (error: any) {
    if (error.code !== "EEXIST") throw error;
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
    await fs.writeFile(
      file,
      JSON.stringify({ pid: process.pid, token, acquiredAt: new Date().toISOString() }),
      { flag: "wx" },
    );
    return () => release(file, token);
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

async function isStale(file: string, staleAfterMs: number): Promise<boolean> {
  let stat;
  try {
    stat = await fs.stat(file);
  } catch (error: any) {
    if (error.code === "ENOENT") return true;
    throw error;
  }

  const age = Date.now() - stat.mtimeMs;
  if (age > staleAfterMs) return true;

  const owner = await readLock(file);
  if (!owner) return age > UNREADABLE_GRACE_MS;
  return !isAlive(owner.pid);
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: any) {
    return error.code === "EPERM";
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
  }: AcquireDistributedLockOptions = {},
): Promise<ReleaseLockFn | null> {
  if (!storage || !lockKey || typeof storage.acquireLock !== "function") {
    return null;
  }

  const token = await storage.acquireLock(lockKey, { holder, ttlMs });
  if (!token) return null;

  const timer = setInterval(async () => {
    try {
      await storage.refreshLock!(lockKey, token, ttlMs);
    } catch (err: any) {
      logger?.warn({ err: err.message }, "Failed to refresh distributed S3 lock heartbeat");
    }
  }, heartbeatIntervalMs);
  timer.unref();

  return async function release() {
    clearInterval(timer);
    try {
      await storage.releaseLock!(lockKey, token);
    } catch (err: any) {
      logger?.warn({ err: err.message }, "Failed to release distributed S3 lock");
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
}: AcquireHybridLockOptions): Promise<ReleaseLockFn | null> {
  const localRelease = await acquireLock(file, { staleAfterMs });
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
