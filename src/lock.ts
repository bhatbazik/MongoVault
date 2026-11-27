import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { withFileLockMutex } from "./storage.js";
import type {
  AcquireDistributedLockOptions,
  AcquireHybridLockOptions,
  AcquireLockOptions,
  LockPayload,
  ReleaseLockFn,
} from "./types/lock.js";
import type { StorageClient } from "./types/storage.js";

const UNREADABLE_GRACE_MS = 5_000;
const PROCESS_INSTANCE_ID = randomUUID();
const activeLocalTokens = new Set<string>();

async function getLinuxPidStartTime(pid: number): Promise<string | undefined> {
  try {
    const raw = await fs.readFile(`/proc/${pid}/stat`, "utf8");
    const rparen = raw.lastIndexOf(")");
    if (rparen === -1) return undefined;
    const fields = raw
      .slice(rparen + 2)
      .trim()
      .split(/\s+/);
    // Field 22 in /proc/<pid>/stat is starttime -> index 19 after comm
    return fields[19];
  } catch {
    return undefined;
  }
}

async function writeLockExclusiveLocal(file: string, content: string): Promise<boolean> {
  const initTmp = `${file}.init.${randomUUID()}`;
  const handle = await fs.open(initTmp, "w", 0o600);
  try {
    await handle.writeFile(content, "utf8");
    await handle.sync();
    await handle.close();
    try {
      await fs.link(initTmp, file);
      return true;
    } catch (err: unknown) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === "EEXIST") return false;
      if (code === "EPERM" || code === "EXDEV" || code === "ENOTSUP") {
        try {
          await fs.writeFile(file, content, { flag: "wx", mode: 0o600 });
          return true;
        } catch (wxErr: unknown) {
          if ((wxErr as NodeJS.ErrnoException).code === "EEXIST") return false;
          throw wxErr;
        }
      }
      throw err;
    }
  } finally {
    await handle.close().catch(() => {});
    await fs.rm(initTmp, { force: true }).catch(() => {});
  }
}

export async function acquireLock(
  file: string,
  {
    staleAfterMs,
    heartbeatIntervalMs = Math.min(10_000, Math.max(10, Math.floor(staleAfterMs / 3))),
    logger,
    onLost,
  }: AcquireLockOptions,
): Promise<ReleaseLockFn | null> {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const token = randomUUID();
  const pidStartTime = await getLinuxPidStartTime(process.pid);
  const hostname = os.hostname();

  const createPayload = (): LockPayload => ({
    pid: process.pid,
    hostname,
    instanceId: PROCESS_INSTANCE_ID,
    ...(pidStartTime ? { pidStartTime } : {}),
    token,
    acquiredAt: new Date().toISOString(),
    lastHeartbeatAt: new Date().toISOString(),
  });

  const buildHandle = (): ReleaseLockFn => {
    activeLocalTokens.add(token);
    let released = false;
    let lost = false;
    let inFlight = false;
    let lostError: Error | null = null;
    let lastSuccessfulHeartbeat = Date.now();
    const lockController = new AbortController();

    const triggerLost = (err: Error) => {
      if (released || lost) return;
      lost = true;
      lostError = err;
      clearInterval(timer);
      activeLocalTokens.delete(token);
      logger?.error({ file }, err.message);
      lockController.abort(err);
      onLost?.(err);
    };

    const timer = setInterval(async () => {
      if (released || lost || inFlight) return;
      inFlight = true;
      try {
        await withFileLockMutex(file, async () => {
          if (released || lost) return;
          const owner = await readLock(file);
          if (!owner || owner.token !== token) {
            const err = new Error(
              `Local lock lost on "${file}": lock file was modified or removed by another process`,
            );
            triggerLost(err);
            return;
          }
          const now = new Date();
          await fs.utimes(file, now, now);
          lastSuccessfulHeartbeat = Date.now();
        });
      } catch (err: unknown) {
        const error = err instanceof Error ? err : new Error(String(err));
        logger?.warn({ err: error, file }, "Failed to update local lock heartbeat timestamp");
        if (!released && !lost && Date.now() - lastSuccessfulHeartbeat > staleAfterMs) {
          triggerLost(error);
        }
      } finally {
        inFlight = false;
      }
    }, heartbeatIntervalMs);
    timer.unref();

    const releaseFn: ReleaseLockFn = async () => {
      if (released) return;
      released = true;
      clearInterval(timer);
      activeLocalTokens.delete(token);
      await release(file, token);
    };
    releaseFn.signal = lockController.signal;
    releaseFn.assertValid = () => {
      if (lostError) throw lostError;
      if (!released && Date.now() - lastSuccessfulHeartbeat > staleAfterMs) {
        const err = new Error(
          `Local lock lease expired on "${file}" after ${staleAfterMs}ms without heartbeat`,
        );
        triggerLost(err);
        throw err;
      }
    };
    return releaseFn;
  };

  const acquired = await withFileLockMutex(file, async (assertHeld) => {
    assertHeld();
    if (await writeLockExclusiveLocal(file, JSON.stringify(createPayload()))) {
      return true;
    }

    const { stale, existed } = await checkLockStaleness(file, staleAfterMs);
    if (!stale) {
      return false;
    }

    if (existed) {
      assertHeld();
      const tombstone = `${file}.stale.${randomUUID()}`;
      try {
        await fs.rename(file, tombstone);
        await fs.rm(tombstone, { force: true });
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
          return false;
        }
      }
    }

    assertHeld();
    return writeLockExclusiveLocal(file, JSON.stringify(createPayload()));
  });

  return acquired ? buildHandle() : null;
}

export async function readLock(file: string): Promise<LockPayload | null> {
  try {
    return JSON.parse(await fs.readFile(file, "utf8")) as LockPayload;
  } catch {
    return null;
  }
}

async function release(file: string, token: string): Promise<void> {
  await withFileLockMutex(file, async (assertHeld) => {
    const owner = await readLock(file);
    if (owner?.token === token) {
      assertHeld();
      await fs.rm(file, { force: true });
    }
  });
}

async function checkLockStaleness(
  file: string,
  staleAfterMs: number,
): Promise<{ stale: boolean; existed: boolean }> {
  let stat;
  try {
    stat = await fs.stat(file);
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { stale: true, existed: false };
    }
    throw error;
  }

  const age = Date.now() - stat.mtimeMs;
  const owner = await readLock(file);
  if (!owner) {
    return { stale: age > UNREADABLE_GRACE_MS, existed: true };
  }

  if (owner.hostname && owner.hostname !== os.hostname()) {
    return { stale: age > staleAfterMs, existed: true };
  }

  if (owner.pid === process.pid) {
    if (
      owner.instanceId &&
      owner.instanceId !== PROCESS_INSTANCE_ID &&
      !activeLocalTokens.has(owner.token)
    ) {
      return { stale: true, existed: true };
    }
    return { stale: age > staleAfterMs, existed: true };
  }

  if (!isAlive(owner.pid)) {
    return { stale: true, existed: true };
  }

  if (owner.pidStartTime) {
    const currentStart = await getLinuxPidStartTime(owner.pid);
    if (currentStart !== undefined && currentStart !== owner.pidStartTime) {
      return { stale: true, existed: true };
    }
  }

  return { stale: age > staleAfterMs, existed: true };
}

export async function isStale(file: string, staleAfterMs: number): Promise<boolean> {
  return (await checkLockStaleness(file, staleAfterMs)).stale;
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

  const effectiveHeartbeatMs = Math.min(heartbeatIntervalMs, Math.max(10, Math.floor(ttlMs / 3)));
  const token = await storage.acquireLock(lockKey, { holder, ttlMs });
  if (!token) return null;

  let lastSuccessfulHeartbeat = Date.now();
  let released = false;
  let lost = false;
  let inFlight = false;
  let lostError: Error | null = null;
  const lockController = new AbortController();

  const triggerLost = (err: Error) => {
    if (released || lost) return;
    lost = true;
    lostError = err;
    clearInterval(timer);
    logger?.error({ lockKey }, err.message);
    lockController.abort(err);
    onLost?.(err);
  };

  const timer = setInterval(async () => {
    if (released || lost || inFlight) return;
    inFlight = true;
    try {
      const refreshed = await storage.refreshLock!(lockKey, token, ttlMs);
      if (released || lost) return;
      if (!refreshed) {
        triggerLost(
          new Error(
            `Distributed lock lease lost on "${lockKey}": lease renewal was rejected (lease held by another owner or expired)`,
          ),
        );
        return;
      }
      lastSuccessfulHeartbeat = Date.now();
    } catch (err: unknown) {
      if (released || lost) return;
      const error = err instanceof Error ? err : new Error(String(err));
      logger?.warn(
        { err: error.message, lockKey },
        "Failed to refresh distributed S3 lock heartbeat",
      );
      if (Date.now() - lastSuccessfulHeartbeat > ttlMs) {
        triggerLost(
          new Error(
            `Distributed lock lease expired on "${lockKey}" after ${ttlMs}ms without successful renewal`,
          ),
        );
      }
    } finally {
      inFlight = false;
    }
  }, effectiveHeartbeatMs);
  timer.unref();

  const releaseFn: ReleaseLockFn = async function release() {
    if (released) return;
    released = true;
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
  releaseFn.signal = lockController.signal;
  releaseFn.assertValid = () => {
    if (lostError) throw lostError;
    if (!released && Date.now() - lastSuccessfulHeartbeat > ttlMs) {
      const expiredErr = new Error(
        `Distributed lock lease expired on "${lockKey}" after ${ttlMs}ms without successful renewal`,
      );
      triggerLost(expiredErr);
      throw expiredErr;
    }
  };

  return releaseFn;
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
  const localHeartbeatMs = Math.min(
    heartbeatIntervalMs,
    Math.max(10, Math.floor(staleAfterMs / 3)),
  );
  const distHeartbeatMs = Math.min(
    heartbeatIntervalMs,
    Math.max(10, Math.floor(distributedTtlMs / 3)),
  );

  const localRelease = await acquireLock(file, {
    staleAfterMs,
    heartbeatIntervalMs: localHeartbeatMs,
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
        heartbeatIntervalMs: distHeartbeatMs,
        logger,
        onLost,
      });

      if (!distRelease) {
        await localRelease();
        return null;
      }

      const hybridController = new AbortController();
      const onLocalAbort = () => hybridController.abort(localRelease.signal?.reason);
      const onDistAbort = () => hybridController.abort(distRelease.signal?.reason);
      if (localRelease.signal?.aborted) onLocalAbort();
      else localRelease.signal?.addEventListener("abort", onLocalAbort, { once: true });
      if (distRelease.signal?.aborted) onDistAbort();
      else distRelease.signal?.addEventListener("abort", onDistAbort, { once: true });

      const releaseAll: ReleaseLockFn = async function releaseAll() {
        localRelease.signal?.removeEventListener("abort", onLocalAbort);
        distRelease.signal?.removeEventListener("abort", onDistAbort);
        await distRelease().catch(() => {});
        await localRelease().catch(() => {});
      };
      releaseAll.signal = hybridController.signal;
      releaseAll.assertValid = () => {
        localRelease.assertValid?.();
        distRelease.assertValid?.();
      };
      return releaseAll;
    } catch (err) {
      await localRelease().catch(() => {});
      throw err;
    }
  }

  return localRelease;
}
