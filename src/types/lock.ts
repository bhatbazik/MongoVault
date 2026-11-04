import type { Logger } from "./logger.js";
import type { StorageClient } from "./storage.js";

export type ReleaseLockFn = () => Promise<void>;

export interface LockPayload {
  pid: number;
  token: string;
  acquiredAt: string;
}

export interface AcquireLockOptions {
  staleAfterMs: number;
}

export interface AcquireDistributedLockOptions {
  holder?: string;
  ttlMs?: number;
  heartbeatIntervalMs?: number;
  logger?: Logger;
}

export interface AcquireHybridLockOptions {
  file: string;
  storage?: StorageClient;
  lockKey?: string;
  staleAfterMs?: number;
  distributedTtlMs?: number;
  heartbeatIntervalMs?: number;
  holder?: string;
  logger?: Logger;
}
