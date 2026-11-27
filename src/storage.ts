import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  type GetObjectCommandOutput,
  HeadObjectCommand,
  PutObjectCommand,
  type PutObjectCommandInput,
  S3Client,
  type S3ClientConfig,
  paginateListObjectsV2,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { runStreamPipeline } from "./stream.js";
import type {
  GetJsonResult,
  PutJsonOptions,
  StorageClient,
  StorageHeadResult,
  StorageObject,
  StorageOptions,
  UploadOptions,
} from "./types/storage.js";

const MIN_S3_PART_SIZE = 5 * 1024 * 1024; // 5 MiB AWS S3 minimum
const MAX_S3_PART_SIZE = 512 * 1024 * 1024; // 512 MiB cap for small-RAM safety (supports up to 5 TiB)
const S3_MAX_PARTS = 10_000;
const DOWNLOAD_MAX_RETRIES = 5;
const MUTEX_HOLD_TIMEOUT_MS = 2_000;
const MUTEX_WAIT_TIMEOUT_MS = 5_000;
const MUTEX_STALE_MS = 10_000;
const UNREADABLE_LOCK_GRACE_MS = 5_000;

export class S3BatchDeleteError extends Error {
  readonly errors: Array<{ key?: string; code?: string; message?: string }>;
  readonly objectLockEnabled: boolean;
  readonly Code?: string;

  constructor(
    message: string,
    errors: Array<{ key?: string; code?: string; message?: string }>,
    objectLockEnabled: boolean,
  ) {
    super(message);
    this.name = "S3BatchDeleteError";
    this.errors = errors;
    this.objectLockEnabled = objectLockEnabled;
    if (errors.length === 1 && errors[0]?.code) {
      this.Code = errors[0].code;
    }
  }
}

export async function withFileLockMutex<T>(
  lockFilePath: string,
  fn: (assertHeld: () => void) => Promise<T>,
): Promise<T> {
  const parentDir = path.dirname(lockFilePath);
  await fs.mkdir(parentDir, { recursive: true });
  const mutexDir = `${lockFilePath}.lck`;
  const deadline = Date.now() + MUTEX_WAIT_TIMEOUT_MS;
  let acquired = false;

  while (Date.now() < deadline) {
    try {
      await fs.mkdir(mutexDir);
      acquired = true;
      break;
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") {
        throw err;
      }
      const st = await fs.stat(mutexDir).catch(() => null);
      if (st && Date.now() - st.mtimeMs > MUTEX_STALE_MS) {
        const staleMutex = `${mutexDir}.stale.${randomUUID()}`;
        try {
          await fs.rename(mutexDir, staleMutex);
          await fs.rm(staleMutex, { recursive: true, force: true }).catch(() => {});
          continue;
        } catch {
          // Lost stale-break race; retry normally
        }
      }
      const jitter = 5 + Math.floor(Math.random() * 10);
      await new Promise((r) => setTimeout(r, jitter));
    }
  }

  if (!acquired) {
    throw new Error(`Timed out waiting for file lock mutex "${mutexDir}"`);
  }

  const myStat = await fs.stat(mutexDir).catch(() => null);
  const myIno = myStat?.ino;
  const acquiredAt = Date.now();
  const assertHeld = () => {
    if (Date.now() - acquiredAt > MUTEX_HOLD_TIMEOUT_MS) {
      throw new Error(`File lock mutex hold timeout exceeded for "${lockFilePath}"`);
    }
  };

  try {
    return await fn(assertHeld);
  } finally {
    if (Date.now() - acquiredAt < MUTEX_STALE_MS - 1_000) {
      const curStat = await fs.stat(mutexDir).catch(() => null);
      if (curStat && myIno !== undefined && curStat.ino === myIno) {
        await fs.rmdir(mutexDir).catch(() => {});
      }
    }
  }
}

async function fsyncDir(dirPath: string): Promise<void> {
  let dirHandle: fs.FileHandle | null = null;
  try {
    dirHandle = await fs.open(dirPath, "r");
    await dirHandle.sync();
  } catch (err: unknown) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== "EINVAL" && code !== "EISDIR" && code !== "EPERM" && code !== "EACCES") {
      throw err;
    }
  } finally {
    await dirHandle?.close().catch(() => {});
  }
}

function isInternalArtifactName(name: string): boolean {
  return (
    name.includes(".tmp-") ||
    name.includes(".stale.") ||
    name.includes(".refresh.") ||
    name.includes(".release.") ||
    name.includes(".init.")
  );
}

function toAbortError(reason: unknown, fallbackMessage: string): Error {
  if (reason instanceof Error) return reason;
  return Object.assign(
    new Error(reason ? `${fallbackMessage}: ${String(reason)}` : fallbackMessage),
    { name: "AbortError", code: "ABORT_ERR" },
  );
}

async function sleepWithAbort(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    throw toAbortError(signal.reason, "Aborted");
  }
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(toAbortError(signal?.reason, "Aborted"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export interface LockRecord {
  token: string;
  holder?: string;
  acquiredAt: string;
  expiresAt: number;
}

export function createLockRecord(
  holder: string | undefined,
  ttlMs: number,
  now = Date.now(),
): { token: string; record: LockRecord } {
  const token = randomUUID();
  return {
    token,
    record: {
      token,
      holder,
      acquiredAt: new Date(now).toISOString(),
      expiresAt: now + ttlMs,
    },
  };
}

export function isLockActive(record: LockRecord | undefined | null, now = Date.now()): boolean {
  return Boolean(record && record.expiresAt && record.expiresAt > now);
}

/**
 * Computes an S3 multipart `partSize` and RAM-safe `queueSize` so large uploads (up to 5 TiB)
 * never exceed AWS S3's 10,000-part limit while keeping memory bounded on small instances.
 */
export function computeS3UploadParams(
  configuredPartSize: number,
  configuredQueueSize: number,
  expectedSizeBytes?: number,
): { partSize: number; queueSize: number } {
  let partSize = Math.max(MIN_S3_PART_SIZE, configuredPartSize);

  if (expectedSizeBytes && expectedSizeBytes > 0) {
    // 1.5x headroom to account for compression variance / live growth
    const minFor10k = Math.ceil((expectedSizeBytes * 1.5) / S3_MAX_PARTS);
    if (minFor10k > partSize) {
      // Round up to 1 MiB boundary
      const mib = 1024 * 1024;
      partSize = Math.ceil(minFor10k / mib) * mib;
    }
  }

  partSize = Math.min(MAX_S3_PART_SIZE, Math.max(MIN_S3_PART_SIZE, partSize));
  // Clamp concurrent in-flight parts on large part sizes to protect small-RAM hosts
  const queueSize = partSize >= 128 * 1024 * 1024 ? 1 : Math.max(1, configuredQueueSize);

  return { partSize, queueSize };
}

/**
 * Determines whether a JSON storage key represents mutable control/state/probe metadata
 * that must NEVER be locked under S3 WORM Object Lock.
 */
function isMutableControlKey(key: string): boolean {
  const norm = key.replace(/\\/g, "/");
  return (
    norm.endsWith("/state.json") ||
    norm === "state.json" ||
    norm.includes("/.locks/") ||
    norm.includes("/locks/") ||
    norm.includes("/.probe-") ||
    norm.startsWith(".probe-")
  );
}

/**
 * StorageClient implementation backed by the local filesystem with path traversal guards
 * and prefix-scoped listing.
 */
export class FilesystemStorageClient implements StorageClient {
  readonly bucket: string;
  private readonly resolvedBase: string;

  constructor(basePath: string) {
    this.resolvedBase = path.resolve(basePath);
    this.bucket = this.resolvedBase;
  }

  private resolveSafePath(key: string): string {
    const cleanKey = key.replace(/\\/g, "/").replace(/^\/+/, "");
    if (!cleanKey || cleanKey === ".") {
      throw new Error(`Invalid or empty storage key: "${key}"`);
    }
    const dest = path.resolve(this.resolvedBase, cleanKey);
    if (dest === this.resolvedBase || !dest.startsWith(`${this.resolvedBase}${path.sep}`)) {
      throw new Error(`Storage key escapes base directory: "${key}"`);
    }
    return dest;
  }

  private async resolveSafeRealPath(
    key: string,
    { createParents = false }: { createParents?: boolean } = {},
  ): Promise<string> {
    const dest = this.resolveSafePath(key);
    if (createParents) {
      await fs.mkdir(this.resolvedBase, { recursive: true });
    }
    let baseReal: string;
    try {
      baseReal = await fs.realpath(this.resolvedBase);
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT" && !createParents) {
        return dest;
      }
      throw err;
    }

    const rel = path.relative(this.resolvedBase, dest);
    const parts = rel.split(path.sep).filter(Boolean);
    let current = this.resolvedBase;
    for (const part of parts) {
      current = path.join(current, part);
      try {
        const lst = await fs.lstat(current);
        if (lst.isSymbolicLink()) {
          throw new Error(`Storage key escapes base directory via symlink: "${key}"`);
        }
        const real = await fs.realpath(current);
        if (real !== baseReal && !real.startsWith(`${baseReal}${path.sep}`)) {
          throw new Error(`Storage key escapes base directory via symlink: "${key}"`);
        }
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code === "ENOENT") {
          break;
        }
        throw err;
      }
    }

    if (createParents) {
      const parentDir = path.dirname(dest);
      await fs.mkdir(parentDir, { recursive: true });
      const parentReal = await fs.realpath(parentDir);
      if (parentReal !== baseReal && !parentReal.startsWith(`${baseReal}${path.sep}`)) {
        throw new Error(`Storage key escapes base directory via symlink: "${key}"`);
      }
      try {
        const destLst = await fs.lstat(dest);
        if (destLst.isSymbolicLink()) {
          throw new Error(`Storage key escapes base directory via symlink: "${key}"`);
        }
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
      }
    }

    return dest;
  }

  async upload(
    key: string,
    body: unknown,
    { signal, onProgress }: UploadOptions = {},
  ): Promise<void> {
    const dest = await this.resolveSafeRealPath(key, { createParents: true });
    const tmp = `${dest}.tmp-${randomUUID()}`;
    const handle = await fs.open(tmp, "w", 0o600);
    let streamOwnedHandle = false;

    try {
      if (body && typeof (body as NodeJS.ReadableStream).pipe === "function") {
        streamOwnedHandle = true;
        const writeStream = handle.createWriteStream();
        let loaded = 0;
        if (onProgress) {
          (body as NodeJS.ReadableStream).on("data", (chunk: Buffer) => {
            loaded += chunk.length;
            onProgress({ loaded });
          });
        }
        await runStreamPipeline([body as NodeJS.ReadableStream, writeStream], { signal });
      } else if (Buffer.isBuffer(body) || body instanceof Uint8Array || typeof body === "string") {
        await handle.writeFile(body);
        if (onProgress) onProgress({ loaded: Buffer.byteLength(body) });
      } else {
        throw new Error(`Unsupported upload body type for key "${key}"`);
      }

      if (!streamOwnedHandle) {
        await handle.sync();
        await handle.close();
      } else {
        await handle.close().catch(() => {});
        const syncHandle = await fs.open(tmp, "r");
        try {
          await syncHandle.sync();
        } finally {
          await syncHandle.close();
        }
      }
      await fs.rename(tmp, dest);
      await fsyncDir(path.dirname(dest));
    } catch (err) {
      await handle.close().catch(() => {});
      await fs.rm(tmp, { force: true }).catch(() => {});
      throw err;
    }
  }

  async head(key: string): Promise<StorageHeadResult | null> {
    const file = await this.resolveSafeRealPath(key);
    try {
      const stat = await fs.stat(file);
      return {
        size: stat.size,
        lastModified: stat.mtime,
        metadata: {},
      };
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw err;
    }
  }

  async download(key: string, { signal }: { signal?: AbortSignal } = {}): Promise<Readable> {
    const file = await this.resolveSafeRealPath(key);
    const stream = fsSync.createReadStream(file);
    if (signal) {
      const onAbort = () => stream.destroy(signal.reason ?? new Error("Aborted"));
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
      stream.once("close", () => signal.removeEventListener("abort", onAbort));
    }
    return stream;
  }

  async putJson(key: string, value: unknown, _options?: PutJsonOptions): Promise<void> {
    const dest = await this.resolveSafeRealPath(key, { createParents: true });
    const tmp = `${dest}.tmp-${randomUUID()}`;
    const handle = await fs.open(tmp, "w", 0o600);
    try {
      await handle.writeFile(JSON.stringify(value, null, 2), "utf8");
      await handle.sync();
      await handle.close();
      await fs.rename(tmp, dest);
      await fsyncDir(path.dirname(dest));
    } catch (err) {
      await handle.close().catch(() => {});
      await fs.rm(tmp, { force: true }).catch(() => {});
      throw err;
    }
  }

  async getJson<T = unknown>(key: string): Promise<GetJsonResult<T> | null> {
    const file = await this.resolveSafeRealPath(key);
    try {
      const [content, stat] = await Promise.all([fs.readFile(file, "utf8"), fs.stat(file)]);
      return {
        data: JSON.parse(content) as T,
        etag: String(stat.mtimeMs),
      };
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw err;
    }
  }

  async *list(prefix: string): AsyncGenerator<StorageObject, void, unknown> {
    const normPrefix = prefix.replace(/\\/g, "/").replace(/^\/+/, "");
    const basePath = this.resolvedBase;

    // Start walking at the deepest existing directory matching `prefix` instead of scanning entire basePath
    let startDir = basePath;
    if (normPrefix) {
      const candidateDir = normPrefix.endsWith("/")
        ? normPrefix.slice(0, -1)
        : path.posix.dirname(normPrefix);
      if (candidateDir && candidateDir !== ".") {
        startDir = await this.resolveSafeRealPath(candidateDir);
      }
    }

    async function* walk(currentDir: string): AsyncGenerator<StorageObject, void, unknown> {
      let entries;
      try {
        entries = await fs.readdir(currentDir, { withFileTypes: true });
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
        throw err;
      }
      for (const entry of entries) {
        const fullPath = path.join(currentDir, entry.name);
        if (entry.isDirectory()) {
          if (entry.name.endsWith(".lck") || entry.name.includes(".lck.stale.")) {
            continue;
          }
          yield* walk(fullPath);
        } else if (entry.isFile() && !isInternalArtifactName(entry.name)) {
          const relKey = path.relative(basePath, fullPath).replace(/\\/g, "/");
          if (!normPrefix || relKey.startsWith(normPrefix)) {
            const stat = await fs.stat(fullPath);
            yield {
              key: relKey,
              size: stat.size,
              lastModified: stat.mtime,
            };
          }
        }
      }
    }

    yield* walk(startDir);
  }

  async cleanupStaleUploads(prefix: string, maxAgeMs: number): Promise<number> {
    const normPrefix = prefix.replace(/\\/g, "/").replace(/^\/+/, "");
    let startDir = this.resolvedBase;
    if (normPrefix) {
      const candidateDir = normPrefix.endsWith("/")
        ? normPrefix.slice(0, -1)
        : path.posix.dirname(normPrefix);
      if (candidateDir && candidateDir !== ".") {
        startDir = await this.resolveSafeRealPath(candidateDir);
      }
    }

    const cutoff = Date.now() - maxAgeMs;
    let removed = 0;

    async function walkCleanup(currentDir: string): Promise<void> {
      let entries;
      try {
        entries = await fs.readdir(currentDir, { withFileTypes: true });
      } catch (err: unknown) {
        if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
        throw err;
      }
      for (const entry of entries) {
        const fullPath = path.join(currentDir, entry.name);
        if (entry.isDirectory()) {
          if (entry.name.endsWith(".lck") || entry.name.includes(".lck.stale.")) {
            const st = await fs.stat(fullPath).catch(() => null);
            if (st && st.mtimeMs < cutoff) {
              await fs.rm(fullPath, { recursive: true, force: true }).catch(() => {});
              removed++;
            }
          } else {
            await walkCleanup(fullPath);
          }
        } else if (entry.isFile() && isInternalArtifactName(entry.name)) {
          const st = await fs.stat(fullPath).catch(() => null);
          if (st && st.mtimeMs < cutoff) {
            await fs.rm(fullPath, { force: true }).catch(() => {});
            removed++;
          }
        }
      }
    }

    await walkCleanup(startDir);
    return removed;
  }

  async remove(key: string): Promise<void> {
    await fs.rm(await this.resolveSafeRealPath(key), { force: true });
  }

  async removeMany(keys: string[]): Promise<void> {
    await Promise.all(
      keys.map(async (k) => fs.rm(await this.resolveSafeRealPath(k), { force: true })),
    );
  }

  private async writeLockExclusive(dest: string, content: string): Promise<boolean> {
    const initTmp = `${dest}.init.${randomUUID()}`;
    const handle = await fs.open(initTmp, "w", 0o600);
    try {
      await handle.writeFile(content, "utf8");
      await handle.sync();
      await handle.close();
      try {
        await fs.link(initTmp, dest);
        await fsyncDir(path.dirname(dest));
        return true;
      } catch (err: unknown) {
        const code = (err as NodeJS.ErrnoException).code;
        if (code === "EEXIST") return false;
        if (code === "EPERM" || code === "EXDEV" || code === "ENOTSUP") {
          try {
            await fs.writeFile(dest, content, { flag: "wx", mode: 0o600 });
            await fsyncDir(path.dirname(dest));
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

  private async readLockState(
    dest: string,
  ): Promise<{ exists: boolean; data: LockRecord | null; corrupt: boolean; mtimeMs: number }> {
    let stat;
    try {
      stat = await fs.stat(dest);
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        return { exists: false, data: null, corrupt: false, mtimeMs: 0 };
      }
      throw err;
    }
    try {
      const content = await fs.readFile(dest, "utf8");
      return {
        exists: true,
        data: JSON.parse(content) as LockRecord,
        corrupt: false,
        mtimeMs: stat.mtimeMs,
      };
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        return { exists: false, data: null, corrupt: false, mtimeMs: 0 };
      }
      return { exists: true, data: null, corrupt: true, mtimeMs: stat.mtimeMs };
    }
  }

  async acquireLock(
    key: string,
    { holder, ttlMs }: { holder?: string; ttlMs: number },
  ): Promise<string | null> {
    const dest = await this.resolveSafeRealPath(key, { createParents: true });
    const { token, record } = createLockRecord(holder, ttlMs);
    const content = JSON.stringify(record, null, 2);

    return withFileLockMutex(dest, async (assertHeld) => {
      assertHeld();
      if (await this.writeLockExclusive(dest, content)) {
        return token;
      }

      const state = await this.readLockState(dest);
      if (state.exists) {
        if (state.corrupt) {
          if (Date.now() - state.mtimeMs <= UNREADABLE_LOCK_GRACE_MS) {
            return null;
          }
        } else if (isLockActive(state.data, Date.now())) {
          return null;
        }

        assertHeld();
        const tombstone = `${dest}.stale.${randomUUID()}`;
        try {
          await fs.rename(dest, tombstone);
          await fs.rm(tombstone, { force: true });
        } catch (err: unknown) {
          if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
            return null;
          }
        }
      }

      assertHeld();
      return (await this.writeLockExclusive(dest, content)) ? token : null;
    });
  }

  async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
    const dest = await this.resolveSafeRealPath(key);
    return withFileLockMutex(dest, async (assertHeld) => {
      const state = await this.readLockState(dest);
      if (!state.exists || state.corrupt || !state.data || state.data.token !== token) {
        return false;
      }

      const updated: LockRecord = {
        ...state.data,
        expiresAt: Date.now() + ttlMs,
      };

      const tmp = `${dest}.refresh.${randomUUID()}`;
      const handle = await fs.open(tmp, "w", 0o600);
      try {
        await handle.writeFile(JSON.stringify(updated, null, 2), "utf8");
        await handle.sync();
        await handle.close();
        assertHeld();
        await fs.rename(tmp, dest);
        await fsyncDir(path.dirname(dest));
        return true;
      } catch {
        await handle.close().catch(() => {});
        await fs.rm(tmp, { force: true }).catch(() => {});
        return false;
      }
    });
  }

  async releaseLock(key: string, token: string): Promise<void> {
    const dest = await this.resolveSafeRealPath(key);
    await withFileLockMutex(dest, async (assertHeld) => {
      const state = await this.readLockState(dest);
      if (state.exists && !state.corrupt && state.data?.token === token) {
        assertHeld();
        const tombstone = `${dest}.release.${randomUUID()}`;
        try {
          await fs.rename(dest, tombstone);
          await fs.rm(tombstone, { force: true });
          await fsyncDir(path.dirname(dest));
        } catch {
          // Ignored if already removed
        }
      }
    });
  }
}

function normalizeEtag(raw: string | undefined): string | null {
  if (!raw) return null;
  if (raw.startsWith("W/")) return raw;
  return raw.replace(/^"|"$/g, "");
}

function formatIfMatch(etag: string): string {
  if (etag.startsWith("W/") || (etag.startsWith('"') && etag.endsWith('"'))) {
    return etag;
  }
  return `"${etag}"`;
}

function destroyStreamSafely(body: unknown): void {
  if (
    body &&
    typeof body === "object" &&
    "destroy" in body &&
    typeof (body as { destroy?: unknown }).destroy === "function" &&
    !(body as { destroyed?: boolean }).destroyed
  ) {
    (body as { destroy: () => void }).destroy();
  }
}

/**
 * StorageClient implementation backed by AWS S3 (or S3-compatible object storage)
 * with adaptive retries, dynamic 10,000-part-safe multipart sizing, and Range-resumable downloads.
 */
export class S3StorageClient implements StorageClient {
  readonly bucket: string;
  private readonly client: S3Client;
  private readonly partSize: number;
  private readonly concurrency: number;
  private readonly objectLock?: StorageOptions["objectLock"];
  private lastServerClockSample: { serverDateMs: number; localAtMs: number } | null = null;
  private readonly responseServerDate = new WeakMap<object, number>();

  constructor(options: StorageOptions) {
    const {
      endpoint,
      region,
      bucket = "mongodb-backups",
      accessKeyId = "",
      secretAccessKey = "",
      forcePathStyle,
      partSizeMb = 32,
      queueSize = 2,
      objectLock,
    } = options;

    this.bucket = bucket;
    this.objectLock = objectLock;
    this.partSize = Math.max(5, partSizeMb) * 1024 * 1024;
    this.concurrency = Math.max(1, queueSize);

    const clientConfig: S3ClientConfig = {
      region,
      forcePathStyle,
      credentials: { accessKeyId, secretAccessKey },
      maxAttempts: 5,
      retryMode: "adaptive",
      requestChecksumCalculation: "WHEN_REQUIRED",
      responseChecksumValidation: "WHEN_REQUIRED",
    };
    if (endpoint) {
      clientConfig.endpoint = endpoint;
    }
    this.client = new S3Client(clientConfig);
    this.client.middlewareStack?.add(
      (next) => async (args) => {
        try {
          const result = await next(args);
          const headers = (
            result.response as { headers?: Record<string, string | undefined> } | undefined
          )?.headers;
          const dateHeader = headers?.date ?? headers?.Date;
          if (dateHeader) {
            const parsed = Date.parse(dateHeader);
            if (!Number.isNaN(parsed)) {
              this.lastServerClockSample = { serverDateMs: parsed, localAtMs: Date.now() };
              if (result.output && typeof result.output === "object") {
                this.responseServerDate.set(result.output as object, parsed);
              }
            }
          }
          return result;
        } catch (err: unknown) {
          const headers = (err as { $response?: { headers?: Record<string, string | undefined> } })
            ?.$response?.headers;
          const dateHeader = headers?.date ?? headers?.Date;
          if (dateHeader) {
            const parsed = Date.parse(dateHeader);
            if (!Number.isNaN(parsed)) {
              this.lastServerClockSample = { serverDateMs: parsed, localAtMs: Date.now() };
            }
          }
          throw err;
        }
      },
      { step: "deserialize", name: "captureS3ServerDate", priority: "low" },
    );
  }

  private getServerNow(response?: unknown): number {
    if (response && typeof response === "object") {
      const exact = this.responseServerDate.get(response);
      if (exact !== undefined) return exact;
    }
    if (this.lastServerClockSample) {
      return (
        this.lastServerClockSample.serverDateMs +
        (Date.now() - this.lastServerClockSample.localAtMs)
      );
    }
    return Date.now();
  }

  async upload(
    key: string,
    body: unknown,
    {
      contentType = "application/octet-stream",
      signal,
      onProgress,
      expectedSizeBytes,
      objectLockMode,
      objectLockRetainUntilDate,
      objectLockLegalHoldStatus,
    }: UploadOptions = {},
  ): Promise<void> {
    if (signal?.aborted) {
      throw toAbortError(signal.reason, "S3 upload aborted");
    }

    const mode = objectLockMode || (this.objectLock?.enabled ? this.objectLock.mode : undefined);
    const retainDate =
      objectLockRetainUntilDate ||
      (this.objectLock?.enabled && this.objectLock.retentionDays
        ? new Date(Date.now() + this.objectLock.retentionDays * 86_400_000)
        : undefined);
    const legalHold =
      objectLockLegalHoldStatus ||
      (this.objectLock?.enabled && this.objectLock.legalHold ? "ON" : undefined);

    const { partSize, queueSize } = computeS3UploadParams(
      this.partSize,
      this.concurrency,
      expectedSizeBytes,
    );

    const params: PutObjectCommandInput = {
      Bucket: this.bucket,
      Key: key,
      Body: body as PutObjectCommandInput["Body"],
      ContentType: contentType,
      ...(mode ? { ObjectLockMode: mode } : {}),
      ...(retainDate ? { ObjectLockRetainUntilDate: retainDate } : {}),
      ...(legalHold ? { ObjectLockLegalHoldStatus: legalHold } : {}),
    };

    const requestAbortController = new AbortController();
    let abortMpuPromise: Promise<unknown> | null = null;
    const baseClient = this.client;

    interface UploadInternalState {
      uploadId?: string;
      createMultiPartPromise?: Promise<{ UploadId?: string }>;
      concurrentUploaders?: Promise<unknown>[];
      abortMultipartUploadCommand?: unknown;
      markUploadAsAborted?: () => Promise<void>;
    }
    let uploadInternal: UploadInternalState = {};

    const uploadClient = {
      config: baseClient.config,
      send: (command: unknown, sendOpts?: { abortSignal?: AbortSignal }) => {
        const cmdName = (command as { constructor?: { name?: string } } | null | undefined)
          ?.constructor?.name;
        if (
          command === uploadInternal.abortMultipartUploadCommand ||
          cmdName === "AbortMultipartUploadCommand"
        ) {
          if (!abortMpuPromise) {
            abortMpuPromise = baseClient.send(command as Parameters<S3Client["send"]>[0], sendOpts);
          }
          return abortMpuPromise;
        }
        if (sendOpts?.abortSignal) {
          if (sendOpts.abortSignal.aborted) {
            requestAbortController.abort(sendOpts.abortSignal.reason);
          } else {
            sendOpts.abortSignal.addEventListener(
              "abort",
              () => requestAbortController.abort(sendOpts.abortSignal?.reason),
              { once: true },
            );
          }
        }
        return baseClient.send(command as Parameters<S3Client["send"]>[0], {
          ...sendOpts,
          abortSignal: requestAbortController.signal,
        });
      },
    } as unknown as S3Client;

    const upload = new Upload({
      client: uploadClient,
      params,
      partSize,
      queueSize,
      leavePartsOnError: false,
    });
    uploadInternal = upload as unknown as UploadInternalState;
    if (onProgress) upload.on("httpUploadProgress", onProgress);

    let abortPromise: Promise<void> | null = null;
    const onAbort = () => {
      requestAbortController.abort(signal?.reason);
      destroyStreamSafely(body);
      abortPromise = upload.abort().catch(() => {});
    };
    signal?.addEventListener("abort", onAbort, { once: true });

    try {
      await upload.done();
      if (abortPromise) await abortPromise;
    } catch (err) {
      requestAbortController.abort(signal?.reason ?? err);
      destroyStreamSafely(body);
      if (abortPromise) await abortPromise;
      else if (signal?.aborted) await upload.abort().catch(() => {});

      const created = uploadInternal.createMultiPartPromise
        ? await uploadInternal.createMultiPartPromise.catch(() => null)
        : null;
      if (!uploadInternal.uploadId && created?.UploadId) {
        uploadInternal.uploadId = created.UploadId;
      }
      if (Array.isArray(uploadInternal.concurrentUploaders)) {
        await Promise.allSettled(uploadInternal.concurrentUploaders);
      }
      const getAbortMpuPromise = (): Promise<unknown> | null => abortMpuPromise;
      const inflightAbortMpu = getAbortMpuPromise();
      if (inflightAbortMpu) {
        await inflightAbortMpu.catch(() => {});
      } else if (
        typeof uploadInternal.markUploadAsAborted === "function" &&
        uploadInternal.uploadId &&
        uploadInternal.abortMultipartUploadCommand
      ) {
        await uploadInternal.markUploadAsAborted().catch(() => {});
      }
      throw err;
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
  }

  async head(key: string): Promise<StorageHeadResult | null> {
    try {
      const response = await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      if (response.DeleteMarker) {
        return null;
      }
      const rawSize = response.ContentLength !== undefined ? Number(response.ContentLength) : 0;
      return {
        size: Number.isFinite(rawSize) ? rawSize : 0,
        lastModified: response.LastModified,
        metadata: response.Metadata ?? {},
      };
    } catch (error) {
      if (isNotFound(error)) return null;
      throw error;
    }
  }

  /**
   * Downloads an object as a `Readable` stream with automatic HTTP Range resumption
   * verified by `IfMatch: <ETag>` so multi-hour restores survive transient TCP resets.
   */
  async download(key: string, { signal }: { signal?: AbortSignal } = {}): Promise<Readable> {
    if (signal?.aborted) {
      throw toAbortError(signal.reason, "S3 download aborted");
    }
    const initial = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: key }),
      { abortSignal: signal },
    );
    const initialBody = initial.Body as Readable;
    if (signal?.aborted) {
      initialBody?.destroy();
      throw toAbortError(signal.reason, "S3 download aborted");
    }
    const etag = initial.ETag;
    const contentLength =
      initial.ContentLength !== undefined ? Number(initial.ContentLength) : undefined;

    // If no ETag or unknown length, return direct stream
    if (!etag || contentLength === undefined || contentLength === 0) {
      if (signal && initialBody) {
        const onDirectAbort = () => {
          const abortErr = toAbortError(signal.reason, "S3 download aborted");
          if (!initialBody.destroyed) {
            initialBody.destroy(abortErr);
          }
        };
        signal.addEventListener("abort", onDirectAbort, { once: true });
        initialBody.once("close", () => {
          signal.removeEventListener("abort", onDirectAbort);
        });
      }
      return initialBody;
    }

    const out = new PassThrough({ highWaterMark: 512 * 1024 });
    let activeStream: Readable = initialBody;

    const onAbort = () => {
      const abortErr = toAbortError(signal?.reason, "S3 download aborted");
      activeStream.destroy();
      if (!out.destroyed) {
        out.destroy(abortErr);
      }
    };
    const cleanupSignal = () => {
      signal?.removeEventListener("abort", onAbort);
    };
    if (signal) {
      if (signal.aborted) {
        activeStream.destroy();
        out.destroy(toAbortError(signal.reason, "S3 download aborted"));
        return out;
      }
      signal.addEventListener("abort", onAbort, { once: true });
      out.once("close", cleanupSignal);
      out.once("end", cleanupSignal);
    }

    const pump = async (): Promise<void> => {
      let currentStream: Readable = initialBody;
      let bytesRead = 0;
      let attempt = 0;

      while (true) {
        try {
          for await (const chunk of currentStream) {
            if (signal?.aborted) {
              throw toAbortError(signal.reason, "S3 download aborted");
            }
            const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
            bytesRead += buf.length;
            if (!out.write(buf)) {
              await new Promise<void>((resolve, reject) => {
                const onDrain = () => {
                  cleanup();
                  resolve();
                };
                const onErr = (err: Error) => {
                  cleanup();
                  reject(err);
                };
                const onClose = () => {
                  cleanup();
                  resolve();
                };
                const onSignalAbort = () => {
                  cleanup();
                  reject(toAbortError(signal?.reason, "S3 download aborted"));
                };
                const cleanup = () => {
                  out.removeListener("drain", onDrain);
                  out.removeListener("error", onErr);
                  out.removeListener("close", onClose);
                  signal?.removeEventListener("abort", onSignalAbort);
                };
                if (signal?.aborted) {
                  onSignalAbort();
                  return;
                }
                out.once("drain", onDrain);
                out.once("error", onErr);
                out.once("close", onClose);
                signal?.addEventListener("abort", onSignalAbort, { once: true });
              });
            }
            if (out.destroyed) {
              currentStream.destroy();
              return;
            }
          }

          if (bytesRead >= contentLength) {
            out.end();
            return;
          }
          // Premature EOF before contentLength
          throw new Error(
            `Truncated S3 stream for ${key}: received ${bytesRead} of ${contentLength} bytes`,
          );
        } catch (err: unknown) {
          currentStream.destroy();
          if (bytesRead === contentLength && !signal?.aborted && !out.destroyed) {
            out.end();
            return;
          }
          if (
            signal?.aborted ||
            out.destroyed ||
            (err as { name?: string })?.name === "AbortError" ||
            (err as { nonRetriable?: boolean })?.nonRetriable ||
            isPreconditionFailed(err) ||
            isNotFound(err) ||
            attempt >= DOWNLOAD_MAX_RETRIES
          ) {
            out.destroy(err instanceof Error ? err : new Error(String(err)));
            return;
          }
          let resumedStream: Readable | null = null;
          while (!resumedStream) {
            attempt++;
            const backoffMs = Math.min(10_000, 500 * 2 ** (attempt - 1));
            await sleepWithAbort(backoffMs, signal);
            try {
              const resumed: GetObjectCommandOutput = await this.client.send(
                new GetObjectCommand({
                  Bucket: this.bucket,
                  Key: key,
                  Range: `bytes=${bytesRead}-`,
                  IfMatch: etag,
                }),
                { abortSignal: signal },
              );
              const expectedRemaining = contentLength - bytesRead;
              const status = resumed.$metadata?.httpStatusCode;
              if (status !== undefined && status !== 206) {
                (resumed.Body as Readable | undefined)?.destroy();
                throw Object.assign(
                  new Error(
                    `Invalid S3 Range resume status ${status} for ${key} (expected 206 Partial Content)`,
                  ),
                  { nonRetriable: true },
                );
              }
              if (
                resumed.ContentRange !== undefined &&
                !resumed.ContentRange.startsWith(`bytes ${bytesRead}-`)
              ) {
                (resumed.Body as Readable | undefined)?.destroy();
                throw Object.assign(
                  new Error(
                    `Invalid S3 Content-Range "${resumed.ContentRange}" for ${key} (expected offset ${bytesRead})`,
                  ),
                  { nonRetriable: true },
                );
              }
              if (resumed.ETag !== undefined && resumed.ETag !== etag) {
                (resumed.Body as Readable | undefined)?.destroy();
                throw Object.assign(new Error(`S3 ETag changed during Range resume for ${key}`), {
                  nonRetriable: true,
                });
              }
              if (
                resumed.ContentLength !== undefined &&
                Number(resumed.ContentLength) !== expectedRemaining
              ) {
                (resumed.Body as Readable | undefined)?.destroy();
                throw Object.assign(
                  new Error(
                    `Unexpected S3 Content-Length ${resumed.ContentLength} on Range resume for ${key} (expected ${expectedRemaining})`,
                  ),
                  { nonRetriable: true },
                );
              }
              resumedStream = resumed.Body as Readable;
            } catch (resumeErr: unknown) {
              if (
                signal?.aborted ||
                out.destroyed ||
                (resumeErr as { name?: string })?.name === "AbortError" ||
                (resumeErr as { nonRetriable?: boolean })?.nonRetriable ||
                isPreconditionFailed(resumeErr) ||
                isNotFound(resumeErr) ||
                attempt >= DOWNLOAD_MAX_RETRIES
              ) {
                out.destroy(resumeErr instanceof Error ? resumeErr : new Error(String(resumeErr)));
                return;
              }
            }
          }
          currentStream = resumedStream;
          activeStream = resumedStream;
          if (signal?.aborted) {
            activeStream.destroy();
            if (!out.destroyed) {
              out.destroy(toAbortError(signal.reason, "S3 download aborted"));
            }
            return;
          }
        }
      }
    };

    void pump().catch((err) => {
      if (!out.destroyed) {
        out.destroy(err instanceof Error ? err : new Error(String(err)));
      }
    });
    return out;
  }

  async putJson(key: string, value: unknown, options: PutJsonOptions = {}): Promise<void> {
    // Only apply WORM Object Lock to immutable keys (manifests/markers), NEVER mutable state/locks/probes
    const applyObjectLock =
      Boolean(this.objectLock?.enabled) &&
      (options.immutable === true || (options.immutable !== false && !isMutableControlKey(key)));

    const mode = applyObjectLock ? this.objectLock?.mode : undefined;
    const retainDate =
      applyObjectLock && this.objectLock?.retentionDays
        ? new Date(Date.now() + this.objectLock.retentionDays * 86_400_000)
        : undefined;
    const legalHold = applyObjectLock && this.objectLock?.legalHold ? "ON" : undefined;

    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: JSON.stringify(value, null, 2),
        ContentType: "application/json",
        ...(mode ? { ObjectLockMode: mode } : {}),
        ...(retainDate ? { ObjectLockRetainUntilDate: retainDate } : {}),
        ...(legalHold ? { ObjectLockLegalHoldStatus: legalHold } : {}),
      }),
    );
  }

  async getJson<T = unknown>(key: string): Promise<GetJsonResult<T> | null> {
    try {
      const response = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      const bodyStr = await (
        response.Body as { transformToString: () => Promise<string> }
      ).transformToString();
      const data = JSON.parse(bodyStr) as T;
      const etag = normalizeEtag(response.ETag);
      return { data, etag };
    } catch (error) {
      if (isNotFound(error)) return null;
      throw error;
    }
  }

  async *list(prefix: string): AsyncGenerator<StorageObject, void, unknown> {
    for await (const page of paginateListObjectsV2(
      { client: this.client },
      { Bucket: this.bucket, Prefix: prefix },
    )) {
      for (const object of page.Contents ?? []) {
        yield { key: object.Key!, size: Number(object.Size), lastModified: object.LastModified };
      }
    }
  }

  async remove(key: string): Promise<void> {
    await this.client.send(new DeleteObjectCommand({ Bucket: this.bucket, Key: key }));
  }

  async removeMany(keys: string[]): Promise<void> {
    if (!keys || keys.length === 0) return;
    const CHUNK_SIZE = 1000;
    const allErrors: Array<{ key?: string; code?: string; message?: string }> = [];
    for (let i = 0; i < keys.length; i += CHUNK_SIZE) {
      const chunk = keys.slice(i, i + CHUNK_SIZE);
      const response = await this.client.send(
        new DeleteObjectsCommand({
          Bucket: this.bucket,
          Delete: {
            Objects: chunk.map((Key) => ({ Key })),
            Quiet: false,
          },
        }),
      );
      if (response.Errors && response.Errors.length > 0) {
        for (const e of response.Errors) {
          allErrors.push({
            key: e.Key,
            code: e.Code,
            message: e.Message,
          });
        }
      }
    }
    if (allErrors.length > 0) {
      const sample = allErrors
        .slice(0, 10)
        .map((e) => `${e.key}: ${e.message || e.code}`)
        .join("; ");
      const suffix = allErrors.length > 10 ? ` (+${allErrors.length - 10} more)` : "";
      throw new S3BatchDeleteError(
        `S3 batch delete failed for ${allErrors.length} objects: ${sample}${suffix}`,
        allErrors,
        Boolean(this.objectLock?.enabled),
      );
    }
  }

  private async readLockObject(key: string): Promise<{
    exists: boolean;
    data: LockRecord | null;
    etag: string | null;
    corrupt: boolean;
    serverNow: number;
  }> {
    try {
      const response = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      const serverNow = this.getServerNow(response);
      const etag = normalizeEtag(response.ETag);
      const bodyStr = await (
        response.Body as { transformToString: () => Promise<string> }
      ).transformToString();
      try {
        const data = JSON.parse(bodyStr) as LockRecord;
        return { exists: true, data, etag, corrupt: false, serverNow };
      } catch {
        return { exists: true, data: null, etag, corrupt: true, serverNow };
      }
    } catch (error) {
      if (isNotFound(error)) {
        return {
          exists: false,
          data: null,
          etag: null,
          corrupt: false,
          serverNow: this.getServerNow(),
        };
      }
      throw error;
    }
  }

  async acquireLock(
    key: string,
    { holder, ttlMs }: { holder?: string; ttlMs: number },
  ): Promise<string | null> {
    const state = await this.readLockObject(key);
    if (state.exists && !state.corrupt && isLockActive(state.data, state.serverNow)) {
      return null;
    }

    let etag = state.etag;
    if (state.exists && !etag) {
      const headRes = await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      etag = normalizeEtag(headRes.ETag);
      if (!etag) {
        throw new Error(
          `Cannot take over S3 lock "${key}": S3 omitted ETag required for atomic CAS`,
        );
      }
    }

    const { token, record } = createLockRecord(holder, ttlMs, state.serverNow);

    try {
      const putParams: PutObjectCommandInput = {
        Bucket: this.bucket,
        Key: key,
        Body: JSON.stringify(record, null, 2),
        ContentType: "application/json",
        ...(state.exists ? { IfMatch: formatIfMatch(etag!) } : { IfNoneMatch: "*" }),
      };
      await this.client.send(new PutObjectCommand(putParams));
      return token;
    } catch (err) {
      if (isPreconditionFailed(err)) {
        return null;
      }
      throw err;
    }
  }

  async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
    const state = await this.readLockObject(key);
    if (state.exists && !state.corrupt && state.data?.token === token && state.etag) {
      const updated: LockRecord = {
        ...state.data,
        expiresAt: state.serverNow + ttlMs,
      };
      try {
        const putParams: PutObjectCommandInput = {
          Bucket: this.bucket,
          Key: key,
          Body: JSON.stringify(updated, null, 2),
          ContentType: "application/json",
          IfMatch: formatIfMatch(state.etag),
        };
        await this.client.send(new PutObjectCommand(putParams));
        return true;
      } catch (err) {
        if (isPreconditionFailed(err)) {
          return false;
        }
        throw err;
      }
    }
    return false;
  }

  async releaseLock(key: string, token: string): Promise<void> {
    const state = await this.readLockObject(key);
    if (state.exists && !state.corrupt && state.data?.token === token) {
      if (state.etag) {
        try {
          await this.client.send(
            new DeleteObjectCommand({
              Bucket: this.bucket,
              Key: key,
              IfMatch: formatIfMatch(state.etag),
            }),
          );
          return;
        } catch (err) {
          if (isPreconditionFailed(err)) return;
          if (!isNotImplemented(err)) throw err;
          // Fallback only if lock is still active per S3 server clock
          if (!isLockActive(state.data, state.serverNow)) {
            return;
          }
        }
      }
      await this.remove(key);
    }
  }
}

/**
 * Factory creating a FilesystemStorageClient.
 */
export function createFilesystemStorage({ basePath }: { basePath: string }): StorageClient {
  return new FilesystemStorageClient(basePath);
}

/**
 * Factory creating either an S3StorageClient or FilesystemStorageClient based on configuration.
 */
export function createStorage(options: StorageOptions): StorageClient {
  if (options.type === "filesystem" || (!options.bucket && options.path)) {
    return createFilesystemStorage({ basePath: options.path || "/tmp/mongodb-backup" });
  }
  return new S3StorageClient(options);
}

interface AwsErrorLike {
  name?: string;
  code?: string;
  $metadata?: { httpStatusCode?: number };
}

function isNotFound(error: unknown): boolean {
  const err = error as AwsErrorLike | null | undefined;
  return (
    err?.name === "NotFound" || err?.name === "NoSuchKey" || err?.$metadata?.httpStatusCode === 404
  );
}

function isPreconditionFailed(error: unknown): boolean {
  const err = error as AwsErrorLike | null | undefined;
  const status = err?.$metadata?.httpStatusCode;
  return (
    err?.name === "PreconditionFailed" ||
    err?.code === "PreconditionFailed" ||
    err?.name === "ConditionalRequestConflict" ||
    status === 412 ||
    status === 409
  );
}

function isNotImplemented(error: unknown): boolean {
  const err = error as AwsErrorLike | null | undefined;
  return err?.name === "NotImplemented" || err?.$metadata?.httpStatusCode === 501;
}
