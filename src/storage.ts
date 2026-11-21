import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
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

export interface LockRecord {
  token: string;
  holder?: string;
  acquiredAt: string;
  expiresAt: number;
}

export function createLockRecord(
  holder: string | undefined,
  ttlMs: number,
): { token: string; record: LockRecord } {
  const token = randomUUID();
  const now = Date.now();
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
 * Mirrors Percona PBM `storage.ComputePartSize`.
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
    const dest = path.resolve(this.resolvedBase, cleanKey);
    if (dest !== this.resolvedBase && !dest.startsWith(`${this.resolvedBase}${path.sep}`)) {
      throw new Error(`Storage key escapes base directory: "${key}"`);
    }
    return dest;
  }

  async upload(
    key: string,
    body: unknown,
    { signal, onProgress }: UploadOptions = {},
  ): Promise<void> {
    const dest = this.resolveSafePath(key);
    await fs.mkdir(path.dirname(dest), { recursive: true });
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
    } catch (err) {
      await handle.close().catch(() => {});
      await fs.rm(tmp, { force: true }).catch(() => {});
      throw err;
    }
  }

  async head(key: string): Promise<StorageHeadResult | null> {
    const file = this.resolveSafePath(key);
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
    const file = this.resolveSafePath(key);
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
    const dest = this.resolveSafePath(key);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    const tmp = `${dest}.tmp-${randomUUID()}`;
    const handle = await fs.open(tmp, "w", 0o600);
    try {
      await handle.writeFile(JSON.stringify(value, null, 2), "utf8");
      await handle.sync();
      await handle.close();
      await fs.rename(tmp, dest);
    } catch (err) {
      await handle.close().catch(() => {});
      await fs.rm(tmp, { force: true }).catch(() => {});
      throw err;
    }
  }

  async getJson<T = unknown>(key: string): Promise<GetJsonResult<T> | null> {
    const file = this.resolveSafePath(key);
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
        startDir = this.resolveSafePath(candidateDir);
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
          yield* walk(fullPath);
        } else if (entry.isFile() && !entry.name.includes(".tmp-")) {
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

  async remove(key: string): Promise<void> {
    await fs.rm(this.resolveSafePath(key), { force: true });
  }

  async removeMany(keys: string[]): Promise<void> {
    await Promise.all(keys.map((k) => fs.rm(this.resolveSafePath(k), { force: true })));
  }

  async acquireLock(
    key: string,
    { holder, ttlMs }: { holder?: string; ttlMs: number },
  ): Promise<string | null> {
    const dest = this.resolveSafePath(key);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    const { token, record } = createLockRecord(holder, ttlMs);
    const content = JSON.stringify(record, null, 2);

    try {
      await fs.writeFile(dest, content, { flag: "wx" });
      return token;
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }

    const now = Date.now();
    const current = await this.getJson<LockRecord>(key);
    if (isLockActive(current?.data, now)) {
      return null;
    }

    const tombstone = `${dest}.stale.${randomUUID()}`;
    try {
      await fs.rename(dest, tombstone);
      await fs.rm(tombstone, { force: true });
    } catch {
      return null;
    }

    try {
      await fs.writeFile(dest, content, { flag: "wx" });
      return token;
    } catch {
      return null;
    }
  }

  async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
    const dest = this.resolveSafePath(key);
    const current = await this.getJson<LockRecord>(key);
    if (!current?.data || current.data.token !== token) return false;

    const updated: LockRecord = {
      ...current.data,
      expiresAt: Date.now() + ttlMs,
    };

    const tmp = `${dest}.refresh.${randomUUID()}`;
    try {
      await fs.writeFile(tmp, JSON.stringify(updated, null, 2), "utf8");
      const verifyCurrent = await this.getJson<LockRecord>(key);
      if (!verifyCurrent?.data || verifyCurrent.data.token !== token) {
        await fs.rm(tmp, { force: true });
        return false;
      }
      await fs.rename(tmp, dest);
      return true;
    } catch {
      await fs.rm(tmp, { force: true }).catch(() => {});
      return false;
    }
  }

  async releaseLock(key: string, token: string): Promise<void> {
    const dest = this.resolveSafePath(key);
    const current = await this.getJson<LockRecord>(key);
    if (current?.data?.token === token) {
      const tombstone = `${dest}.release.${randomUUID()}`;
      try {
        await fs.rename(dest, tombstone);
        await fs.rm(tombstone, { force: true });
      } catch {
        // Ignored if already removed
      }
    }
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

    const upload = new Upload({
      client: this.client,
      params,
      partSize,
      queueSize,
      leavePartsOnError: false,
    });
    if (onProgress) upload.on("httpUploadProgress", onProgress);

    const onAbort = () => void upload.abort().catch(() => {});
    if (signal?.aborted) onAbort();
    else signal?.addEventListener("abort", onAbort, { once: true });

    try {
      await upload.done();
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
  }

  async head(key: string): Promise<StorageHeadResult | null> {
    try {
      const response = await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      return {
        size: Number(response.ContentLength),
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
    const initial = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: key }),
      { abortSignal: signal },
    );
    const initialBody = initial.Body as Readable;
    const etag = initial.ETag;
    const contentLength =
      initial.ContentLength !== undefined ? Number(initial.ContentLength) : undefined;

    // If no ETag or unknown length, return direct stream
    if (!etag || contentLength === undefined || contentLength === 0) {
      return initialBody;
    }

    const out = new PassThrough({ highWaterMark: 512 * 1024 });

    const pump = async (): Promise<void> => {
      let currentStream: Readable = initialBody;
      let bytesRead = 0;
      let attempt = 0;

      while (true) {
        try {
          for await (const chunk of currentStream) {
            if (signal?.aborted) {
              const abortErr = new Error("S3 download aborted");
              abortErr.name = "AbortError";
              throw abortErr;
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
                const cleanup = () => {
                  out.removeListener("drain", onDrain);
                  out.removeListener("error", onErr);
                  out.removeListener("close", onClose);
                };
                out.once("drain", onDrain);
                out.once("error", onErr);
                out.once("close", onClose);
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
          if (
            signal?.aborted ||
            out.destroyed ||
            (err as { name?: string })?.name === "AbortError" ||
            isPreconditionFailed(err) ||
            isNotFound(err) ||
            attempt >= DOWNLOAD_MAX_RETRIES
          ) {
            out.destroy(err instanceof Error ? err : new Error(String(err)));
            return;
          }
          attempt++;
          const backoffMs = Math.min(10_000, 500 * 2 ** (attempt - 1));
          await new Promise((r) => setTimeout(r, backoffMs));
          const resumed = await this.client.send(
            new GetObjectCommand({
              Bucket: this.bucket,
              Key: key,
              Range: `bytes=${bytesRead}-`,
              IfMatch: etag,
            }),
            { abortSignal: signal },
          );
          currentStream = resumed.Body as Readable;
        }
      }
    };

    void pump();
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
      const etag = response.ETag ? response.ETag.replace(/"/g, "") : null;
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
        const sample = response.Errors.map((e) => `${e.Key}: ${e.Message || e.Code}`).join("; ");
        throw new Error(`S3 batch delete failed for ${response.Errors.length} objects: ${sample}`);
      }
    }
  }

  async acquireLock(
    key: string,
    { holder, ttlMs }: { holder?: string; ttlMs: number },
  ): Promise<string | null> {
    const now = Date.now();
    const current = await this.getJson<LockRecord>(key);
    if (isLockActive(current?.data, now)) {
      return null;
    }

    const { token, record } = createLockRecord(holder, ttlMs);

    try {
      const putParams: PutObjectCommandInput = {
        Bucket: this.bucket,
        Key: key,
        Body: JSON.stringify(record, null, 2),
        ContentType: "application/json",
        ...(current?.etag ? { IfMatch: `"${current.etag}"` } : { IfNoneMatch: "*" }),
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
    const current = await this.getJson<LockRecord>(key);
    if (current?.data?.token === token && current.etag) {
      const updated: LockRecord = {
        ...current.data,
        expiresAt: Date.now() + ttlMs,
      };
      try {
        const putParams: PutObjectCommandInput = {
          Bucket: this.bucket,
          Key: key,
          Body: JSON.stringify(updated, null, 2),
          ContentType: "application/json",
          IfMatch: `"${current.etag}"`,
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
    const current = await this.getJson<LockRecord>(key);
    if (current?.data?.token === token) {
      if (current.etag) {
        try {
          await this.client.send(
            new DeleteObjectCommand({
              Bucket: this.bucket,
              Key: key,
              IfMatch: `"${current.etag}"`,
            }),
          );
          return;
        } catch (err) {
          if (isPreconditionFailed(err)) return;
          if (!isNotImplemented(err)) throw err;
          // Fallback only for S3-compatible stores that return 501 NotImplemented on conditional delete
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
