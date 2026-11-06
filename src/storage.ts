import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";
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
  StorageClient,
  StorageHeadResult,
  StorageObject,
  StorageOptions,
  UploadOptions,
} from "./types/storage.js";

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
 * StorageClient implementation backed by the local filesystem.
 */
export class FilesystemStorageClient implements StorageClient {
  readonly bucket: string;
  private readonly resolvedBase: string;

  constructor(basePath: string) {
    this.resolvedBase = path.resolve(basePath);
    this.bucket = this.resolvedBase;
  }

  async upload(
    key: string,
    body: unknown,
    { signal, onProgress }: UploadOptions = {},
  ): Promise<void> {
    const dest = path.resolve(this.resolvedBase, key);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    const tmp = `${dest}.tmp-${randomUUID()}`;
    const handle = await fs.open(tmp, "w", 0o600);
    const writeStream = handle.createWriteStream();

    try {
      if (body && typeof (body as NodeJS.ReadableStream).pipe === "function") {
        let loaded = 0;
        if (onProgress) {
          (body as NodeJS.ReadableStream).on("data", (chunk: Buffer) => {
            loaded += chunk.length;
            onProgress({ loaded });
          });
        }
        await runStreamPipeline([body as NodeJS.ReadableStream, writeStream], { signal });
      } else if (Buffer.isBuffer(body) || typeof body === "string") {
        await handle.writeFile(body);
        if (onProgress) onProgress({ loaded: Buffer.byteLength(body) });
      }
      await handle.close();
      await fs.rename(tmp, dest);
    } catch (err) {
      await handle.close().catch(() => {});
      await fs.rm(tmp, { force: true }).catch(() => {});
      throw err;
    }
  }

  async head(key: string): Promise<StorageHeadResult | null> {
    const file = path.resolve(this.resolvedBase, key);
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
    const file = path.resolve(this.resolvedBase, key);
    const stream = fsSync.createReadStream(file);
    if (signal) {
      const onAbort = () => stream.destroy(signal.reason ?? new Error("Aborted"));
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
      stream.once("close", () => signal.removeEventListener("abort", onAbort));
    }
    return stream;
  }

  async putJson(key: string, value: unknown): Promise<void> {
    const dest = path.resolve(this.resolvedBase, key);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    const tmp = `${dest}.tmp-${randomUUID()}`;
    await fs.writeFile(tmp, JSON.stringify(value, null, 2), "utf8");
    await fs.rename(tmp, dest);
  }

  async getJson<T = unknown>(key: string): Promise<GetJsonResult<T> | null> {
    const file = path.resolve(this.resolvedBase, key);
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
    const normPrefix = prefix.replace(/\\/g, "/");
    const basePath = this.resolvedBase;

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

    yield* walk(this.resolvedBase);
  }

  async remove(key: string): Promise<void> {
    await fs.rm(path.resolve(this.resolvedBase, key), { force: true });
  }

  async removeMany(keys: string[]): Promise<void> {
    await Promise.all(keys.map((k) => fs.rm(path.resolve(this.resolvedBase, k), { force: true })));
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
    await this.putJson(key, record);
    return token;
  }

  async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
    const current = await this.getJson<LockRecord>(key);
    if (!current?.data || current.data.token !== token) return false;
    await this.putJson(key, { ...current.data, expiresAt: Date.now() + ttlMs });
    return true;
  }

  async releaseLock(key: string, token: string): Promise<void> {
    const current = await this.getJson<LockRecord>(key);
    if (current?.data?.token === token) {
      await this.remove(key);
    }
  }
}

/**
 * StorageClient implementation backed by AWS S3 or S3-compatible object storage (e.g. Cloudflare R2, MinIO).
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
      partSize: this.partSize,
      queueSize: this.concurrency,
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

  async download(key: string, { signal }: { signal?: AbortSignal } = {}): Promise<Readable> {
    const response = await this.client.send(
      new GetObjectCommand({ Bucket: this.bucket, Key: key }),
      {
        abortSignal: signal,
      },
    );
    return response.Body as Readable;
  }

  async putJson(key: string, value: unknown): Promise<void> {
    const mode = this.objectLock?.enabled ? this.objectLock.mode : undefined;
    const retainDate =
      this.objectLock?.enabled && this.objectLock.retentionDays
        ? new Date(Date.now() + this.objectLock.retentionDays * 86_400_000)
        : undefined;
    const legalHold = this.objectLock?.legalHold ? "ON" : undefined;

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
      if (isConditionalHeaderUnsupported(err)) {
        await this.putJson(key, record);
        const readBack = await this.getJson<LockRecord>(key);
        return readBack?.data?.token === token ? token : null;
      }
      throw err;
    }
  }

  async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
    const current = await this.getJson<LockRecord>(key);
    if (current?.data?.token === token) {
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
          ...(current.etag ? { IfMatch: `"${current.etag}"` } : {}),
        };
        await this.client.send(new PutObjectCommand(putParams));
        return true;
      } catch (err) {
        if (isConditionalHeaderUnsupported(err)) {
          await this.putJson(key, updated);
          return true;
        }
        return false;
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
          if (!isConditionalHeaderUnsupported(err)) throw err;
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

function isConditionalHeaderUnsupported(error: unknown): boolean {
  const err = error as AwsErrorLike | null | undefined;
  const status = err?.$metadata?.httpStatusCode;
  return (
    err?.name === "NotImplemented" ||
    err?.code === "NotImplemented" ||
    err?.name === "InvalidArgument" ||
    status === 501
  );
}
