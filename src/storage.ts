import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
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
import type {
  GetJsonResult,
  StorageClient,
  StorageHeadResult,
  StorageObject,
  StorageOptions,
  UploadOptions,
} from "./types/storage.js";

export function createFilesystemStorage({ basePath }: { basePath: string }): StorageClient {
  const resolvedBase = path.resolve(basePath);

  return {
    bucket: resolvedBase,

    async upload(
      key: string,
      body: unknown,
      { signal, onProgress }: UploadOptions = {},
    ): Promise<void> {
      const dest = path.resolve(resolvedBase, key);
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
          await (pipeline as unknown as (...args: unknown[]) => Promise<void>)(body, writeStream, {
            signal,
          });
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
    },

    async head(key: string): Promise<StorageHeadResult | null> {
      const file = path.resolve(resolvedBase, key);
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
    },

    async download(key: string, { signal }: { signal?: AbortSignal } = {}): Promise<Readable> {
      const file = path.resolve(resolvedBase, key);
      const stream = fsSync.createReadStream(file);
      if (signal) {
        const onAbort = () => stream.destroy(signal.reason ?? new Error("Aborted"));
        if (signal.aborted) onAbort();
        else signal.addEventListener("abort", onAbort, { once: true });
        stream.once("close", () => signal.removeEventListener("abort", onAbort));
      }
      return stream;
    },

    async putJson(key: string, value: unknown): Promise<void> {
      const dest = path.resolve(resolvedBase, key);
      await fs.mkdir(path.dirname(dest), { recursive: true });
      const tmp = `${dest}.tmp-${randomUUID()}`;
      await fs.writeFile(tmp, JSON.stringify(value, null, 2), "utf8");
      await fs.rename(tmp, dest);
    },

    async getJson<T = unknown>(key: string): Promise<GetJsonResult<T> | null> {
      const file = path.resolve(resolvedBase, key);
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
    },

    async *list(prefix: string): AsyncGenerator<StorageObject, void, unknown> {
      const normPrefix = prefix.replace(/\\/g, "/");
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
            const relKey = path.relative(resolvedBase, fullPath).replace(/\\/g, "/");
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
      yield* walk(resolvedBase);
    },

    async remove(key: string): Promise<void> {
      await fs.rm(path.resolve(resolvedBase, key), { force: true });
    },

    async removeMany(keys: string[]): Promise<void> {
      await Promise.all(keys.map((k) => fs.rm(path.resolve(resolvedBase, k), { force: true })));
    },

    async acquireLock(
      key: string,
      { holder, ttlMs }: { holder?: string; ttlMs: number },
    ): Promise<string | null> {
      const now = Date.now();
      const current = await this.getJson<LockRecord>(key);
      const currentData = current?.data;
      if (currentData && currentData.expiresAt && currentData.expiresAt > now) {
        return null;
      }
      const token = randomUUID();
      const lockData: LockRecord = {
        token,
        holder,
        acquiredAt: new Date(now).toISOString(),
        expiresAt: now + ttlMs,
      };
      await this.putJson(key, lockData);
      return token;
    },

    async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
      const current = await this.getJson<LockRecord>(key);
      if (!current?.data || current.data.token !== token) return false;
      await this.putJson(key, { ...current.data, expiresAt: Date.now() + ttlMs });
      return true;
    },

    async releaseLock(key: string, token: string): Promise<void> {
      const current = await this.getJson<LockRecord>(key);
      if (current?.data?.token === token) {
        await this.remove(key);
      }
    },
  };
}

export function createStorage(options: StorageOptions): StorageClient {
  if (options.type === "filesystem" || (!options.bucket && options.path)) {
    return createFilesystemStorage({ basePath: options.path || "/tmp/mongodb-backup" });
  }

  const {
    endpoint,
    region,
    bucket = "mongodb-backups",
    accessKeyId = "",
    secretAccessKey = "",
    forcePathStyle,
    partSizeMb = 32,
    queueSize = 2,
  } = options;
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
  const client = new S3Client(clientConfig);

  const partSize = Math.max(5, partSizeMb) * 1024 * 1024;
  const concurrency = Math.max(1, queueSize);

  return {
    bucket,

    async upload(
      key: string,
      body: unknown,
      { contentType = "application/octet-stream", signal, onProgress }: UploadOptions = {},
    ): Promise<void> {
      const upload = new Upload({
        client,
        params: {
          Bucket: bucket,
          Key: key,
          Body: body as PutObjectCommandInput["Body"],
          ContentType: contentType,
        },
        partSize,
        queueSize: concurrency,
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
    },

    async head(key: string): Promise<StorageHeadResult | null> {
      try {
        const response = await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
        return {
          size: Number(response.ContentLength),
          lastModified: response.LastModified,
          metadata: response.Metadata ?? {},
        };
      } catch (error) {
        if (isNotFound(error)) return null;
        throw error;
      }
    },

    async download(key: string, { signal }: { signal?: AbortSignal } = {}): Promise<Readable> {
      const response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }), {
        abortSignal: signal,
      });
      return response.Body as Readable;
    },

    async putJson(key: string, value: unknown): Promise<void> {
      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: JSON.stringify(value, null, 2),
          ContentType: "application/json",
        }),
      );
    },

    async getJson<T = unknown>(key: string): Promise<GetJsonResult<T> | null> {
      try {
        const response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
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
    },

    async *list(prefix: string): AsyncGenerator<StorageObject, void, unknown> {
      for await (const page of paginateListObjectsV2(
        { client },
        { Bucket: bucket, Prefix: prefix },
      )) {
        for (const object of page.Contents ?? []) {
          yield { key: object.Key!, size: Number(object.Size), lastModified: object.LastModified };
        }
      }
    },

    async remove(key: string): Promise<void> {
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
    },

    async removeMany(keys: string[]): Promise<void> {
      if (!keys || keys.length === 0) return;
      const CHUNK_SIZE = 1000;
      for (let i = 0; i < keys.length; i += CHUNK_SIZE) {
        const chunk = keys.slice(i, i + CHUNK_SIZE);
        const response = await client.send(
          new DeleteObjectsCommand({
            Bucket: bucket,
            Delete: {
              Objects: chunk.map((Key) => ({ Key })),
              Quiet: false,
            },
          }),
        );
        if (response.Errors && response.Errors.length > 0) {
          const sample = response.Errors.map((e) => `${e.Key}: ${e.Message || e.Code}`).join("; ");
          throw new Error(
            `S3 batch delete failed for ${response.Errors.length} objects: ${sample}`,
          );
        }
      }
    },

    async acquireLock(
      key: string,
      { holder, ttlMs }: { holder?: string; ttlMs: number },
    ): Promise<string | null> {
      const now = Date.now();
      const current = await this.getJson<LockRecord>(key);
      const currentData = current?.data;
      if (currentData && currentData.expiresAt && currentData.expiresAt > now) {
        return null;
      }
      const token = randomUUID();
      const lockData: LockRecord = {
        token,
        holder,
        acquiredAt: new Date(now).toISOString(),
        expiresAt: now + ttlMs,
      };

      try {
        const putParams: PutObjectCommandInput = {
          Bucket: bucket,
          Key: key,
          Body: JSON.stringify(lockData, null, 2),
          ContentType: "application/json",
          ...(current?.etag ? { IfMatch: `"${current.etag}"` } : { IfNoneMatch: "*" }),
        };
        await client.send(new PutObjectCommand(putParams));
        return token;
      } catch (err) {
        if (isPreconditionFailed(err)) {
          return null;
        }
        if (isConditionalHeaderUnsupported(err)) {
          await this.putJson(key, lockData);
          const readBack = await this.getJson<LockRecord>(key);
          return readBack?.data?.token === token ? token : null;
        }
        throw err;
      }
    },

    async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
      const current = await this.getJson<LockRecord>(key);
      if (current?.data?.token === token) {
        const updated: LockRecord = {
          ...current.data,
          expiresAt: Date.now() + ttlMs,
        };
        try {
          const putParams: PutObjectCommandInput = {
            Bucket: bucket,
            Key: key,
            Body: JSON.stringify(updated, null, 2),
            ContentType: "application/json",
            ...(current.etag ? { IfMatch: `"${current.etag}"` } : {}),
          };
          await client.send(new PutObjectCommand(putParams));
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
    },

    async releaseLock(key: string, token: string): Promise<void> {
      const current = await this.getJson<LockRecord>(key);
      if (current?.data?.token === token) {
        if (current.etag) {
          try {
            await client.send(
              new DeleteObjectCommand({
                Bucket: bucket,
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
    },
  };
}

interface LockRecord {
  token: string;
  holder?: string;
  acquiredAt: string;
  expiresAt: number;
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
