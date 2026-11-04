import { randomUUID } from "node:crypto";
import { Readable } from "node:stream";
import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
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

export function createStorage({
  endpoint,
  region,
  bucket,
  accessKeyId,
  secretAccessKey,
  forcePathStyle,
  partSizeMb = 32,
  queueSize = 2,
}: StorageOptions): StorageClient {
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

    async upload(key: string, body: unknown, { contentType = "application/octet-stream", signal, onProgress }: UploadOptions = {}): Promise<void> {
      const upload = new Upload({
        client,
        params: { Bucket: bucket, Key: key, Body: body as any, ContentType: contentType },
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
        const data = JSON.parse(await (response.Body as any).transformToString()) as T;
        const etag = response.ETag ? response.ETag.replace(/"/g, "") : null;
        return { data, etag };
      } catch (error) {
        if (isNotFound(error)) return null;
        throw error;
      }
    },

    async *list(prefix: string): AsyncGenerator<StorageObject, void, unknown> {
      for await (const page of paginateListObjectsV2({ client }, { Bucket: bucket, Prefix: prefix })) {
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
          throw new Error(`S3 batch delete failed for ${response.Errors.length} objects: ${sample}`);
        }
      }
    },

    async acquireLock(key: string, { holder, ttlMs }: { holder?: string; ttlMs: number }): Promise<string | null> {
      const now = Date.now();
      const current = await this.getJson<any>(key);
      const currentData = current?.data;
      if (currentData && currentData.expiresAt && currentData.expiresAt > now) {
        return null;
      }
      const token = randomUUID();
      const lockData = {
        token,
        holder,
        acquiredAt: new Date(now).toISOString(),
        expiresAt: now + ttlMs,
      };

      try {
        const putParams: any = {
          Bucket: bucket,
          Key: key,
          Body: JSON.stringify(lockData, null, 2),
          ContentType: "application/json",
        };
        // Use conditional write:
        // If current lock file existed and had an etag (stale lock takeover), require matching ETag.
        // If it did not exist, require IfNoneMatch: "*" to prevent concurrent creation race.
        if (current?.etag) {
          putParams.IfMatch = `"${current.etag}"`;
        } else {
          putParams.IfNoneMatch = "*";
        }
        await client.send(new PutObjectCommand(putParams));
        return token;
      } catch (err) {
        if (isPreconditionFailed(err)) {
          return null;
        }
        if (isConditionalHeaderUnsupported(err)) {
          await this.putJson(key, lockData);
          const readBack = await this.getJson<any>(key);
          return readBack?.data?.token === token ? token : null;
        }
        throw err;
      }
    },

    async refreshLock(key: string, token: string, ttlMs: number): Promise<boolean> {
      const current = await this.getJson<any>(key);
      if (current?.data?.token === token) {
        const updated = {
          ...current.data,
          expiresAt: Date.now() + ttlMs,
        };
        try {
          const putParams: any = {
            Bucket: bucket,
            Key: key,
            Body: JSON.stringify(updated, null, 2),
            ContentType: "application/json",
          };
          if (current.etag) {
            putParams.IfMatch = `"${current.etag}"`;
          }
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
      const current = await this.getJson<any>(key);
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

function isNotFound(error: any): boolean {
  return (
    error?.name === "NotFound" ||
    error?.name === "NoSuchKey" ||
    error?.$metadata?.httpStatusCode === 404
  );
}

function isPreconditionFailed(error: any): boolean {
  const status = error?.$metadata?.httpStatusCode;
  return (
    error?.name === "PreconditionFailed" ||
    error?.code === "PreconditionFailed" ||
    error?.name === "ConditionalRequestConflict" ||
    status === 412 ||
    status === 409
  );
}

function isConditionalHeaderUnsupported(error: any): boolean {
  const status = error?.$metadata?.httpStatusCode;
  return (
    error?.name === "NotImplemented" ||
    error?.code === "NotImplemented" ||
    error?.name === "InvalidArgument" ||
    status === 501
  );
}
