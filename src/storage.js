import { randomUUID } from "node:crypto";
import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
  paginateListObjectsV2,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

export function createStorage({
  endpoint,
  region,
  bucket,
  accessKeyId,
  secretAccessKey,
  forcePathStyle,
  partSizeMb = 32,
  queueSize = 2,
}) {
  const clientConfig = {
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

    async upload(key, body, { contentType = "application/octet-stream", signal, onProgress } = {}) {
      const upload = new Upload({
        client,
        params: { Bucket: bucket, Key: key, Body: body, ContentType: contentType },
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

    async head(key) {
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

    async download(key, { signal } = {}) {
      const response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }), {
        abortSignal: signal,
      });
      return response.Body;
    },

    async putJson(key, value) {
      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: JSON.stringify(value, null, 2),
          ContentType: "application/json",
        }),
      );
    },

    async getJson(key) {
      try {
        const response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        return JSON.parse(await response.Body.transformToString());
      } catch (error) {
        if (isNotFound(error)) return null;
        throw error;
      }
    },

    async *list(prefix) {
      for await (const page of paginateListObjectsV2({ client }, { Bucket: bucket, Prefix: prefix })) {
        for (const object of page.Contents ?? []) {
          yield { key: object.Key, size: Number(object.Size), lastModified: object.LastModified };
        }
      }
    },

    async remove(key) {
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
    },

    async removeMany(keys) {
      if (!keys || keys.length === 0) return;
      const CHUNK_SIZE = 1000;
      for (let i = 0; i < keys.length; i += CHUNK_SIZE) {
        const chunk = keys.slice(i, i + CHUNK_SIZE);
        await client.send(
          new DeleteObjectsCommand({
            Bucket: bucket,
            Delete: {
              Objects: chunk.map((Key) => ({ Key })),
              Quiet: true,
            },
          }),
        );
      }
    },

    async acquireLock(key, { holder, ttlMs }) {
      const now = Date.now();
      const current = await this.getJson(key);
      if (current && current.expiresAt && current.expiresAt > now) {
        return null;
      }
      const token = randomUUID();
      const lockData = {
        token,
        holder,
        acquiredAt: new Date(now).toISOString(),
        expiresAt: now + ttlMs,
      };
      await this.putJson(key, lockData);
      const readBack = await this.getJson(key);
      if (readBack?.token !== token) {
        return null;
      }
      return token;
    },

    async refreshLock(key, token, ttlMs) {
      const current = await this.getJson(key);
      if (current?.token === token) {
        current.expiresAt = Date.now() + ttlMs;
        await this.putJson(key, current);
        return true;
      }
      return false;
    },

    async releaseLock(key, token) {
      const current = await this.getJson(key);
      if (current?.token === token) {
        await this.remove(key);
      }
    },
  };
}

function isNotFound(error) {
  return (
    error?.name === "NotFound" ||
    error?.name === "NoSuchKey" ||
    error?.$metadata?.httpStatusCode === 404
  );
}
