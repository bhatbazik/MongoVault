import type { Readable } from "node:stream";
export type StorageType = "s3" | "filesystem";

export interface StorageOptions {
  type?: StorageType;
  path?: string;
  endpoint?: string;
  region?: string;
  bucket?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  forcePathStyle?: boolean;
  partSizeMb?: number;
  queueSize?: number;
}

export interface UploadOptions {
  contentType?: string;
  signal?: AbortSignal;
  onProgress?: (progress: { loaded?: number; total?: number }) => void;
}

export interface StorageHeadResult {
  size: number;
  lastModified?: Date;
  metadata: Record<string, string>;
}

export interface StorageObject {
  key: string;
  size: number;
  lastModified?: Date;
}

export interface GetJsonResult<T = unknown> {
  data: T;
  etag: string | null;
}

export interface StorageClient {
  bucket: string;
  upload: (
    key: string,
    body: Readable | Buffer | Uint8Array | unknown,
    options?: UploadOptions,
  ) => Promise<void>;
  head: (key: string) => Promise<StorageHeadResult | null>;
  download: (key: string, options?: { signal?: AbortSignal }) => Promise<Readable>;
  putJson: (key: string, value: unknown) => Promise<void>;
  getJson: <T = unknown>(key: string) => Promise<GetJsonResult<T> | null>;
  list: (prefix: string) => AsyncGenerator<StorageObject, void, unknown>;
  remove: (key: string) => Promise<void>;
  removeMany?: (keys: string[]) => Promise<void>;
  acquireLock?: (
    key: string,
    options: { holder?: string; ttlMs: number },
  ) => Promise<string | null>;
  refreshLock?: (key: string, token: string, ttlMs: number) => Promise<boolean>;
  releaseLock?: (key: string, token: string) => Promise<void>;
}
