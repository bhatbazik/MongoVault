import type { Readable } from "node:stream";
export type StorageType = "s3" | "filesystem";

export interface ObjectLockOptions {
  enabled?: boolean;
  mode?: "GOVERNANCE" | "COMPLIANCE";
  retentionDays?: number;
  legalHold?: boolean;
}

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
  objectLock?: ObjectLockOptions;
}

export interface UploadOptions {
  contentType?: string;
  signal?: AbortSignal;
  onProgress?: (progress: { loaded?: number; total?: number }) => void;
  /** Expected total byte size hint used to auto-scale S3 multipart `partSize` under the 10,000-part limit. */
  expectedSizeBytes?: number;
  objectLockMode?: "GOVERNANCE" | "COMPLIANCE";
  objectLockRetainUntilDate?: Date;
  objectLockLegalHoldStatus?: "ON" | "OFF";
}

export interface PutJsonOptions {
  /** Apply WORM Object Lock headers only when true (manifests/immutable markers). Never set for mutable state/locks/probes. */
  immutable?: boolean;
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
  putJson: (key: string, value: unknown, options?: PutJsonOptions) => Promise<void>;
  getJson: <T = unknown>(key: string) => Promise<GetJsonResult<T> | null>;
  list: (
    prefix: string,
    options?: { signal?: AbortSignal },
  ) => AsyncGenerator<StorageObject, void, unknown>;
  remove: (key: string) => Promise<void>;
  removeMany?: (keys: string[]) => Promise<void>;
  cleanupStaleUploads?: (prefix: string, maxAgeMs: number) => Promise<number>;
  acquireLock?: (
    key: string,
    options: { holder?: string; ttlMs: number },
  ) => Promise<string | null>;
  refreshLock?: (key: string, token: string, ttlMs: number) => Promise<boolean>;
  releaseLock?: (key: string, token: string) => Promise<void>;
}
