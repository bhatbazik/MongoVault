import { keyId, signManifest } from "./crypto.js";
import type {
  ArchiveKeyOptions,
  BackupManifest,
  BackupScope,
  BackupSummary,
} from "./types/catalog.js";
import type { StorageClient, StorageObject } from "./types/storage.js";

export const MANIFEST_SUFFIX = ".manifest.json";
export const MANIFEST_VERSION = 2;

const ARCHIVE = /\.(?:archive\.gz|physical\.tar\.gz|cluster)(\.enc)?$/;
const STAMP =
  /(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(?:\.(\d{1,3}))?(?:-[A-Za-z0-9_-]+)?Z\.(?:archive|physical|cluster)/;
const RESERVED_DB_NAMES = new Set(["shards", "configsvr"]);

export function isClusterScope(database?: string | null): boolean {
  return !database || database === "*" || database === "all";
}

export function scopeOf(config: {
  storage: { prefix: string };
  mongo: { database: string };
}): BackupScope {
  return { prefix: config.storage.prefix, database: config.mongo.database };
}

export function databasePrefix({ prefix, database }: BackupScope): string {
  let target: string;
  if (isClusterScope(database) || database === "_cluster") {
    target = "_cluster";
  } else {
    if (
      database.includes("/") ||
      database.includes("\\") ||
      database.includes("..") ||
      database === "." ||
      RESERVED_DB_NAMES.has(database)
    ) {
      throw new Error(`Invalid or reserved database scope name: "${database}"`);
    }
    target = database;
  }
  return `${[prefix, target].filter(Boolean).join("/")}/`;
}

export function archiveKey(
  scope: BackupScope,
  date: Date,
  { encrypted, suffix }: ArchiveKeyOptions,
): string {
  const iso = date.toISOString();
  const day = iso.slice(0, 10);
  const ms = date.getUTCMilliseconds();
  const msPart = ms > 0 ? `.${String(ms).padStart(3, "0")}` : "";
  const cleanSuffix = suffix ? suffix.replace(/[^A-Za-z0-9_-]/g, "") : "";
  const suffixPart = cleanSuffix ? `-${cleanSuffix}` : "";
  const stamp = `${iso.slice(0, 19).replace(/[-:]/g, "")}${msPart}${suffixPart}Z`;
  return `${databasePrefix(scope)}${day}/${stamp}.archive.gz${encrypted ? ".enc" : ""}`;
}

export function manifestKey(key: string): string {
  return `${key}${MANIFEST_SUFFIX}`;
}

export function parseCreatedAt(key: string): Date | null {
  const match = STAMP.exec(key);
  if (!match) return null;
  const [, year, month, day, hour, minute, second, msStr] = match;
  const ms = msStr ? msStr.padEnd(3, "0") : "000";
  return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}.${ms}Z`);
}

export async function listBackups(
  storage: StorageClient,
  scope: BackupScope,
): Promise<BackupSummary[]> {
  const scopePrefix = databasePrefix(scope);
  const archives: StorageObject[] = [];
  const manifests = new Set<string>();

  for await (const object of storage.list(scopePrefix)) {
    const relKey = object.key.startsWith(scopePrefix)
      ? object.key.slice(scopePrefix.length)
      : object.key;
    if (
      relKey.startsWith("_pitr/") ||
      relKey.startsWith(".locks/") ||
      relKey.startsWith("_bridging/") ||
      relKey.includes("/_pitr/") ||
      relKey.includes("/_bridging/")
    ) {
      continue;
    }
    if (object.key.endsWith(MANIFEST_SUFFIX)) {
      manifests.add(object.key.slice(0, -MANIFEST_SUFFIX.length));
    } else if (ARCHIVE.test(object.key)) {
      archives.push(object);
    }
  }

  return archives
    .map((object) => {
      const parsed = parseCreatedAt(object.key);
      const modDate = object.lastModified ? new Date(object.lastModified) : null;
      const createdAt =
        parsed && !Number.isNaN(parsed.getTime())
          ? parsed
          : modDate && !Number.isNaN(modDate.getTime())
            ? modDate
            : new Date(0);
      return {
        key: object.key,
        size: object.size,
        createdAt,
        lastModified: object.lastModified,
        encrypted: object.key.endsWith(".enc"),
        complete: manifests.has(object.key),
      };
    })
    .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
}

export async function resolveBackup(
  storage: StorageClient,
  scope: BackupScope,
  ref: string = "latest",
): Promise<string> {
  if (ref !== "latest") return ref;
  const latest = (await listBackups(storage, scope)).find((backup) => backup.complete);
  if (!latest) {
    throw new Error(`No complete backups found under ${databasePrefix(scope)}`);
  }
  return latest.key;
}

export async function readManifest(
  storage: StorageClient,
  key: string,
): Promise<BackupManifest | null> {
  const result = await storage.getJson<BackupManifest | { data: BackupManifest }>(manifestKey(key));
  if (!result) return null;
  const raw =
    typeof result === "object" && "data" in result && ("etag" in result || !("key" in result))
      ? (result as { data: unknown }).data
      : result;
  if (raw === null || raw === undefined) return null;
  if (typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error(`Invalid manifest for ${key}: expected JSON object`);
  }
  const candidate = raw as Record<string, unknown>;
  if (typeof candidate.key !== "string" || candidate.key.length === 0) {
    throw new Error(`Invalid manifest for ${key}: missing required "key" field`);
  }
  if (candidate.version !== undefined) {
    if (
      typeof candidate.version !== "number" ||
      !Number.isInteger(candidate.version) ||
      candidate.version < 1 ||
      candidate.version > MANIFEST_VERSION
    ) {
      throw new Error(
        `Unsupported manifest version ${String(candidate.version)} for ${key} (supported: 1..${MANIFEST_VERSION})`,
      );
    }
  }
  return candidate as unknown as BackupManifest;
}

/**
 * Signs manifest if encryptionKey is provided and uploads it to storage.
 */
export async function writeManifest(
  storage: StorageClient,
  manifest: BackupManifest,
  encryptionKey?: Buffer | null,
): Promise<void> {
  if (encryptionKey) {
    const hmac = signManifest(encryptionKey, manifest);
    manifest.signature = {
      algorithm: "HMAC-SHA256",
      keyId: keyId(encryptionKey),
      hmac,
    };
    if (manifest.encryption) {
      manifest.encryption.manifestHmac = hmac;
    }
  }
  await storage.putJson(manifestKey(manifest.key), manifest, { immutable: true });
}

/**
 * Resolves the active key ring from config.
 */
export function resolveKeyRing(config: {
  encryptionKeyRing?: Buffer[];
  encryptionKey?: Buffer | null;
}): Buffer[] {
  return config.encryptionKeyRing || (config.encryptionKey ? [config.encryptionKey] : []);
}
