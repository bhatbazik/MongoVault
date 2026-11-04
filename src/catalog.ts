import type { ArchiveKeyOptions, BackupManifest, BackupScope, BackupSummary } from "./types/catalog.js";
import type { StorageClient, StorageObject } from "./types/storage.js";

export const MANIFEST_SUFFIX = ".manifest.json";
export const MANIFEST_VERSION = 2;

const ARCHIVE = /\.archive\.gz(\.enc)?$/;
const STAMP = /(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z\.archive/;

export function isClusterScope(database?: string | null): boolean {
  return !database || database === "*" || database === "all";
}

export function scopeOf(config: { storage: { prefix: string }; mongo: { database: string } }): BackupScope {
  return { prefix: config.storage.prefix, database: config.mongo.database };
}

export function databasePrefix({ prefix, database }: BackupScope): string {
  const target = isClusterScope(database) ? "_cluster" : database;
  return `${[prefix, target].filter(Boolean).join("/")}/`;
}

export function archiveKey(scope: BackupScope, date: Date, { encrypted }: ArchiveKeyOptions): string {
  const iso = date.toISOString();
  const day = iso.slice(0, 10);
  const stamp = `${iso.slice(0, 19).replace(/[-:]/g, "")}Z`;
  return `${databasePrefix(scope)}${day}/${stamp}.archive.gz${encrypted ? ".enc" : ""}`;
}

export function manifestKey(key: string): string {
  return `${key}${MANIFEST_SUFFIX}`;
}

export function parseCreatedAt(key: string): Date | null {
  const match = STAMP.exec(key);
  if (!match) return null;
  const [, year, month, day, hour, minute, second] = match;
  return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`);
}

export async function listBackups(storage: StorageClient, scope: BackupScope): Promise<BackupSummary[]> {
  const archives: StorageObject[] = [];
  const manifests = new Set<string>();

  for await (const object of storage.list(databasePrefix(scope))) {
    if (object.key.endsWith(MANIFEST_SUFFIX)) {
      manifests.add(object.key.slice(0, -MANIFEST_SUFFIX.length));
    } else if (ARCHIVE.test(object.key)) {
      archives.push(object);
    }
  }

  return archives
    .map((object) => ({
      key: object.key,
      size: object.size,
      createdAt: parseCreatedAt(object.key) ?? (object.lastModified as Date),
      encrypted: object.key.endsWith(".enc"),
      complete: manifests.has(object.key),
    }))
    .sort((a, b) => (b.createdAt as unknown as number) - (a.createdAt as unknown as number));
}

export async function resolveBackup(storage: StorageClient, scope: BackupScope, ref: string = "latest"): Promise<string> {
  if (ref !== "latest") return ref;
  const latest = (await listBackups(storage, scope)).find((backup) => backup.complete);
  if (!latest) {
    throw new Error(`No complete backups found under ${databasePrefix(scope)}`);
  }
  return latest.key;
}

export async function readManifest(storage: StorageClient, key: string): Promise<BackupManifest | null> {
  const result = await storage.getJson<BackupManifest | { data: BackupManifest }>(manifestKey(key));
  if (!result) return null;
  return result && typeof result === "object" && "data" in result ? (result.data as unknown as BackupManifest) : (result as unknown as BackupManifest);
}
