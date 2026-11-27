import os from "node:os";
import path from "node:path";
import { MongoClient } from "mongodb";
import { databasePrefix, listBackups, readManifest, scopeOf } from "./catalog.js";
import { readLock } from "./lock.js";
import { maskMongoUri } from "./mongo.js";
import { checkDatabaseTools } from "./tools.js";
import type { AppConfig } from "./types/config.js";
import type { StorageClient } from "./types/storage.js";

const MONGO_URI_RE = /mongodb(?:\+srv)?:\/\/[^\s"'`]+/gi;

function sanitizeValue<T>(value: T): T {
  if (typeof value === "string") {
    return value.replace(MONGO_URI_RE, (match) => maskMongoUri(match)) as unknown as T;
  }
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeValue(item)) as unknown as T;
  }
  if (value && typeof value === "object" && !(value instanceof Date)) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = sanitizeValue(v);
    }
    return out as T;
  }
  return value;
}

export interface DiagnosticBundleContext {
  config: AppConfig;
  storage: StorageClient;
  createMongoClient?: (uri: string) => MongoClient;
}

export interface DiagnosticBundle {
  generatedAt: string;
  system: {
    nodeVersion: string;
    platform: string;
    arch: string;
    cpus: number;
    totalMemoryBytes: number;
    freeMemoryBytes: number;
    processMemory: NodeJS.MemoryUsage;
    uptimeSeconds: number;
  };
  tools: {
    mongodump: { available: boolean; version?: string; error?: string };
    mongorestore: { available: boolean; version?: string; error?: string };
  };
  database: {
    connected: boolean;
    maskedUri: string;
    targetDatabase: string;
    readPreference?: string;
    buildInfo?: Record<string, unknown>;
    hello?: Record<string, unknown>;
    error?: string;
  };
  storage: {
    type: string;
    bucket: string;
    prefix: string;
    totalBackups: number;
    error?: string;
  };
  pitr?: {
    enabled: boolean;
    flushIntervalSeconds: number;
  };
  sharding?: {
    enabled: boolean;
  };
  recentManifests: Array<Record<string, unknown>>;
  activeLocks: {
    local?: Record<string, unknown> | null;
    distributed?: Record<string, unknown> | null;
    error?: string;
  };
}

export async function generateDiagnosticBundle({
  config,
  storage,
  createMongoClient = (u) => new MongoClient(u),
}: DiagnosticBundleContext): Promise<DiagnosticBundle> {
  const generatedAt = new Date().toISOString();

  // 1. System info
  const system = {
    nodeVersion: process.version,
    platform: process.platform,
    arch: process.arch,
    cpus: os.cpus().length,
    totalMemoryBytes: os.totalmem(),
    freeMemoryBytes: os.freemem(),
    processMemory: process.memoryUsage(),
    uptimeSeconds: Math.floor(process.uptime()),
  };

  // 2. Tools
  let tools: DiagnosticBundle["tools"];
  try {
    tools = sanitizeValue(await checkDatabaseTools());
  } catch (error) {
    const msg = sanitizeValue(error instanceof Error ? error.message : String(error));
    tools = {
      mongodump: { available: false, error: msg },
      mongorestore: { available: false, error: msg },
    };
  }

  // 3. Database
  let database: DiagnosticBundle["database"];
  const client = createMongoClient(config.mongo.uri);
  try {
    await client.connect();
    const admin = client.db("admin");
    const [hello, buildInfo] = await Promise.all([
      admin.command({ hello: 1 }).catch(() => admin.command({ isMaster: 1 })),
      admin.command({ buildInfo: 1 }).catch(() => ({})),
    ]);

    database = {
      connected: true,
      maskedUri: maskMongoUri(config.mongo.uri),
      targetDatabase: config.mongo.database,
      readPreference: config.mongo.readPreference,
      buildInfo: sanitizeValue(buildInfo),
      hello: sanitizeValue(hello),
    };
  } catch (error) {
    database = {
      connected: false,
      maskedUri: maskMongoUri(config.mongo.uri),
      targetDatabase: config.mongo.database,
      error: sanitizeValue(error instanceof Error ? error.message : String(error)),
    };
  } finally {
    await client.close().catch(() => {});
  }

  // 4. Storage & Backups
  const scope = scopeOf(config);
  let totalBackups = 0;
  let storageError: string | undefined;
  const recentManifests: Array<Record<string, unknown>> = [];
  try {
    const backups = await listBackups(storage, scope);
    const complete = backups.filter((b) => b.complete);
    totalBackups = complete.length;

    for (const b of complete.slice(0, 3)) {
      const manifest = await readManifest(storage, b.key).catch(() => null);
      if (manifest) {
        recentManifests.push(sanitizeValue(manifest as unknown as Record<string, unknown>));
      }
    }
  } catch (error) {
    storageError = sanitizeValue(error instanceof Error ? error.message : String(error));
  }

  const storageInfo: DiagnosticBundle["storage"] = {
    type: config.storage.type,
    bucket: config.storage.bucket,
    prefix: config.storage.prefix,
    totalBackups,
    ...(storageError ? { error: storageError } : {}),
  };

  // 5. Active Locks
  let localLock: Record<string, unknown> | null = null;
  let distributedLock: Record<string, unknown> | null = null;
  let lockError: string | undefined;
  try {
    const lockFile = path.join(config.workDir, ".backup.lock");
    localLock = sanitizeValue((await readLock(lockFile)) as Record<string, unknown> | null);
    if (typeof storage.getJson === "function") {
      const primaryLockKey = `${databasePrefix(scope)}.locks/backup.lock.json`;
      const legacyLockKey = `${config.storage.prefix}/locks/backup.lock`;
      const dist =
        (await storage.getJson(primaryLockKey).catch(() => null)) ??
        (await storage.getJson(legacyLockKey).catch(() => null));
      distributedLock = sanitizeValue((dist?.data as Record<string, unknown> | null) ?? null);
    }
  } catch (error) {
    lockError = sanitizeValue(error instanceof Error ? error.message : String(error));
  }

  return {
    generatedAt,
    system,
    tools,
    database,
    storage: storageInfo,
    pitr: {
      enabled: config.pitr?.enabled ?? false,
      flushIntervalSeconds: config.pitr?.flushIntervalSeconds ?? 60,
    },
    sharding: {
      enabled: Boolean(config.sharding?.enabled),
    },
    recentManifests,
    activeLocks: {
      local: localLock,
      distributed: distributedLock,
      ...(lockError ? { error: lockError } : {}),
    },
  };
}
