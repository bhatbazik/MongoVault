import os from "node:os";
import { MongoClient } from "mongodb";
import { listBackups, readManifest, scopeOf } from "./catalog.js";
import { readLock } from "./lock.js";
import { maskMongoUri } from "./mongo.js";
import { checkDatabaseTools } from "./tools.js";
import type { AppConfig } from "./types/config.js";
import type { StorageClient } from "./types/storage.js";

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
  };
  recentManifests: Array<Record<string, unknown>>;
  activeLocks: {
    local?: Record<string, unknown> | null;
    distributed?: Record<string, unknown> | null;
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
  const tools = await checkDatabaseTools();

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
      buildInfo,
      hello,
    };
  } catch (error) {
    database = {
      connected: false,
      maskedUri: maskMongoUri(config.mongo.uri),
      targetDatabase: config.mongo.database,
      error: error instanceof Error ? error.message : String(error),
    };
  } finally {
    await client.close().catch(() => {});
  }

  // 4. Storage & Backups
  const scope = scopeOf(config);
  let totalBackups = 0;
  const recentManifests: Array<Record<string, unknown>> = [];
  try {
    const backups = await listBackups(storage, scope);
    const complete = backups.filter((b) => b.complete);
    totalBackups = complete.length;

    for (const b of complete.slice(0, 3)) {
      const manifest = await readManifest(storage, b.key);
      if (manifest) {
        recentManifests.push(manifest as unknown as Record<string, unknown>);
      }
    }
  } catch {}

  const storageInfo = {
    type: config.storage.type,
    bucket: config.storage.bucket,
    prefix: config.storage.prefix,
    totalBackups,
  };

  // 5. Active Locks
  let localLock: Record<string, unknown> | null = null;
  let distributedLock: Record<string, unknown> | null = null;
  try {
    const lockFile = `${config.workDir}/.backup.lock`;
    localLock = (await readLock(lockFile)) as Record<string, unknown> | null;
    if (typeof storage.getJson === "function") {
      const dist = await storage.getJson(`${config.storage.prefix}/locks/backup.lock`);
      distributedLock = dist?.data as Record<string, unknown> | null;
    }
  } catch {}

  return {
    generatedAt,
    system,
    tools,
    database,
    storage: storageInfo,
    recentManifests,
    activeLocks: {
      local: localLock,
      distributed: distributedLock,
    },
  };
}
