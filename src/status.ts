import path from "node:path";
import { MongoClient } from "mongodb";
import { databasePrefix, listBackups, readManifest, scopeOf } from "./catalog.js";
import { formatBytes, formatDate } from "./format.js";
import { readLock } from "./lock.js";
import { inspectMongoCluster } from "./mongo.js";
import { formatOplogTimestamp, listOplogSlices, pitrStateKey } from "./oplog.js";
import type { AppConfig } from "./types/config.js";
import type { Logger } from "./types/logger.js";
import type {
  ClusterStatus,
  OperationLockStatus,
  PitrStatus,
  StorageStatus,
  SystemStatus,
} from "./types/status.js";
import type { StorageClient } from "./types/storage.js";

export interface StatusContext {
  config: AppConfig;
  storage: StorageClient;
  logger?: Logger;
  createMongoClient?: (uri: string) => MongoClient;
}

export async function getStatus({
  config,
  storage,
  createMongoClient = (u) => new MongoClient(u),
}: StatusContext): Promise<SystemStatus> {
  const timestamp = new Date().toISOString();

  // 1. MongoDB Cluster Status & Topology
  let cluster: ClusterStatus;
  const client = createMongoClient(config.mongo.uri);
  try {
    await client.connect();
    cluster = await inspectMongoCluster(client, config.mongo.uri, {
      readPreference: config.mongo.readPreference || "secondaryPreferred",
      createMongoClient,
    });
  } catch (error) {
    cluster = {
      connected: false,
      error: error instanceof Error ? error.message : String(error),
    };
  } finally {
    await client.close().catch(() => {});
  }

  // 2. Storage Health & Inventory
  let storageStat: StorageStatus;
  const scope = scopeOf(config);
  try {
    const allBackups = await listBackups(storage, scope);
    const completeBackups = allBackups.filter((b) => b.complete);
    const totalSizeBytes = completeBackups.reduce((acc, b) => acc + (b.size || 0), 0);
    const latest = completeBackups[0] || null;

    let latestDetails: StorageStatus["latestBackup"] = undefined;
    if (latest) {
      const manifest = await readManifest(storage, latest.key);
      latestDetails = {
        key: latest.key,
        createdAt: manifest?.createdAt || new Date(latest.createdAt).toISOString(),
        sizeBytes: latest.size || 0,
        encrypted: Boolean(manifest?.encryption),
        oplog: Boolean(manifest?.oplog),
      };
    }

    const destination =
      config.storage.type === "filesystem"
        ? `file://${config.storage.bucket}`
        : `s3://${config.storage.bucket}/${config.storage.prefix}`;

    storageStat = {
      type: config.storage.type,
      destination,
      prefix: config.storage.prefix,
      accessible: true,
      totalBackups: completeBackups.length,
      totalSizeBytes,
      latestBackup: latestDetails,
    };
  } catch (error) {
    storageStat = {
      type: config.storage.type,
      destination: config.storage.bucket,
      prefix: config.storage.prefix,
      accessible: false,
      totalBackups: 0,
      totalSizeBytes: 0,
      error: error instanceof Error ? error.message : String(error),
    };
  }

  // 3. PITR Continuous Slicing Timeline
  let pitr: PitrStatus;
  try {
    const slices = (await listOplogSlices(storage, scope)).filter((s) => s.complete);
    let earliestTs: string | undefined = undefined;
    let latestTs: string | undefined = undefined;
    let timelineContinuous = true;
    let gapWarning: string | undefined = undefined;

    if (slices.length > 0) {
      const first = slices[0];
      const last = slices[slices.length - 1];
      if (first && last) {
        earliestTs = formatOplogTimestamp(first.startTs);
        latestTs = formatOplogTimestamp(last.endTs);
      }

      for (const slice of slices) {
        if (slice?.manifest?.oplogWindow?.gapDetected) {
          timelineContinuous = false;
          gapWarning = `Gap flagged in slice ${slice.key}`;
          break;
        }
      }

      if (timelineContinuous && typeof storage.getJson === "function") {
        const pitrState = await storage
          .getJson<{ gapDetected?: boolean; lastSliceKey?: string }>(pitrStateKey(scope))
          .catch(() => null);
        if (pitrState?.data?.gapDetected) {
          timelineContinuous = false;
          gapWarning = `Gap flagged in ${pitrState.data.lastSliceKey || "PITR state"}`;
        }
      }
    }

    pitr = {
      enabled: config.pitr.enabled,
      activeArchiver: config.pitr.enabled,
      flushIntervalSeconds: config.pitr.flushIntervalSeconds,
      slicesCount: slices.length,
      earliestTs,
      latestTs,
      timelineContinuous,
      gapWarning,
    };
  } catch (error) {
    pitr = {
      enabled: config.pitr.enabled,
      activeArchiver: false,
      flushIntervalSeconds: config.pitr.flushIntervalSeconds,
      slicesCount: 0,
      timelineContinuous: false,
      gapWarning: error instanceof Error ? error.message : String(error),
    };
  }

  // 4. Active Operations & Lock Status
  let operation: OperationLockStatus = { running: false };
  try {
    const lockFile = path.join(config.workDir, ".backup.lock");
    const local = await readLock(lockFile);

    let distributed: {
      data?: { holder?: string; acquiredAt?: string; expiresAt?: number };
    } | null = null;
    if (typeof storage.getJson === "function") {
      const primaryLockKey = `${databasePrefix(scope)}.locks/backup.lock.json`;
      const legacyLockKey = `${config.storage.prefix}/locks/backup.lock`;
      distributed =
        (await storage
          .getJson<{ holder?: string; acquiredAt?: string; expiresAt?: number }>(primaryLockKey)
          .catch(() => null)) ??
        (await storage
          .getJson<{ holder?: string; acquiredAt?: string; expiresAt?: number }>(legacyLockKey)
          .catch(() => null));
    }

    const distData = distributed?.data;
    const now = Date.now();
    const isDistActive = Boolean(distData?.expiresAt && distData.expiresAt > now);

    if (local || isDistActive) {
      operation = {
        running: true,
        lockType: local && isDistActive ? "hybrid" : isDistActive ? "distributed" : "local",
        holder: distData?.holder || (local ? `pid:${local.pid}` : undefined),
        acquiredAt: distData?.acquiredAt || local?.acquiredAt,
        expiresAt: distData?.expiresAt ? new Date(distData.expiresAt).toISOString() : undefined,
      };
    }
  } catch {
    operation = { running: false };
  }

  // 5. Recent Backups List (last 5)
  let recentBackups: SystemStatus["recentBackups"] = [];
  try {
    const backups = await listBackups(storage, scope);
    recentBackups = backups.filter((b) => b.complete).slice(0, 5);
  } catch {}

  return {
    timestamp,
    cluster,
    storage: storageStat,
    pitr,
    operation,
    recentBackups,
    engine: config.engine || "logical",
    objectLock: config.storage.objectLock
      ? {
          enabled: Boolean(config.storage.objectLock.enabled),
          mode: config.storage.objectLock.mode,
          retentionDays: config.storage.objectLock.retentionDays,
        }
      : undefined,
    sharding: config.sharding
      ? {
          isSharded: cluster.role === "mongos",
          shardsCount: cluster.hosts?.length,
        }
      : undefined,
  };
}

export function formatStatus(status: SystemStatus): string {
  const lines: string[] = [];

  // Cluster Section
  lines.push("Cluster Topology & Health:");
  lines.push("===========================");
  if (!status.cluster.connected) {
    lines.push(`  Connection: FAILED (${status.cluster.error || "unable to connect"})`);
  } else {
    const topo = status.cluster.setName
      ? `Replica Set [${status.cluster.setName}]`
      : status.cluster.role === "mongos"
        ? "Sharded Cluster (mongos)"
        : "Standalone Instance";
    lines.push(`  Topology:        ${topo}`);
    lines.push(`  Server Version:  MongoDB v${status.cluster.version}`);
    lines.push(
      `  Node Address:    ${status.cluster.me || "unknown"} [${status.cluster.role?.toUpperCase()}]`,
    );
    if (status.cluster.primaryHost) {
      lines.push(`  Primary Host:    ${status.cluster.primaryHost}`);
    }
    lines.push(`  Read Preference: ${status.cluster.readPreference}`);
    if (status.cluster.oplog) {
      if (status.cluster.oplog.available) {
        lines.push(
          `  Oplog Window:    ${status.cluster.oplog.oldestTs} -> ${status.cluster.oplog.newestTs} (${status.cluster.oplog.retentionHours} hours)`,
        );
      } else {
        lines.push(`  Oplog Window:    Not Available / Empty`);
      }
    }
  }
  lines.push("");

  // Storage Section
  lines.push("Storage Destination:");
  lines.push("====================");
  lines.push(`  Backend Type:    ${status.storage.type.toUpperCase()}`);
  lines.push(`  Destination:     ${status.storage.destination}`);
  lines.push(
    `  Status:          ${status.storage.accessible ? "OK" : `UNREACHABLE (${status.storage.error})`}`,
  );
  lines.push(
    `  Total Backups:   ${status.storage.totalBackups} (${formatBytes(status.storage.totalSizeBytes)})`,
  );
  if (status.storage.latestBackup) {
    const lat = status.storage.latestBackup;
    const flags = [
      lat.encrypted ? "encrypted" : "unencrypted",
      lat.oplog ? "oplog-consistent" : "snapshot-only",
    ].join(", ");
    lines.push(
      `  Latest Backup:   ${formatDate(lat.createdAt)} (${formatBytes(lat.sizeBytes)}, ${flags})`,
    );
  }
  lines.push("");

  // PITR Section
  lines.push("Point-in-Time Recovery (PITR):");
  lines.push("==============================");
  lines.push(
    `  Status:          ${status.pitr.enabled ? `Active (${status.pitr.flushIntervalSeconds}s flush interval)` : "Disabled"}`,
  );
  lines.push(`  Archived Slices: ${status.pitr.slicesCount}`);
  if (status.pitr.earliestTs && status.pitr.latestTs) {
    lines.push(`  Timeline Window: ${status.pitr.earliestTs} -> ${status.pitr.latestTs}`);
    lines.push(
      `  Contiguity:      ${status.pitr.timelineContinuous ? "Continuous (no gaps detected)" : `WARNING: ${status.pitr.gapWarning}`}`,
    );
  }
  lines.push("");

  // Running Operation Section
  lines.push("Running Operations:");
  lines.push("===================");
  if (status.operation.running) {
    lines.push(`  Status:          ACTIVE (${status.operation.lockType} lock)`);
    lines.push(`  Holder:          ${status.operation.holder || "unknown"}`);
    lines.push(`  Started At:      ${status.operation.acquiredAt || "unknown"}`);
    if (status.operation.expiresAt) {
      lines.push(`  Lease Expires:   ${status.operation.expiresAt}`);
    }
  } else {
    lines.push("  Status:          Idle (no active backup/restore operation)");
  }
  lines.push("");

  // Engine & Security Section
  lines.push("Engine & Compliance:");
  lines.push("====================");
  lines.push(`  Backup Engine:   ${(status.engine || "logical").toUpperCase()}`);
  if (status.objectLock?.enabled) {
    lines.push(
      `  S3 Object Lock:  ACTIVE (WORM ${status.objectLock.mode}, ${status.objectLock.retentionDays} days retention)`,
    );
  } else {
    lines.push("  S3 Object Lock:  Disabled");
  }
  if (status.sharding) {
    lines.push(
      `  Sharded Cluster: ${status.sharding.isSharded ? "Active (Multi-Shard Coordinated)" : "Inactive"}`,
    );
  }

  // Recent Backups Table
  if (status.recentBackups && status.recentBackups.length > 0) {
    lines.push("");
    lines.push("Recent Completed Backups:");
    lines.push("=========================");
    for (const b of status.recentBackups) {
      lines.push(`  • ${b.key} | ${formatDate(b.createdAt)} | ${formatBytes(b.size || 0)}`);
    }
  }

  return lines.join("\n");
}
