import path from "node:path";
import { MongoClient } from "mongodb";
import { databasePrefix, listBackups, readManifest, scopeOf } from "./catalog.js";
import { formatBytes, formatDate } from "./format.js";
import { isStale, readLock } from "./lock.js";
import { inspectMongoCluster, sanitizeSecrets } from "./mongo.js";
import {
  compareTimestamps,
  formatOplogTimestamp,
  listOplogSlices,
  parseOplogTimestamp,
  pitrStateKey,
} from "./oplog.js";
import {
  badge,
  renderBanner,
  renderKeyValue,
  renderPanel,
  renderTable,
  style,
  type KeyValueRow,
} from "./tui.js";
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
  let discoveredShards: string[] | undefined;
  const client = createMongoClient(config.mongo.uri);
  try {
    await client.connect();
    cluster = await inspectMongoCluster(client, config.mongo.uri, {
      readPreference: config.mongo.readPreference || "secondaryPreferred",
      createMongoClient,
    });
    if (cluster.role === "mongos") {
      try {
        const shards = await client.db("config").collection("shards").find({}).toArray();
        discoveredShards = shards.map((s) => String(s._id));
      } catch {
        // Best-effort shard discovery
      }
    }
  } catch (error) {
    cluster = {
      connected: false,
      error: sanitizeSecrets(error instanceof Error ? error.message : String(error)),
    };
  } finally {
    await client.close().catch(() => {});
  }

  // 2. Storage Health & Inventory
  let storageStat: StorageStatus;
  let recentBackups: SystemStatus["recentBackups"] = [];
  const scope = scopeOf(config);
  try {
    const allBackups = await listBackups(storage, scope);
    const completeBackups = allBackups.filter((b) => b.complete);
    recentBackups = completeBackups.slice(0, 5);
    const totalSizeBytes = completeBackups.reduce((acc, b) => acc + (b.size || 0), 0);
    const latest = completeBackups[0] || null;

    let latestDetails: StorageStatus["latestBackup"] = undefined;
    if (latest) {
      const manifest = await readManifest(storage, latest.key).catch(() => null);
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
      error: sanitizeSecrets(error instanceof Error ? error.message : String(error)),
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

      for (let i = 0; i < slices.length; i++) {
        const slice = slices[i]!;
        const sliceManifest =
          slice.manifest ??
          (typeof storage.getJson === "function"
            ? await readManifest(storage, slice.key).catch(() => null)
            : null);

        if (sliceManifest?.oplogWindow?.gapDetected) {
          timelineContinuous = false;
          gapWarning = `Gap flagged in slice ${slice.key}`;
          break;
        }

        if (i > 0) {
          const prevSlice = slices[i - 1]!;
          if (compareTimestamps(slice.startTs, prevSlice.endTs) > 0) {
            timelineContinuous = false;
            gapWarning = `Discontinuity detected between ${prevSlice.key} (end ${formatOplogTimestamp(prevSlice.endTs)}) and ${slice.key} (start ${formatOplogTimestamp(slice.startTs)})`;
            break;
          }
          if (sliceManifest?.oplogWindow?.previousEndTs) {
            try {
              const prevEndTs = parseOplogTimestamp(sliceManifest.oplogWindow.previousEndTs);
              if (
                prevEndTs &&
                compareTimestamps(prevEndTs, prevSlice.endTs) !== 0 &&
                compareTimestamps(slice.startTs, prevSlice.endTs) > 0
              ) {
                timelineContinuous = false;
                gapWarning = `Discontinuity detected between ${prevSlice.key} (end ${formatOplogTimestamp(prevSlice.endTs)}) and ${slice.key} (expected previousEndTs ${sliceManifest.oplogWindow.previousEndTs})`;
                break;
              }
            } catch {
              // Ignore malformed previousEndTs in individual slice manifest
            }
          }
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
    const rawLocal = await readLock(lockFile);
    const local =
      rawLocal && !(await isStale(lockFile, config.timeoutMs + 60_000)) ? rawLocal : null;

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
          shardsCount: discoveredShards?.length ?? cluster.hosts?.length,
          ...(discoveredShards ? { shards: discoveredShards } : {}),
        }
      : undefined,
  };
}

export function formatStatus(status: SystemStatus): string {
  const sections: string[] = [renderBanner("System Status")];

  // Cluster Section
  const clusterRows: KeyValueRow[] = [];
  if (status.cluster.connected) {
    const topo = status.cluster.setName
      ? `Replica Set [${status.cluster.setName}]`
      : status.cluster.role === "mongos"
        ? "Sharded Cluster (mongos)"
        : "Standalone Instance";
    clusterRows.push(
      { label: "Topology", value: topo, tone: "info" },
      { label: "Server Version", value: `MongoDB v${status.cluster.version}` },
      {
        label: "Node Address",
        value: `${status.cluster.me || "unknown"} [${status.cluster.role?.toUpperCase()}]`,
      },
    );
    if (status.cluster.primaryHost) {
      clusterRows.push({ label: "Primary Host", value: status.cluster.primaryHost });
    }
    clusterRows.push({
      label: "Read Preference",
      value: status.cluster.readPreference || "secondaryPreferred",
    });
    if (status.cluster.oplog) {
      clusterRows.push(
        status.cluster.oplog.available
          ? {
              label: "Oplog Window",
              value: `${status.cluster.oplog.oldestTs} -> ${status.cluster.oplog.newestTs} (${status.cluster.oplog.retentionHours} hours)`,
              tone: "success",
            }
          : { label: "Oplog Window", value: "Not Available / Empty", tone: "warn" },
      );
    }
  }
  const clusterLines = !status.cluster.connected
    ? [`  ${badge("Connection: FAILED", "error")} (${status.cluster.error || "unable to connect"})`]
    : renderKeyValue(clusterRows, { indent: 1 });
  sections.push(
    renderPanel(clusterLines, {
      title: "Cluster Topology & Health",
      tone: status.cluster.connected ? "brand" : "error",
    }),
  );

  // Storage Section
  const storageRows: KeyValueRow[] = [
    { label: "Backend Type", value: status.storage.type.toUpperCase() },
    { label: "Destination", value: status.storage.destination },
    {
      label: "Status",
      value: status.storage.accessible
        ? badge("OK", "success")
        : badge(`UNREACHABLE (${status.storage.error})`, "error"),
    },
    {
      label: "Total Backups",
      value: `${status.storage.totalBackups} (${formatBytes(status.storage.totalSizeBytes)})`,
    },
  ];
  if (status.storage.latestBackup) {
    const lat = status.storage.latestBackup;
    const flags = [
      lat.encrypted ? badge("encrypted", "success") : badge("unencrypted", "warn"),
      lat.oplog ? "oplog-consistent" : "snapshot-only",
    ].join(" · ");
    storageRows.push({
      label: "Latest Backup",
      value: `${formatDate(lat.createdAt)} (${formatBytes(lat.sizeBytes)})  ${flags}`,
    });
  }
  sections.push(
    renderPanel(renderKeyValue(storageRows, { indent: 1 }), {
      title: "Storage Destination",
      tone: status.storage.accessible ? "info" : "error",
    }),
  );

  // PITR Section
  const pitrRows: KeyValueRow[] = [
    {
      label: "Status",
      value: status.pitr.enabled
        ? badge(`Active (${status.pitr.flushIntervalSeconds}s flush interval)`, "success")
        : badge("Disabled", "muted"),
    },
    { label: "Archived Slices", value: String(status.pitr.slicesCount) },
  ];
  if (status.pitr.earliestTs && status.pitr.latestTs) {
    pitrRows.push(
      { label: "Timeline Window", value: `${status.pitr.earliestTs} -> ${status.pitr.latestTs}` },
      {
        label: "Contiguity",
        value: status.pitr.timelineContinuous
          ? badge("Continuous (no gaps detected)", "success")
          : badge(`WARNING: ${status.pitr.gapWarning}`, "warn"),
      },
    );
  } else if (status.pitr.gapWarning) {
    pitrRows.push({
      label: "Contiguity",
      value: badge(`WARNING: ${status.pitr.gapWarning}`, "warn"),
    });
  }
  sections.push(
    renderPanel(renderKeyValue(pitrRows, { indent: 1 }), {
      title: "Point-in-Time Recovery (PITR)",
      tone: !status.pitr.enabled ? "default" : status.pitr.timelineContinuous ? "success" : "warn",
    }),
  );

  // Running Operation & Engine/Compliance Sections
  const opRows: KeyValueRow[] = status.operation.running
    ? [
        { label: "Status", value: badge(`ACTIVE (${status.operation.lockType} lock)`, "info") },
        { label: "Holder", value: status.operation.holder || "unknown" },
        { label: "Started At", value: status.operation.acquiredAt || "unknown" },
        ...(status.operation.expiresAt
          ? [{ label: "Lease Expires", value: status.operation.expiresAt }]
          : []),
      ]
    : [{ label: "Status", value: badge("Idle (no active backup/restore operation)", "muted") }];
  sections.push(
    renderPanel(renderKeyValue(opRows, { indent: 1 }), {
      title: "Running Operations",
      tone: status.operation.running ? "info" : "default",
    }),
  );

  const engineRows: KeyValueRow[] = [
    { label: "Backup Engine", value: (status.engine || "logical").toUpperCase(), tone: "info" },
    {
      label: "S3 Object Lock",
      value: status.objectLock?.enabled
        ? badge(
            `ACTIVE (WORM ${status.objectLock.mode}, ${status.objectLock.retentionDays}d retention)`,
            "success",
          )
        : style.dim("Disabled"),
    },
    ...(status.sharding
      ? [
          {
            label: "Sharded Cluster",
            value: status.sharding.isSharded
              ? badge("Active (Multi-Shard Coordinated)", "info")
              : style.dim("Inactive"),
          },
        ]
      : []),
  ];
  sections.push(
    renderPanel(renderKeyValue(engineRows, { indent: 1 }), {
      title: "Engine & Compliance",
    }),
  );

  // Recent Backups Table
  if (status.recentBackups && status.recentBackups.length > 0) {
    const tbl = renderTable(
      [
        { header: "Backup Key", maxWidth: 56 },
        { header: "Created" },
        { header: "Size", align: "right" },
        { header: "Security" },
      ],
      status.recentBackups.map((b) => [
        b.key,
        formatDate(b.createdAt),
        formatBytes(b.size || 0),
        b.encrypted ? badge("AES-256", "success") : style.dim("plain"),
      ]),
      { indent: 1 },
    );
    sections.push(
      renderPanel(tbl.split("\n"), {
        title: "Recent Completed Backups",
        subtitle: `latest ${status.recentBackups.length}`,
      }),
    );
  }

  return sections.join("\n\n");
}
