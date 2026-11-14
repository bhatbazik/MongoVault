import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { MongoClient } from "mongodb";
import { linkedController } from "./abort.js";
import { runBackup } from "./backup.js";
import {
  MANIFEST_VERSION,
  databasePrefix,
  listBackups,
  manifestKey,
  readManifest,
  resolveBackup,
  resolveKeyRing,
  scopeOf,
  writeManifest,
} from "./catalog.js";
import { verifyManifest } from "./crypto.js";
import { formatDuration } from "./format.js";
import {
  captureOplogSlice,
  compareTimestamps,
  downloadAndExtractOplogSlice,
  filterOplogSliceFile,
  formatOplogTimestamp,
  fromBsonTimestamp,
  incrementOplogTimestamp,
  parseOplogTimestamp,
  parsePitrTarget,
  resolvePitrPlan,
} from "./oplog.js";
import { restoreBackup } from "./restore.js";
import { replayOplogFile } from "./tools.js";
import type {
  BackupManifest,
  BackupScope,
  ShardedClusterMeta,
  ShardedClusterShardBackup,
} from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type { Logger } from "./types/logger.js";
import type { OplogTimestamp, PitrPlan } from "./types/oplog.js";
import type {
  ClusterTopology,
  ShardInfo,
  ShardedBackupOptions,
  ShardedBackupResult,
  ShardedRestoreOptions,
  ShardedRestoreResult,
} from "./types/sharding.js";
import type { StorageClient } from "./types/storage.js";

/**
 * Builds a MongoDB connection URI for an individual shard replica set while
 * preserving credentials and connection parameters (authSource, tls, etc.) from baseUri.
 */
export function buildShardUri(baseUri: string, shardHost: string): string {
  const slashIdx = shardHost.indexOf("/");
  const rsName = slashIdx !== -1 ? shardHost.slice(0, slashIdx).trim() : "";
  const hosts = (slashIdx !== -1 ? shardHost.slice(slashIdx + 1) : shardHost).trim();

  const match = /^mongodb(?:\+srv)?:\/\/(?:([^@/]+)@)?[^/?]+(\/[^?]*)?(?:\?(.*))?$/i.exec(
    baseUri.trim(),
  );
  const authPrefix = match?.[1] ? `${match[1]}@` : "";
  const rawQuery = match?.[3] ?? "";

  const params = new URLSearchParams(rawQuery);
  if (rsName) {
    params.set("replicaSet", rsName);
  } else {
    params.delete("replicaSet");
  }
  // Remove SRV-only or conflicting options when connecting directly to shard seed hosts
  params.delete("directConnection");

  const queryStr = params.toString();
  return `mongodb://${authPrefix}${hosts}/${queryStr ? `?${queryStr}` : ""}`;
}

export class ActiveChunkMigrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ActiveChunkMigrationError";
  }
}

/**
 * Controller for coordinating MongoDB sharded cluster balancer states.
 */
export class BalancerController {
  constructor(
    private readonly client: MongoClient,
    private readonly logger?: Logger,
  ) {}

  async stopAndDrain(timeoutMs = 30_000): Promise<boolean> {
    this.logger?.info("Stopping balancer and waiting for active chunk migrations to complete...");
    try {
      await this.client.db("admin").command({ balancerStop: 1 });
    } catch (err) {
      this.logger?.error({ err }, "Could not issue balancerStop command to cluster");
      throw err;
    }

    const bStatus = await waitForBalancerDisabled(this.client, timeoutMs);
    if (bStatus.inBalancerRound || bStatus.mode !== "off") {
      throw new ActiveChunkMigrationError(
        `Balancer failed to disable or active chunk migration round did not conclude within ${Math.round(timeoutMs / 1000)}s (mode: "${bStatus.mode}", inBalancerRound: ${bStatus.inBalancerRound}). Aborting backup to prevent cross-shard data loss or duplicate documents.`,
      );
    }

    this.logger?.info("Cluster balancer disabled and quiet");
    return true;
  }

  async start(): Promise<void> {
    try {
      await this.client.db("admin").command({ balancerStart: 1 });
      this.logger?.info("Cluster balancer restarted");
    } catch (err) {
      this.logger?.warn({ err }, "Could not restart cluster balancer automatically");
    }
  }
}

/**
 * Waits for MongoDB balancer to be fully disabled with no active balancing round in flight.
 */
export async function waitForBalancerDisabled(
  client: MongoClient,
  timeoutMs = 30_000,
  pollIntervalMs = 500,
): Promise<{ inBalancerRound: boolean; mode: string }> {
  const admin = client.db("admin");
  const deadline = Date.now() + timeoutMs;
  let status = { inBalancerRound: false, mode: "unknown" };

  while (Date.now() < deadline) {
    try {
      const res = await admin
        .command({ balancerStatus: 1 })
        .catch(() => admin.command({ _configsvrBalancerStatus: 1 }));
      status = {
        inBalancerRound: Boolean(res?.inBalancerRound),
        mode: String(res?.mode || "unknown"),
      };
      if (!status.inBalancerRound && status.mode === "off") {
        return status;
      }
    } catch (err) {
      status = {
        inBalancerRound: true,
        mode: `error: ${err instanceof Error ? err.message : String(err)}`,
      };
    }
    await new Promise((r) => setTimeout(r, pollIntervalMs));
  }
  return status;
}

/**
 * Discovers the topology of a MongoDB sharded cluster via mongos.
 */
export async function discoverClusterTopology(client: MongoClient): Promise<ClusterTopology> {
  const admin = client.db("admin");
  const hello = await admin.command({ hello: 1 }).catch(() => admin.command({ isMaster: 1 }));

  const isSharded = hello?.msg === "isdbgrid";
  if (!isSharded) {
    return {
      isSharded: false,
      shards: [],
      balancerEnabled: false,
      balancerRunning: false,
    };
  }

  const shardsColl = client
    .db("config")
    .collection<{ _id: string; host: string; tags?: string[] }>("shards");
  const rawShards = await shardsColl.find({}).toArray();
  const shards: ShardInfo[] = rawShards
    .filter((s) => s._id !== "config")
    .map((s) => ({
      id: s._id,
      host: s.host,
      tags: s.tags,
    }));

  let balancerEnabled = true;
  let balancerRunning = false;
  try {
    const balancerStatus = await admin
      .command({ balancerStatus: 1 })
      .catch(() => admin.command({ _configsvrBalancerStatus: 1 }));
    balancerEnabled = balancerStatus?.mode !== "off";
    balancerRunning = Boolean(balancerStatus?.inBalancerRound);
  } catch {
    // Some mongos versions do not support balancerStatus command
  }

  let configsvrHost: string | undefined;
  try {
    const shardMap = await admin.command({ getShardMap: 1 }).catch(() => null);
    if (typeof shardMap?.map?.config === "string") {
      configsvrHost = shardMap.map.config;
    } else {
      const serverStatus = await admin.command({ serverStatus: 1 }).catch(() => null);
      if (typeof serverStatus?.sharding?.configsvrConnectionString === "string") {
        configsvrHost = serverStatus.sharding.configsvrConnectionString;
      }
    }
  } catch {
    // Ignore if configsvr connection string cannot be inspected
  }

  return {
    isSharded: true,
    mongosHost: hello.me || "mongos",
    ...(configsvrHost ? { configsvr: { host: configsvrHost } } : {}),
    shards,
    balancerEnabled,
    balancerRunning,
  };
}

/**
 * Executes a distributed, coordinated backup across all shards and configsvr.
 */
export async function runShardedClusterBackup({
  config,
  storage,
  logger,
  signal,
  createMongoClient = (u) => new MongoClient(u),
  backupRunner = runBackup,
}: ShardedBackupOptions): Promise<ShardedBackupResult> {
  const startedAt = new Date();
  const controller = linkedController(signal);
  const log = logger.child({ engine: "sharded-cluster" });

  log.info("Inspecting sharded cluster topology...");
  const client = createMongoClient(config.mongo.uri);
  const balancer = new BalancerController(client, log);
  let balancerStopped = false;
  let topology: ClusterTopology | null = null;

  try {
    await client.connect();
    topology = await discoverClusterTopology(client);

    if (!topology.isSharded) {
      throw new Error(
        "Connected MongoDB instance is not a sharded cluster (mongos). Topology check returned standalone/replica set.",
      );
    }

    log.info(
      {
        shardsCount: topology.shards.length,
        shards: topology.shards.map((s) => s.id),
      },
      "Discovered sharded cluster topology",
    );

    log.warn(
      "EXPERIMENTAL: The sharded cluster engine coordinates multi-shard backups by stopping chunk balancing and anchoring shard restores to a common $clusterTime. For strict cross-shard consistency with multi-shard distributed transactions, application writes must be quiesced during the backup window or cloud volume snapshots must be used.",
    );

    // 1. Quiesce cluster chunk balancing to prevent migrations during snapshot
    if (topology.balancerEnabled) {
      balancerStopped = await balancer.stopAndDrain(30_000);
    }

    // 2. Capture cluster-wide committed timestamp
    let clusterTimeStr = `${Math.floor(Date.now() / 1000)}:1`;
    try {
      const pingRes = await client.db("admin").command({ ping: 1 });
      if (pingRes?.$clusterTime?.clusterTime) {
        const parsed = fromBsonTimestamp(pingRes.$clusterTime.clusterTime);
        if (parsed) {
          clusterTimeStr = `${parsed.t}:${parsed.i}`;
        }
      }
    } catch {
      // Ignore
    }

    // 3. Back up Config Server and all shards in parallel so their oplog windows overlap
    const configsvrDirectUri = topology.configsvr?.host
      ? buildShardUri(config.mongo.uri, topology.configsvr.host)
      : null;

    log.info(
      { directConfigsvr: Boolean(configsvrDirectUri), shardsCount: topology.shards.length },
      "Backing up Config Server and shard replica sets in parallel...",
    );

    const configsvrConfig: AppConfig = {
      ...config,
      storage: {
        ...config.storage,
        prefix: `${config.storage.prefix}/configsvr`,
      },
      mongo: {
        ...config.mongo,
        uri: configsvrDirectUri ?? config.mongo.uri,
        database: configsvrDirectUri ? "*" : "config",
        oplog: Boolean(configsvrDirectUri),
      },
    };

    const configsvrPromise = backupRunner({
      config: configsvrConfig,
      storage,
      logger: log.child({ component: "configsvr" }),
      signal: controller.signal,
    });

    const shardBackups: Record<string, ShardedClusterShardBackup> = {};
    const shardEndTimestamps: Record<string, string> = {};
    const shardConfigs: Record<string, AppConfig> = {};

    const shardPromises = topology.shards.map(async (shard) => {
      log.info({ shardId: shard.id, host: shard.host }, "Backing up shard...");
      const shardUri = buildShardUri(config.mongo.uri, shard.host);

      const shardConfig: AppConfig = {
        ...config,
        storage: {
          ...config.storage,
          prefix: `${config.storage.prefix}/shards/${shard.id}`,
        },
        mongo: {
          ...config.mongo,
          uri: shardUri,
          database: "*",
          oplog: true,
        },
      };
      shardConfigs[shard.id] = shardConfig;

      try {
        const shardManifest = await backupRunner({
          config: shardConfig,
          storage,
          logger: log.child({ shardId: shard.id }),
          signal: controller.signal,
        });

        shardBackups[shard.id] = {
          shardId: shard.id,
          host: shard.host,
          backupKey: shardManifest.key,
          manifestKey: manifestKey(shardManifest.key),
          status: "success",
          size: shardManifest.size,
        };
        if (shardManifest.oplogWindow?.endTs) {
          shardEndTimestamps[shard.id] = shardManifest.oplogWindow.endTs;
        }
      } catch (shardError) {
        log.error({ shardId: shard.id, err: shardError }, "Shard backup failed");
        shardBackups[shard.id] = {
          shardId: shard.id,
          host: shard.host,
          backupKey: "",
          manifestKey: "",
          status: "failed",
        };
        throw shardError;
      }
    });

    const [configsvrManifest] = await Promise.all([configsvrPromise, ...shardPromises]);

    // Determine common clusterTime >= all shard oplogWindow.endTs
    let clusterTimeTs = parseOplogTimestamp(clusterTimeStr)!;
    for (const endStr of Object.values(shardEndTimestamps)) {
      const endTs = parseOplogTimestamp(endStr);
      if (endTs && compareTimestamps(endTs, clusterTimeTs) > 0) {
        clusterTimeTs = endTs;
      }
    }
    try {
      const postPing = await client.db("admin").command({ ping: 1 });
      const parsed = fromBsonTimestamp(postPing?.$clusterTime?.clusterTime);
      if (parsed && compareTimestamps(parsed, clusterTimeTs) > 0) {
        clusterTimeTs = parsed;
      }
    } catch {
      // Ignore
    }
    clusterTimeStr = formatOplogTimestamp(clusterTimeTs);

    // Capture bridging oplog slices for any shard whose dump ended before clusterTimeTs
    await Promise.all(
      topology.shards.map(async (shard) => {
        const endStr = shardEndTimestamps[shard.id];
        const shardCfg = shardConfigs[shard.id];
        const endTs = endStr ? parseOplogTimestamp(endStr) : null;
        if (endTs && shardCfg && compareTimestamps(endTs, clusterTimeTs) < 0) {
          const sliceManifest = await captureOplogSlice({
            config: shardCfg,
            storage,
            logger: log.child({ shardId: shard.id, phase: "bridge-oplog" }),
            signal: controller.signal,
            sinceTs: endTs,
            createMongoClient,
          });
          if (sliceManifest) {
            shardBackups[shard.id]!.bridgingSliceKey = sliceManifest.key;
          }
        }
      }),
    );

    // 5. Restart cluster balancer if it was originally enabled
    if (balancerStopped && topology?.balancerEnabled) {
      await balancer.start();
      balancerStopped = false;
    }

    const completedAt = new Date();
    const durationMs = completedAt.getTime() - startedAt.getTime();
    const scope = scopeOf(config);
    const encrypted = Boolean(config.encryptionKey);
    const stamp = startedAt
      .toISOString()
      .replace(/[-:]/g, "")
      .replace(/\.\d{3}Z$/, "Z");
    const clusterKey = `${databasePrefix(scope)}${startedAt.toISOString().slice(0, 10)}/${stamp}.cluster${encrypted ? ".enc" : ""}`;

    const clusterMeta: ShardedClusterMeta = {
      clusterTime: clusterTimeStr,
      configsvr: {
        backupKey: configsvrManifest.key,
        manifestKey: manifestKey(configsvrManifest.key),
        host: topology.configsvr?.host || topology.mongosHost,
      },
      shards: shardBackups,
    };

    const clusterManifest: BackupManifest = {
      version: MANIFEST_VERSION,
      type: "sharded-cluster-backup",
      engine: "logical",
      key: clusterKey,
      database: config.mongo.database,
      createdAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs,
      size:
        Object.values(shardBackups).reduce((acc, s) => acc + (s.size || 0), 0) +
        configsvrManifest.size,
      sha256: "sharded-cluster",
      compression: "gzip",
      encryption: configsvrManifest.encryption,
      oplog: true,
      excludedCollections: [],
      collections: {},
      sharding: clusterMeta,
    };

    // Write the .cluster catalog marker object so listBackups() and resolveBackup("latest") discover it
    await storage.putJson(clusterKey, clusterMeta);
    await writeManifest(storage, clusterManifest, config.encryptionKey);

    log.info(
      {
        clusterKey,
        duration: formatDuration(durationMs),
        shardsBackedUp: Object.keys(shardBackups).length,
      },
      "Sharded cluster distributed backup completed successfully",
    );

    return { manifest: clusterManifest, clusterMeta, durationMs };
  } finally {
    if (balancerStopped && topology?.balancerEnabled) {
      await balancer.start();
    }
    await client.close().catch(() => {});
    controller.dispose();
  }
}

export interface ShardedPitrPlan {
  clusterBackupKey: string;
  clusterManifest: BackupManifest;
  clusterMeta: ShardedClusterMeta;
  targetTs: OplogTimestamp;
  configsvrPlan: PitrPlan | null;
  shardPlans: Record<string, PitrPlan>;
}

/**
 * Captures continuous PITR oplog slices in parallel across configsvr and all shards.
 */
export async function captureShardedOplogSlices({
  config,
  storage,
  logger,
  signal,
  createMongoClient = (u) => new MongoClient(u),
}: {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  createMongoClient?: (uri: string) => MongoClient;
}): Promise<{
  configsvr: BackupManifest | null;
  shards: Record<string, BackupManifest | null>;
} | null> {
  const client = createMongoClient(config.mongo.uri);
  let topology: ClusterTopology;
  try {
    await client.connect();
    topology = await discoverClusterTopology(client);
  } finally {
    await client.close().catch(() => {});
  }

  if (!topology.isSharded) {
    return null;
  }

  const configsvrDirectUri = topology.configsvr?.host
    ? buildShardUri(config.mongo.uri, topology.configsvr.host)
    : config.sharding?.configsvrUri;

  const configsvrPromise = configsvrDirectUri
    ? captureOplogSlice({
        config: {
          ...config,
          storage: { ...config.storage, prefix: `${config.storage.prefix}/configsvr` },
          mongo: { ...config.mongo, uri: configsvrDirectUri, database: "*", oplog: true },
        },
        storage,
        logger: logger.child({ component: "configsvr" }),
        signal,
        createMongoClient,
      })
    : Promise.resolve(null);

  const shardEntries = await Promise.all(
    topology.shards.map(async (shard) => {
      const shardUri = buildShardUri(config.mongo.uri, shard.host);
      const manifest = await captureOplogSlice({
        config: {
          ...config,
          storage: { ...config.storage, prefix: `${config.storage.prefix}/shards/${shard.id}` },
          mongo: { ...config.mongo, uri: shardUri, database: "*", oplog: true },
        },
        storage,
        logger: logger.child({ shardId: shard.id }),
        signal,
        createMongoClient,
      });
      return [shard.id, manifest] as const;
    }),
  );

  const configsvr = await configsvrPromise;
  return {
    configsvr,
    shards: Object.fromEntries(shardEntries),
  };
}

/**
 * Resolves a consistent sharded cluster PITR plan across configsvr and all shards.
 */
export async function resolveShardedPitrPlan(
  storage: StorageClient,
  scope: BackupScope,
  targetInput: string | OplogTimestamp,
): Promise<ShardedPitrPlan> {
  const targetTs = parsePitrTarget(targetInput);
  const backups = (await listBackups(storage, scope)).filter(
    (b) => b.complete && /\.cluster(\.enc)?$/.test(b.key),
  );

  if (backups.length === 0) {
    throw new Error(`No complete sharded cluster backups found under ${databasePrefix(scope)}`);
  }

  let selectedKey: string | null = null;
  let selectedManifest: BackupManifest | null = null;
  let selectedMeta: ShardedClusterMeta | null = null;

  for (const backup of backups) {
    const manifest = await readManifest(storage, backup.key);
    const meta = manifest?.sharding;
    if (!manifest || !meta) continue;
    const cTs = meta.clusterTime
      ? parseOplogTimestamp(meta.clusterTime)
      : {
          t:
            Math.floor(new Date(manifest.completedAt || manifest.createdAt).getTime() / 1000) >>> 0,
          i: 0,
        };
    if (cTs && compareTimestamps(cTs, targetTs) <= 0) {
      selectedKey = backup.key;
      selectedManifest = manifest;
      selectedMeta = meta;
      break;
    }
  }

  if (!selectedKey || !selectedManifest || !selectedMeta) {
    throw new Error(
      `No sharded cluster backup found at or before target PITR timestamp ${formatOplogTimestamp(targetTs)}`,
    );
  }

  const configsvrManifest = await readManifest(storage, selectedMeta.configsvr.backupKey);
  const configsvrPlan = configsvrManifest?.oplog
    ? await resolvePitrPlan(
        storage,
        { prefix: `${scope.prefix}/configsvr`, database: "*" },
        targetTs,
        { engine: "logical", includeBaseWindow: true },
      )
    : null;

  const shardPlans: Record<string, PitrPlan> = {};
  for (const [shardId, shard] of Object.entries(selectedMeta.shards)) {
    if (shard.status !== "success" || !shard.backupKey) {
      throw new Error(`Cannot perform sharded PITR: shard "${shardId}" has incomplete base backup`);
    }
    shardPlans[shardId] = await resolvePitrPlan(
      storage,
      { prefix: `${scope.prefix}/shards/${shardId}`, database: "*" },
      targetTs,
      { engine: "logical" },
    );
  }

  return {
    clusterBackupKey: selectedKey,
    clusterManifest: selectedManifest,
    clusterMeta: selectedMeta,
    targetTs,
    configsvrPlan,
    shardPlans,
  };
}

async function replayOplogSlices({
  config,
  storage,
  signal,
  logger,
  slices,
  targetTs,
  uri,
  nsInclude,
  oplogLimit,
}: {
  config: AppConfig;
  storage: StorageClient;
  signal?: AbortSignal;
  logger: Logger;
  slices: Array<{ key: string }>;
  targetTs: OplogTimestamp;
  uri: string;
  nsInclude?: string;
  oplogLimit: string;
}): Promise<number> {
  let replayed = 0;
  for (const slice of slices) {
    const rawSlice = path.join(config.workDir, `.shard-pitr-${randomUUID()}.oplog.bson`);
    const filteredSlice = path.join(config.workDir, `.shard-pitr-f-${randomUUID()}.oplog.bson`);
    try {
      await downloadAndExtractOplogSlice({ config, storage, signal }, slice.key, rawSlice);
      const { keptEntries } = await filterOplogSliceFile(rawSlice, filteredSlice, {
        targetTs,
        nsInclude,
      });
      if (keptEntries === 0) continue;
      await replayOplogFile({
        uri,
        workDir: config.workDir,
        oplogFile: filteredSlice,
        oplogLimit,
        timeoutMs: config.timeoutMs,
        signal,
        logger,
      });
      replayed += keptEntries;
    } finally {
      await Promise.allSettled([
        fs.rm(rawSlice, { force: true }),
        fs.rm(filteredSlice, { force: true }),
      ]);
    }
  }
  return replayed;
}

/**
 * Restores a sharded cluster backup, with optional Point-in-Time Recovery (PITR).
 */
export async function restoreShardedCluster({
  config,
  storage,
  logger,
  ref = "latest",
  pitr,
  signal,
  drop = false,
  createMongoClient = (u) => new MongoClient(u),
  restoreRunner = restoreBackup,
}: ShardedRestoreOptions): Promise<ShardedRestoreResult> {
  const scope = scopeOf(config);
  const pitrPlan = pitr ? await resolveShardedPitrPlan(storage, scope, pitr) : null;
  const resolvedKey = pitrPlan
    ? pitrPlan.clusterBackupKey
    : ref === "latest" && typeof storage.list === "function"
      ? await resolveBackup(storage, scope, ref)
      : ref;

  const log = logger.child({ ref: resolvedKey, ...(pitr ? { pitr } : {}) });
  log.info("Initiating sharded cluster restore");

  const manifest = pitrPlan?.clusterManifest ?? (await readManifest(storage, resolvedKey));
  const clusterMeta = pitrPlan?.clusterMeta ?? manifest?.sharding;
  if (!manifest || !clusterMeta) {
    throw new Error(`No sharded cluster metadata found in manifest for ${resolvedKey}`);
  }

  const keyRing = resolveKeyRing(config);
  if (manifest.encryption?.manifestHmac && keyRing.length > 0) {
    verifyManifest(keyRing, manifest);
  }

  const client = createMongoClient(config.mongo.uri);
  const balancer = new BalancerController(client, log);
  let balancerStopped = false;
  let replayedSlices = 0;

  try {
    await client.connect();

    // 1. Discover target cluster topology and quiesce balancer if enabled
    const targetTopology = await discoverClusterTopology(client).catch(() => null);
    if (targetTopology?.balancerEnabled) {
      balancerStopped = await balancer.stopAndDrain(30_000);
    } else if (!targetTopology) {
      try {
        await client.db("admin").command({ balancerStop: 1 });
        balancerStopped = true;
      } catch {}
    }
    const targetShardMap = new Map((targetTopology?.shards ?? []).map((s) => [s.id, s.host]));
    const targetConfigsvrUri = targetTopology?.configsvr?.host
      ? buildShardUri(config.mongo.uri, targetTopology.configsvr.host)
      : config.mongo.uri;

    const clusterTs = parseOplogTimestamp(clusterMeta.clusterTime);
    const effectiveTargetTs = pitrPlan ? pitrPlan.targetTs : clusterTs;
    const exclusiveLimit = effectiveTargetTs
      ? formatOplogTimestamp(incrementOplogTimestamp(effectiveTargetTs))
      : clusterMeta.clusterTime;

    const configsvrNsInclude =
      "config.databases,config.collections,config.chunks,config.tags,config.settings";

    // 2. Restore configsvr routing tables directly to configsvr (if discovered)
    log.info(
      {
        configsvrKey: clusterMeta.configsvr.backupKey,
        directConfigsvr: Boolean(targetTopology?.configsvr?.host),
      },
      "Restoring Config Server routing metadata...",
    );
    await restoreRunner(
      { config, storage, logger: log.child({ target: "configsvr" }), signal },
      {
        ref: clusterMeta.configsvr.backupKey,
        uri: targetConfigsvrUri,
        database: targetTopology?.configsvr?.host ? "*" : "config",
        ...(targetTopology?.configsvr?.host ? { nsInclude: configsvrNsInclude } : {}),
        drop,
      },
    );

    if (pitrPlan?.configsvrPlan && pitrPlan.configsvrPlan.slices.length > 0) {
      await replayOplogSlices({
        config,
        storage,
        signal,
        logger: log.child({ target: "configsvr-pitr" }),
        slices: pitrPlan.configsvrPlan.slices,
        targetTs: pitrPlan.targetTs,
        uri: targetConfigsvrUri,
        nsInclude: targetTopology?.configsvr?.host ? configsvrNsInclude : undefined,
        oplogLimit: exclusiveLimit,
      });
      replayedSlices += pitrPlan.configsvrPlan.slices.length;
    }

    // 3. Restore each shard (using target cluster shard host mapping when available)
    const restoredShards: string[] = [];
    for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
      if (shard.status !== "success" || !shard.backupKey) {
        log.warn({ shardId }, "Skipping unbacked or failed shard");
        continue;
      }

      const resolvedHost = targetShardMap.get(shardId) || shard.host;
      const targetShardUri = resolvedHost
        ? buildShardUri(config.mongo.uri, resolvedHost)
        : config.mongo.uri;
      const shardPitrPlan = pitrPlan?.shardPlans[shardId] ?? null;

      log.info({ shardId, backupKey: shard.backupKey }, "Restoring shard...");
      await restoreRunner(
        { config, storage, logger: log.child({ target: shardId }), signal },
        {
          ref: shard.backupKey,
          uri: targetShardUri,
          database: "*",
          drop,
          oplogReplay: true,
          oplogLimit: shardPitrPlan && shardPitrPlan.slices.length > 0 ? undefined : exclusiveLimit,
        },
      );

      if (shard.bridgingSliceKey) {
        await replayOplogSlices({
          config,
          storage,
          signal,
          logger: log.child({ target: shardId }),
          slices: [{ key: shard.bridgingSliceKey }],
          targetTs: effectiveTargetTs!,
          uri: targetShardUri,
          oplogLimit: exclusiveLimit,
        });
      }

      if (shardPitrPlan && shardPitrPlan.slices.length > 0) {
        await replayOplogSlices({
          config,
          storage,
          signal,
          logger: log.child({ target: shardId }),
          slices: shardPitrPlan.slices,
          targetTs: pitrPlan!.targetTs,
          uri: targetShardUri,
          oplogLimit: exclusiveLimit,
        });
        replayedSlices += shardPitrPlan.slices.length;
      }

      restoredShards.push(shardId);
    }

    // 4. Flush mongos router config and restart balancer
    await client
      .db("admin")
      .command({ flushRouterConfig: 1 })
      .catch(() => {});
    if (balancerStopped) {
      await balancer.start();
      balancerStopped = false;
    }

    const pitrTarget = pitrPlan ? formatOplogTimestamp(pitrPlan.targetTs) : undefined;
    log.info(
      {
        restoredShards,
        clusterTime: clusterMeta.clusterTime,
        ...(pitrTarget ? { pitrTarget, replayedSlices } : {}),
      },
      "Sharded cluster restore completed successfully",
    );

    return {
      key: resolvedKey,
      clusterTime: clusterMeta.clusterTime,
      restoredShards,
      configsvrRestored: true,
      ...(pitrTarget ? { pitrTarget, replayedSlices } : {}),
    };
  } finally {
    if (balancerStopped) {
      await balancer.start();
    }
    await client.close().catch(() => {});
  }
}
