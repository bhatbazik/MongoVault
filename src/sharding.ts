import { MongoClient } from "mongodb";
import { linkedController } from "./abort.js";
import { runBackup } from "./backup.js";
import { MANIFEST_VERSION, manifestKey, scopeOf } from "./catalog.js";
import { formatDuration } from "./format.js";
import { fromBsonTimestamp } from "./oplog.js";
import { restoreBackup } from "./restore.js";
import type {
  BackupManifest,
  ShardedClusterMeta,
  ShardedClusterShardBackup,
} from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type {
  ClusterTopology,
  ShardInfo,
  ShardedBackupOptions,
  ShardedBackupResult,
  ShardedRestoreOptions,
  ShardedRestoreResult,
} from "./types/sharding.js";

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
      const res = await admin.command({ balancerStatus: 1 });
      status = {
        inBalancerRound: Boolean(res?.inBalancerRound),
        mode: String(res?.mode || "unknown"),
      };
      if (!status.inBalancerRound && status.mode === "off") {
        return status;
      }
    } catch {
      return status;
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
  const shards: ShardInfo[] = rawShards.map((s) => ({
    id: s._id,
    host: s.host,
    tags: s.tags,
  }));

  let balancerEnabled = true;
  let balancerRunning = false;
  try {
    const balancerStatus = await admin.command({ balancerStatus: 1 });
    balancerEnabled = balancerStatus?.mode !== "off";
    balancerRunning = Boolean(balancerStatus?.inBalancerRound);
  } catch {
    // Some mongos versions do not support balancerStatus command
  }

  return {
    isSharded: true,
    mongosHost: hello.me || "mongos",
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
}: ShardedBackupOptions): Promise<ShardedBackupResult> {
  const startedAt = new Date();
  const controller = linkedController(signal);
  const log = logger.child({ engine: "sharded-cluster" });

  log.info("Inspecting sharded cluster topology...");
  const client = new MongoClient(config.mongo.uri);
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

    // 1. Quiesce cluster chunk balancing to prevent migrations during snapshot
    if (topology.balancerEnabled) {
      try {
        log.info("Stopping balancer and waiting for active chunk migrations to complete...");
        await client.db("admin").command({ balancerStop: 1 });
        balancerStopped = true;
        const bStatus = await waitForBalancerDisabled(client, 30_000);
        if (bStatus.inBalancerRound) {
          log.warn("Active balancer round did not conclude within 30s; proceeding with backup");
        } else {
          log.info("Cluster balancer disabled and quiet");
        }
      } catch (err) {
        log.warn({ err }, "Could not stop balancer; proceeding with cluster backup");
      }
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

    // 3. Back up Config Server metadata (config database routing tables, collections, chunk maps)
    log.info("Backing up Config Server metadata...");
    const configsvrConfig: AppConfig = {
      ...config,
      mongo: {
        ...config.mongo,
        database: "config",
        oplog: false,
      },
    };
    const configsvrManifest = await runBackup({
      config: configsvrConfig,
      storage,
      logger: log.child({ component: "configsvr" }),
      signal: controller.signal,
    });

    // 4. Back up each shard replica set
    const shardBackups: Record<string, ShardedClusterShardBackup> = {};

    for (const shard of topology.shards) {
      log.info({ shardId: shard.id, host: shard.host }, "Backing up shard...");
      const shardHosts = shard.host.includes("/") ? shard.host.split("/")[1] : shard.host;
      const shardUri = `mongodb://${shardHosts}/?replicaSet=${shard.host.split("/")[0]}`;

      const shardConfig: AppConfig = {
        ...config,
        mongo: {
          ...config.mongo,
          uri: shardUri,
          database: "*",
          oplog: true,
        },
      };

      try {
        const shardManifest = await runBackup({
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
    }

    // 5. Restart cluster balancer if it was originally enabled
    if (balancerStopped && topology?.balancerEnabled) {
      try {
        await client.db("admin").command({ balancerStart: 1 });
        balancerStopped = false;
        log.info("Cluster balancer restarted");
      } catch (err) {
        log.warn({ err }, "Could not restart cluster balancer automatically");
      }
    }

    const completedAt = new Date();
    const durationMs = completedAt.getTime() - startedAt.getTime();
    const scope = scopeOf(config);
    const clusterKey = `${scope.prefix}/cluster/${startedAt.toISOString().slice(0, 10)}/${startedAt.toISOString().replace(/[:.]/g, "")}.cluster`;

    const clusterMeta: ShardedClusterMeta = {
      clusterTime: clusterTimeStr,
      configsvr: {
        backupKey: configsvrManifest.key,
        manifestKey: manifestKey(configsvrManifest.key),
        host: topology.mongosHost,
      },
      shards: shardBackups,
    };

    const clusterManifest: BackupManifest = {
      version: MANIFEST_VERSION,
      type: "sharded-cluster-backup",
      engine: "logical",
      key: clusterKey,
      database: "*",
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

    await storage.putJson(manifestKey(clusterKey), clusterManifest);

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
      await client
        .db("admin")
        .command({ balancerStart: 1 })
        .catch(() => {});
    }
    await client.close().catch(() => {});
    controller.dispose();
  }
}

/**
 * Restores a sharded cluster backup.
 */
export async function restoreShardedCluster({
  config,
  storage,
  logger,
  ref = "latest",
  signal,
  drop = false,
}: ShardedRestoreOptions): Promise<ShardedRestoreResult> {
  const log = logger.child({ ref });
  log.info("Initiating sharded cluster restore");

  const manifest = await storage.getJson<BackupManifest>(manifestKey(ref));
  const clusterMeta = manifest?.data?.sharding;
  if (!clusterMeta) {
    throw new Error(`No sharded cluster metadata found in manifest for ${ref}`);
  }

  const client = new MongoClient(config.mongo.uri);
  try {
    await client.connect();

    // 1. Quiesce balancer on restore target
    try {
      await client.db("admin").command({ balancerStop: 1 });
    } catch {}

    // 2. Restore configsvr routing tables
    log.info(
      { configsvrKey: clusterMeta.configsvr.backupKey },
      "Restoring Config Server routing metadata...",
    );
    await restoreBackup(
      { config, storage, logger: log.child({ target: "configsvr" }), signal },
      {
        ref: clusterMeta.configsvr.backupKey,
        uri: config.mongo.uri,
        database: "config",
        drop,
      },
    );

    // 3. Restore each shard
    const restoredShards: string[] = [];
    for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
      if (shard.status !== "success" || !shard.backupKey) {
        log.warn({ shardId }, "Skipping unbacked or failed shard");
        continue;
      }

      log.info({ shardId, backupKey: shard.backupKey }, "Restoring shard...");
      await restoreBackup(
        { config, storage, logger: log.child({ target: shardId }), signal },
        {
          ref: shard.backupKey,
          uri: config.mongo.uri,
          database: "*",
          drop,
        },
      );
      restoredShards.push(shardId);
    }

    // 4. Restart balancer
    try {
      await client.db("admin").command({ balancerStart: 1 });
    } catch {}

    log.info(
      {
        restoredShards,
        clusterTime: clusterMeta.clusterTime,
      },
      "Sharded cluster restore completed successfully",
    );

    return {
      key: ref,
      clusterTime: clusterMeta.clusterTime,
      restoredShards,
      configsvrRestored: true,
    };
  } finally {
    await client.close().catch(() => {});
  }
}
