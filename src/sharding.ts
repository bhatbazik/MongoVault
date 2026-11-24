import { createHash } from "node:crypto";
import { PassThrough, Readable } from "node:stream";
import { createGunzip } from "node:zlib";
import { BSON, Binary, MongoClient } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import { streamArchiveCollections } from "./archive-stream.js";
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
import { createDecryptor, createEncryptor, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { formatDuration } from "./format.js";
import { MongoOplogTargetClient, OplogApplier, mergeOplogStats } from "./oplog-applier.js";
import {
  captureOplogSlice,
  compareTimestamps,
  formatOplogTimestamp,
  fromBsonTimestamp,
  getOplogBounds,
  incrementOplogTimestamp,
  listOplogSlices,
  parseOplogTimestamp,
  parsePitrTarget,
  pitrStateKey,
  resolvePitrPlan,
  streamOplogSlice,
} from "./oplog.js";
import { restoreBackup, verifyArchiveObject } from "./restore.js";
import { runStreamPipeline } from "./stream.js";
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

const MAX_CONCURRENT_SHARD_DUMPS = 4;
const MAX_BRIDGE_SLICE_ITERATIONS = 1000;
const CSRS_BULK_BATCH_SIZE = 500;
const CSRS_ALLOWED_COLLECTIONS = new Set([
  "databases",
  "collections",
  "chunks",
  "tags",
  "settings",
]);
/**
 * Transient/internal CSRS collections excluded when dumping `config` directly.
 * Excluding `reshardingOperations` avoids mongodump 100.x aborting on MongoDB 8.0 CSRS
 * where `config.reshardingOperations` exists even when no resharding is active.
 */
const CSRS_DUMP_EXCLUDE_COLLECTIONS = [
  "reshardingOperations",
  "localReshardingOperations.coordinator",
  "localReshardingOperations.donor",
  "localReshardingOperations.recipient",
  "transactions",
  "transaction_coordinators",
  "system.sessions",
  "system.preimages",
  "locks",
  "lockpings",
  "mongos",
  "actionlog",
  "changelog",
  "rangeDeletions",
  "shards",
  "version",
];
const SYSTEM_DATABASES = new Set(["admin", "local", "config"]);

async function runWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let nextIdx = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (nextIdx < items.length) {
      const idx = nextIdx++;
      results[idx] = await worker(items[idx]!, idx);
    }
  });
  await Promise.all(workers);
  return results;
}

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
      await this.client.db("admin").command({ balancerStop: 1, maxTimeMS: timeoutMs });
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
  const hasConfigShard = rawShards.some((s) => s._id === "config");
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
    hasConfigShard,
    mongosHost: hello.me || "mongos",
    ...(configsvrHost ? { configsvr: { host: configsvrHost } } : {}),
    shards,
    balancerEnabled,
    balancerRunning,
  };
}

function binaryToHex(value: unknown): string | null {
  if (!value) return null;
  if (value instanceof Binary) {
    return Buffer.from(value.buffer).toString("hex");
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("hex");
  }
  if (typeof value === "object" && "buffer" in (value as Record<string, unknown>)) {
    const buf = (value as { buffer?: unknown }).buffer;
    if (buf instanceof Uint8Array) return Buffer.from(buf).toString("hex");
  }
  return null;
}

/**
 * Zero-disk streaming Config Server (CSRS) metadata restorer.
 * Streams `config.databases`, `config.collections`, `config.chunks`, `config.tags`,
 * and `config.settings` directly from the compressed/encrypted configsvr archive in
 * 500-document `bulkWrite` batches while preserving target `config.system.sessions`
 * UUID and target `balancer`/`automerge` settings.
 */
export class CsrsRestorer {
  async restore({
    config,
    storage,
    backupKey,
    targetConfigsvrUri,
    drop,
    logger,
    signal,
    createMongoClient = (u) => new MongoClient(u),
  }: {
    config: AppConfig;
    storage: StorageClient;
    backupKey: string;
    targetConfigsvrUri: string;
    drop: boolean;
    logger: Logger;
    signal?: AbortSignal;
    createMongoClient?: (uri: string) => MongoClient;
  }): Promise<{ restoredDocs: number; backupSessionsUuidHex: string | null }> {
    const manifest = await readManifest(storage, backupKey);
    const keyRing = resolveKeyRing(config);
    const isSigned = Boolean(manifest?.signature?.hmac || manifest?.encryption?.manifestHmac);
    if (isSigned && manifest) {
      if (keyRing.length === 0) {
        throw new Error(
          `Config server manifest for ${backupKey} is signed, but ENCRYPTION_KEY is not configured`,
        );
      }
      verifyManifest(keyRing, manifest);
    } else if (manifest?.encryption && keyRing.length === 0) {
      throw new Error(
        `Config server backup ${backupKey} is encrypted, but ENCRYPTION_KEY is not set`,
      );
    }

    const client = createMongoClient(targetConfigsvrUri);
    await client.connect();
    const controller = linkedController(signal);

    try {
      const configDb = client.db("config");

      // Preserve target cluster's live `config.system.sessions` UUID
      const targetSessionsDoc = await configDb
        .collection("collections")
        .findOne({ _id: "config.system.sessions" as unknown as never })
        .catch(() => null);
      const targetSessionsUuid = targetSessionsDoc?.uuid ?? null;

      if (drop) {
        await configDb
          .collection("databases")
          .deleteMany({})
          .catch(() => {});
        await configDb
          .collection("tags")
          .deleteMany({})
          .catch(() => {});
        await configDb
          .collection("collections")
          .deleteMany({ _id: { $ne: "config.system.sessions" } } as unknown as never)
          .catch(() => {});
        await configDb
          .collection("chunks")
          .deleteMany(targetSessionsUuid ? { uuid: { $ne: targetSessionsUuid } } : {})
          .catch(() => {});
        await configDb
          .collection("settings")
          .deleteMany({ _id: { $nin: ["balancer", "automerge"] } } as unknown as never)
          .catch(() => {});
      }

      const batches = new Map<string, Array<Record<string, unknown>>>();
      let restoredDocs = 0;
      let backupSessionsUuid: unknown = null;
      let backupSessionsUuidHex: string | null = null;

      const flushBatch = async (collName: string) => {
        const docs = batches.get(collName);
        if (!docs || docs.length === 0) return;
        batches.set(collName, []);
        const ops = docs.map((doc) => ({
          replaceOne: {
            filter: { _id: doc._id },
            replacement: doc,
            upsert: true,
          },
        }));
        await configDb.collection(collName).bulkWrite(ops as never[], { ordered: false });
        restoredDocs += docs.length;
      };

      const downloadStream = await storage.download(backupKey, { signal: controller.signal });
      const digest = new DigestStream();
      const decryptStage = manifest?.encryption
        ? [createDecryptor(keyRing, manifest.encryption)]
        : [];
      const gunzip = createGunzip();
      const archivePass = new PassThrough();

      const stages = [downloadStream, digest, ...decryptStage, gunzip, archivePass];
      let oplogApplier: OplogApplier | null = null;
      let flushedBeforeOplog = false;
      const csrsNsInclude =
        "config.databases,config.collections,config.chunks,config.tags,config.settings";

      try {
        await settleAll(controller, [
          runStreamPipeline(stages, { signal: controller.signal }),
          streamArchiveCollections(
            archivePass,
            (db, coll) =>
              (db === "config" && CSRS_ALLOWED_COLLECTIONS.has(coll)) || (db === "" && coll === ""),
            async ({ db, collection, rawDoc }) => {
              // Embedded oplog chunk at the end of mongodump --archive --oplog
              if (db === "" && collection === "") {
                if (!flushedBeforeOplog) {
                  for (const collName of batches.keys()) {
                    await flushBatch(collName);
                  }
                  if (backupSessionsUuid) {
                    await configDb
                      .collection("chunks")
                      .deleteMany({ uuid: backupSessionsUuid })
                      .catch(() => {});
                  }
                  flushedBeforeOplog = true;
                  oplogApplier = new OplogApplier({
                    target: new MongoOplogTargetClient(client),
                    workDir: config.workDir,
                    logger,
                    filter: {
                      nsInclude: csrsNsInclude,
                      sessionsUuidHex: backupSessionsUuidHex ?? undefined,
                    },
                    backupType: "logical",
                  });
                }
                await oplogApplier!.applyRawDoc(rawDoc);
                return;
              }

              const doc = BSON.deserialize(rawDoc, { promoteValues: false }) as Record<
                string,
                unknown
              >;
              if (collection === "collections" && doc._id === "config.system.sessions") {
                backupSessionsUuid = doc.uuid ?? null;
                backupSessionsUuidHex = binaryToHex(doc.uuid);
                return;
              }
              if (
                collection === "chunks" &&
                backupSessionsUuidHex &&
                binaryToHex(doc.uuid) === backupSessionsUuidHex
              ) {
                return;
              }
              if (
                collection === "settings" &&
                (doc._id === "balancer" || doc._id === "automerge")
              ) {
                return;
              }

              let list = batches.get(collection);
              if (!list) {
                list = [];
                batches.set(collection, list);
              }
              list.push(doc);
              if (list.length >= CSRS_BULK_BATCH_SIZE) {
                await flushBatch(collection);
              }
            },
            controller.signal,
          ),
        ]);

        if (!flushedBeforeOplog) {
          for (const collName of batches.keys()) {
            await flushBatch(collName);
          }
          if (backupSessionsUuid) {
            await configDb
              .collection("chunks")
              .deleteMany({ uuid: backupSessionsUuid })
              .catch(() => {});
          }
        }
        if (oplogApplier) {
          await (oplogApplier as OplogApplier).buildPendingIndexes();
        }
      } finally {
        if (oplogApplier) {
          await (oplogApplier as OplogApplier).dispose();
        }
      }

      const actualSha256 = digest.digest();
      if (manifest?.sha256 && manifest.sha256 !== actualSha256) {
        throw new Error(
          `Config server archive checksum mismatch for ${backupKey}: expected ${manifest.sha256}, got ${actualSha256}`,
        );
      }
      if (manifest?.size !== undefined && manifest.size !== digest.bytes) {
        throw new Error(
          `Config server archive size mismatch for ${backupKey}: expected ${manifest.size}, got ${digest.bytes}`,
        );
      }

      logger.info(
        { backupKey, restoredDocs, hasBackupSessionsUuid: Boolean(backupSessionsUuidHex) },
        "Programmatic CSRS metadata restore completed",
      );
      return { restoredDocs, backupSessionsUuidHex };
    } finally {
      controller.dispose();
      await client.close().catch(() => {});
    }
  }
}

/**
 * Captures one or more backup-scoped bridging oplog slices from `sinceTs` through `untilTs`
 * without mutating the live PITR archiver's `state.json`.
 */
async function captureBridgingSlices({
  config,
  storage,
  logger,
  signal,
  sinceTs,
  untilTs,
  keyPrefix,
  createMongoClient,
  activeTxnWaitTimeoutMs = 15_000,
}: {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  sinceTs: OplogTimestamp;
  untilTs: OplogTimestamp;
  keyPrefix: string;
  createMongoClient: (uri: string) => MongoClient;
  activeTxnWaitTimeoutMs?: number;
}): Promise<string[]> {
  const keys: string[] = [];
  let cursorTs = sinceTs;
  let blockedSinceMs: number | null = null;

  for (let i = 0; i < MAX_BRIDGE_SLICE_ITERATIONS; i++) {
    if (compareTimestamps(cursorTs, untilTs) >= 0) break;
    let cappedTxTs: OplogTimestamp | null = null;
    const sliceManifest = await captureOplogSlice({
      config,
      storage,
      logger,
      signal,
      sinceTs: cursorTs,
      untilTs,
      keyPrefix,
      updateState: false,
      onCappedByActiveTxn: (txTs) => {
        cappedTxTs = txTs;
      },
      createMongoClient,
    });
    if (!sliceManifest) {
      if (cappedTxTs) {
        if (blockedSinceMs === null) blockedSinceMs = Date.now();
        if (Date.now() - blockedSinceMs >= activeTxnWaitTimeoutMs) {
          throw new Error(
            `Failed to capture bridging oplog slices up to ${formatOplogTimestamp(untilTs)}: active transaction at ${formatOplogTimestamp(cappedTxTs)} did not conclude within ${Math.round(activeTxnWaitTimeoutMs / 1000)}s`,
          );
        }
        await new Promise((r) => setTimeout(r, 250));
        continue;
      }
      break;
    }
    blockedSinceMs = null;
    keys.push(sliceManifest.key);

    const endStr = sliceManifest.oplogWindow?.endTs;
    const nextTs = endStr ? parseOplogTimestamp(endStr) : null;
    if (!nextTs || compareTimestamps(nextTs, cursorTs) <= 0) {
      break;
    }
    cursorTs = nextTs;
  }

  return keys;
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

  if (config.requireEncryption && !config.encryptionKey) {
    throw new Error(
      "Refusing to create unencrypted sharded cluster backup: REQUIRE_ENCRYPTION is enabled but ENCRYPTION_KEY is not configured",
    );
  }
  if (!config.encryptionKey) {
    log.warn(
      "ENCRYPTION_KEY is not configured; sharded cluster backup archives and manifests will be stored unencrypted and unsigned",
    );
  }

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

    if (topology.hasConfigShard && topology.shards.length === 0) {
      throw new Error(
        "Embedded Config Shard-only cluster (_id: 'config' with no dedicated shards) is not supported by logical sharded backup. Add dedicated shards or use replica-set backup.",
      );
    }
    if (topology.hasConfigShard) {
      log.warn(
        "Detected 'config' entry in config.shards; if running MongoDB 7.0/8.0 Embedded Config Shard mode, ensure user collections are migrated to dedicated shards before logical sharded restore.",
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

    // 3. Back up Config Server and shards with bounded concurrency (small-RAM safety)
    const configsvrDirectUri = topology.configsvr?.host
      ? buildShardUri(config.mongo.uri, topology.configsvr.host)
      : null;

    const shardConcurrency = Math.min(
      MAX_CONCURRENT_SHARD_DUMPS,
      Math.max(1, topology.shards.length),
    );
    const boundedQueueSize = topology.shards.length > 1 ? 1 : config.storage.queueSize;

    log.info(
      {
        directConfigsvr: Boolean(configsvrDirectUri),
        shardsCount: topology.shards.length,
        concurrency: shardConcurrency,
      },
      "Backing up Config Server and shard replica sets...",
    );

    // For real MongoDB 8.0 CSRS dumps, dump `database: "config"` with transient/resharding
    // collections excluded and capture pre-dump CSRS oplog anchor so bridging slices cover
    // [configsvrPreTs, clusterTimeTs] cleanly without triggering mongodump's resharding check.
    const useSafeCsrsConfigDump = Boolean(configsvrDirectUri) && backupRunner === runBackup;
    const configsvrPreBounds = useSafeCsrsConfigDump
      ? await getOplogBounds(configsvrDirectUri!, { phase: "pre", createMongoClient })
      : null;
    if (useSafeCsrsConfigDump && !configsvrPreBounds?.newestTs) {
      throw new Error(
        "Failed to determine pre-backup CSRS oplog anchor; refusing to produce an unanchored sharded backup",
      );
    }

    const configsvrConfig: AppConfig = {
      ...config,
      storage: {
        ...config.storage,
        prefix: `${config.storage.prefix}/configsvr`,
        queueSize: boundedQueueSize,
      },
      mongo: {
        ...config.mongo,
        uri: configsvrDirectUri ?? config.mongo.uri,
        database: useSafeCsrsConfigDump ? "config" : configsvrDirectUri ? "*" : "config",
        excludeCollections: useSafeCsrsConfigDump
          ? CSRS_DUMP_EXCLUDE_COLLECTIONS
          : config.mongo.excludeCollections,
        oplog: useSafeCsrsConfigDump ? false : Boolean(configsvrDirectUri),
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

    const shardsPromise = runWithConcurrency(topology.shards, shardConcurrency, async (shard) => {
      log.info({ shardId: shard.id, host: shard.host }, "Backing up shard...");
      const shardUri = buildShardUri(config.mongo.uri, shard.host);

      const shardConfig: AppConfig = {
        ...config,
        storage: {
          ...config.storage,
          prefix: `${config.storage.prefix}/shards/${shard.id}`,
          queueSize: boundedQueueSize,
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

    const [configsvrManifest] = await Promise.all([configsvrPromise, shardsPromise]);

    // Determine common clusterTime >= configsvr and all shard oplogWindow.endTs
    let clusterTimeTs = parseOplogTimestamp(clusterTimeStr)!;
    const allEndTimestamps = [
      ...(configsvrManifest.oplogWindow?.endTs ? [configsvrManifest.oplogWindow.endTs] : []),
      ...Object.values(shardEndTimestamps),
    ];
    for (const endStr of allEndTimestamps) {
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

    const stamp = startedAt
      .toISOString()
      .replace(/[-:]/g, "")
      .replace(/\.\d{3}Z$/, "Z");

    // 4. Capture backup-scoped multi-slice bridging oplogs (never mutating PITR archiver state.json)
    const configsvrBridgeStartTs = configsvrManifest.oplogWindow?.endTs
      ? parseOplogTimestamp(configsvrManifest.oplogWindow.endTs)
      : (configsvrPreBounds?.newestTs ?? null);

    const configsvrOplogConfig: AppConfig = {
      ...configsvrConfig,
      mongo: { ...configsvrConfig.mongo, database: "*", excludeCollections: [], oplog: true },
    };

    const configsvrBridgePromise =
      configsvrDirectUri &&
      configsvrBridgeStartTs &&
      compareTimestamps(configsvrBridgeStartTs, clusterTimeTs) < 0
        ? captureBridgingSlices({
            config: configsvrOplogConfig,
            storage,
            logger: log.child({ component: "configsvr", phase: "bridge-oplog" }),
            signal: controller.signal,
            sinceTs: configsvrBridgeStartTs,
            untilTs: clusterTimeTs,
            keyPrefix: `${config.storage.prefix}/configsvr/_cluster/_bridging/${stamp}/`,
            createMongoClient,
          })
        : Promise.resolve<string[]>([]);

    const [configsvrBridgingSliceKeys] = await Promise.all([
      configsvrBridgePromise,
      runWithConcurrency(topology.shards, shardConcurrency, async (shard) => {
        const endStr = shardEndTimestamps[shard.id];
        const shardCfg = shardConfigs[shard.id];
        const endTs = endStr ? parseOplogTimestamp(endStr) : null;
        if (endTs && shardCfg && compareTimestamps(endTs, clusterTimeTs) < 0) {
          const keys = await captureBridgingSlices({
            config: shardCfg,
            storage,
            logger: log.child({ shardId: shard.id, phase: "bridge-oplog" }),
            signal: controller.signal,
            sinceTs: endTs,
            untilTs: clusterTimeTs,
            keyPrefix: `${config.storage.prefix}/shards/${shard.id}/_cluster/_bridging/${stamp}/`,
            createMongoClient,
          });
          if (keys.length > 0) {
            shardBackups[shard.id]!.bridgingSliceKey = keys[0];
            shardBackups[shard.id]!.bridgingSliceKeys = keys;
          }
        }
      }),
    ]);

    // 5. Restart cluster balancer if it was originally enabled
    if (balancerStopped && topology?.balancerEnabled) {
      await balancer.start();
      balancerStopped = false;
    }

    const completedAt = new Date();
    const durationMs = completedAt.getTime() - startedAt.getTime();
    const scope = scopeOf(config);
    const encrypted = Boolean(config.encryptionKey);
    const clusterKey = `${databasePrefix(scope)}${startedAt.toISOString().slice(0, 10)}/${stamp}.cluster${encrypted ? ".enc" : ""}`;

    const clusterMeta: ShardedClusterMeta = {
      clusterTime: clusterTimeStr,
      configsvr: {
        backupKey: configsvrManifest.key,
        manifestKey: manifestKey(configsvrManifest.key),
        host: topology.configsvr?.host || topology.mongosHost,
        ...(configsvrBridgingSliceKeys.length > 0
          ? {
              bridgingSliceKey: configsvrBridgingSliceKeys[0],
              bridgingSliceKeys: configsvrBridgingSliceKeys,
            }
          : {}),
      },
      shards: shardBackups,
    };

    // Persist `.cluster[.enc]` payload with real encryption & real SHA-256 digest
    const rawMetaBuf = Buffer.from(JSON.stringify(clusterMeta, null, 2), "utf8");
    let clusterSha256: string;
    let clusterEncryption = configsvrManifest.encryption;

    if (encrypted && config.encryptionKey && typeof storage.upload === "function") {
      const encryptor = createEncryptor(config.encryptionKey);
      const digest = new DigestStream();
      const body = new PassThrough();
      await settleAll(controller, [
        storage.upload(clusterKey, body, {
          contentType: "application/octet-stream",
          signal: controller.signal,
        }),
        runStreamPipeline([Readable.from([rawMetaBuf]), encryptor.stream, digest, body], {
          signal: controller.signal,
        }),
      ]);
      clusterSha256 = digest.digest();
      clusterEncryption = encryptor.envelope();
    } else if (typeof storage.upload === "function") {
      const digest = new DigestStream();
      const body = new PassThrough();
      await settleAll(controller, [
        storage.upload(clusterKey, body, {
          contentType: "application/json",
          signal: controller.signal,
        }),
        runStreamPipeline([Readable.from([rawMetaBuf]), digest, body], {
          signal: controller.signal,
        }),
      ]);
      clusterSha256 = digest.digest();
    } else {
      // Minimal mock storage fallback (unit tests implementing only putJson/getJson)
      await storage.putJson(clusterKey, clusterMeta, { immutable: true });
      clusterSha256 = createHash("sha256").update(rawMetaBuf).digest("hex");
    }

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
      sha256: clusterSha256,
      compression: "none",
      encryption: clusterEncryption,
      oplog: true,
      excludedCollections: [],
      collections: {},
      sharding: clusterMeta,
    };

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
 * Resolves a consistent sharded cluster PITR plan across configsvr and all shards,
 * anchored explicitly to the exact child base backups referenced by the selected `.cluster` manifest.
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
  const clusterTs = (selectedMeta.clusterTime
    ? parseOplogTimestamp(selectedMeta.clusterTime)
    : null) ?? {
    t:
      Math.floor(
        new Date(selectedManifest.completedAt || selectedManifest.createdAt).getTime() / 1000,
      ) >>> 0,
    i: 0,
  };

  let configsvrPlan: PitrPlan | null = null;
  if (configsvrManifest?.oplog) {
    configsvrPlan = await resolvePitrPlan(
      storage,
      { prefix: `${scope.prefix}/configsvr`, database: "*" },
      targetTs,
      {
        engine: "logical",
        baseKey: selectedMeta.configsvr.backupKey,
      },
    );
  } else if (selectedMeta.configsvr.backupKey && selectedMeta.configsvr.host) {
    // CSRS was dumped via `database: "config"` (oplog: false) + bridging slices up to clusterTime;
    // collect contiguous continuous PITR slices from `{ prefix: "<prefix>/configsvr", database: "*" }`
    // anchored at `clusterTs`.
    const cfgScope: BackupScope = { prefix: `${scope.prefix}/configsvr`, database: "*" };
    const allCfgSlices = (await listOplogSlices(storage, cfgScope)).filter((s) => s.complete);
    const candidateSlices = allCfgSlices.filter(
      (s) =>
        compareTimestamps(s.endTs, clusterTs) >= 0 && compareTimestamps(s.startTs, targetTs) <= 0,
    );
    const verifiedSlices = [];
    let expectedPrev = clusterTs;
    let lastCovered = clusterTs;
    for (const slice of candidateSlices) {
      const sManifest = await readManifest(storage, slice.key);
      if (sManifest?.oplogWindow?.gapDetected) {
        throw new Error(`PITR timeline gap detected on configsvr before slice ${slice.key}`);
      }
      const prevEnd = sManifest?.oplogWindow?.previousEndTs
        ? parseOplogTimestamp(sManifest.oplogWindow.previousEndTs)
        : null;
      if (prevEnd) {
        if (compareTimestamps(prevEnd, expectedPrev) > 0) {
          throw new Error(`PITR timeline gap detected on configsvr before slice ${slice.key}`);
        }
      } else if (sManifest?.oplogWindow && compareTimestamps(slice.startTs, expectedPrev) > 0) {
        throw new Error(`PITR timeline gap detected on configsvr before slice ${slice.key}`);
      }
      verifiedSlices.push({ ...slice, manifest: sManifest });
      if (compareTimestamps(slice.endTs, expectedPrev) > 0) expectedPrev = slice.endTs;
      const covEnd = sManifest?.oplogWindow?.coverageEndTs
        ? parseOplogTimestamp(sManifest.oplogWindow.coverageEndTs)
        : null;
      const bestEnd = covEnd && compareTimestamps(covEnd, slice.endTs) > 0 ? covEnd : slice.endTs;
      if (compareTimestamps(bestEnd, lastCovered) > 0) lastCovered = bestEnd;
    }
    if (compareTimestamps(lastCovered, targetTs) < 0) {
      const nextSlice = allCfgSlices.find((s) => compareTimestamps(s.startTs, targetTs) > 0);
      if (nextSlice) {
        const nextManifest = await readManifest(storage, nextSlice.key);
        const nextPrevEnd = nextManifest?.oplogWindow?.previousEndTs
          ? parseOplogTimestamp(nextManifest.oplogWindow.previousEndTs)
          : null;
        if (
          !nextManifest?.oplogWindow?.gapDetected &&
          nextPrevEnd &&
          compareTimestamps(nextPrevEnd, expectedPrev) <= 0
        ) {
          lastCovered = targetTs;
        }
      }
    }
    if (compareTimestamps(lastCovered, targetTs) < 0 && typeof storage.getJson === "function") {
      const stateObj = await storage
        .getJson<{ coverageEndTs?: string; lastTs?: string; gapDetected?: boolean }>(
          pitrStateKey(cfgScope),
        )
        .catch(() => null);
      const stateCov = stateObj?.data?.coverageEndTs
        ? parseOplogTimestamp(stateObj.data.coverageEndTs)
        : null;
      const stateLast = stateObj?.data?.lastTs ? parseOplogTimestamp(stateObj.data.lastTs) : null;
      if (
        !stateObj?.data?.gapDetected &&
        stateCov &&
        (!stateLast || compareTimestamps(stateLast, expectedPrev) <= 0) &&
        compareTimestamps(stateCov, lastCovered) > 0
      ) {
        lastCovered = stateCov;
      }
    }
    if (
      compareTimestamps(targetTs, clusterTs) > 0 &&
      verifiedSlices.length === 0 &&
      compareTimestamps(lastCovered, targetTs) < 0
    ) {
      throw new Error(
        `Target timestamp ${formatOplogTimestamp(targetTs)} is newer than sharded backup clusterTime (${formatOplogTimestamp(clusterTs)}), but no configsvr oplog slices are available to bridge the window`,
      );
    }
    if (compareTimestamps(lastCovered, targetTs) < 0) {
      throw new Error(
        `PITR oplog coverage on configsvr ends at ${formatOplogTimestamp(lastCovered)}, which is before target ${formatOplogTimestamp(targetTs)}`,
      );
    }
    configsvrPlan = {
      baseBackup: {
        key: selectedMeta.configsvr.backupKey,
        size: configsvrManifest?.size ?? 0,
        createdAt: new Date(configsvrManifest?.createdAt ?? selectedManifest.createdAt),
        encrypted: Boolean(configsvrManifest?.encryption),
        complete: true,
      },
      baseManifest: configsvrManifest,
      baseEndTs: clusterTs,
      slices: verifiedSlices,
      lastCoveredTs: lastCovered,
      targetTs,
    };
  }

  const shardPlans: Record<string, PitrPlan> = {};
  for (const [shardId, shard] of Object.entries(selectedMeta.shards)) {
    if (shard.status !== "success" || !shard.backupKey) {
      throw new Error(`Cannot perform sharded PITR: shard "${shardId}" has incomplete base backup`);
    }
    shardPlans[shardId] = await resolvePitrPlan(
      storage,
      { prefix: `${scope.prefix}/shards/${shardId}`, database: "*" },
      targetTs,
      {
        engine: "logical",
        baseKey: shard.backupKey,
      },
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

function collectOrderedSliceKeys(
  bridgingKeys?: string[],
  legacyBridgingKey?: string,
  pitrSlices?: Array<{ key: string }>,
): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  const add = (k?: string) => {
    if (k && !seen.has(k)) {
      seen.add(k);
      out.push(k);
    }
  };
  for (const k of bridgingKeys ?? []) add(k);
  add(legacyBridgingKey);
  for (const s of pitrSlices ?? []) add(s.key);
  return out;
}

/**
 * Restores a sharded cluster backup, with optional Point-in-Time Recovery (PITR),
 * programmatic CSRS restore, zero-disk streaming oplog replay, and
 * cross-shard distributed transaction reconciliation.
 */
export async function restoreShardedCluster({
  config,
  storage,
  logger,
  ref = "latest",
  pitr,
  signal,
  drop = false,
  preVerify = drop,
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

  // Fail closed on HMAC manifest verification
  const keyRing = resolveKeyRing(config);
  const isSigned = Boolean(manifest.signature?.hmac || manifest.encryption?.manifestHmac);
  if (isSigned) {
    if (keyRing.length === 0) {
      throw new Error(
        `Sharded cluster manifest for ${resolvedKey} is signed with HMAC, but ENCRYPTION_KEY / key ring is not configured`,
      );
    }
    verifyManifest(keyRing, manifest);
  } else if (manifest.encryption && keyRing.length === 0) {
    throw new Error(
      `Sharded cluster backup ${resolvedKey} is encrypted, but ENCRYPTION_KEY is not set`,
    );
  }

  // Fail fast if any shard in the backup failed or is missing its backupKey
  for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
    if (shard.status !== "success" || !shard.backupKey) {
      throw new Error(
        `Cannot restore sharded cluster backup ${resolvedKey}: shard "${shardId}" has incomplete or failed status ("${shard.status}")`,
      );
    }
  }

  const client = createMongoClient(config.mongo.uri);
  const balancer = new BalancerController(client, log);
  let balancerStopped = false;
  let balancerWasEnabled = false;
  let replayedSlices = 0;
  let oplogStats: ShardedRestoreResult["oplogStats"];

  try {
    await client.connect();

    // 1. Discover target cluster topology and quiesce balancer if enabled
    const targetTopology = await discoverClusterTopology(client).catch(() => null);
    balancerWasEnabled = Boolean(targetTopology?.balancerEnabled);
    if (balancerWasEnabled) {
      balancerStopped = await balancer.stopAndDrain(30_000);
    } else if (!targetTopology) {
      try {
        await client.db("admin").command({ balancerStop: 1 });
        balancerStopped = true;
        balancerWasEnabled = true;
      } catch {}
    }

    const targetShardMap = new Map((targetTopology?.shards ?? []).map((s) => [s.id, s.host]));
    for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
      if (!targetShardMap.has(shardId)) {
        throw new Error(
          `Target cluster topology is missing mapping for shard "${shardId}" (source host: ${shard.host ?? "unknown"}). Refusing to fall back to source shard host.`,
        );
      }
    }

    // Pre-verify ALL child archives, bridging slices, and PITR slices BEFORE dropping any user
    // databases or mutating CSRS/shards so a corrupt/missing object never leaves the cluster torn.
    const configsvrSliceKeys = collectOrderedSliceKeys(
      clusterMeta.configsvr.bridgingSliceKeys,
      clusterMeta.configsvr.bridgingSliceKey,
      pitrPlan?.configsvrPlan?.slices,
    );
    const shardSliceKeyMap = new Map<string, string[]>();
    let totalSlicesToReplay = configsvrSliceKeys.length;
    for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
      const keys = collectOrderedSliceKeys(
        shard.bridgingSliceKeys,
        shard.bridgingSliceKey,
        pitrPlan?.shardPlans[shardId]?.slices,
      );
      shardSliceKeyMap.set(shardId, keys);
      totalSlicesToReplay += keys.length;
    }

    const preVerifiedAllChildren =
      Boolean(preVerify) &&
      typeof storage.download === "function" &&
      restoreRunner === restoreBackup;
    if (preVerifiedAllChildren) {
      const allKeysToVerify = new Set<string>();
      if (clusterMeta.configsvr?.backupKey) {
        allKeysToVerify.add(clusterMeta.configsvr.backupKey);
      }
      for (const sKey of configsvrSliceKeys) {
        allKeysToVerify.add(sKey);
      }
      for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
        if (shard.backupKey) allKeysToVerify.add(shard.backupKey);
        for (const sKey of shardSliceKeyMap.get(shardId) ?? []) {
          allKeysToVerify.add(sKey);
        }
      }
      log.info(
        { objectsCount: allKeysToVerify.size, drop },
        "Pre-verifying all sharded child archives, bridging slices, and PITR slices before destructive restore",
      );
      for (const childKey of allKeysToVerify) {
        await verifyArchiveObject({ config, storage, logger: log, signal }, childKey);
      }
    }

    // If drop=true, drop existing user databases via mongos before restoring routing + shard data
    if (drop) {
      try {
        const listRes = (await client
          .db("admin")
          .command({ listDatabases: 1, nameOnly: true })) as {
          databases?: Array<{ name?: string }>;
        };
        if (Array.isArray(listRes?.databases)) {
          for (const dbInfo of listRes.databases) {
            const dbName = dbInfo?.name;
            if (dbName && !SYSTEM_DATABASES.has(dbName)) {
              log.info(
                { database: dbName },
                "Dropping existing user database before sharded restore",
              );
              await client
                .db(dbName)
                .dropDatabase()
                .catch(() => {});
            }
          }
        }
      } catch {
        // Non-fatal if target mongos does not support listDatabases or mock doesn't return databases
      }
    }

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

    // 2. Restore Config Server routing metadata
    // In production (real restoreBackup + direct CSRS host), use programmatic streaming CsrsRestorer
    // to preserve target config.system.sessions UUID and balancer/automerge settings with 0 B disk.
    let backupSessionsUuidHex: string | null = null;
    const useProgrammaticCsrs =
      restoreRunner === restoreBackup &&
      Boolean(targetTopology?.configsvr?.host) &&
      typeof storage.download === "function";

    log.info(
      {
        configsvrKey: clusterMeta.configsvr.backupKey,
        directConfigsvr: Boolean(targetTopology?.configsvr?.host),
        programmaticCsrs: useProgrammaticCsrs,
      },
      "Restoring Config Server routing metadata...",
    );

    if (useProgrammaticCsrs) {
      const csrsRes = await new CsrsRestorer().restore({
        config,
        storage,
        backupKey: clusterMeta.configsvr.backupKey,
        targetConfigsvrUri,
        drop,
        logger: log.child({ target: "configsvr" }),
        signal,
        createMongoClient,
      });
      backupSessionsUuidHex = csrsRes.backupSessionsUuidHex;
    } else {
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
    }

    // 3. Restore base snapshot on every shard
    const restoredShards: string[] = [];
    const shardUris = new Map<string, string>();
    const shardBaseEndTs = new Map<string, OplogTimestamp | undefined>();

    for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
      const resolvedHost = targetShardMap.get(shardId);
      if (!resolvedHost) {
        throw new Error(`Target cluster topology is missing mapping for shard "${shardId}"`);
      }
      const targetShardUri = buildShardUri(config.mongo.uri, resolvedHost);
      shardUris.set(shardId, targetShardUri);
      const shardPitrPlan = pitrPlan?.shardPlans[shardId] ?? null;
      const shardBridgeKeys = collectOrderedSliceKeys(
        shard.bridgingSliceKeys,
        shard.bridgingSliceKey,
      );
      const hasPostBaseSlices =
        shardBridgeKeys.length > 0 || Boolean(shardPitrPlan && shardPitrPlan.slices.length > 0);

      log.info({ shardId, backupKey: shard.backupKey }, "Restoring shard base backup...");
      await restoreRunner(
        { config, storage, logger: log.child({ target: shardId }), signal },
        {
          ref: shard.backupKey,
          uri: targetShardUri,
          database: "*",
          drop,
          ...(preVerifiedAllChildren ? { preVerify: false } : {}),
          oplogReplay: true,
          oplogLimit: hasPostBaseSlices ? undefined : exclusiveLimit,
        },
      );

      if (shardPitrPlan?.baseEndTs) {
        shardBaseEndTs.set(shardId, shardPitrPlan.baseEndTs);
      } else {
        const sManifest = await readManifest(storage, shard.backupKey).catch(() => null);
        const endTs = sManifest?.oplogWindow?.endTs
          ? parseOplogTimestamp(sManifest.oplogWindow.endTs)
          : undefined;
        shardBaseEndTs.set(shardId, endTs ?? undefined);
      }

      restoredShards.push(shardId);
    }

    // 4. Streaming oplog replay across configsvr & all shards + cross-shard distributed txn reconciliation
    if (totalSlicesToReplay > 0 && effectiveTargetTs) {
      const activeNodes: Array<{
        id: string;
        client: MongoClient;
        applier: OplogApplier;
        sliceKeys: string[];
      }> = [];

      try {
        if (configsvrSliceKeys.length > 0) {
          const cfgClient = createMongoClient(targetConfigsvrUri);
          await cfgClient.connect();
          const cfgApplier = new OplogApplier({
            target: new MongoOplogTargetClient(cfgClient),
            workDir: config.workDir,
            logger: log.child({ target: "configsvr-oplog" }),
            endTs: effectiveTargetTs,
            filter: {
              nsInclude: targetTopology?.configsvr?.host ? configsvrNsInclude : undefined,
              sessionsUuidHex: backupSessionsUuidHex ?? undefined,
            },
            backupType: "logical",
          });
          activeNodes.push({
            id: "configsvr",
            client: cfgClient,
            applier: cfgApplier,
            sliceKeys: configsvrSliceKeys,
          });
        }

        for (const shardId of restoredShards) {
          const sliceKeys = shardSliceKeyMap.get(shardId) ?? [];
          if (sliceKeys.length === 0) continue;
          const shardClient = createMongoClient(shardUris.get(shardId)!);
          await shardClient.connect();
          const shardApplier = new OplogApplier({
            target: new MongoOplogTargetClient(shardClient),
            workDir: config.workDir,
            logger: log.child({ target: `${shardId}-oplog` }),
            startExclusiveTs: shardBaseEndTs.get(shardId),
            endTs: effectiveTargetTs,
            backupType: "logical",
          });
          activeNodes.push({
            id: shardId,
            client: shardClient,
            applier: shardApplier,
            sliceKeys,
          });
        }

        // Stream all slices through each node's OplogApplier in parallel across nodes
        await Promise.all(
          activeNodes.map(async (node) => {
            for (const sliceKey of node.sliceKeys) {
              await streamOplogSlice({ config, storage, signal }, sliceKey, (stream) =>
                node.applier.applyStream(stream, signal),
              );
            }
          }),
        );
        replayedSlices = totalSlicesToReplay;

        // Cross-shard distributed transaction reconciliation:
        // Union all committed distributed txns across shards/configsvr, then resolve
        // any buffered prepared/uncommitted txns on every node.
        const clusterCommits = new Map<string, OplogTimestamp | null>();
        for (const node of activeNodes) {
          const { committedDistTxns } = node.applier.getTxnLeftovers();
          for (const committed of committedDistTxns) {
            clusterCommits.set(committed.id, committed.commitTs);
          }
        }
        for (const node of activeNodes) {
          const { uncommittedIds } = node.applier.getTxnLeftovers();
          if (uncommittedIds.length > 0) {
            await node.applier.handleUncommittedTxn(clusterCommits);
          }
          await node.applier.buildPendingIndexes();
          node.applier.assertComplete();
        }
        oplogStats = mergeOplogStats(activeNodes.map((n) => n.applier.getStats()));
        log.info(
          {
            appliedEntries: oplogStats.appliedEntries,
            skippedEntries: oplogStats.skippedEntries,
            skipBreakdown: oplogStats.skipBreakdown,
            indexesBuilt: oplogStats.indexesBuilt,
            uncommittedTxns: oplogStats.uncommittedTxns,
            partialTxns: oplogStats.partialTxns,
          },
          "Completed sharded cluster oplog replay and distributed transaction reconciliation",
        );
      } finally {
        await Promise.allSettled(
          activeNodes.map(async (node) => {
            await node.applier.dispose().catch(() => {});
            await node.client.close().catch(() => {});
          }),
        );
      }
    }

    // 5. Flush routing metadata on mongos AND every shard primary
    await client
      .db("admin")
      .command({ flushRouterConfig: 1 })
      .catch(() => {});
    for (const shardUri of shardUris.values()) {
      const sClient = createMongoClient(shardUri);
      try {
        await sClient.connect();
        await sClient
          .db("admin")
          .command({ flushRouterConfig: 1 })
          .catch(() => {});
      } catch {
        // Non-fatal
      } finally {
        await sClient.close().catch(() => {});
      }
    }

    if (balancerStopped && balancerWasEnabled) {
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
      ...(oplogStats ? { oplogStats } : {}),
    };
  } finally {
    if (balancerStopped && balancerWasEnabled) {
      await balancer.start();
    }
    await client.close().catch(() => {});
  }
}
