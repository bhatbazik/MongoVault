import { createHash, randomUUID } from "node:crypto";
import { PassThrough, Readable } from "node:stream";
import { createGunzip } from "node:zlib";
import { BSON, Binary, Long, MongoClient } from "mongodb";
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
  markPitrRestore,
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
  const settled = await Promise.allSettled(workers);
  const firstRejection = settled.find((r): r is PromiseRejectedResult => r.status === "rejected");
  if (firstRejection) {
    throw firstRejection.reason;
  }
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

  const trimmed = baseUri.trim();
  const isSrv = /^mongodb\+srv:\/\//i.test(trimmed);
  const match = /^mongodb(?:\+srv)?:\/\/(?:([^@/]+)@)?[^/?]+(\/[^?]*)?(?:\?(.*))?$/i.exec(trimmed);
  const authPrefix = match?.[1] ? `${match[1]}@` : "";
  const pathDb = match?.[2] ? match[2].replace(/^\/+/, "").trim() : "";
  const rawQuery = match?.[3] ?? "";

  const params = new URLSearchParams(rawQuery);
  if (authPrefix && pathDb && !params.has("authSource")) {
    params.set("authSource", pathDb);
  }
  if (isSrv && !params.has("tls") && !params.has("ssl")) {
    params.set("tls", "true");
  }
  if (rsName) {
    params.set("replicaSet", rsName);
  } else {
    params.delete("replicaSet");
  }
  // Remove SRV-only or conflicting options when connecting directly to shard seed hosts
  params.delete("directConnection");
  params.delete("loadBalanced");
  params.delete("srvMaxHosts");
  params.delete("srvServiceName");

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
  public stopIssued = false;

  constructor(
    private readonly client: MongoClient,
    private readonly logger?: Logger,
  ) {}

  async stopAndDrain(timeoutMs = 30_000): Promise<boolean> {
    this.logger?.info("Stopping balancer and waiting for active chunk migrations to complete...");
    this.stopIssued = true;
    try {
      await this.client.db("admin").command({ balancerStop: 1, maxTimeMS: timeoutMs });
    } catch (err) {
      const code = (err as { code?: unknown })?.code;
      const codeName = (err as { codeName?: unknown })?.codeName;
      if (code === 50 || codeName === "MaxTimeMSExpired") {
        throw new ActiveChunkMigrationError(
          `balancerStop timed out after ${Math.round(timeoutMs / 1000)}s waiting for active chunk migration to drain. Aborting to prevent cross-shard inconsistency.`,
        );
      }
      this.stopIssued = false;
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
      this.stopIssued = false;
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
    targetShardIds,
    createMongoClient = (u) => new MongoClient(u),
  }: {
    config: AppConfig;
    storage: StorageClient;
    backupKey: string;
    targetConfigsvrUri: string;
    drop: boolean;
    logger: Logger;
    signal?: AbortSignal;
    targetShardIds?: Set<string>;
    createMongoClient?: (uri: string) => MongoClient;
  }): Promise<{ restoredDocs: number; backupSessionsUuidHex: string | null }> {
    const manifest = await readManifest(storage, backupKey);
    if (config.requireEncryption && (!manifest || !manifest.encryption)) {
      throw new Error(
        `REQUIRE_ENCRYPTION is enabled, but config server backup ${backupKey} lacks an encrypted manifest`,
      );
    }
    const keyRing = resolveKeyRing(config);
    const isSigned = Boolean(manifest?.signature?.hmac || manifest?.encryption?.manifestHmac);
    const mustVerify =
      isSigned ||
      Boolean(manifest?.encryption) ||
      keyRing.length > 0 ||
      Boolean(config.requireEncryption);
    if (mustVerify && manifest) {
      if ((isSigned || manifest.encryption || config.requireEncryption) && keyRing.length === 0) {
        throw new Error(
          `Config server manifest for ${backupKey} requires HMAC verification, but ENCRYPTION_KEY is not configured`,
        );
      }
      verifyManifest(keyRing, manifest, {
        requireSigned: Boolean(config.requireEncryption || manifest.encryption),
      });
    }

    // Verify CSRS archive checksum & size BEFORE mutating target CSRS collections
    if (
      manifest &&
      (manifest.sha256 || manifest.size !== undefined) &&
      typeof storage.download === "function"
    ) {
      await verifyArchiveObject({ config, storage, logger, signal }, backupKey);
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
      const targetSessionsUuidHex = binaryToHex(targetSessionsUuid);

      if (drop) {
        await configDb.collection("databases").deleteMany({});
        await configDb.collection("tags").deleteMany({});
        await configDb
          .collection("collections")
          .deleteMany({ _id: { $ne: "config.system.sessions" } } as unknown as never);
        await configDb
          .collection("chunks")
          .deleteMany(targetSessionsUuid ? { uuid: { $ne: targetSessionsUuid } } : {});
        await configDb
          .collection("settings")
          .deleteMany({ _id: { $nin: ["balancer", "automerge"] } } as unknown as never);
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

      const cleanupStaleBackupSessionsChunks = async () => {
        if (
          backupSessionsUuid &&
          (!targetSessionsUuidHex || backupSessionsUuidHex !== targetSessionsUuidHex)
        ) {
          await configDb.collection("chunks").deleteMany({ uuid: backupSessionsUuid });
        }
      };

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
                  await cleanupStaleBackupSessionsChunks();
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
              if (targetShardIds && targetShardIds.size > 0) {
                if (
                  collection === "databases" &&
                  typeof doc.primary === "string" &&
                  doc.primary !== "config" &&
                  !targetShardIds.has(doc.primary)
                ) {
                  throw new Error(
                    `CSRS backup references unknown primary shard "${doc.primary}" for database "${String(doc._id)}"`,
                  );
                }
                if (
                  collection === "chunks" &&
                  typeof doc.shard === "string" &&
                  doc.shard !== "config" &&
                  !targetShardIds.has(doc.shard)
                ) {
                  throw new Error(`CSRS backup references unknown chunk shard "${doc.shard}"`);
                }
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
          await cleanupStaleBackupSessionsChunks();
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
  onSliceUploaded,
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
  onSliceUploaded?: (key: string) => void;
  activeTxnWaitTimeoutMs?: number;
}): Promise<string[]> {
  const keys: string[] = [];
  let cursorTs = sinceTs;
  let blockedSinceMs: number | null = null;
  let confirmedQuietUpToTarget = false;

  for (let i = 0; i < MAX_BRIDGE_SLICE_ITERATIONS; i++) {
    if (compareTimestamps(cursorTs, untilTs) >= 0) {
      confirmedQuietUpToTarget = true;
      break;
    }
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

      // Verify whether the window (cursorTs, untilTs] was genuinely quiet at majority commit point
      // or whether replication/majority commit is lagging behind `untilTs` or oplog rolled over.
      const checkClient = createMongoClient(config.mongo.uri);
      let majorityTs: OplogTimestamp | null;
      let oldestTs: OplogTimestamp | null;
      let newestTs: OplogTimestamp | null;
      try {
        await checkClient.connect();
        const adminDb = typeof checkClient.db === "function" ? checkClient.db("admin") : null;
        const localDb = typeof checkClient.db === "function" ? checkClient.db("local") : null;
        const oplogCol =
          localDb && typeof localDb.collection === "function"
            ? localDb.collection("oplog.rs")
            : null;
        const [replStatus, oldestDoc, newestDoc] = await Promise.all([
          adminDb && typeof adminDb.command === "function"
            ? adminDb.command({ replSetGetStatus: 1 }).catch(() => null)
            : Promise.resolve(null),
          oplogCol && typeof oplogCol.find === "function"
            ? oplogCol
                .find({}, { projection: { ts: 1 } })
                .sort({ $natural: 1 })
                .limit(1)
                .next()
                .catch(() => null)
            : Promise.resolve(null),
          oplogCol && typeof oplogCol.find === "function"
            ? oplogCol
                .find({}, { projection: { ts: 1 } })
                .sort({ $natural: -1 })
                .limit(1)
                .next()
                .catch(() => null)
            : Promise.resolve(null),
        ]);
        oldestTs = fromBsonTimestamp((oldestDoc as { ts?: unknown } | null)?.ts);
        newestTs = fromBsonTimestamp((newestDoc as { ts?: unknown } | null)?.ts);
        const committed = fromBsonTimestamp(
          (replStatus as { optimes?: { lastCommittedOpTime?: { ts?: unknown } } } | null)?.optimes
            ?.lastCommittedOpTime?.ts,
        );
        const applied = fromBsonTimestamp(
          (replStatus as { optimes?: { appliedOpTime?: { ts?: unknown } } } | null)?.optimes
            ?.appliedOpTime?.ts,
        );
        majorityTs =
          committed && applied
            ? compareTimestamps(committed, applied) <= 0
              ? committed
              : applied
            : (committed ?? applied);
      } finally {
        await checkClient.close().catch(() => {});
      }

      if (oldestTs && compareTimestamps(oldestTs, cursorTs) > 0) {
        throw new Error(
          `Bridging oplog gap detected: local.oplog.rs rolled over to ${formatOplogTimestamp(oldestTs)} past ${formatOplogTimestamp(cursorTs)}`,
        );
      }
      // On a quiet shard, `majorityTs` may be < `untilTs` (because `untilTs` came from another
      // shard or mongos $clusterTime), while `majorityTs >= newestTs` and `majorityTs >= cursorTs`
      // proves all local oplog writes on this shard are majority-committed and archived.
      const shardCaughtUpLocally =
        majorityTs &&
        newestTs &&
        compareTimestamps(majorityTs, newestTs) >= 0 &&
        compareTimestamps(majorityTs, cursorTs) >= 0;
      if (majorityTs && (compareTimestamps(majorityTs, untilTs) >= 0 || shardCaughtUpLocally)) {
        confirmedQuietUpToTarget = true;
        break;
      }

      if (blockedSinceMs === null) blockedSinceMs = Date.now();
      if (Date.now() - blockedSinceMs >= activeTxnWaitTimeoutMs) {
        throw new Error(
          `Failed to capture bridging oplog slices up to ${formatOplogTimestamp(untilTs)}: majority commit point (${majorityTs ? formatOplogTimestamp(majorityTs) : "unknown"}) did not reach target within ${Math.round(activeTxnWaitTimeoutMs / 1000)}s`,
        );
      }
      await new Promise((r) => setTimeout(r, 250));
      continue;
    }

    blockedSinceMs = null;
    keys.push(sliceManifest.key);
    onSliceUploaded?.(sliceManifest.key);

    if (sliceManifest.oplogWindow?.gapDetected) {
      throw new Error(
        `Bridging oplog gap detected in slice ${sliceManifest.key} while bridging to ${formatOplogTimestamp(untilTs)}`,
      );
    }

    const covStr = sliceManifest.oplogWindow?.coverageEndTs;
    const covTs = covStr ? parseOplogTimestamp(covStr) : null;
    if (covTs && compareTimestamps(covTs, untilTs) >= 0) {
      confirmedQuietUpToTarget = true;
      break;
    }

    const endStr = sliceManifest.oplogWindow?.endTs;
    const nextTs = endStr ? parseOplogTimestamp(endStr) : null;
    if (!nextTs || compareTimestamps(nextTs, cursorTs) <= 0) {
      break;
    }
    cursorTs = nextTs;
  }

  if (!confirmedQuietUpToTarget && compareTimestamps(cursorTs, untilTs) < 0) {
    throw new Error(
      `Failed to capture bridging oplog slices up to ${formatOplogTimestamp(untilTs)}: stopped at ${formatOplogTimestamp(cursorTs)}`,
    );
  }

  return keys;
}

/**
 * Waits for any in-flight `prepared` distributed transactions on the given replica set
 * whose `lastWriteOpTime.ts` or `startOpTime.ts` is `<= boundaryTs` to conclude (commit or abort),
 * and ensures the node's majority-committed optime catches up to its latest oplog entry.
 * Returns the node's post-settle majority-committed timestamp (or null if unavailable).
 */
async function settleShardPreparedTxnsAndOptime({
  uri,
  boundaryTs,
  createMongoClient,
  logger: _logger,
  signal,
  timeoutMs = 15_000,
}: {
  uri: string;
  boundaryTs: OplogTimestamp;
  createMongoClient: (u: string) => MongoClient;
  logger: Logger;
  signal?: AbortSignal;
  timeoutMs?: number;
}): Promise<OplogTimestamp | null> {
  const shardClient = createMongoClient(uri);
  try {
    await shardClient.connect();
    const configDb = typeof shardClient.db === "function" ? shardClient.db("config") : null;
    const txCol =
      configDb && typeof configDb.collection === "function"
        ? configDb.collection("transactions")
        : null;
    const adminDb = typeof shardClient.db === "function" ? shardClient.db("admin") : null;
    const localDb = typeof shardClient.db === "function" ? shardClient.db("local") : null;
    const oplogCol =
      localDb && typeof localDb.collection === "function" ? localDb.collection("oplog.rs") : null;

    const deadline = Date.now() + timeoutMs;

    // 1. Wait for prepared 2PC transactions straddling boundaryTs to finish committing/aborting
    if (txCol && typeof txCol.find === "function") {
      while (!signal?.aborted) {
        const prepared = await txCol
          .find({ state: "prepared" }, { readConcern: { level: "local" } })
          .toArray();
        const straddling = prepared.filter((doc) => {
          const ts =
            fromBsonTimestamp((doc as { startOpTime?: { ts?: unknown } }).startOpTime?.ts) ??
            fromBsonTimestamp((doc as { lastWriteOpTime?: { ts?: unknown } }).lastWriteOpTime?.ts);
          return ts !== null && compareTimestamps(ts, boundaryTs) <= 0;
        });
        if (straddling.length === 0) break;
        if (Date.now() >= deadline) {
          throw new Error(
            `Timed out waiting for ${straddling.length} prepared distributed transaction(s) at or before ${formatOplogTimestamp(boundaryTs)} to settle`,
          );
        }
        await new Promise((r) => setTimeout(r, 100));
      }
    }

    // 2. Wait briefly for majority-committed optime to catch up to the node's own newest oplog entry
    let latestMajorityTs: OplogTimestamp | null = null;
    let latestNewestTs: OplogTimestamp | null = null;
    if (adminDb && typeof adminDb.command === "function") {
      const catchupDeadline = Math.min(deadline, Date.now() + 5_000);
      while (!signal?.aborted) {
        const [replStatus, newestDoc] = await Promise.all([
          adminDb.command({ replSetGetStatus: 1 }).catch(() => null) as Promise<{
            optimes?: {
              lastCommittedOpTime?: { ts?: unknown };
              appliedOpTime?: { ts?: unknown };
            };
          } | null>,
          oplogCol && typeof oplogCol.find === "function"
            ? oplogCol
                .find({}, { projection: { ts: 1 } })
                .sort({ $natural: -1 })
                .limit(1)
                .next()
                .catch(() => null)
            : Promise.resolve(null),
        ]);

        const committed = fromBsonTimestamp(replStatus?.optimes?.lastCommittedOpTime?.ts);
        const applied = fromBsonTimestamp(replStatus?.optimes?.appliedOpTime?.ts);
        const majorityTs =
          committed && applied
            ? compareTimestamps(committed, applied) <= 0
              ? committed
              : applied
            : (committed ?? applied);
        if (majorityTs) {
          latestMajorityTs = majorityTs;
        }
        const newestTs = fromBsonTimestamp((newestDoc as { ts?: unknown } | null)?.ts);
        latestNewestTs = newestTs;
        if (
          !newestTs ||
          !majorityTs ||
          compareTimestamps(majorityTs, newestTs) >= 0 ||
          Date.now() >= catchupDeadline
        ) {
          break;
        }
        await new Promise((r) => setTimeout(r, 50));
      }
    }

    if (
      latestMajorityTs &&
      latestNewestTs &&
      compareTimestamps(latestMajorityTs, latestNewestTs) < 0 &&
      compareTimestamps(latestMajorityTs, boundaryTs) < 0
    ) {
      throw new Error(
        `Majority commit point (${formatOplogTimestamp(latestMajorityTs)}) did not catch up to boundary ${formatOplogTimestamp(boundaryTs)} (newest oplog: ${formatOplogTimestamp(latestNewestTs)})`,
      );
    }

    return latestMajorityTs;
  } finally {
    await shardClient.close().catch(() => {});
  }
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
  const uploadedArtifacts = new Set<string>();
  const inFlightChildTasks: Array<Promise<unknown>> = [];
  const trackArtifact = (key: string | undefined) => {
    if (!key) return;
    uploadedArtifacts.add(key);
    uploadedArtifacts.add(manifestKey(key));
  };

  if (config.requireEncryption && !config.encryptionKey) {
    throw new Error(
      "Refusing to create unencrypted sharded cluster backup: REQUIRE_ENCRYPTION is enabled but ENCRYPTION_KEY is not configured",
    );
  }
  if (config.mongo.database && config.mongo.database !== "*" && config.mongo.database !== "all") {
    throw new Error(
      `Sharded cluster backup requires cluster-wide scope (database="*" or "all"), got "${config.mongo.database}"`,
    );
  }
  if (config.mongo.excludeCollections && config.mongo.excludeCollections.length > 0) {
    throw new Error(
      "Sharded cluster backup does not support excludeCollections; cluster backups must be full-cluster",
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
  let shouldRestartBalancer = false;
  let topology: ClusterTopology;

  try {
    await client.connect();
    topology = await discoverClusterTopology(client);

    if (!topology.isSharded) {
      throw new Error(
        "Connected MongoDB instance is not a sharded cluster (mongos). Topology check returned standalone/replica set.",
      );
    }

    if (topology.hasConfigShard && (backupRunner === runBackup || topology.shards.length === 0)) {
      throw new Error(
        "Embedded Config Shard mode (_id: 'config' in config.shards) is not supported by logical sharded backup because user databases hosted on the config shard would be omitted.",
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
    shouldRestartBalancer = Boolean(topology.balancerEnabled);
    if (topology.balancerEnabled) {
      balancerStopped = await balancer.stopAndDrain(30_000);
    } else if (topology.balancerRunning) {
      const bStatus = await waitForBalancerDisabled(client, 30_000);
      if (bStatus.inBalancerRound || bStatus.mode !== "off") {
        throw new ActiveChunkMigrationError(
          `Active chunk migration round did not conclude within 30s (mode: "${bStatus.mode}", inBalancerRound: ${bStatus.inBalancerRound}). Aborting backup.`,
        );
      }
    }

    // 2. Capture cluster-wide committed timestamp
    let initialClusterTs: OplogTimestamp | null = null;
    try {
      const pingRes = await client.db("admin").command({ ping: 1 });
      if (pingRes?.$clusterTime?.clusterTime) {
        initialClusterTs = fromBsonTimestamp(pingRes.$clusterTime.clusterTime);
      }
    } catch {
      // Ignore
    }

    // 3. Back up Config Server and shards with bounded concurrency (small-RAM safety)
    const configsvrDirectUri = topology.configsvr?.host
      ? buildShardUri(config.mongo.uri, topology.configsvr.host)
      : config.sharding?.configsvrUri;
    if (backupRunner === runBackup && !configsvrDirectUri) {
      throw new Error(
        "Failed to discover Config Server (CSRS) connection string via getShardMap/serverStatus; refusing to fall back to mongos dump.",
      );
    }

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
    })
      .then((manifest) => {
        trackArtifact(manifest.key);
        return manifest;
      })
      .catch((err) => {
        controller.abort(err);
        throw err;
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

        trackArtifact(shardManifest.key);
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
        controller.abort(shardError);
        throw shardError;
      }
    });

    inFlightChildTasks.push(
      configsvrPromise.catch(() => {}),
      shardsPromise.catch(() => {}),
    );
    const [configsvrManifest] = await Promise.all([configsvrPromise, shardsPromise]);

    // Verify shard topology did not change mid-backup (e.g. addShard / removeShard)
    const postTopology = await discoverClusterTopology(client).catch(() => null);
    if (postTopology?.isSharded) {
      const beforeIds = topology.shards.map((s) => s.id).sort();
      const afterIds = postTopology.shards.map((s) => s.id).sort();
      if (
        beforeIds.length !== afterIds.length ||
        beforeIds.some((id, idx) => id !== afterIds[idx])
      ) {
        throw new Error(
          `Sharded cluster topology changed during backup (before: [${beforeIds.join(", ")}], after: [${afterIds.join(", ")}]); aborting inconsistent cluster backup`,
        );
      }
    }

    // Determine common clusterTime >= configsvr, all shard oplogWindow.endTs, and postPing $clusterTime
    let clusterTimeTs: OplogTimestamp | null = initialClusterTs;
    const considerTs = (ts: OplogTimestamp | null) => {
      if (!ts) return;
      if (!clusterTimeTs || compareTimestamps(ts, clusterTimeTs) > 0) {
        clusterTimeTs = ts;
      }
    };

    try {
      const postPing = await client.db("admin").command({ ping: 1 });
      considerTs(fromBsonTimestamp(postPing?.$clusterTime?.clusterTime));
    } catch {
      // Ignore
    }

    const allEndTimestamps = [
      ...(configsvrManifest.oplogWindow?.endTs ? [configsvrManifest.oplogWindow.endTs] : []),
      ...Object.values(shardEndTimestamps),
    ];
    for (const endStr of allEndTimestamps) {
      considerTs(parseOplogTimestamp(endStr));
    }
    if (configsvrPreBounds?.newestTs) {
      considerTs(configsvrPreBounds.newestTs);
    }

    if (!clusterTimeTs) {
      if (backupRunner === runBackup) {
        throw new Error(
          "Failed to determine clusterTime from $clusterTime or child oplogWindow bounds; refusing to fabricate wall-clock clusterTime",
        );
      }
      clusterTimeTs = { t: Math.floor(Date.now() / 1000) >>> 0, i: 1 };
    }
    let finalClusterTimeTs: OplogTimestamp = clusterTimeTs;

    // Wait for any boundary-straddling prepared 2PC transactions across all shards to settle
    // and for majority-committed optimes to catch up before picking the final common clusterTime.
    // This guarantees that any cross-shard txn already committed inside one shard's base dump
    // also has its commitTransaction oplog entry landed <= clusterTimeTs on every other shard.
    if (backupRunner === runBackup) {
      const settleUris = [
        ...(configsvrDirectUri ? [configsvrDirectUri] : []),
        ...topology.shards.map((s) => buildShardUri(config.mongo.uri, s.host)),
      ];
      const settledOptimes = await Promise.all(
        settleUris.map((u) =>
          settleShardPreparedTxnsAndOptime({
            uri: u,
            boundaryTs: finalClusterTimeTs,
            createMongoClient,
            logger: log,
            signal: controller.signal,
          }),
        ),
      );
      for (const optTs of settledOptimes) {
        if (optTs && compareTimestamps(optTs, finalClusterTimeTs) > 0) {
          finalClusterTimeTs = optTs;
        }
      }
    }

    const clusterTimeStr = formatOplogTimestamp(finalClusterTimeTs);

    const ms = startedAt.getUTCMilliseconds();
    const msPart = ms > 0 ? `.${String(ms).padStart(3, "0")}` : "";
    const randSuffix = randomUUID().slice(0, 8);
    const stamp = `${startedAt.toISOString().slice(0, 19).replace(/[-:]/g, "")}${msPart}-${randSuffix}Z`;

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
      compareTimestamps(configsvrBridgeStartTs, finalClusterTimeTs) < 0
        ? captureBridgingSlices({
            config: configsvrOplogConfig,
            storage,
            logger: log.child({ component: "configsvr", phase: "bridge-oplog" }),
            signal: controller.signal,
            sinceTs: configsvrBridgeStartTs,
            untilTs: finalClusterTimeTs,
            keyPrefix: `${config.storage.prefix}/configsvr/_cluster/_bridging/${stamp}/`,
            createMongoClient,
            onSliceUploaded: (k) => trackArtifact(k),
          })
        : Promise.resolve<string[]>([]);

    const [configsvrBridgingSliceKeys] = await Promise.all([
      configsvrBridgePromise,
      runWithConcurrency(topology.shards, shardConcurrency, async (shard) => {
        const endStr = shardEndTimestamps[shard.id];
        const shardCfg = shardConfigs[shard.id];
        const endTs = endStr ? parseOplogTimestamp(endStr) : null;
        if (endTs && shardCfg && compareTimestamps(endTs, finalClusterTimeTs) < 0) {
          const keys = await captureBridgingSlices({
            config: shardCfg,
            storage,
            logger: log.child({ shardId: shard.id, phase: "bridge-oplog" }),
            signal: controller.signal,
            sinceTs: endTs,
            untilTs: finalClusterTimeTs,
            keyPrefix: `${config.storage.prefix}/shards/${shard.id}/_cluster/_bridging/${stamp}/`,
            createMongoClient,
            onSliceUploaded: (k) => trackArtifact(k),
          });
          if (keys.length > 0) {
            shardBackups[shard.id]!.bridgingSliceKey = keys[0];
            shardBackups[shard.id]!.bridgingSliceKeys = keys;
          }
        }
      }),
    ]);

    // 5. Restart cluster balancer if it was originally enabled
    if ((balancerStopped || balancer.stopIssued) && shouldRestartBalancer) {
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
    trackArtifact(clusterKey);
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
      database: "*",
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
  } catch (error) {
    controller.abort(error);
    // Wait for aborted/finishing sibling backup tasks to settle before removing uploaded artifacts
    // so a sibling finishing its upload concurrently cannot leave orphaned files behind.
    if (inFlightChildTasks.length > 0) {
      await Promise.allSettled(inFlightChildTasks);
    }
    if (uploadedArtifacts.size > 0 && typeof storage.remove === "function") {
      log.warn(
        { artifactCount: uploadedArtifacts.size },
        "Sharded cluster backup failed; cleaning up partial child archives and bridging slices",
      );
      await Promise.allSettled(
        [...uploadedArtifacts].map((artifactKey) => storage.remove(artifactKey).catch(() => {})),
      );
    }
    throw error;
  } finally {
    if ((balancerStopped || balancer.stopIssued) && shouldRestartBalancer) {
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

  const shardSettled = await Promise.allSettled(
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

  const [configsvrResult] = await Promise.allSettled([configsvrPromise]);
  const configsvr =
    configsvrResult.status === "fulfilled"
      ? configsvrResult.value
      : (logger.error({ err: configsvrResult.reason }, "configsvr PITR slice capture failed"),
        null);

  const shards: Record<string, BackupManifest | null> = {};
  for (let idx = 0; idx < topology.shards.length; idx++) {
    const shard = topology.shards[idx]!;
    const res = shardSettled[idx]!;
    if (res.status === "fulfilled") {
      shards[shard.id] = res.value[1];
    } else {
      logger.error({ shardId: shard.id, err: res.reason }, "Shard PITR slice capture failed");
      shards[shard.id] = null;
    }
  }

  return {
    configsvr,
    shards,
  };
}

async function resolveClusterAnchoredPitrPlan(
  storage: StorageClient,
  scope: BackupScope,
  label: string,
  baseBackupKey: string,
  baseManifest: BackupManifest | null,
  anchorTs: OplogTimestamp,
  targetTs: OplogTimestamp,
  fallbackCreatedAt: string,
): Promise<PitrPlan> {
  const allSlices = (await listOplogSlices(storage, scope)).filter((s) => s.complete);
  const candidateSlices = allSlices.filter(
    (s) => compareTimestamps(s.endTs, anchorTs) >= 0 && compareTimestamps(s.startTs, targetTs) <= 0,
  );
  const verifiedSlices = [];
  let expectedPrev = anchorTs;
  let lastCovered = anchorTs;
  for (const slice of candidateSlices) {
    const sManifest = await readManifest(storage, slice.key);
    if (sManifest?.oplogWindow?.gapDetected) {
      throw new Error(`PITR timeline gap detected on ${label} before slice ${slice.key}`);
    }
    const prevEnd = sManifest?.oplogWindow?.previousEndTs
      ? parseOplogTimestamp(sManifest.oplogWindow.previousEndTs)
      : null;
    if (prevEnd) {
      if (compareTimestamps(prevEnd, expectedPrev) > 0) {
        throw new Error(`PITR timeline gap detected on ${label} before slice ${slice.key}`);
      }
    } else if (sManifest?.oplogWindow && compareTimestamps(slice.startTs, expectedPrev) > 0) {
      throw new Error(`PITR timeline gap detected on ${label} before slice ${slice.key}`);
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
    const nextSlice = allSlices.find((s) => compareTimestamps(s.startTs, targetTs) > 0);
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
        pitrStateKey(scope),
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
    compareTimestamps(targetTs, anchorTs) > 0 &&
    verifiedSlices.length === 0 &&
    compareTimestamps(lastCovered, targetTs) < 0
  ) {
    throw new Error(
      `Target timestamp ${formatOplogTimestamp(targetTs)} is newer than sharded backup clusterTime (${formatOplogTimestamp(anchorTs)}), but no ${label} oplog slices are available to bridge the window`,
    );
  }
  if (compareTimestamps(lastCovered, targetTs) < 0) {
    throw new Error(
      `PITR oplog coverage on ${label} ends at ${formatOplogTimestamp(lastCovered)}, which is before target ${formatOplogTimestamp(targetTs)}`,
    );
  }
  return {
    baseBackup: {
      key: baseBackupKey,
      size: baseManifest?.size ?? 0,
      createdAt: new Date(baseManifest?.createdAt ?? fallbackCreatedAt),
      encrypted: Boolean(baseManifest?.encryption),
      complete: true,
    },
    baseManifest,
    baseEndTs: anchorTs,
    slices: verifiedSlices,
    lastCoveredTs: lastCovered,
    targetTs,
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
    configsvrPlan = await resolveClusterAnchoredPitrPlan(
      storage,
      { prefix: `${scope.prefix}/configsvr`, database: "*" },
      "configsvr",
      selectedMeta.configsvr.backupKey,
      configsvrManifest,
      clusterTs,
      targetTs,
      selectedManifest.createdAt,
    );
  }

  const shardPlans: Record<string, PitrPlan> = {};
  for (const [shardId, shard] of Object.entries(selectedMeta.shards)) {
    if (shard.status !== "success" || !shard.backupKey) {
      throw new Error(`Cannot perform sharded PITR: shard "${shardId}" has incomplete base backup`);
    }
    const shardScope: BackupScope = { prefix: `${scope.prefix}/shards/${shardId}`, database: "*" };
    try {
      shardPlans[shardId] = await resolvePitrPlan(storage, shardScope, targetTs, {
        engine: "logical",
        baseKey: shard.backupKey,
      });
    } catch (err) {
      const hasBridging = Boolean(shard.bridgingSliceKeys?.length || shard.bridgingSliceKey);
      if (!hasBridging) throw err;
      const shardManifest = await readManifest(storage, shard.backupKey);
      shardPlans[shardId] = await resolveClusterAnchoredPitrPlan(
        storage,
        shardScope,
        shardId,
        shard.backupKey,
        shardManifest,
        clusterTs,
        targetTs,
        selectedManifest.createdAt,
      );
    }
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
function isCommandNotFoundError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as { code?: unknown }).code;
  const codeName = (err as { codeName?: unknown }).codeName;
  return code === 59 || codeName === "CommandNotFound";
}

function isIgnorableCacheFlushError(err: unknown): boolean {
  if (isCommandNotFoundError(err)) return true;
  if (!err || typeof err !== "object") return false;
  const code = (err as { code?: unknown }).code;
  const codeName = (err as { codeName?: unknown }).codeName;
  return code === 445 || codeName === "MetadataRefreshCanceledDueToFCVTransition";
}

export async function restoreShardedCluster({
  config,
  storage,
  logger,
  ref = "latest",
  pitr,
  uri,
  parallelCollections,
  signal,
  drop = false,
  preVerify = true,
  skipPitrRestoreMarker = false,
  createMongoClient = (u) => new MongoClient(u),
  restoreRunner = restoreBackup,
}: ShardedRestoreOptions): Promise<ShardedRestoreResult> {
  const effectiveUri = uri ?? config.mongo.uri;
  const effectiveConfig: AppConfig =
    uri || parallelCollections !== undefined
      ? {
          ...config,
          mongo: {
            ...config.mongo,
            uri: effectiveUri,
            ...(parallelCollections !== undefined ? { parallelCollections } : {}),
          },
        }
      : config;
  const scope = scopeOf(effectiveConfig);
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
  if (effectiveConfig.requireEncryption && !manifest.encryption) {
    throw new Error(
      `REQUIRE_ENCRYPTION is enabled, but sharded cluster backup ${resolvedKey} lacks an encrypted manifest`,
    );
  }
  const keyRing = resolveKeyRing(effectiveConfig);
  const isSigned = Boolean(manifest.signature?.hmac || manifest.encryption?.manifestHmac);
  const mustVerify =
    isSigned ||
    Boolean(manifest.encryption) ||
    keyRing.length > 0 ||
    Boolean(effectiveConfig.requireEncryption);
  if (mustVerify) {
    if (
      (isSigned || manifest.encryption || effectiveConfig.requireEncryption) &&
      keyRing.length === 0
    ) {
      throw new Error(
        `Sharded cluster manifest for ${resolvedKey} requires HMAC verification, but ENCRYPTION_KEY / key ring is not configured`,
      );
    }
    verifyManifest(keyRing, manifest, {
      requireSigned: Boolean(effectiveConfig.requireEncryption || manifest.encryption),
    });
  }

  // Fail fast if any shard in the backup failed or is missing its backupKey
  for (const [shardId, shard] of Object.entries(clusterMeta.shards)) {
    if (shard.status !== "success" || !shard.backupKey) {
      throw new Error(
        `Cannot restore sharded cluster backup ${resolvedKey}: shard "${shardId}" has incomplete or failed status ("${shard.status}")`,
      );
    }
  }

  const client = createMongoClient(effectiveUri);
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
    } else if (targetTopology?.balancerRunning) {
      const bStatus = await waitForBalancerDisabled(client, 30_000);
      if (bStatus.inBalancerRound || bStatus.mode !== "off") {
        throw new ActiveChunkMigrationError(
          `Active chunk migration round did not conclude within 30s (mode: "${bStatus.mode}", inBalancerRound: ${bStatus.inBalancerRound}). Aborting restore.`,
        );
      }
    } else if (!targetTopology) {
      try {
        await client.db("admin").command({ balancerStop: 1 });
        balancerStopped = true;
        balancerWasEnabled = true;
        await waitForBalancerDisabled(client, 30_000);
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
      const adminDb = client.db("admin");
      const listRes =
        typeof adminDb.command === "function"
          ? ((await adminDb.command({ listDatabases: 1, nameOnly: true }).catch(() => null)) as {
              databases?: Array<{ name?: string }>;
            } | null)
          : null;
      if (Array.isArray(listRes?.databases)) {
        for (const dbInfo of listRes.databases) {
          const dbName = dbInfo?.name;
          if (dbName && !SYSTEM_DATABASES.has(dbName)) {
            log.info(
              { database: dbName },
              "Dropping existing user database before sharded restore",
            );
            await client.db(dbName).dropDatabase();
          }
        }
      }
    }

    const targetConfigsvrUri = targetTopology?.configsvr?.host
      ? buildShardUri(effectiveUri, targetTopology.configsvr.host)
      : effectiveUri;

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
        config: effectiveConfig,
        storage,
        backupKey: clusterMeta.configsvr.backupKey,
        targetConfigsvrUri,
        drop,
        logger: log.child({ target: "configsvr" }),
        signal,
        targetShardIds: new Set(targetShardMap.keys()),
        createMongoClient,
      });
      backupSessionsUuidHex = csrsRes.backupSessionsUuidHex;
    } else {
      await restoreRunner(
        { config: effectiveConfig, storage, logger: log.child({ target: "configsvr" }), signal },
        {
          ref: clusterMeta.configsvr.backupKey,
          uri: targetConfigsvrUri,
          database: targetTopology?.configsvr?.host ? "*" : "config",
          ...(targetTopology?.configsvr?.host ? { nsInclude: configsvrNsInclude } : {}),
          drop,
        },
      );
    }

    // 3. Restore base snapshot on every shard (bounded concurrency)
    const shardEntries = Object.entries(clusterMeta.shards);
    const shardUris = new Map<string, string>();
    const shardBaseEndTs = new Map<string, OplogTimestamp | undefined>();
    for (const [shardId] of shardEntries) {
      const resolvedHost = targetShardMap.get(shardId);
      if (!resolvedHost) {
        throw new Error(`Target cluster topology is missing mapping for shard "${shardId}"`);
      }
      shardUris.set(shardId, buildShardUri(effectiveUri, resolvedHost));
    }

    let restoredShards: string[];
    try {
      restoredShards = await runWithConcurrency(
        shardEntries,
        Math.min(MAX_CONCURRENT_SHARD_DUMPS, Math.max(1, shardEntries.length)),
        async ([shardId, shard]) => {
          const targetShardUri = shardUris.get(shardId)!;
          const shardPitrPlan = pitrPlan?.shardPlans[shardId] ?? null;
          const shardBridgeKeys = collectOrderedSliceKeys(
            shard.bridgingSliceKeys,
            shard.bridgingSliceKey,
          );
          const hasPostBaseSlices =
            shardBridgeKeys.length > 0 || Boolean(shardPitrPlan && shardPitrPlan.slices.length > 0);

          log.info({ shardId, backupKey: shard.backupKey }, "Restoring shard base backup...");
          await restoreRunner(
            { config: effectiveConfig, storage, logger: log.child({ target: shardId }), signal },
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

          return shardId;
        },
      );
    } catch (shardRestoreErr) {
      log.error(
        { err: shardRestoreErr },
        "CRITICAL: Shard restore failed after CSRS metadata was already mutated; cluster is in a partial restore state and requires a full re-restore",
      );
      throw shardRestoreErr;
    }

    // 4. Streaming oplog replay across configsvr & all shards + cross-shard distributed txn reconciliation
    if (totalSlicesToReplay > 0 && effectiveTargetTs) {
      const configsvrManifest = await readManifest(storage, clusterMeta.configsvr.backupKey).catch(
        () => null,
      );
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
            workDir: effectiveConfig.workDir,
            logger: log.child({ target: "configsvr-oplog" }),
            endTs: effectiveTargetTs,
            unsafe: !configsvrManifest?.oplog,
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

        const zeroSliceShards: string[] = [];
        for (const shardId of restoredShards) {
          const sliceKeys = shardSliceKeyMap.get(shardId) ?? [];
          if (sliceKeys.length === 0) {
            zeroSliceShards.push(shardId);
            continue;
          }
          const shardClient = createMongoClient(shardUris.get(shardId)!);
          await shardClient.connect();
          const shardApplier = new OplogApplier({
            target: new MongoOplogTargetClient(shardClient),
            workDir: effectiveConfig.workDir,
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
              await streamOplogSlice(
                { config: effectiveConfig, storage, signal },
                sliceKey,
                (stream) => node.applier.applyStream(stream, signal),
              );
            }
          }),
        );
        replayedSlices = totalSlicesToReplay;

        // Cross-shard distributed transaction reconciliation:
        // Union all committed distributed txns across shards/configsvr (including 0-slice shards
        // whose base dump already included the commit), then resolve buffered prepared txns.
        const clusterCommits = new Map<string, OplogTimestamp | null>();
        for (const node of activeNodes) {
          const { committedDistTxns } = node.applier.getTxnLeftovers();
          for (const committed of committedDistTxns) {
            clusterCommits.set(committed.id, committed.commitTs);
          }
        }
        for (const zShardId of zeroSliceShards) {
          const zUri = shardUris.get(zShardId);
          if (!zUri) continue;
          const zClient = createMongoClient(zUri);
          try {
            await zClient.connect();
            const zCfg = typeof zClient.db === "function" ? zClient.db("config") : null;
            const zTxCol =
              zCfg && typeof zCfg.collection === "function"
                ? zCfg.collection("transactions")
                : null;
            if (zTxCol && typeof zTxCol.find === "function") {
              const committedDocs = await zTxCol.find({ state: "committed" }).toArray();
              for (const txDoc of committedDocs) {
                const lsid = (txDoc as { _id?: unknown })._id;
                const txnNum = (txDoc as { txnNum?: unknown }).txnNum;
                if (lsid && typeof lsid === "object" && txnNum !== undefined) {
                  const lsidBytes = BSON.serialize(lsid as Record<string, unknown>);
                  const txnId = `${Buffer.from(lsidBytes).toString("base64url")}-${String(txnNum)}`;
                  const commitTs = fromBsonTimestamp(
                    (txDoc as { lastWriteOpTime?: { ts?: unknown } }).lastWriteOpTime?.ts,
                  );
                  clusterCommits.set(txnId, commitTs);
                }
              }
            }
          } finally {
            await zClient.close().catch(() => {});
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

    // 5. Synchronize routing & authoritative shard catalog metadata across CSRS, all shards, and mongos
    let restoredConfigDbs: Array<{ _id: string; primary?: string; version?: unknown }> = [];
    let restoredConfigColls: Array<{ _id: string }> = [];

    if (targetTopology?.configsvr?.host) {
      const cfgSyncClient = createMongoClient(targetConfigsvrUri);
      try {
        await cfgSyncClient.connect();
        const cfgAdmin = typeof cfgSyncClient.db === "function" ? cfgSyncClient.db("admin") : null;
        if (cfgAdmin && typeof cfgAdmin.command === "function") {
          await cfgAdmin.command({ flushRouterConfig: 1 });
        }
        const cfgDb = typeof cfgSyncClient.db === "function" ? cfgSyncClient.db("config") : null;
        if (cfgDb && typeof cfgDb.collection === "function") {
          const dbsColl = cfgDb.collection<{ _id: string; primary?: string; version?: unknown }>(
            "databases",
          );
          if (typeof dbsColl.find === "function") {
            restoredConfigDbs = await dbsColl.find({}).toArray();
          }
          const collsColl = cfgDb.collection<{ _id: string }>("collections");
          if (typeof collsColl.find === "function") {
            const allColls = await collsColl.find({}).toArray();
            restoredConfigColls = allColls.filter(
              (c) => typeof c._id === "string" && c._id !== "config.system.sessions",
            );
          }
        }
      } finally {
        await cfgSyncClient.close().catch(() => {});
      }
    }

    const dbPrimaryMap = new Map<string, string>();
    for (const dbDoc of restoredConfigDbs) {
      if (typeof dbDoc._id === "string" && typeof dbDoc.primary === "string") {
        dbPrimaryMap.set(dbDoc._id, dbDoc.primary);
      }
    }

    for (const [, shardUri] of shardUris.entries()) {
      const sClient = createMongoClient(shardUri);
      try {
        await sClient.connect();
        const sAdmin = typeof sClient.db === "function" ? sClient.db("admin") : null;
        if (!sAdmin || typeof sAdmin.command !== "function") continue;

        if (
          (restoredConfigDbs.length > 0 || restoredConfigColls.length > 0) &&
          typeof sClient.startSession === "function"
        ) {
          const syncSession = sClient.startSession();
          let txnSeq = 1;
          try {
            for (const dbDoc of restoredConfigDbs) {
              if (dbDoc.version) {
                try {
                  await sClient.db(dbDoc._id).command(
                    {
                      _shardsvrCommitCreateDatabaseMetadata: 1,
                      dbVersion: dbDoc.version,
                      txnNumber: Long.fromNumber(txnSeq++),
                      writeConcern: { w: "majority" },
                    },
                    { session: syncSession },
                  );
                } catch (err) {
                  if (!isCommandNotFoundError(err)) throw err;
                }
              }
            }
            for (const collDoc of restoredConfigColls) {
              const dot = collDoc._id.indexOf(".");
              if (dot <= 0) continue;
              const dbName = collDoc._id.slice(0, dot);
              const primaryShardId = dbPrimaryMap.get(dbName);
              if (!primaryShardId) continue;
              try {
                await sAdmin.command(
                  {
                    _shardsvrCommitCreateCollectionMetadata: collDoc._id,
                    primaryShardId,
                    txnNumber: Long.fromNumber(txnSeq++),
                    writeConcern: { w: "majority" },
                  },
                  { session: syncSession },
                );
              } catch (err) {
                if (!isCommandNotFoundError(err)) throw err;
              }
            }
          } finally {
            await syncSession.endSession().catch(() => {});
          }
        }

        // MongoDB 7.0 / 8.0 legacy cache refresh (ignore CommandNotFound on MongoDB 9.0+
        // and MetadataRefreshCanceledDueToFCVTransition / code 445 on MongoDB 8.0+ where shards are authoritative)
        for (const dbDoc of restoredConfigDbs) {
          try {
            await sAdmin.command({ _flushDatabaseCacheUpdates: dbDoc._id, syncFromConfig: true });
          } catch (err) {
            if (!isIgnorableCacheFlushError(err)) throw err;
          }
        }
        for (const collDoc of restoredConfigColls) {
          try {
            await sAdmin.command({
              _flushRoutingTableCacheUpdates: collDoc._id,
              syncFromConfig: true,
            });
          } catch (err) {
            if (!isIgnorableCacheFlushError(err)) throw err;
          }
        }

        await sAdmin.command({ flushRouterConfig: 1 });
      } finally {
        await sClient.close().catch(() => {});
      }
    }

    await client.db("admin").command({ flushRouterConfig: 1 });

    // Clear stale PITR state.json checkpoints across configsvr and shards so post-restore PITR
    // re-anchors cleanly rather than skipping new writes behind pre-restore future timestamps.
    if (!skipPitrRestoreMarker && typeof storage.remove === "function") {
      const stateKeysToReset = [
        pitrStateKey(scope),
        pitrStateKey({ prefix: `${scope.prefix}/configsvr`, database: "*" }),
        ...restoredShards.map((sid) =>
          pitrStateKey({ prefix: `${scope.prefix}/shards/${sid}`, database: "*" }),
        ),
      ];
      await Promise.allSettled(stateKeysToReset.map((k) => storage.remove(k).catch(() => {})));
    }
    if (!skipPitrRestoreMarker && typeof storage.putJson === "function") {
      const scopesToMark = [
        scope,
        { prefix: `${scope.prefix}/configsvr`, database: "*" },
        ...restoredShards.map((sid) => ({
          prefix: `${scope.prefix}/shards/${sid}`,
          database: "*",
        })),
      ];
      await Promise.allSettled(
        scopesToMark.map((sc) =>
          markPitrRestore(storage, sc).catch((err) => {
            log.warn({ err, scope: sc.prefix }, "Failed to write PITR restore marker");
          }),
        ),
      );
    }

    if ((balancerStopped || balancer.stopIssued) && balancerWasEnabled) {
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
    if ((balancerStopped || balancer.stopIssued) && balancerWasEnabled) {
      await balancer.start();
    }
    await client.close().catch(() => {});
  }
}
