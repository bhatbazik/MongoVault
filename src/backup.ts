import { PassThrough } from "node:stream";
import { MongoClient } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import { MANIFEST_VERSION, archiveKey, scopeOf, writeManifest } from "./catalog.js";
import { createEncryptor } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { formatOplogTimestamp, getOplogBounds } from "./oplog.js";
import { runStreamPipeline } from "./stream.js";
import { isClusterDatabase, mongodump } from "./tools.js";
import type { BackupContext } from "./types/backup.js";
import type {
  BackupManifest,
  BackupSchemaMetadata,
  SchemaCollectionMeta,
  SchemaCollectionOptions,
  SchemaIndexSpec,
  SchemaViewMeta,
} from "./types/catalog.js";
import type { BackupTools } from "./types/tools.js";

const SYSTEM_DBS_FOR_SCHEMA = new Set(["admin", "local", "config"]);

function normalizeIndexSpec(raw: Record<string, unknown>): SchemaIndexSpec | null {
  if (typeof raw.name !== "string" || !raw.key || typeof raw.key !== "object") {
    return null;
  }
  const spec: SchemaIndexSpec = {
    name: raw.name,
    key: raw.key as Record<string, unknown>,
  };
  if (raw.unique === true) spec.unique = true;
  if (raw.sparse === true) spec.sparse = true;
  if (typeof raw.expireAfterSeconds === "number") {
    spec.expireAfterSeconds = raw.expireAfterSeconds;
  }
  if (raw.partialFilterExpression && typeof raw.partialFilterExpression === "object") {
    spec.partialFilterExpression = raw.partialFilterExpression as Record<string, unknown>;
  }
  if (raw.hidden === true) spec.hidden = true;
  return spec;
}

function extractCollectionOptions(
  rawOptions?: Record<string, unknown>,
): SchemaCollectionOptions | undefined {
  if (!rawOptions || typeof rawOptions !== "object") return undefined;
  const out: SchemaCollectionOptions = {};
  if (rawOptions.validator && typeof rawOptions.validator === "object") {
    out.validator = rawOptions.validator as Record<string, unknown>;
  }
  if (typeof rawOptions.validationLevel === "string") {
    out.validationLevel = rawOptions.validationLevel;
  }
  if (typeof rawOptions.validationAction === "string") {
    out.validationAction = rawOptions.validationAction;
  }
  if (rawOptions.capped === true) out.capped = true;
  if (typeof rawOptions.size === "number") out.size = rawOptions.size;
  if (typeof rawOptions.max === "number") out.max = rawOptions.max;
  if (rawOptions.timeseries && typeof rawOptions.timeseries === "object") {
    out.timeseries = rawOptions.timeseries as Record<string, unknown>;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Lightweight pre-backup capture of database schema metadata (indexes, validators,
 * collection options, and views) so automated restore drills can verify full structural
 * fidelity beyond document counts. Non-fatal if metadata queries are unavailable.
 */
async function collectSchemaMetadata(
  uri: string,
  database: string,
  excludeCollections: string[] = [],
): Promise<BackupSchemaMetadata | undefined> {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5_000 });
  const isCluster = isClusterDatabase(database);
  const excludedSet = new Set(excludeCollections);

  try {
    await client.connect();
    let dbNames: string[];
    if (isCluster) {
      const res = (await client.db("admin").command({ listDatabases: 1, nameOnly: true })) as {
        databases?: Array<{ name?: string }>;
      };
      dbNames = (res.databases ?? [])
        .map((d) => d.name)
        .filter((n): n is string => Boolean(n) && !SYSTEM_DBS_FOR_SCHEMA.has(n!));
    } else {
      dbNames = [database];
    }

    const collections: Record<string, SchemaCollectionMeta> = {};
    const views: SchemaViewMeta[] = [];

    for (const dbName of dbNames) {
      const db = client.db(dbName);
      const infos = await db.listCollections({}, { nameOnly: false }).toArray();
      for (const info of infos) {
        const collName = info.name;
        if (!collName || collName.startsWith("system.")) continue;
        if (!isCluster && excludedSet.has(collName)) continue;

        const key = isCluster ? `${dbName}.${collName}` : collName;
        if (info.type === "view") {
          const opts = (
            info as { options?: { viewOn?: string; pipeline?: Record<string, unknown>[] } }
          ).options;
          if (opts?.viewOn) {
            views.push({
              name: key,
              viewOn: opts.viewOn,
              pipeline: Array.isArray(opts.pipeline) ? opts.pipeline : [],
            });
          }
          continue;
        }

        const rawIndexes = await db
          .collection(collName)
          .indexes()
          .catch(() => [] as Record<string, unknown>[]);
        const indexes = rawIndexes
          .map((idx) => normalizeIndexSpec(idx as Record<string, unknown>))
          .filter((idx): idx is SchemaIndexSpec => idx !== null);
        const options = extractCollectionOptions(
          (info as { options?: Record<string, unknown> }).options,
        );
        collections[key] = {
          indexes,
          ...(options ? { options } : {}),
        };
      }
    }

    if (Object.keys(collections).length === 0 && views.length === 0) {
      return undefined;
    }
    return { collections, views };
  } catch {
    return undefined;
  } finally {
    await client.close().catch(() => {});
  }
}

/**
 * Lightweight pre-backup estimate of database size (non-fatal) so S3 multipart
 * upload can auto-scale partSize above 5 MiB for large databases (avoiding the
 * 10,000-part S3 limit on >50 GiB archives).
 */
async function estimateDatabaseSizeBytes(
  uri: string,
  database: string,
): Promise<number | undefined> {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 5_000 });
  try {
    await client.connect();
    if (isClusterDatabase(database)) {
      const res = (await client.db("admin").command({ listDatabases: 1 })) as {
        totalSize?: number;
        databases?: Array<{ sizeOnDisk?: number }>;
      };
      if (typeof res.totalSize === "number" && res.totalSize > 0) {
        return res.totalSize;
      }
      if (Array.isArray(res.databases)) {
        const sum = res.databases.reduce((acc, d) => acc + (d.sizeOnDisk ?? 0), 0);
        return sum > 0 ? sum : undefined;
      }
      return undefined;
    }
    const stats = (await client.db(database).command({ dbStats: 1 })) as {
      dataSize?: number;
      storageSize?: number;
    };
    const size = stats.dataSize ?? stats.storageSize;
    return typeof size === "number" && size > 0 ? size : undefined;
  } catch {
    return undefined;
  } finally {
    await client.close().catch(() => {});
  }
}

/**
 * Runs a logical mongodump backup with optional encryption and streaming upload.
 */
export async function runBackup({
  config,
  storage,
  logger,
  signal,
  tools,
}: BackupContext): Promise<BackupManifest> {
  const effectiveTools: BackupTools = tools ?? { mongodump, getOplogBounds };
  const startedAt = new Date();
  const encrypted = Boolean(config.encryptionKey);
  if (config.requireEncryption && !encrypted) {
    throw new Error(
      "Refusing to create unencrypted backup: REQUIRE_ENCRYPTION is enabled but ENCRYPTION_KEY is not configured",
    );
  }
  const key = archiveKey(scopeOf(config), startedAt, { encrypted });
  const log = logger.child({ key });
  const controller = linkedController(signal);

  if (!encrypted) {
    log.warn(
      "ENCRYPTION_KEY is not configured; backup archive and manifest will be stored unencrypted and unsigned",
    );
  }
  log.info({ database: config.mongo.database, encrypted }, "Backup started");

  try {
    const requireOplogBounds = Boolean(config.pitr?.enabled);
    const [preOplog, expectedSizeBytes, schemaMetadata] = await Promise.all([
      config.mongo.oplog && effectiveTools.getOplogBounds
        ? effectiveTools.getOplogBounds(config.mongo.uri, { phase: "pre" }).catch((err) => {
            if (requireOplogBounds) {
              throw new Error(
                `Failed to determine pre-backup oplog boundary for PITR-enabled backup: ${err instanceof Error ? err.message : String(err)}`,
              );
            }
            log.warn({ err }, "Failed to determine pre-backup oplog boundary");
            return null;
          })
        : Promise.resolve(null),
      !tools
        ? estimateDatabaseSizeBytes(config.mongo.uri, config.mongo.database)
        : Promise.resolve(undefined),
      !tools
        ? collectSchemaMetadata(
            config.mongo.uri,
            config.mongo.database,
            config.mongo.excludeCollections,
          )
        : Promise.resolve(undefined),
    ]);

    const dump = await effectiveTools.mongodump({
      uri: config.mongo.uri,
      database: config.mongo.database,
      excludeCollections: config.mongo.excludeCollections,
      parallelCollections: config.mongo.parallelCollections,
      oplog: config.mongo.oplog,
      readPreference: config.mongo.readPreference,
      workDir: config.workDir,
      timeoutMs: config.timeoutMs,
      signal: controller.signal,
      logger: log,
    });

    const encryptor =
      encrypted && config.encryptionKey ? createEncryptor(config.encryptionKey) : null;
    const digest = new DigestStream();
    const body = new PassThrough({ highWaterMark: 1024 * 1024 });
    const stages = [dump.stdout, encryptor?.stream, digest, body];

    // Query oplog boundary immediately when mongodump finishes collection dumping
    // as a fallback when ArchiveOplogTap bounds are not returned (e.g. mocked tools).
    const dumpTask = dump.done.then(async (res) => {
      const hasTapBounds = res.oplogEndTs !== undefined || res.oplogEntries !== undefined;
      const dumpEndOplog =
        !hasTapBounds && config.mongo.oplog && effectiveTools.getOplogBounds
          ? await effectiveTools
              .getOplogBounds(config.mongo.uri, { phase: "post" })
              .catch((err) => {
                if (requireOplogBounds) {
                  throw new Error(
                    `Failed to determine post-backup oplog boundary for PITR-enabled backup: ${err instanceof Error ? err.message : String(err)}`,
                  );
                }
                log.warn({ err }, "Failed to determine post-backup oplog boundary");
                return null;
              })
          : null;
      return {
        collections: res.collections,
        oplogStartTs: res.oplogStartTs,
        oplogEndTs: res.oplogEndTs,
        oplogEntries: res.oplogEntries,
        dumpEndOplog,
      };
    });

    const [dumpSummary] = await settleAll(controller, [
      dumpTask,
      storage.upload(key, body, {
        contentType: encrypted ? "application/octet-stream" : "application/gzip",
        expectedSizeBytes,
        signal: controller.signal,
        onProgress: ({ loaded }) => log.debug({ uploadedBytes: loaded }, "Upload progress"),
      }),
      runStreamPipeline(stages, { signal: controller.signal }),
    ]);

    const { collections, dumpEndOplog } = dumpSummary;

    if (Object.keys(collections).length === 0) {
      log.warn(
        { database: config.mongo.database },
        "mongodump completed with 0 collections (empty database or collections had 0 documents)",
      );
    }

    const stored = await storage.head(key);
    if (stored?.size !== digest.bytes) {
      throw new Error(
        `Upload verification failed: expected ${digest.bytes} bytes, found ${stored?.size ?? "no object"}`,
      );
    }

    const completedAt = new Date();
    // Exact embedded oplog bounds from ArchiveOplogTap prevent post-dump timeline gaps:
    // - If embedded oplog has >= 1 entry: endTs = exact last embedded oplog entry.
    // - If ArchiveOplogTap saw 0 embedded entries (quiet dump): endTs = preOplog.newestTs (never postOplog > dumpEnd).
    // - Otherwise (mocked tool without tap): fall back to dumpEndOplog?.newestTs ?? preOplog?.newestTs.
    let oplogEndTs = dumpSummary.oplogEndTs ?? null;
    let oplogStartTs = dumpSummary.oplogStartTs ?? null;
    if (!oplogEndTs) {
      if (dumpSummary.oplogEntries === 0) {
        oplogEndTs = preOplog?.newestTs ?? null;
        oplogStartTs = preOplog?.newestTs ?? null;
      } else {
        oplogEndTs = dumpEndOplog?.newestTs ?? preOplog?.newestTs ?? null;
        oplogStartTs = preOplog?.newestTs ?? oplogEndTs;
      }
    } else if (!oplogStartTs) {
      oplogStartTs = preOplog?.newestTs ?? oplogEndTs;
    }

    const oplogWindow = oplogEndTs
      ? {
          startTs: formatOplogTimestamp(oplogStartTs!),
          endTs: formatOplogTimestamp(oplogEndTs),
          ...(dumpSummary.oplogEntries !== undefined ? { entries: dumpSummary.oplogEntries } : {}),
        }
      : null;

    if (requireOplogBounds && config.mongo.oplog && effectiveTools.getOplogBounds && !oplogWindow) {
      throw new Error(
        "PITR-enabled backup failed to establish oplogWindow boundaries; refusing to produce an unanchored PITR base backup",
      );
    }

    const manifest: BackupManifest = {
      version: MANIFEST_VERSION,
      key,
      database: config.mongo.database,
      createdAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs: completedAt.getTime() - startedAt.getTime(),
      size: digest.bytes,
      sha256: digest.digest(),
      compression: "gzip",
      encryption: encryptor?.envelope() ?? null,
      excludedCollections: config.mongo.excludeCollections,
      oplog: Boolean(config.mongo.oplog),
      ...(oplogWindow ? { oplogWindow } : {}),
      collections,
      ...(schemaMetadata ? { schema: schemaMetadata } : {}),
    };

    await writeManifest(storage, manifest, config.encryptionKey);

    log.info(
      {
        size: manifest.size,
        durationMs: manifest.durationMs,
        collections: Object.keys(collections).length,
      },
      "Backup completed",
    );
    return manifest;
  } catch (error) {
    await storage.remove(key).catch((cleanupError) => {
      log.warn({ err: cleanupError }, "Could not remove partial upload");
    });
    throw error;
  } finally {
    controller.dispose();
  }
}
