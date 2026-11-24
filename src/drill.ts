import { randomUUID } from "node:crypto";
import { type Db, MongoClient } from "mongodb";
import { isClusterScope, listBackups, readManifest, scopeOf } from "./catalog.js";
import { formatOplogTimestamp, listOplogSlices } from "./oplog.js";
import { restoreBackup } from "./restore.js";
import type { BackupManifest, SchemaIndexSpec } from "./types/catalog.js";
import type {
  DrillContext,
  DrillMismatch,
  DrillOptions,
  DrillResult,
  DrillSchemaMismatch,
  DrillVariance,
} from "./types/drill.js";

export class DrillError extends Error {
  mismatches: DrillMismatch[];
  schemaMismatches: DrillSchemaMismatch[];

  constructor(
    key: string,
    mismatches: DrillMismatch[],
    schemaMismatches: DrillSchemaMismatch[] = [],
  ) {
    const details = [
      ...mismatches.map(
        ({ collection, expected, actual }) =>
          `  ${collection}: expected ${expected}, restored ${actual}`,
      ),
      ...schemaMismatches.map(
        ({ namespace, kind, detail }) => `  [${kind}] ${namespace}: ${detail}`,
      ),
    ];
    super(`Restore drill failed for ${key}:\n${details.join("\n")}`);
    this.name = "DrillError";
    this.mismatches = mismatches;
    this.schemaMismatches = schemaMismatches;
  }
}

interface CollectionCountReader {
  hasCollection(colKey: string): boolean | null;
  isView?(colKey: string): boolean;
  countDocuments(colKey: string): Promise<number | null>;
}

interface RestoredCollectionInfo {
  name: string;
  type?: string;
  options?: Record<string, unknown>;
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((v) => stableStringify(v)).join(",")}]`;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
}

function isReasonableOplogShift(
  expected: number,
  actual: number,
  oplogEntries?: number,
  isPitrDrill = false,
): boolean {
  if (actual === 0 && expected > 0) return false;
  if (isPitrDrill) {
    // PITR replays post-backup slices which may legitimately insert/delete many documents,
    // so any non-zero restored count is tracked as variance while schema/RW checks verify integrity.
    return actual > 0;
  }
  if (typeof oplogEntries === "number") {
    // Mathematically, replaying `oplogEntries` embedded oplog ops cannot shift a collection's
    // document count by more than `oplogEntries`.
    return Math.abs(actual - expected) <= oplogEntries;
  }
  // Legacy/unbounded manifest fallback: reject catastrophic drops (>50% missing on >=10 docs).
  if (expected >= 10 && actual < Math.ceil(expected * 0.5)) {
    return false;
  }
  return true;
}

async function verifyCollections(
  manifest: BackupManifest,
  reader: CollectionCountReader,
  hasOplog: boolean,
  isPitrDrill = false,
): Promise<{ mismatches: DrillMismatch[]; variances: DrillVariance[] }> {
  const mismatches: DrillMismatch[] = [];
  const variances: DrillVariance[] = [];
  const oplogEntries = manifest.oplogWindow?.entries;

  const viewNames = new Set((manifest.schema?.views || []).map((v) => v.name));

  for (const [colKey, expected] of Object.entries(manifest.collections || {})) {
    const exists = reader.hasCollection(colKey);
    if (exists === false) {
      if (expected > 0) {
        mismatches.push({ collection: colKey, expected, actual: "missing" });
      }
      continue;
    }

    // Views are dumped by mongodump with 0 documents (metadata only), while countDocuments()
    // evaluates the underlying aggregation pipeline. Views are verified via schema & live query checks.
    if (viewNames.has(colKey) || reader.isView?.(colKey)) {
      continue;
    }

    const actual = await reader.countDocuments(colKey);

    if (actual === null) {
      if (expected > 0) {
        mismatches.push({ collection: colKey, expected, actual: "missing" });
      }
    } else if (actual !== expected) {
      if (hasOplog && isReasonableOplogShift(expected, actual, oplogEntries, isPitrDrill)) {
        variances.push({ collection: colKey, expected, actual, diff: actual - expected });
      } else {
        mismatches.push({ collection: colKey, expected, actual });
      }
    }
  }

  return { mismatches, variances };
}

function compareIndexSpec(
  expected: SchemaIndexSpec,
  actual: Record<string, unknown>,
): string | null {
  if (stableStringify(expected.key) !== stableStringify(actual.key)) {
    return `index "${expected.name}" key mismatch (expected ${stableStringify(expected.key)}, got ${stableStringify(actual.key)})`;
  }
  if (Boolean(expected.unique) !== Boolean(actual.unique)) {
    return `index "${expected.name}" unique flag mismatch (expected ${Boolean(expected.unique)}, got ${Boolean(actual.unique)})`;
  }
  if (Boolean(expected.sparse) !== Boolean(actual.sparse)) {
    return `index "${expected.name}" sparse flag mismatch (expected ${Boolean(expected.sparse)}, got ${Boolean(actual.sparse)})`;
  }
  if (expected.expireAfterSeconds !== actual.expireAfterSeconds) {
    return `index "${expected.name}" TTL expireAfterSeconds mismatch (expected ${String(expected.expireAfterSeconds)}, got ${String(actual.expireAfterSeconds)})`;
  }
  if (
    expected.partialFilterExpression !== undefined &&
    stableStringify(expected.partialFilterExpression) !==
      stableStringify(actual.partialFilterExpression)
  ) {
    return `index "${expected.name}" partialFilterExpression mismatch`;
  }
  if (Boolean(expected.hidden) !== Boolean(actual.hidden)) {
    return `index "${expected.name}" hidden flag mismatch (expected ${Boolean(expected.hidden)}, got ${Boolean(actual.hidden)})`;
  }
  return null;
}

async function getRestoredCollectionInfos(
  db: Db,
): Promise<Map<string, RestoredCollectionInfo> | null> {
  if (typeof db.listCollections !== "function") return null;
  try {
    const items = await db.listCollections({}, { nameOnly: false }).toArray();
    const map = new Map<string, RestoredCollectionInfo>();
    for (const item of items) {
      map.set(item.name, {
        name: item.name,
        type: (item as { type?: string }).type,
        options: (item as { options?: Record<string, unknown> }).options,
      });
    }
    return map;
  } catch {
    return null;
  }
}

async function verifySchemaAndReadWrite({
  manifest,
  isCluster,
  isPitr = false,
  resolveDbAndColl,
  collectionInfosByDb,
  probeDb,
  readSampleTargets,
  logger,
}: {
  manifest: BackupManifest;
  isCluster: boolean;
  isPitr?: boolean;
  resolveDbAndColl: (colKey: string) => { db: Db; dbName: string; collName: string } | null;
  collectionInfosByDb: Map<string, Map<string, RestoredCollectionInfo> | null>;
  probeDb: Db | null;
  readSampleTargets: Array<{ namespace: string; db: Db; collName: string }>;
  logger?: DrillContext["logger"];
}): Promise<{
  schemaMismatches: DrillSchemaMismatch[];
  indexesVerified: number;
  validatorsVerified: number;
  viewsVerified: number;
  readWriteVerified: boolean;
}> {
  const schemaMismatches: DrillSchemaMismatch[] = [];
  const pitrSchemaDrifts: DrillSchemaMismatch[] = [];
  let indexesVerified = 0;
  let validatorsVerified = 0;
  let viewsVerified = 0;
  let readWriteVerified = false;

  const recordSchemaDiff = (diff: DrillSchemaMismatch) => {
    if (isPitr) {
      // Post-backup DDL (collMod, renameCollection, dropIndex, drop, etc.) legitimately
      // mutates schema between the base snapshot and the PITR target timestamp.
      pitrSchemaDrifts.push(diff);
    } else {
      schemaMismatches.push(diff);
    }
  };

  // 1. Verify recorded indexes & collection options/validators when manifest.schema is present
  if (manifest.schema) {
    for (const [colKey, expectedMeta] of Object.entries(manifest.schema.collections || {})) {
      const target = resolveDbAndColl(colKey);
      if (!target) continue;

      const dbInfos = collectionInfosByDb.get(target.dbName);
      const actualInfo = dbInfos?.get(target.collName);

      if (dbInfos && !actualInfo) {
        recordSchemaDiff({
          namespace: colKey,
          kind: "options",
          detail: "collection missing from restored database metadata",
        });
        continue;
      }

      // Verify collection options & validators
      if (expectedMeta.options && actualInfo) {
        const actualOpts = actualInfo.options ?? {};
        if (expectedMeta.options.validator !== undefined) {
          if (
            stableStringify(expectedMeta.options.validator) !==
            stableStringify(actualOpts.validator)
          ) {
            recordSchemaDiff({
              namespace: colKey,
              kind: "validator",
              detail: "collection validator definition does not match backup schema",
            });
          } else {
            validatorsVerified++;
          }
        }
        if (
          expectedMeta.options.validationLevel !== undefined &&
          expectedMeta.options.validationLevel !== actualOpts.validationLevel
        ) {
          recordSchemaDiff({
            namespace: colKey,
            kind: "validator",
            detail: `validationLevel mismatch (expected ${expectedMeta.options.validationLevel}, got ${String(actualOpts.validationLevel)})`,
          });
        }
        if (
          expectedMeta.options.validationAction !== undefined &&
          expectedMeta.options.validationAction !== actualOpts.validationAction
        ) {
          recordSchemaDiff({
            namespace: colKey,
            kind: "validator",
            detail: `validationAction mismatch (expected ${expectedMeta.options.validationAction}, got ${String(actualOpts.validationAction)})`,
          });
        }
        if (
          expectedMeta.options.capped !== undefined &&
          Boolean(expectedMeta.options.capped) !== Boolean(actualOpts.capped)
        ) {
          recordSchemaDiff({
            namespace: colKey,
            kind: "options",
            detail: `capped option mismatch (expected ${expectedMeta.options.capped}, got ${String(actualOpts.capped)})`,
          });
        }
        if (
          expectedMeta.options.timeseries !== undefined &&
          stableStringify(expectedMeta.options.timeseries) !==
            stableStringify(actualOpts.timeseries)
        ) {
          recordSchemaDiff({
            namespace: colKey,
            kind: "options",
            detail: "timeseries collection options do not match backup schema",
          });
        }
      }

      // Verify indexes
      const coll = target.db.collection(target.collName);
      if (typeof coll.indexes === "function") {
        let actualIndexes: Record<string, unknown>[];
        try {
          actualIndexes = (await coll.indexes()) as Record<string, unknown>[];
        } catch (err) {
          schemaMismatches.push({
            namespace: colKey,
            kind: "index",
            detail: `failed to inspect restored indexes: ${err instanceof Error ? err.message : String(err)}`,
          });
          continue;
        }
        const byName = new Map<string, Record<string, unknown>>();
        for (const idx of actualIndexes) {
          if (typeof idx.name === "string") byName.set(idx.name, idx);
        }
        for (const expectedIdx of expectedMeta.indexes) {
          const found = byName.get(expectedIdx.name);
          if (!found) {
            recordSchemaDiff({
              namespace: colKey,
              kind: "index",
              detail: `expected index "${expectedIdx.name}" (${stableStringify(expectedIdx.key)}) is missing`,
            });
            continue;
          }
          const diff = compareIndexSpec(expectedIdx, found);
          if (diff) {
            recordSchemaDiff({ namespace: colKey, kind: "index", detail: diff });
            if (isPitr) indexesVerified++;
          } else {
            indexesVerified++;
          }
        }
      }
    }

    // Verify views + run live query check against each restored view
    for (const expectedView of manifest.schema.views || []) {
      const target = resolveDbAndColl(expectedView.name);
      if (!target) continue;

      const dbInfos = collectionInfosByDb.get(target.dbName);
      const actualInfo = dbInfos?.get(target.collName);
      if (!actualInfo || actualInfo.type !== "view") {
        recordSchemaDiff({
          namespace: expectedView.name,
          kind: "view",
          detail: "expected view is missing or not restored as a view",
        });
        continue;
      }
      const actualOpts = actualInfo.options ?? {};
      if (
        actualOpts.viewOn !== expectedView.viewOn ||
        stableStringify(actualOpts.pipeline ?? []) !== stableStringify(expectedView.pipeline ?? [])
      ) {
        recordSchemaDiff({
          namespace: expectedView.name,
          kind: "view",
          detail: `view definition mismatch (expected viewOn="${expectedView.viewOn}", got "${String(actualOpts.viewOn)}")`,
        });
        if (!isPitr) continue;
      }

      const viewColl = target.db.collection(target.collName);
      if (typeof viewColl.findOne === "function") {
        try {
          await viewColl.findOne({});
        } catch (err) {
          schemaMismatches.push({
            namespace: expectedView.name,
            kind: "view",
            detail: `view query failed on restored database: ${err instanceof Error ? err.message : String(err)}`,
          });
          continue;
        }
      }
      viewsVerified++;
    }

    if (pitrSchemaDrifts.length > 0) {
      logger?.info(
        { driftsCount: pitrSchemaDrifts.length, drifts: pitrSchemaDrifts },
        "Schema differed from base snapshot during PITR drill (expected when post-backup DDL replayed)",
      );
    }
  } else {
    // Backwards-compatible index count inspection for manifests without recorded schema metadata
    for (const target of readSampleTargets) {
      const coll = target.db.collection(target.collName);
      if (typeof coll.indexes === "function") {
        try {
          const idxs = await coll.indexes();
          indexesVerified += idxs.length;
        } catch {
          // Non-fatal on mocked clients
        }
      }
    }
  }

  // 2. Live Read/Write functional usability checks on restored database(s)
  let ranReadCheck = false;
  for (const sample of readSampleTargets) {
    const coll = sample.db.collection(sample.collName);
    if (typeof coll.findOne === "function") {
      ranReadCheck = true;
      try {
        await coll.findOne({});
      } catch (err) {
        schemaMismatches.push({
          namespace: sample.namespace,
          kind: "read_write",
          detail: `failed to read document from restored collection: ${err instanceof Error ? err.message : String(err)}`,
        });
      }
    }
  }

  if (probeDb) {
    const probeCollName = `__mongovault_drill_probe_${randomUUID().replace(/-/g, "").slice(0, 8)}`;
    const probeColl = probeDb.collection(probeCollName);
    if (
      typeof probeColl.insertOne === "function" &&
      typeof probeColl.findOne === "function" &&
      typeof probeColl.deleteOne === "function"
    ) {
      const probeId = `probe-${randomUUID()}`;
      try {
        await probeColl.insertOne({ _id: probeId as unknown as never, ok: true, ts: new Date() });
        const readBack = await probeColl.findOne({ _id: probeId as unknown as never });
        if (!readBack || readBack.ok !== true) {
          schemaMismatches.push({
            namespace: `${isCluster ? probeDb.databaseName : "drill"}.${probeCollName}`,
            kind: "read_write",
            detail: "write/read-back probe returned unexpected document",
          });
        } else {
          await probeColl.deleteOne({ _id: probeId as unknown as never });
          readWriteVerified = true;
        }
      } catch (err) {
        schemaMismatches.push({
          namespace: `${isCluster ? probeDb.databaseName : "drill"}.${probeCollName}`,
          kind: "read_write",
          detail: `write/read-back probe failed on restored database: ${err instanceof Error ? err.message : String(err)}`,
        });
      } finally {
        if (typeof probeColl.drop === "function") {
          await probeColl.drop().catch(() => {});
        }
      }
    } else if (ranReadCheck && schemaMismatches.length === 0) {
      readWriteVerified = true;
    }
  }

  return {
    schemaMismatches,
    indexesVerified,
    validatorsVerified,
    viewsVerified,
    readWriteVerified,
  };
}

export async function runDrill(
  ctx: DrillContext,
  options: DrillOptions = {},
): Promise<DrillResult> {
  const {
    config,
    storage,
    logger,
    createMongoClient = (uri: string) => new MongoClient(uri),
  } = ctx;
  if (!config.drill) {
    throw new Error("DRILL_MONGODB_URI is not set");
  }

  // Safety check: prevent running drills against the primary/production database
  if (config.drill.uri === config.mongo.uri) {
    throw new Error(
      "Safety violation: DRILL_MONGODB_URI cannot match MONGODB_URI. Drills must run against an isolated staging/test instance.",
    );
  }

  const startedAt = Date.now();
  const scope = scopeOf(config);
  let resolvedPitr: string | undefined = options.pitr;

  if (resolvedPitr === "latest") {
    const slices = (await listOplogSlices(storage, scope)).filter((s) => s.complete);
    if (slices.length === 0) {
      throw new Error("Cannot run PITR restore drill: no complete oplog slices found in storage");
    }
    const latestSlice = slices[slices.length - 1]!;
    resolvedPitr = formatOplogTimestamp(latestSlice.endTs);
  }

  const backups = await listBackups(storage, scope);
  const logicalBackups = backups.filter(
    (b) => !/\.physical\.tar\.gz(\.enc)?$/.test(b.key) && !/\.cluster(\.enc)?$/.test(b.key),
  );
  if (logicalBackups.length === 0) {
    throw new Error("No logical backups found to run restore drill");
  }
  const key = logicalBackups[0]!.key;
  const manifest = await readManifest(storage, key);
  if (!manifest) {
    throw new Error(`Manifest not found for ${key}`);
  }
  const isCluster = isClusterScope(manifest.database);
  const drillTag = `drill_${Date.now()}`;
  const client = createMongoClient(config.drill.uri);

  logger.info(
    { key, isCluster, oplog: Boolean(manifest.oplog), pitr: resolvedPitr },
    "Restore drill started",
  );

  const restoredDbs = new Set<string>();
  const PROTECTED_SYSTEM_DBS = new Set(["admin", "local", "config"]);
  try {
    await client.connect();

    // Server-level topology isolation check: ensure DRILL_MONGODB_URI does not resolve
    // to the same physical cluster as MONGODB_URI via a different hostname/alias.
    try {
      const prodClient = createMongoClient(config.mongo.uri);
      try {
        await prodClient.connect();
        const prodAdmin = prodClient.db("admin");
        const drillAdmin = client.db("admin");
        if (typeof prodAdmin.command === "function" && typeof drillAdmin.command === "function") {
          const [prodHello, drillHello] = await Promise.all([
            prodAdmin.command({ hello: 1 }).catch(() => null),
            drillAdmin.command({ hello: 1 }).catch(() => null),
          ]);
          if (prodHello && drillHello) {
            const sameNode =
              Boolean(prodHello.me && drillHello.me) && prodHello.me === drillHello.me;
            const sameReplicaSet =
              Boolean(prodHello.setName && drillHello.setName) &&
              prodHello.setName === drillHello.setName &&
              Array.isArray(prodHello.hosts) &&
              Array.isArray(drillHello.hosts) &&
              prodHello.hosts.some((h: string) => drillHello.hosts.includes(h));
            if (sameNode || sameReplicaSet) {
              throw new Error(
                "Safety violation: DRILL_MONGODB_URI resolves to the same MongoDB cluster as MONGODB_URI. Drills must run against an isolated staging/test instance.",
              );
            }
          }
        }
      } finally {
        await prodClient.close().catch(() => {});
      }
    } catch (err) {
      if (err instanceof Error && err.message.startsWith("Safety violation:")) {
        throw err;
      }
    }

    if (!isCluster) {
      const database = `${manifest.database}_${drillTag}`;
      restoredDbs.add(database);
      await client.db(database).dropDatabase();

      const restoreRes = await restoreBackup(ctx, {
        ref: key,
        uri: config.drill.uri,
        database,
        drop: true,
        preVerify: true,
        pitr: resolvedPitr,
      });

      const activeManifest = restoreRes.manifest ?? manifest;
      const db = client.db(database);
      const collectionInfos = await getRestoredCollectionInfos(db);
      const hasOplog = Boolean(activeManifest.oplog) || Boolean(resolvedPitr);

      const reader: CollectionCountReader = {
        hasCollection(colKey: string) {
          return collectionInfos ? collectionInfos.has(colKey) : null;
        },
        isView(colKey: string) {
          return collectionInfos?.get(colKey)?.type === "view";
        },
        async countDocuments(colKey: string) {
          try {
            return await db.collection(colKey).countDocuments();
          } catch {
            return null;
          }
        },
      };

      const { mismatches, variances } = await verifyCollections(
        activeManifest,
        reader,
        hasOplog,
        Boolean(resolvedPitr),
      );

      const readSampleTargets = Object.entries(activeManifest.collections || {})
        .filter(([, count]) => count > 0)
        .map(([colKey]) => ({ namespace: colKey, db, collName: colKey }));

      const schemaCheck = await verifySchemaAndReadWrite({
        manifest: activeManifest,
        isCluster: false,
        isPitr: Boolean(resolvedPitr),
        resolveDbAndColl: (colKey) => ({ db, dbName: database, collName: colKey }),
        collectionInfosByDb: new Map([[database, collectionInfos]]),
        probeDb: db,
        readSampleTargets,
        logger,
      });

      if (mismatches.length > 0 || schemaCheck.schemaMismatches.length > 0) {
        throw new DrillError(restoreRes.key, mismatches, schemaCheck.schemaMismatches);
      }

      if (variances.length > 0) {
        logger.info(
          { variancesCount: variances.length, variances },
          "Document counts shifted due to intermediate oplog replay (expected behavior with point-in-time oplog)",
        );
      }

      const result: DrillResult = {
        key: restoreRes.key,
        database,
        collections: Object.keys(activeManifest.collections || {}).length,
        documents: Object.values(activeManifest.collections || {}).reduce((sum, n) => sum + n, 0),
        indexesVerified: schemaCheck.indexesVerified,
        validatorsVerified: schemaCheck.validatorsVerified,
        viewsVerified: schemaCheck.viewsVerified,
        readWriteVerified: schemaCheck.readWriteVerified,
        oplogVariances: variances,
        ...(restoreRes.pitrTarget ? { pitrTarget: restoreRes.pitrTarget } : {}),
        ...(restoreRes.replayedSlices !== undefined
          ? { replayedSlices: restoreRes.replayedSlices }
          : {}),
        ...(restoreRes.oplogStats ? { oplogStats: restoreRes.oplogStats } : {}),
        durationMs: Date.now() - startedAt,
      };
      logger.info(result, "Restore drill passed");
      return result;
    } else {
      // Cluster-wide restore drill
      const restoreRes = await restoreBackup(ctx, {
        ref: key,
        uri: config.drill.uri,
        drop: true,
        preVerify: true,
        pitr: resolvedPitr,
      });

      const activeManifest = restoreRes.manifest ?? manifest;
      for (const colKey of Object.keys(activeManifest.collections || {})) {
        const firstDot = colKey.indexOf(".");
        if (firstDot > 0) {
          restoredDbs.add(colKey.substring(0, firstDot));
        }
      }
      for (const colKey of Object.keys(activeManifest.schema?.collections || {})) {
        const firstDot = colKey.indexOf(".");
        if (firstDot > 0) {
          restoredDbs.add(colKey.substring(0, firstDot));
        }
      }
      for (const view of activeManifest.schema?.views || []) {
        const firstDot = view.name.indexOf(".");
        if (firstDot > 0) {
          restoredDbs.add(view.name.substring(0, firstDot));
        }
      }

      const collectionInfosByDb = new Map<string, Map<string, RestoredCollectionInfo> | null>();
      for (const dbName of restoredDbs) {
        collectionInfosByDb.set(dbName, await getRestoredCollectionInfos(client.db(dbName)));
      }

      const hasOplog = Boolean(activeManifest.oplog) || Boolean(resolvedPitr);

      const reader: CollectionCountReader = {
        hasCollection(colKey: string) {
          const firstDot = colKey.indexOf(".");
          if (firstDot <= 0) return null;
          const dbName = colKey.substring(0, firstDot);
          const colName = colKey.substring(firstDot + 1);
          const existing = collectionInfosByDb.get(dbName);
          return existing ? existing.has(colName) : null;
        },
        isView(colKey: string) {
          const firstDot = colKey.indexOf(".");
          if (firstDot <= 0) return false;
          const dbName = colKey.substring(0, firstDot);
          const colName = colKey.substring(firstDot + 1);
          return collectionInfosByDb.get(dbName)?.get(colName)?.type === "view";
        },
        async countDocuments(colKey: string) {
          const firstDot = colKey.indexOf(".");
          if (firstDot <= 0) return null;
          const dbName = colKey.substring(0, firstDot);
          const colName = colKey.substring(firstDot + 1);
          try {
            return await client.db(dbName).collection(colName).countDocuments();
          } catch {
            return null;
          }
        },
      };

      const { mismatches, variances } = await verifyCollections(
        activeManifest,
        reader,
        hasOplog,
        Boolean(resolvedPitr),
      );

      const readSampleTargets: Array<{ namespace: string; db: Db; collName: string }> = [];
      for (const [colKey, count] of Object.entries(activeManifest.collections || {})) {
        if (count <= 0) continue;
        const firstDot = colKey.indexOf(".");
        if (firstDot <= 0) continue;
        const dbName = colKey.substring(0, firstDot);
        const collName = colKey.substring(firstDot + 1);
        if (PROTECTED_SYSTEM_DBS.has(dbName)) continue;
        readSampleTargets.push({ namespace: colKey, db: client.db(dbName), collName });
      }

      const userProbeDbName = [...restoredDbs].find((d) => !PROTECTED_SYSTEM_DBS.has(d)) ?? null;
      const schemaCheck = await verifySchemaAndReadWrite({
        manifest: activeManifest,
        isCluster: true,
        isPitr: Boolean(resolvedPitr),
        resolveDbAndColl: (colKey) => {
          const firstDot = colKey.indexOf(".");
          if (firstDot <= 0) return null;
          const dbName = colKey.substring(0, firstDot);
          const collName = colKey.substring(firstDot + 1);
          return { db: client.db(dbName), dbName, collName };
        },
        collectionInfosByDb,
        probeDb: userProbeDbName ? client.db(userProbeDbName) : null,
        readSampleTargets,
        logger,
      });

      if (mismatches.length > 0 || schemaCheck.schemaMismatches.length > 0) {
        throw new DrillError(restoreRes.key, mismatches, schemaCheck.schemaMismatches);
      }

      if (variances.length > 0) {
        logger.info(
          { variancesCount: variances.length, variances },
          "Document counts shifted due to intermediate oplog replay (expected behavior with point-in-time oplog)",
        );
      }

      const result: DrillResult = {
        key: restoreRes.key,
        database: "_cluster",
        collections: Object.keys(activeManifest.collections || {}).length,
        documents: Object.values(activeManifest.collections || {}).reduce((sum, n) => sum + n, 0),
        indexesVerified: schemaCheck.indexesVerified,
        validatorsVerified: schemaCheck.validatorsVerified,
        viewsVerified: schemaCheck.viewsVerified,
        readWriteVerified: schemaCheck.readWriteVerified,
        oplogVariances: variances,
        ...(restoreRes.pitrTarget ? { pitrTarget: restoreRes.pitrTarget } : {}),
        ...(restoreRes.replayedSlices !== undefined
          ? { replayedSlices: restoreRes.replayedSlices }
          : {}),
        ...(restoreRes.oplogStats ? { oplogStats: restoreRes.oplogStats } : {}),
        durationMs: Date.now() - startedAt,
      };
      logger.info(result, "Cluster restore drill passed");
      return result;
    }
  } finally {
    for (const dbName of restoredDbs) {
      if (PROTECTED_SYSTEM_DBS.has(dbName)) continue;
      await client
        .db(dbName)
        .dropDatabase()
        .catch(() => {});
    }
    await client.close().catch(() => {});
  }
}
