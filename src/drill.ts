import { type Db, MongoClient } from "mongodb";
import { isClusterScope, readManifest, resolveBackup, scopeOf } from "./catalog.js";
import { restoreBackup } from "./restore.js";
import type {
  DrillContext,
  DrillMismatch,
  DrillResult,
  DrillVariance,
} from "./types/drill.js";

export class DrillError extends Error {
  mismatches: DrillMismatch[];

  constructor(key: string, mismatches: DrillMismatch[]) {
    const details = mismatches.map(
      ({ collection, expected, actual }) => `  ${collection}: expected ${expected}, restored ${actual}`,
    );
    super(`Restore drill failed for ${key}:\n${details.join("\n")}`);
    this.name = "DrillError";
    this.mismatches = mismatches;
  }
}

export async function runDrill(ctx: DrillContext): Promise<DrillResult> {
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
  const key = await resolveBackup(storage, scopeOf(config), "latest");
  const manifest = await readManifest(storage, key);
  if (!manifest) {
    throw new Error(`Manifest not found for ${key}`);
  }
  const isCluster = isClusterScope(manifest.database);
  const drillTag = `drill_${Date.now()}`;
  const client = createMongoClient(config.drill.uri);

  logger.info({ key, isCluster, oplog: Boolean(manifest.oplog) }, "Restore drill started");

  const restoredDbs = new Set<string>();
  try {
    await client.connect();

    if (!isCluster) {
      const database = `${manifest.database}_${drillTag}`;
      restoredDbs.add(database);
      await client.db(database).dropDatabase();

      await restoreBackup(ctx, {
        ref: key,
        uri: config.drill.uri,
        database,
        drop: true,
        preVerify: false,
      });

      const db = client.db(database);
      const existingCollections = await getExistingCollectionNames(db);
      const hasOplog = Boolean(manifest.oplog);
      const mismatches: DrillMismatch[] = [];
      const variances: DrillVariance[] = [];

      for (const [colKey, expected] of Object.entries(manifest.collections || {})) {
        if (existingCollections && !existingCollections.has(colKey)) {
          if (expected > 0) {
            mismatches.push({ collection: colKey, expected, actual: "missing" });
          }
          continue;
        }

        let actual: number | null;
        try {
          actual = await db.collection(colKey).countDocuments();
        } catch {
          actual = null;
        }

        if (actual === null) {
          if (expected > 0) {
            mismatches.push({ collection: colKey, expected, actual: "missing" });
          }
        } else if (actual !== expected) {
          if (hasOplog && actual > 0) {
            variances.push({ collection: colKey, expected, actual, diff: actual - expected });
          } else {
            mismatches.push({ collection: colKey, expected, actual });
          }
        }
      }

      if (mismatches.length > 0) {
        throw new DrillError(key, mismatches);
      }

      if (variances.length > 0) {
        logger.info(
          { variancesCount: variances.length, variances },
          "Document counts shifted due to intermediate oplog replay (expected behavior with point-in-time oplog)",
        );
      }

      const result: DrillResult = {
        key,
        database,
        collections: Object.keys(manifest.collections || {}).length,
        documents: Object.values(manifest.collections || {}).reduce((sum, n) => sum + n, 0),
        oplogVariances: variances,
        durationMs: Date.now() - startedAt,
      };
      logger.info(result, "Restore drill passed");
      return result;
    } else {
      // Cluster-wide restore drill
      // Identify all databases present in the manifest collections
      for (const colKey of Object.keys(manifest.collections || {})) {
        const firstDot = colKey.indexOf(".");
        if (firstDot > 0) {
          restoredDbs.add(colKey.substring(0, firstDot));
        }
      }

      // Restore directly onto the dedicated drill MongoDB instance
      await restoreBackup(ctx, {
        ref: key,
        uri: config.drill.uri,
        drop: true,
        preVerify: false,
      });

      const existingByDb = new Map<string, Set<string> | null>();
      for (const dbName of restoredDbs) {
        existingByDb.set(dbName, await getExistingCollectionNames(client.db(dbName)));
      }

      const hasOplog = Boolean(manifest.oplog);
      const mismatches: DrillMismatch[] = [];
      const variances: DrillVariance[] = [];

      for (const [colKey, expected] of Object.entries(manifest.collections || {})) {
        const firstDot = colKey.indexOf(".");
        if (firstDot <= 0) continue;
        const dbName = colKey.substring(0, firstDot);
        const colName = colKey.substring(firstDot + 1);

        const existingCollections = existingByDb.get(dbName);
        if (existingCollections && !existingCollections.has(colName)) {
          if (expected > 0) {
            mismatches.push({ collection: colKey, expected, actual: "missing" });
          }
          continue;
        }

        let actual: number | null;
        try {
          actual = await client.db(dbName).collection(colName).countDocuments();
        } catch {
          actual = null;
        }

        if (actual === null) {
          if (expected > 0) {
            mismatches.push({ collection: colKey, expected, actual: "missing" });
          }
        } else if (actual !== expected) {
          if (hasOplog && actual > 0) {
            variances.push({ collection: colKey, expected, actual, diff: actual - expected });
          } else {
            mismatches.push({ collection: colKey, expected, actual });
          }
        }
      }

      if (mismatches.length > 0) {
        throw new DrillError(key, mismatches);
      }

      if (variances.length > 0) {
        logger.info(
          { variancesCount: variances.length, variances },
          "Document counts shifted due to intermediate oplog replay (expected behavior with point-in-time oplog)",
        );
      }

      const result: DrillResult = {
        key,
        database: "_cluster",
        collections: Object.keys(manifest.collections || {}).length,
        documents: Object.values(manifest.collections || {}).reduce((sum, n) => sum + n, 0),
        oplogVariances: variances,
        durationMs: Date.now() - startedAt,
      };
      logger.info(result, "Cluster restore drill passed");
      return result;
    }
  } finally {
    for (const dbName of restoredDbs) {
      await client.db(dbName).dropDatabase().catch(() => {});
    }
    await client.close().catch(() => {});
  }
}

async function getExistingCollectionNames(db: Db): Promise<Set<string> | null> {
  if (typeof db.listCollections !== "function") return null;
  try {
    const items = await db.listCollections({}, { nameOnly: true }).toArray();
    return new Set(items.map((item) => item.name));
  } catch {
    return null;
  }
}
