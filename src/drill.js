import { MongoClient } from "mongodb";
import { isClusterScope, readManifest, resolveBackup, scopeOf } from "./catalog.js";
import { restoreBackup } from "./restore.js";

export class DrillError extends Error {
  constructor(key, mismatches) {
    const details = mismatches.map(
      ({ collection, expected, actual }) => `  ${collection}: expected ${expected}, restored ${actual}`,
    );
    super(`Restore drill failed for ${key}:\n${details.join("\n")}`);
    this.name = "DrillError";
    this.mismatches = mismatches;
  }
}

export async function runDrill(ctx) {
  const { config, storage, logger } = ctx;
  if (!config.drill) {
    throw new Error("DRILL_MONGODB_URI is not set");
  }

  const startedAt = Date.now();
  const key = await resolveBackup(storage, scopeOf(config), "latest");
  const manifest = await readManifest(storage, key);
  const isCluster = isClusterScope(manifest.database);
  const database = isCluster ? "cluster_restore_drill" : `${manifest.database}_restore_drill`;
  const client = new MongoClient(config.drill.uri);

  logger.info({ key, database, isCluster, oplog: Boolean(manifest.oplog) }, "Restore drill started");

  try {
    await client.connect();
    const db = client.db(database);
    await db.dropDatabase();

    await restoreBackup(ctx, { ref: key, uri: config.drill.uri, database, drop: true });

    const hasOplog = Boolean(manifest.oplog);
    const mismatches = [];
    const variances = [];

    for (const [colKey, expected] of Object.entries(manifest.collections || {})) {
      const collectionName = isCluster ? (colKey.split(".")[1] || colKey) : colKey;
      const targetDb = isCluster ? client.db(database) : db;

      let actual;
      try {
        actual = await targetDb.collection(collectionName).countDocuments();
      } catch {
        actual = null;
      }

      if (actual === null) {
        if (expected > 0) {
          mismatches.push({ collection: colKey, expected, actual: "missing" });
        }
      } else if (actual !== expected) {
        if (hasOplog) {
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

    const result = {
      key,
      database,
      collections: Object.keys(manifest.collections || {}).length,
      documents: Object.values(manifest.collections || {}).reduce((sum, n) => sum + n, 0),
      oplogVariances: variances,
      durationMs: Date.now() - startedAt,
    };
    logger.info(result, "Restore drill passed");
    return result;
  } finally {
    await client
      .db(database)
      .dropDatabase()
      .catch(() => {});
    await client.close();
  }
}
