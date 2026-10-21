import { MongoClient } from "mongodb";
import { readManifest, resolveBackup, scopeOf } from "./catalog.js";
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
  const database = `${manifest.database}_restore_drill`;
  const client = new MongoClient(config.drill.uri);

  logger.info({ key, database }, "Restore drill started");

  try {
    await client.connect();
    const db = client.db(database);
    await db.dropDatabase();

    await restoreBackup(ctx, { ref: key, uri: config.drill.uri, database, drop: true });

    const mismatches = [];
    for (const [collection, expected] of Object.entries(manifest.collections)) {
      const actual = await db.collection(collection).countDocuments();
      if (actual !== expected) mismatches.push({ collection, expected, actual });
    }
    if (mismatches.length > 0) {
      throw new DrillError(key, mismatches);
    }

    const result = {
      key,
      database,
      collections: Object.keys(manifest.collections).length,
      documents: Object.values(manifest.collections).reduce((sum, n) => sum + n, 0),
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
