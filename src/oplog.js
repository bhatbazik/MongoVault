import fs from "node:fs/promises";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGunzip, createGzip } from "node:zlib";
import { BSON, MongoClient, Timestamp } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import {
  MANIFEST_SUFFIX,
  MANIFEST_VERSION,
  databasePrefix,
  listBackups,
  manifestKey,
  readManifest,
  scopeOf,
} from "./catalog.js";
import { createDecryptor, createEncryptor, signManifest, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";

const SLICE_PATTERN =
  /(\d{10})_(\d{1,10})__(\d{10})_(\d{1,10})\.oplog\.bson\.gz(\.enc)?$/;

export function parseOplogTimestamp(value) {
  if (!value) return null;
  if (typeof value === "object" && typeof value.t === "number" && typeof value.i === "number") {
    return { t: value.t >>> 0, i: value.i >>> 0 };
  }
  const str = String(value).trim();
  const match = /^(\d+)[:_](\d+)$/.exec(str);
  if (!match) {
    throw new Error(
      `Invalid oplog timestamp "${value}". Expected format "<seconds>:<ordinal>" (e.g. 1774092000:1)`,
    );
  }
  const t = Number(match[1]);
  const i = Number(match[2]);
  if (!Number.isInteger(t) || t < 0 || t > 0xffffffff || !Number.isInteger(i) || i < 0 || i > 0xffffffff) {
    throw new Error(`Oplog timestamp components out of 32-bit range: "${value}"`);
  }
  return { t, i };
}

export function parsePitrTarget(value) {
  if (!value) {
    throw new Error("Point-in-time target is required");
  }
  const str = String(value).trim();
  if (/^\d+[:_]\d+$/.test(str)) {
    return parseOplogTimestamp(str);
  }
  if (/^\d{10}$/.test(str)) {
    return { t: Number(str), i: 0xffffffff };
  }
  const ms = Date.parse(str);
  if (Number.isNaN(ms)) {
    throw new Error(
      `Invalid point-in-time target "${value}". Provide an ISO-8601 date (e.g. 2026-03-10T14:30:00Z) or "<seconds>:<ordinal>"`,
    );
  }
  return { t: Math.floor(ms / 1000) >>> 0, i: 0xffffffff };
}

export function formatOplogTimestamp(ts) {
  if (typeof ts === "string") return ts;
  return `${ts.t >>> 0}:${ts.i >>> 0}`;
}

export function compareTimestamps(a, b) {
  if (a.t !== b.t) return Math.sign(a.t - b.t);
  return Math.sign(a.i - b.i);
}

export function toBsonTimestamp(ts) {
  return new Timestamp({ t: ts.t >>> 0, i: ts.i >>> 0 });
}

export function fromBsonTimestamp(bsonTs) {
  if (!bsonTs) return null;
  if (typeof bsonTs.getHighBits === "function") {
    return {
      t: bsonTs.getHighBits() >>> 0,
      i: bsonTs.getLowBits() >>> 0,
    };
  }
  if (typeof bsonTs.t === "number" && typeof bsonTs.i === "number") {
    return { t: bsonTs.t >>> 0, i: bsonTs.i >>> 0 };
  }
  return null;
}

export function pitrPrefix(scope) {
  return `${databasePrefix(scope)}_pitr/`;
}

export function pitrStateKey(scope) {
  return `${pitrPrefix(scope)}state.json`;
}

export function oplogSliceKey(scope, startTs, endTs, { encrypted = false } = {}) {
  const date = new Date((endTs.t >>> 0) * 1000);
  const day = date.toISOString().slice(0, 10);
  const sT = String(startTs.t >>> 0).padStart(10, "0");
  const sI = String(startTs.i >>> 0).padStart(8, "0");
  const eT = String(endTs.t >>> 0).padStart(10, "0");
  const eI = String(endTs.i >>> 0).padStart(8, "0");
  return `${pitrPrefix(scope)}${day}/${sT}_${sI}__${eT}_${eI}.oplog.bson.gz${encrypted ? ".enc" : ""}`;
}

export function parseSliceKeyTimestamps(key) {
  const match = SLICE_PATTERN.exec(key);
  if (!match) return null;
  return {
    startTs: { t: Number(match[1]), i: Number(match[2]) },
    endTs: { t: Number(match[3]), i: Number(match[4]) },
    encrypted: Boolean(match[5]),
  };
}

export async function listOplogSlices(storage, scope) {
  const slices = [];
  const manifests = new Set();

  for await (const object of storage.list(pitrPrefix(scope))) {
    if (object.key.endsWith(MANIFEST_SUFFIX)) {
      manifests.add(object.key.slice(0, -MANIFEST_SUFFIX.length));
    } else {
      const parsed = parseSliceKeyTimestamps(object.key);
      if (parsed) {
        slices.push({
          key: object.key,
          size: object.size,
          lastModified: object.lastModified,
          startTs: parsed.startTs,
          endTs: parsed.endTs,
          encrypted: parsed.encrypted,
        });
      }
    }
  }

  return slices
    .map((slice) => ({
      ...slice,
      complete: manifests.has(slice.key),
    }))
    .sort((a, b) => compareTimestamps(a.startTs, b.startTs) || compareTimestamps(a.endTs, b.endTs));
}

export async function getOplogBounds(
  uri,
  { createMongoClient = (u) => new MongoClient(u) } = {},
) {
  const client = createMongoClient(uri);
  try {
    await client.connect();
    const oplog = client.db("local").collection("oplog.rs");
    const [oldestDoc, newestDoc] = await Promise.all([
      oplog.find({}, { projection: { ts: 1 } }).sort({ $natural: 1 }).limit(1).next(),
      oplog.find({}, { projection: { ts: 1 } }).sort({ $natural: -1 }).limit(1).next(),
    ]);
    return {
      oldestTs: fromBsonTimestamp(oldestDoc?.ts),
      newestTs: fromBsonTimestamp(newestDoc?.ts),
    };
  } finally {
    await client.close().catch(() => {});
  }
}

export async function uploadOplogSlice({
  config,
  storage,
  logger,
  rawDocs,
  startTs,
  endTs,
  previousEndTs = null,
  gapDetected = false,
  signal,
}) {
  if (!rawDocs || rawDocs.length === 0) return null;

  const startedAt = new Date();
  const encrypted = Boolean(config.encryptionKey);
  const scope = scopeOf(config);
  const key = oplogSliceKey(scope, startTs, endTs, { encrypted });
  const log = logger.child({ key });
  const controller = linkedController(signal);

  try {
    const gzip = createGzip();
    const encryptor = encrypted ? createEncryptor(config.encryptionKey) : null;
    const digest = new DigestStream();
    const body = new PassThrough({ highWaterMark: 1024 * 1024 });
    const stages = [Readable.from(rawDocs), gzip, encryptor?.stream, digest, body].filter(Boolean);

    await settleAll(controller, [
      storage.upload(key, body, {
        contentType: encrypted ? "application/octet-stream" : "application/gzip",
        signal: controller.signal,
      }),
      pipeline(...stages, { signal: controller.signal }),
    ]);

    const stored = await storage.head(key);
    if (stored?.size !== digest.bytes) {
      throw new Error(
        `Oplog slice upload verification failed: expected ${digest.bytes} bytes, found ${stored?.size ?? "no object"}`,
      );
    }

    const completedAt = new Date();
    const manifest = {
      version: MANIFEST_VERSION,
      type: "oplog-slice",
      key,
      database: config.mongo.database,
      createdAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs: completedAt - startedAt,
      size: digest.bytes,
      sha256: digest.digest(),
      compression: "gzip",
      encryption: encryptor?.envelope() ?? null,
      oplog: true,
      oplogWindow: {
        previousEndTs: previousEndTs ? formatOplogTimestamp(previousEndTs) : null,
        startTs: formatOplogTimestamp(startTs),
        endTs: formatOplogTimestamp(endTs),
        entries: rawDocs.length,
        gapDetected: Boolean(gapDetected),
      },
      excludedCollections: [],
      collections: {},
    };

    if (manifest.encryption && config.encryptionKey) {
      manifest.encryption.manifestHmac = signManifest(config.encryptionKey, manifest);
    }

    await storage.putJson(manifestKey(key), manifest);
    log.info(
      {
        startTs: manifest.oplogWindow.startTs,
        endTs: manifest.oplogWindow.endTs,
        entries: rawDocs.length,
        size: manifest.size,
      },
      "Oplog slice archived",
    );
    return manifest;
  } catch (error) {
    await storage.remove(key).catch(() => {});
    throw error;
  } finally {
    controller.dispose();
  }
}

export async function captureOplogSlice({
  config,
  storage,
  logger,
  signal,
  sinceTs,
  createMongoClient = (u) => new MongoClient(u),
}) {
  const scope = scopeOf(config);
  const stateObj = await storage.getJson(pitrStateKey(scope));
  const state = stateObj && "data" in stateObj ? stateObj.data : stateObj;

  let cursorStartTs = sinceTs ? parseOplogTimestamp(sinceTs) : null;
  if (!cursorStartTs && state?.lastTs) {
    cursorStartTs = parseOplogTimestamp(state.lastTs);
  }
  if (!cursorStartTs) {
    const backups = (await listBackups(storage, scope)).filter((b) => b.complete);
    if (backups.length > 0) {
      const latestManifest = await readManifest(storage, backups[0].key);
      if (latestManifest?.oplogWindow?.endTs) {
        cursorStartTs = parseOplogTimestamp(latestManifest.oplogWindow.endTs);
      }
    }
  }

  const client = createMongoClient(config.mongo.uri);
  try {
    await client.connect();
    const oplog = client.db("local").collection("oplog.rs");

    const oldestDoc = await oplog
      .find({}, { projection: { ts: 1 } })
      .sort({ $natural: 1 })
      .limit(1)
      .next();
    const oldestTs = fromBsonTimestamp(oldestDoc?.ts);

    if (!oldestTs) {
      throw new Error("local.oplog.rs is empty or target is not a replica set");
    }

    let gapDetected = false;
    if (cursorStartTs && compareTimestamps(oldestTs, cursorStartTs) > 0) {
      gapDetected = true;
      logger.warn(
        {
          lastArchivedTs: formatOplogTimestamp(cursorStartTs),
          oldestAvailableTs: formatOplogTimestamp(oldestTs),
        },
        "PITR oplog gap detected: MongoDB oplog rolled over past last archived timestamp",
      );
    }

    const query = cursorStartTs
      ? { ts: { $gt: toBsonTimestamp(cursorStartTs) } }
      : {};

    const maxBytes = (config.pitr?.maxSliceMb ?? 16) * 1024 * 1024;
    const cursor = oplog.find(query).sort({ $natural: 1 });

    const rawDocs = [];
    let accumulatedBytes = 0;
    let firstTs = null;
    let lastTs = null;

    for await (const doc of cursor) {
      if (signal?.aborted) break;
      const docTs = fromBsonTimestamp(doc.ts);
      if (!docTs) continue;
      const rawBuf = Buffer.isBuffer(doc) ? doc : Buffer.from(BSON.serialize(doc));

      if (!firstTs) firstTs = docTs;
      lastTs = docTs;
      rawDocs.push(rawBuf);
      accumulatedBytes += rawBuf.length;

      if (accumulatedBytes >= maxBytes) {
        break;
      }
    }

    if (rawDocs.length === 0) {
      logger.debug("No new oplog entries since last checkpoint");
      return null;
    }

    const manifest = await uploadOplogSlice({
      config,
      storage,
      logger,
      rawDocs,
      startTs: firstTs,
      endTs: lastTs,
      previousEndTs: gapDetected ? null : cursorStartTs,
      gapDetected,
      signal,
    });

    await storage.putJson(pitrStateKey(scope), {
      lastTs: formatOplogTimestamp(lastTs),
      lastSliceKey: manifest.key,
      updatedAt: new Date().toISOString(),
      gapDetected,
    });

    return manifest;
  } finally {
    await client.close().catch(() => {});
  }
}

export async function resolvePitrPlan(storage, scope, targetInput) {
  const targetTs = parsePitrTarget(targetInput);
  const backups = (await listBackups(storage, scope)).filter((b) => b.complete);
  if (backups.length === 0) {
    throw new Error(`No complete base backups found under ${databasePrefix(scope)}`);
  }

  let selectedBackup = null;
  let selectedManifest = null;
  let baseEndTs = null;

  for (const backup of backups) {
    const manifest = await readManifest(storage, backup.key);
    if (!manifest) continue;
    const endTs = manifest.oplogWindow?.endTs
      ? parseOplogTimestamp(manifest.oplogWindow.endTs)
      : {
          t: Math.floor(new Date(manifest.completedAt || manifest.createdAt).getTime() / 1000) >>> 0,
          i: 0,
        };

    if (compareTimestamps(endTs, targetTs) <= 0) {
      selectedBackup = backup;
      selectedManifest = manifest;
      baseEndTs = endTs;
      break;
    }
  }

  if (!selectedBackup) {
    throw new Error(
      `No base backup found at or before target PITR timestamp ${formatOplogTimestamp(targetTs)}`,
    );
  }

  const allSlices = (await listOplogSlices(storage, scope)).filter((s) => s.complete);
  const candidateSlices = allSlices.filter(
    (s) =>
      compareTimestamps(s.endTs, baseEndTs) > 0 &&
      compareTimestamps(s.startTs, targetTs) <= 0,
  );

  // Read manifests and verify contiguity of the oplog chain
  const verifiedSlices = [];
  let expectedPrevEndTs = baseEndTs;

  for (const slice of candidateSlices) {
    const sliceManifest = await readManifest(storage, slice.key);
    const prevEndStr = sliceManifest?.oplogWindow?.previousEndTs;
    const prevEndTs = prevEndStr ? parseOplogTimestamp(prevEndStr) : null;

    if (prevEndTs) {
      // Because previousEndTs is the exact lastTs of the preceding checkpoint,
      // it must be <= expectedPrevEndTs to ensure zero missing operations.
      if (compareTimestamps(prevEndTs, expectedPrevEndTs) > 0) {
        throw new Error(
          `PITR timeline gap detected between ${formatOplogTimestamp(expectedPrevEndTs)} and slice ${slice.key} (starts after ${formatOplogTimestamp(prevEndTs)})`,
        );
      }
    } else if (sliceManifest?.oplogWindow && compareTimestamps(slice.startTs, expectedPrevEndTs) > 0) {
      throw new Error(
        `PITR timeline gap detected before slice ${slice.key} (oplog rolled over after ${formatOplogTimestamp(expectedPrevEndTs)})`,
      );
    }

    verifiedSlices.push({
      ...slice,
      manifest: sliceManifest,
    });
    if (compareTimestamps(slice.endTs, expectedPrevEndTs) > 0) {
      expectedPrevEndTs = slice.endTs;
    }
  }

  if (
    compareTimestamps(targetTs, baseEndTs) > 0 &&
    verifiedSlices.length === 0
  ) {
    throw new Error(
      `Target timestamp ${formatOplogTimestamp(targetTs)} is newer than base backup (${formatOplogTimestamp(baseEndTs)}), but no oplog slices are available to bridge the window`,
    );
  }

  const isInsufficientCoverage =
    targetTs.i === 0xffffffff
      ? expectedPrevEndTs.t < targetTs.t
      : compareTimestamps(expectedPrevEndTs, targetTs) < 0;

  if (isInsufficientCoverage) {
    throw new Error(
      `PITR oplog coverage ends at ${formatOplogTimestamp(expectedPrevEndTs)}, which is before target ${formatOplogTimestamp(targetTs)}`,
    );
  }

  return {
    baseBackup: selectedBackup,
    baseManifest: selectedManifest,
    baseEndTs,
    slices: verifiedSlices,
    lastCoveredTs: expectedPrevEndTs,
    targetTs,
  };
}

export async function downloadAndExtractOplogSlice({ config, storage, signal }, sliceKey, destBsonFile) {
  const manifest = await readManifest(storage, sliceKey);
  const keyRing = config.encryptionKeyRing || (config.encryptionKey ? [config.encryptionKey] : []);

  if (manifest?.encryption) {
    if (keyRing.length === 0) {
      throw new Error(`Oplog slice ${sliceKey} is encrypted, but ENCRYPTION_KEY is not set`);
    }
    verifyManifest(keyRing, manifest);
  }

  const digest = new DigestStream();
  const decryptStage = manifest?.encryption
    ? [createDecryptor(keyRing, manifest.encryption)]
    : [];

  await fs.mkdir(path.dirname(destBsonFile), { recursive: true });
  const fileHandle = await fs.open(destBsonFile, "w", 0o600);
  const writeStream = fileHandle.createWriteStream();

  try {
    const downloadStream = await storage.download(sliceKey, { signal });
    await pipeline(
      downloadStream,
      digest,
      ...decryptStage,
      createGunzip(),
      writeStream,
      { signal },
    );
  } finally {
    await fileHandle.close().catch(() => {});
  }

  const sha256 = digest.digest();
  if (manifest?.sha256 && manifest.sha256 !== sha256) {
    throw new Error(
      `Oplog slice checksum mismatch for ${sliceKey}: expected ${manifest.sha256}, got ${sha256}`,
    );
  }
  if (manifest?.size !== undefined && manifest.size !== digest.bytes) {
    throw new Error(
      `Oplog slice size mismatch for ${sliceKey}: expected ${manifest.size}, got ${digest.bytes}`,
    );
  }

  return { bytes: digest.bytes, sha256, manifest };
}

export function startOplogArchiver({
  config,
  storage,
  logger,
  notifier,
  signal,
  createMongoClient = (u) => new MongoClient(u),
}) {
  const intervalMs = (config.pitr?.flushIntervalSeconds ?? 60) * 1000;
  let stopped = false;
  let activePromise = null;
  let timer = null;

  async function tick() {
    if (stopped || signal?.aborted) return;
    try {
      activePromise = captureOplogSlice({
        config,
        storage,
        logger,
        signal,
        createMongoClient,
      });
      await activePromise;
    } catch (error) {
      logger.error({ err: error }, "Continuous PITR oplog slice capture failed");
      if (notifier) {
        await notifier.notifyFailure("pitr-oplog", error).catch(() => {});
      }
    } finally {
      activePromise = null;
    }
  }

  return {
    start() {
      logger.info(
        {
          flushIntervalSeconds: config.pitr?.flushIntervalSeconds ?? 60,
          maxSliceMb: config.pitr?.maxSliceMb ?? 16,
        },
        "Starting continuous PITR oplog archiver",
      );
      void tick();
      timer = setInterval(() => {
        if (!activePromise) void tick();
      }, intervalMs);
      timer.unref();
    },

    async stop() {
      stopped = true;
      if (timer) clearInterval(timer);
      if (activePromise) {
        try {
          await activePromise;
        } catch {}
      }
    },
  };
}
