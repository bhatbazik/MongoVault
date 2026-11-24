import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";
import { createGunzip, createGzip } from "node:zlib";
import { BSON, MongoClient, Timestamp } from "mongodb";
import { linkedController, settleAll } from "./abort.js";
import { readBsonDocumentsFromStream } from "./archive-stream.js";
import {
  MANIFEST_SUFFIX,
  MANIFEST_VERSION,
  databasePrefix,
  listBackups,
  readManifest,
  resolveKeyRing,
  scopeOf,
  writeManifest,
} from "./catalog.js";
import { createDecryptor, createEncryptor, verifyManifest } from "./crypto.js";
import { DigestStream } from "./digest.js";
import { acquireHybridLock } from "./lock.js";
import {
  OplogFilter,
  type OplogDoc,
  type OplogFilterOptions,
  matchesNamespacePattern,
} from "./oplog-applier.js";
import { runStreamPipeline } from "./stream.js";
import type { BackupManifest, BackupScope, BackupSummary } from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type {
  CaptureOplogSliceOptions,
  ExtractOplogSliceResult,
  OplogArchiver,
  OplogBounds,
  OplogSlice,
  OplogSliceKeyTimestamps,
  OplogTimestamp,
  PitrPlan,
  StartOplogArchiverOptions,
  UploadOplogSliceOptions,
} from "./types/oplog.js";
import type { StorageClient } from "./types/storage.js";

const SLICE_PATTERN = /(\d{10})_(\d{1,10})__(\d{10})_(\d{1,10})\.oplog\.bson\.gz(\.enc)?$/;

export function parseOplogTimestamp(value: unknown): OplogTimestamp | null {
  if (!value) return null;
  if (
    typeof value === "object" &&
    value !== null &&
    "t" in value &&
    "i" in value &&
    typeof (value as { t: unknown }).t === "number" &&
    typeof (value as { i: unknown }).i === "number"
  ) {
    const v = value as { t: number; i: number };
    return { t: v.t >>> 0, i: v.i >>> 0 };
  }
  const str = String(value).trim();
  const match = /^(\d+)[:_](\d+)$/.exec(str);
  if (!match || !match[1] || !match[2]) {
    throw new Error(
      `Invalid oplog timestamp "${String(value)}". Expected format "<seconds>:<ordinal>" (e.g. 1774092000:1)`,
    );
  }
  const t = Number(match[1]);
  const i = Number(match[2]);
  if (
    !Number.isInteger(t) ||
    t < 0 ||
    t > 0xffffffff ||
    !Number.isInteger(i) ||
    i < 0 ||
    i > 0xffffffff
  ) {
    throw new Error(`Oplog timestamp components out of 32-bit range: "${String(value)}"`);
  }
  return { t, i };
}

export function parsePitrTarget(value: unknown): OplogTimestamp {
  if (!value) {
    throw new Error("Point-in-time target is required");
  }
  if (
    typeof value === "object" &&
    value !== null &&
    "t" in value &&
    "i" in value &&
    typeof (value as { t: unknown }).t === "number" &&
    typeof (value as { i: unknown }).i === "number"
  ) {
    const v = value as { t: number; i: number };
    return { t: v.t >>> 0, i: v.i >>> 0 };
  }
  const str = String(value).trim();
  if (/^\d+[:_]\d+$/.test(str)) {
    return parseOplogTimestamp(str)!;
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

export function formatOplogTimestamp(ts: OplogTimestamp | string): string {
  if (typeof ts === "string") return ts;
  return `${ts.t >>> 0}:${ts.i >>> 0}`;
}

export function compareTimestamps(a: OplogTimestamp, b: OplogTimestamp): number {
  if (a.t !== b.t) return Math.sign(a.t - b.t);
  return Math.sign(a.i - b.i);
}

/**
 * Advances an oplog timestamp by 1 ordinal so exclusive `--oplogLimit` includes `ts` itself.
 */
export function incrementOplogTimestamp(ts: OplogTimestamp): OplogTimestamp {
  const t = ts.t >>> 0;
  const i = ts.i >>> 0;
  if (i < 0xffffffff) {
    return { t, i: i + 1 };
  }
  return { t: (t + 1) >>> 0, i: 0 };
}

export { matchesNamespacePattern };

/**
 * Streaming filter over a raw BSON oplog file (O(1) RAM — never loads whole file into memory).
 * Removes system/no-op entries, enforces timeframe `[startTs, startExclusiveTs, targetTs]`
 * and namespace/config/sessions/UUID rules. Returns the number of kept entries.
 */
export async function filterOplogSliceFile(
  srcBsonFile: string,
  destBsonFile: string,
  options: OplogFilterOptions & {
    startTs?: OplogTimestamp;
    startExclusiveTs?: OplogTimestamp;
    targetTs?: OplogTimestamp;
  } = {},
): Promise<{ keptEntries: number }> {
  const filter = new OplogFilter(options);
  await fs.mkdir(path.dirname(destBsonFile), { recursive: true });
  const outHandle = await fs.open(destBsonFile, "w", 0o600);
  let keptEntries = 0;

  async function writeBuf(buf: Buffer): Promise<void> {
    let written = 0;
    while (written < buf.length) {
      const res = await outHandle.write(buf, written, buf.length - written);
      written += res.bytesWritten;
    }
  }

  const readStream = fsSync.createReadStream(srcBsonFile);
  try {
    await readBsonDocumentsFromStream(readStream, async (rawSlice) => {
      const doc = BSON.deserialize(rawSlice, { promoteValues: false }) as OplogDoc;

      const docTs = fromBsonTimestamp(doc.ts);
      if (docTs) {
        if (options.startTs && compareTimestamps(docTs, options.startTs) < 0) return;
        if (options.startExclusiveTs && compareTimestamps(docTs, options.startExclusiveTs) <= 0) {
          return;
        }
        if (options.targetTs && compareTimestamps(docTs, options.targetTs) > 0) return;
      }

      if (!filter.isIncluded(doc)) return;

      if (doc.op === "c" && doc.o && Array.isArray(doc.o.applyOps)) {
        let innerModified = false;
        const origOps = doc.o.applyOps as OplogDoc[];
        const filteredOps = origOps.filter((inner) => {
          if (!filter.isIncluded(inner)) return false;
          if (filter.sanitizeAndClone(inner)) innerModified = true;
          return true;
        });
        if (filteredOps.length === 0) return;
        if (innerModified || filteredOps.length !== origOps.length) {
          doc.o.applyOps = filteredOps;
          await writeBuf(Buffer.from(BSON.serialize(doc)));
          keptEntries++;
          return;
        }
      }

      if (filter.sanitizeAndClone(doc)) {
        await writeBuf(Buffer.from(BSON.serialize(doc)));
      } else {
        await writeBuf(rawSlice);
      }
      keptEntries++;
    });
  } finally {
    readStream.destroy();
    await outHandle.close().catch(() => {});
  }

  return { keptEntries };
}

export function toBsonTimestamp(ts: OplogTimestamp): Timestamp {
  return new Timestamp({ t: ts.t >>> 0, i: ts.i >>> 0 });
}

export function fromBsonTimestamp(bsonTs: unknown): OplogTimestamp | null {
  if (!bsonTs) return null;
  const ts = bsonTs as {
    getHighBits?: () => number;
    getLowBits?: () => number;
    t?: number;
    i?: number;
  };
  if (typeof ts.getHighBits === "function" && typeof ts.getLowBits === "function") {
    return {
      t: ts.getHighBits() >>> 0,
      i: ts.getLowBits() >>> 0,
    };
  }
  if (typeof ts.t === "number" && typeof ts.i === "number") {
    return { t: ts.t >>> 0, i: ts.i >>> 0 };
  }
  return null;
}

export function pitrPrefix(scope: BackupScope): string {
  return `${databasePrefix(scope)}_pitr/`;
}

export function pitrStateKey(scope: BackupScope): string {
  return `${pitrPrefix(scope)}state.json`;
}

export function oplogSliceKey(
  scope: BackupScope,
  startTs: OplogTimestamp,
  endTs: OplogTimestamp,
  { encrypted = false, keyPrefix }: { encrypted?: boolean; keyPrefix?: string } = {},
): string {
  const date = new Date((endTs.t >>> 0) * 1000);
  const day = date.toISOString().slice(0, 10);
  const sT = String(startTs.t >>> 0).padStart(10, "0");
  const sI = String(startTs.i >>> 0).padStart(8, "0");
  const eT = String(endTs.t >>> 0).padStart(10, "0");
  const eI = String(endTs.i >>> 0).padStart(8, "0");
  const basePrefix = keyPrefix
    ? keyPrefix.endsWith("/")
      ? keyPrefix
      : `${keyPrefix}/`
    : `${pitrPrefix(scope)}${day}/`;
  return `${basePrefix}${sT}_${sI}__${eT}_${eI}.oplog.bson.gz${encrypted ? ".enc" : ""}`;
}

export function parseSliceKeyTimestamps(key: string): OplogSliceKeyTimestamps | null {
  const match = SLICE_PATTERN.exec(key);
  if (!match) return null;
  return {
    startTs: { t: Number(match[1]), i: Number(match[2]) },
    endTs: { t: Number(match[3]), i: Number(match[4]) },
    encrypted: Boolean(match[5]),
  };
}

export async function listOplogSlices(
  storage: StorageClient,
  scope: BackupScope,
): Promise<OplogSlice[]> {
  const slices: Array<Omit<OplogSlice, "complete">> = [];
  const manifests = new Set<string>();

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

function extractReplSetUpperBound(
  replStatus: Record<string, unknown> | null,
): OplogTimestamp | null {
  if (!replStatus) return null;
  const optimes = replStatus.optimes as
    | {
        lastCommittedOpTime?: { ts?: unknown };
        readConcernMajorityOpTime?: { ts?: unknown };
        appliedOpTime?: { ts?: unknown };
      }
    | undefined;
  const majorityTs = optimes?.lastCommittedOpTime?.ts
    ? fromBsonTimestamp(optimes.lastCommittedOpTime.ts)
    : optimes?.readConcernMajorityOpTime?.ts
      ? fromBsonTimestamp(optimes.readConcernMajorityOpTime.ts)
      : null;
  if (!majorityTs) return null;

  const appliedTs = optimes?.appliedOpTime?.ts ? fromBsonTimestamp(optimes.appliedOpTime.ts) : null;
  // Clamp to node's appliedOpTime if available so reads never outrun the queried node's oplog
  if (appliedTs && compareTimestamps(appliedTs, majorityTs) < 0) {
    return appliedTs;
  }
  return majorityTs;
}

export async function getOplogBounds(
  uri: string,
  {
    phase = "pre",
    createMongoClient = (u: string) => new MongoClient(u),
  }: { phase?: "pre" | "post"; createMongoClient?: (u: string) => MongoClient } = {},
): Promise<OplogBounds> {
  const client = createMongoClient(uri);
  try {
    await client.connect();
    const oplog = client.db("local").collection("oplog.rs");
    const [oldestDoc, newestDoc] = await Promise.all([
      oplog
        .find({}, { projection: { ts: 1 } })
        .sort({ $natural: 1 })
        .limit(1)
        .next(),
      oplog
        .find({}, { projection: { ts: 1 } })
        .sort({ $natural: -1 })
        .limit(1)
        .next(),
    ]);

    const oldestTs = fromBsonTimestamp(oldestDoc?.ts);
    let newestTs = fromBsonTimestamp(newestDoc?.ts);

    // Prefer majority-committed opTime when replica set status is available
    try {
      const adminDb = typeof client.db === "function" ? client.db("admin") : null;
      const replStatus =
        adminDb && typeof adminDb.command === "function"
          ? ((await adminDb.command({ replSetGetStatus: 1 }).catch(() => null)) as Record<
              string,
              unknown
            > | null)
          : null;
      const majorityTs = extractReplSetUpperBound(replStatus);
      if (majorityTs) {
        newestTs = majorityTs;
      }
    } catch {
      // Ignore if replSetGetStatus is unavailable
    }

    // On pre-backup boundary discovery, check config.transactions for uncommitted/prepared
    // transactions so long-running transactions started before the snapshot aren't missed.
    if (phase === "pre") {
      const configDb = client.db("config");
      const txCol =
        configDb && typeof configDb.collection === "function"
          ? configDb.collection("transactions")
          : null;
      if (txCol && typeof txCol.find === "function") {
        try {
          const activeTx = await txCol
            .find({ state: { $in: ["prepared", "inProgress"] } })
            .sort({ "startOpTime.ts": 1 })
            .limit(1)
            .next();
          const txTs = fromBsonTimestamp(activeTx?.startOpTime?.ts);
          if (txTs && newestTs && compareTimestamps(txTs, newestTs) < 0) {
            newestTs = txTs;
          }
        } catch (err) {
          throw new Error(
            `Failed to verify transaction boundaries in config.transactions during pre-backup boundary discovery: ${err instanceof Error ? err.message : String(err)}`,
            { cause: err },
          );
        }
      }
    }

    return {
      oldestTs,
      newestTs,
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
  coverageStartTs = null,
  coverageEndTs = null,
  previousEndTs = null,
  gapDetected = false,
  keyPrefix,
  signal,
}: UploadOplogSliceOptions): Promise<BackupManifest | null> {
  if (!rawDocs || rawDocs.length === 0) return null;

  const startedAt = new Date();
  const encrypted = Boolean(config.encryptionKey);
  const scope = scopeOf(config);
  const key = oplogSliceKey(scope, startTs, endTs, { encrypted, keyPrefix });
  const log = logger.child({ key });
  const controller = linkedController(signal);

  try {
    const gzip = createGzip();
    const encryptor =
      encrypted && config.encryptionKey ? createEncryptor(config.encryptionKey) : null;
    const digest = new DigestStream();
    const body = new PassThrough({ highWaterMark: 1024 * 1024 });
    const stages = [Readable.from(rawDocs), gzip, encryptor?.stream, digest, body];

    await settleAll(controller, [
      storage.upload(key, body, {
        contentType: encrypted ? "application/octet-stream" : "application/gzip",
        signal: controller.signal,
      }),
      runStreamPipeline(stages, { signal: controller.signal }),
    ]);

    const stored = await storage.head(key);
    if (stored?.size !== digest.bytes) {
      throw new Error(
        `Oplog slice upload verification failed: expected ${digest.bytes} bytes, found ${stored?.size ?? "no object"}`,
      );
    }

    const completedAt = new Date();
    const manifest: BackupManifest = {
      version: MANIFEST_VERSION,
      type: "oplog-slice",
      key,
      database: config.mongo.database,
      createdAt: startedAt.toISOString(),
      completedAt: completedAt.toISOString(),
      durationMs: completedAt.getTime() - startedAt.getTime(),
      size: digest.bytes,
      sha256: digest.digest(),
      compression: "gzip",
      encryption: encryptor?.envelope() ?? null,
      oplog: true,
      oplogWindow: {
        previousEndTs: previousEndTs ? formatOplogTimestamp(previousEndTs) : null,
        startTs: formatOplogTimestamp(startTs),
        endTs: formatOplogTimestamp(endTs),
        ...(coverageStartTs ? { coverageStartTs: formatOplogTimestamp(coverageStartTs) } : {}),
        ...(coverageEndTs ? { coverageEndTs: formatOplogTimestamp(coverageEndTs) } : {}),
        entries: rawDocs.length,
        gapDetected: Boolean(gapDetected),
      },
      excludedCollections: [],
      collections: {},
    };

    await writeManifest(storage, manifest, config.encryptionKey);

    log.info(
      {
        startTs: manifest.oplogWindow!.startTs,
        endTs: manifest.oplogWindow!.endTs,
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

interface PitrStateRecord {
  lastTs?: string;
  coverageEndTs?: string;
  lastSliceKey?: string;
  updatedAt?: string;
  gapDetected?: boolean;
}

function inspectRawOrObjectOplogDoc(doc: unknown): {
  ts: OplogTimestamp | null;
  rawBuf: Buffer;
  isPartialTxn: boolean;
} {
  if (Buffer.isBuffer(doc) || doc instanceof Uint8Array) {
    const rawBuf = Buffer.isBuffer(doc) ? doc : Buffer.from(doc);
    // Cheap partial decode ONLY for `ts` and `o.partialTxn` without re-serializing `rawBuf`
    const parsed = BSON.deserialize(rawBuf, { promoteValues: false }) as {
      ts?: unknown;
      o?: { partialTxn?: unknown };
    };
    return {
      ts: fromBsonTimestamp(parsed.ts),
      rawBuf,
      isPartialTxn: Boolean(parsed.o?.partialTxn),
    };
  }
  const obj = doc as { ts?: unknown; o?: { partialTxn?: unknown } };
  return {
    ts: fromBsonTimestamp(obj.ts),
    rawBuf: Buffer.from(BSON.serialize(obj as Record<string, unknown>)),
    isPartialTxn: Boolean(obj.o?.partialTxn),
  };
}

export async function captureOplogSlice({
  config,
  storage,
  logger,
  signal,
  sinceTs,
  untilTs,
  keyPrefix,
  updateState = true,
  onCappedByActiveTxn,
  createMongoClient = (u) => new MongoClient(u),
}: CaptureOplogSliceOptions): Promise<BackupManifest | null> {
  const scope = scopeOf(config);
  const stateObj = updateState ? await storage.getJson<PitrStateRecord>(pitrStateKey(scope)) : null;
  const state =
    stateObj && "data" in stateObj ? stateObj.data : (stateObj as PitrStateRecord | null);

  let cursorStartTs = sinceTs ? parseOplogTimestamp(sinceTs) : null;
  if (!cursorStartTs && state?.lastTs) {
    cursorStartTs = parseOplogTimestamp(state.lastTs);
  }
  if (!cursorStartTs) {
    const backups = (await listBackups(storage, scope)).filter((b) => b.complete);
    const firstBackup = backups[0];
    if (firstBackup) {
      const latestManifest = await readManifest(storage, firstBackup.key);
      // Anchor initial slice to startTs (or endTs) so any writes during base dump are covered
      const anchorStr = latestManifest?.oplogWindow?.startTs ?? latestManifest?.oplogWindow?.endTs;
      if (anchorStr) {
        cursorStartTs = parseOplogTimestamp(anchorStr);
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

    // Query replica set status to identify majority commit point (clamped to appliedOpTime)
    const adminDb = typeof client.db === "function" ? client.db("admin") : null;
    const replStatus =
      adminDb && typeof adminDb.command === "function"
        ? ((await adminDb.command({ replSetGetStatus: 1 }).catch((err) => {
            logger.warn({ err }, "replSetGetStatus command failed");
            return null;
          })) as Record<string, unknown> | null)
        : null;
    const majorityTs = extractReplSetUpperBound(replStatus);

    if (!majorityTs) {
      throw new Error(
        "Unable to determine majority-committed oplog timestamp: replSetGetStatus failed or did not return optimes. Aborting oplog slice capture to prevent archiving uncommitted writes.",
      );
    }

    let effectiveUpperTs = majorityTs;
    const explicitUntil = untilTs ? parseOplogTimestamp(untilTs) : null;
    if (explicitUntil && compareTimestamps(explicitUntil, effectiveUpperTs) < 0) {
      effectiveUpperTs = explicitUntil;
    }

    // Inspect config.transactions for active or prepared distributed transactions.
    // The slice upper boundary must stop STRICTLY BEFORE ($lt) an in-progress transaction's
    // startOpTime so uncommitted transaction ops are never split across slices.
    let cappedByActiveTx = false;
    try {
      const configDb = typeof client.db === "function" ? client.db("config") : null;
      const txCol =
        configDb && typeof configDb.collection === "function"
          ? configDb.collection("transactions")
          : null;
      if (txCol && typeof txCol.find === "function") {
        const activeTx = await txCol
          .find({ state: { $in: ["prepared", "inProgress"] } }, { readConcern: { level: "local" } })
          .sort({ "startOpTime.ts": 1 })
          .limit(1)
          .next();
        const txTs = fromBsonTimestamp(activeTx?.startOpTime?.ts);
        if (txTs && compareTimestamps(txTs, effectiveUpperTs) <= 0) {
          logger.info(
            {
              activeTxStartTs: formatOplogTimestamp(txTs),
              majorityTs: formatOplogTimestamp(majorityTs),
            },
            "Active transaction in progress; capping oplog slice strictly before transaction startOpTime",
          );
          effectiveUpperTs = txTs;
          cappedByActiveTx = true;
          onCappedByActiveTxn?.(txTs);
        }
      }
    } catch (err: unknown) {
      logger.error(
        { err },
        "Failed to inspect config.transactions for active transactions. Aborting oplog slice capture to prevent inconsistent transaction replay.",
      );
      throw new Error(
        `Failed to verify transaction boundaries in config.transactions: ${err instanceof Error ? err.message : String(err)}. Aborting slice capture to guarantee transaction safety.`,
        { cause: err },
      );
    }

    // Detect post-restore timeline divergence: if persisted state.lastTs is strictly ahead
    // of the cluster's majority commit point (not merely capped by an active txn), refuse to silently stall.
    if (
      updateState &&
      !sinceTs &&
      !cappedByActiveTx &&
      cursorStartTs &&
      compareTimestamps(cursorStartTs, majorityTs) > 0
    ) {
      throw new Error(
        `PITR state cursor (${formatOplogTimestamp(cursorStartTs)}) is ahead of cluster majority oplog (${formatOplogTimestamp(majorityTs)}). Timeline diverged (e.g. after restore); take a new base backup to re-anchor PITR.`,
      );
    }

    if (cursorStartTs) {
      const cmp = compareTimestamps(cursorStartTs, effectiveUpperTs);
      if (cmp >= 0) {
        // On a quiet cluster (cmp === 0 and not capped), advance coverageEndTs in state.json
        if (updateState && !cappedByActiveTx && cmp === 0) {
          await storage.putJson(pitrStateKey(scope), {
            ...state,
            lastTs: formatOplogTimestamp(cursorStartTs),
            coverageEndTs: formatOplogTimestamp(effectiveUpperTs),
            updatedAt: new Date().toISOString(),
            gapDetected: Boolean(state?.gapDetected),
          });
        }
        logger.debug(
          {
            cursorStartTs: formatOplogTimestamp(cursorStartTs),
            effectiveUpperTs: formatOplogTimestamp(effectiveUpperTs),
          },
          "No new majority-committed, transaction-consistent oplog entries",
        );
        return null;
      }
    }

    const query: Record<string, unknown> = {
      ts: {
        ...(cursorStartTs ? { $gt: toBsonTimestamp(cursorStartTs) } : {}),
        ...(cappedByActiveTx
          ? { $lt: toBsonTimestamp(effectiveUpperTs) }
          : { $lte: toBsonTimestamp(effectiveUpperTs) }),
      },
    };

    const maxBytes = (config.pitr?.maxSliceMb ?? 16) * 1024 * 1024;
    // Use `raw: true` so MongoDB driver returns raw wire BSON bytes directly without JS decode/re-encode
    const cursor = oplog.find(query, { raw: true, promoteValues: false }).sort({ $natural: 1 });

    const rawDocs: Buffer[] = [];
    let accumulatedBytes = 0;
    let firstTs: OplogTimestamp | null = null;
    let lastTs: OplogTimestamp | null = null;
    let hitMaxBytes = false;

    try {
      for await (const doc of cursor) {
        if (signal?.aborted) break;
        const { ts: docTs, rawBuf, isPartialTxn } = inspectRawOrObjectOplogDoc(doc);
        if (!docTs) continue;

        // Defensive client-side enforcement in case a mock or driver ignores $lt
        if (cappedByActiveTx && compareTimestamps(docTs, effectiveUpperTs) >= 0) {
          break;
        }

        if (!firstTs) {
          firstTs = docTs;
          // Post-cursor sufficiency check: verify that local.oplog.rs
          // did not roll over past cursorStartTs between the initial check and cursor positioning.
          if (!gapDetected && cursorStartTs) {
            const postCursorOldestDoc = await oplog
              .find({}, { projection: { ts: 1 } })
              .sort({ $natural: 1 })
              .limit(1)
              .next();
            const postCursorOldestTs = fromBsonTimestamp(postCursorOldestDoc?.ts);
            if (postCursorOldestTs && compareTimestamps(postCursorOldestTs, cursorStartTs) > 0) {
              gapDetected = true;
              logger.warn(
                {
                  lastArchivedTs: formatOplogTimestamp(cursorStartTs),
                  oldestAvailableTs: formatOplogTimestamp(postCursorOldestTs),
                },
                "PITR oplog gap detected after cursor open: MongoDB oplog rolled over past last archived timestamp",
              );
            }
          }
        }
        lastTs = docTs;
        rawDocs.push(rawBuf);
        accumulatedBytes += rawBuf.length;

        // Never split an in-flight chained multi-document transaction (partialTxn: true) across slices
        if (accumulatedBytes >= maxBytes && !isPartialTxn) {
          hitMaxBytes = true;
          break;
        }
      }
    } catch (cursorError: unknown) {
      const err = cursorError as { code?: number; message?: string };
      const isRollover =
        err?.code === 50917 || // OplogRolledOver
        err?.code === 136 || // CappedPositionLost
        String(err?.message || "")
          .toLowerCase()
          .includes("cappedpositionlost") ||
        String(err?.message || "")
          .toLowerCase()
          .includes("oplog rolled over");

      if (isRollover && updateState) {
        logger.error(
          {
            err: cursorError,
            lastArchivedTs: cursorStartTs ? formatOplogTimestamp(cursorStartTs) : null,
          },
          "CRITICAL: MongoDB oplog rolled over while cursor was streaming. Timeline gap created.",
        );
        // Never advance lastTs past un-uploaded entries; preserve the last confirmed archived timestamp
        // while flagging gapDetected so operators and planners refuse inconsistent recovery.
        const confirmedLastTs =
          state?.lastTs ?? (cursorStartTs ? formatOplogTimestamp(cursorStartTs) : undefined);
        await storage.putJson(pitrStateKey(scope), {
          ...state,
          ...(confirmedLastTs ? { lastTs: confirmedLastTs } : {}),
          updatedAt: new Date().toISOString(),
          gapDetected: true,
        });
      }
      throw cursorError;
    }

    if (rawDocs.length === 0 || !firstTs || !lastTs) {
      // Quiet window: no docs in (cursorStartTs, effectiveUpperTs], record observed coverage
      if (updateState && !cappedByActiveTx) {
        await storage.putJson(pitrStateKey(scope), {
          ...state,
          ...(cursorStartTs ? { lastTs: formatOplogTimestamp(cursorStartTs) } : {}),
          coverageEndTs: formatOplogTimestamp(effectiveUpperTs),
          updatedAt: new Date().toISOString(),
          gapDetected: Boolean(gapDetected || state?.gapDetected),
        });
      }
      logger.debug("No new oplog entries since last checkpoint");
      return null;
    }

    // Observed coverage reaches `effectiveUpperTs` when we drained the full window (not capped/truncated)
    const sliceCoverageEndTs =
      !hitMaxBytes && !cappedByActiveTx && compareTimestamps(effectiveUpperTs, lastTs) >= 0
        ? effectiveUpperTs
        : lastTs;

    const manifest = await uploadOplogSlice({
      config,
      storage,
      logger,
      rawDocs,
      startTs: firstTs,
      endTs: lastTs,
      coverageStartTs: cursorStartTs ?? firstTs,
      coverageEndTs: sliceCoverageEndTs,
      previousEndTs: gapDetected ? null : cursorStartTs,
      gapDetected,
      keyPrefix,
      signal,
    });

    if (manifest && updateState) {
      await storage.putJson(pitrStateKey(scope), {
        lastTs: formatOplogTimestamp(lastTs),
        coverageEndTs: formatOplogTimestamp(sliceCoverageEndTs),
        lastSliceKey: manifest.key,
        updatedAt: new Date().toISOString(),
        gapDetected,
      });
    }

    return manifest;
  } finally {
    await client.close().catch(() => {});
  }
}

export async function resolvePitrPlan(
  storage: StorageClient,
  scope: BackupScope,
  targetInput: string | OplogTimestamp,
  options: {
    engine?: "logical" | "physical";
    includeBaseWindow?: boolean;
    /** Force anchoring to a specific base backup key (used by sharded PITR). */
    baseKey?: string;
  } = {},
): Promise<PitrPlan> {
  const targetEngine = options.engine ?? "logical";
  const includeBaseWindow = Boolean(options.includeBaseWindow);
  const targetTs = parsePitrTarget(targetInput);

  let selectedBackup: BackupSummary | null = null;
  let selectedManifest: BackupManifest | null = null;
  let baseEndTs: OplogTimestamp | null = null;

  if (options.baseKey) {
    const manifest = await readManifest(storage, options.baseKey);
    if (!manifest) {
      throw new Error(`Base backup manifest not found for anchored key: ${options.baseKey}`);
    }
    const endTs: OplogTimestamp = manifest.oplogWindow?.endTs
      ? parseOplogTimestamp(manifest.oplogWindow.endTs)!
      : {
          t:
            Math.floor(new Date(manifest.completedAt || manifest.createdAt).getTime() / 1000) >>> 0,
          i: 0,
        };
    if (compareTimestamps(endTs, targetTs) > 0) {
      throw new Error(
        `Anchored base backup ${options.baseKey} (${formatOplogTimestamp(endTs)}) is newer than target PITR timestamp ${formatOplogTimestamp(targetTs)}`,
      );
    }
    selectedBackup = {
      key: options.baseKey,
      size: manifest.size,
      createdAt: new Date(manifest.createdAt),
      encrypted: Boolean(manifest.encryption),
      complete: true,
    };
    selectedManifest = manifest;
    baseEndTs = endTs;
  } else {
    const backups = (await listBackups(storage, scope)).filter((b) => {
      if (!b.complete) return false;
      const isPhysical = /\.physical\.tar\.gz(\.enc)?$/.test(b.key);
      const isSharded = /\.cluster(\.enc)?$/.test(b.key);
      return targetEngine === "physical" ? isPhysical : !isPhysical && !isSharded;
    });
    if (backups.length === 0) {
      throw new Error(
        `No complete ${targetEngine} base backups found under ${databasePrefix(scope)}`,
      );
    }

    for (const backup of backups) {
      const manifest = await readManifest(storage, backup.key);
      if (!manifest) continue;
      if (
        targetEngine === "logical" &&
        (manifest.engine === "physical" || manifest.type === "sharded-cluster-backup")
      ) {
        continue;
      }
      if (targetEngine === "physical" && manifest.engine !== "physical") {
        continue;
      }
      const endTs: OplogTimestamp = manifest.oplogWindow?.endTs
        ? parseOplogTimestamp(manifest.oplogWindow.endTs)!
        : {
            t:
              Math.floor(new Date(manifest.completedAt || manifest.createdAt).getTime() / 1000) >>>
              0,
            i: 0,
          };

      if (compareTimestamps(endTs, targetTs) <= 0) {
        selectedBackup = backup;
        selectedManifest = manifest;
        baseEndTs = endTs;
        break;
      }
    }
  }

  if (!selectedBackup || !baseEndTs) {
    throw new Error(
      `No base backup found at or before target PITR timestamp ${formatOplogTimestamp(targetTs)}`,
    );
  }

  const baseAnchorTs =
    includeBaseWindow && selectedManifest?.oplogWindow?.startTs
      ? (parseOplogTimestamp(selectedManifest.oplogWindow.startTs) ?? baseEndTs)
      : baseEndTs;

  const allSlices = (await listOplogSlices(storage, scope)).filter((s) => s.complete);
  const candidateSlices = allSlices.filter(
    (s) =>
      compareTimestamps(s.endTs, baseAnchorTs) >= 0 && compareTimestamps(s.startTs, targetTs) <= 0,
  );

  // Read manifests and verify contiguity of the oplog chain starting strictly from `baseAnchorTs`
  const verifiedSlices: OplogSlice[] = [];
  let expectedPrevEndTs = baseAnchorTs;
  let lastCoveredTs = baseAnchorTs;

  for (const slice of candidateSlices) {
    const sliceManifest = await readManifest(storage, slice.key);
    if (sliceManifest?.oplogWindow?.gapDetected) {
      throw new Error(
        `PITR timeline gap detected before slice ${slice.key} (oplog rolled over after ${formatOplogTimestamp(expectedPrevEndTs)})`,
      );
    }

    const prevEndStr = sliceManifest?.oplogWindow?.previousEndTs;
    const prevEndTs = prevEndStr ? parseOplogTimestamp(prevEndStr) : null;

    if (prevEndTs) {
      if (compareTimestamps(prevEndTs, expectedPrevEndTs) > 0) {
        throw new Error(
          `PITR timeline gap detected between ${formatOplogTimestamp(expectedPrevEndTs)} and slice ${slice.key} (starts after ${formatOplogTimestamp(prevEndTs)})`,
        );
      }
    } else if (
      sliceManifest?.oplogWindow &&
      compareTimestamps(slice.startTs, expectedPrevEndTs) > 0
    ) {
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
    const sliceCovEnd = sliceManifest?.oplogWindow?.coverageEndTs
      ? parseOplogTimestamp(sliceManifest.oplogWindow.coverageEndTs)
      : null;
    const bestEnd =
      sliceCovEnd && compareTimestamps(sliceCovEnd, slice.endTs) > 0 ? sliceCovEnd : slice.endTs;
    if (compareTimestamps(bestEnd, lastCoveredTs) > 0) {
      lastCoveredTs = bestEnd;
    }
  }

  // Quiet-cluster coverage proof:
  // 1. A subsequent contiguous slice starting after `targetTs` whose `previousEndTs <= expectedPrevEndTs`
  //    proves zero writes occurred in `(expectedPrevEndTs, nextSlice.startTs)`.
  // 2. Or `state.json.coverageEndTs >= targetTs` (when `!gapDetected` and `state.lastTs <= expectedPrevEndTs`).
  if (compareTimestamps(lastCoveredTs, targetTs) < 0) {
    const nextSlice = allSlices.find((s) => compareTimestamps(s.startTs, targetTs) > 0);
    if (nextSlice) {
      const nextManifest = await readManifest(storage, nextSlice.key);
      const nextPrevEnd = nextManifest?.oplogWindow?.previousEndTs
        ? parseOplogTimestamp(nextManifest.oplogWindow.previousEndTs)
        : null;
      if (
        !nextManifest?.oplogWindow?.gapDetected &&
        nextPrevEnd &&
        compareTimestamps(nextPrevEnd, expectedPrevEndTs) <= 0
      ) {
        lastCoveredTs = targetTs;
      }
    }
  }

  if (compareTimestamps(lastCoveredTs, targetTs) < 0 && typeof storage.getJson === "function") {
    const stateObj = await storage.getJson<PitrStateRecord>(pitrStateKey(scope)).catch(() => null);
    const state =
      stateObj && "data" in stateObj ? stateObj.data : (stateObj as PitrStateRecord | null);
    const stateCov = state?.coverageEndTs ? parseOplogTimestamp(state.coverageEndTs) : null;
    const stateLast = state?.lastTs ? parseOplogTimestamp(state.lastTs) : null;
    if (
      !state?.gapDetected &&
      stateCov &&
      (!stateLast || compareTimestamps(stateLast, expectedPrevEndTs) <= 0) &&
      compareTimestamps(stateCov, lastCoveredTs) > 0
    ) {
      lastCoveredTs = stateCov;
    }
  }

  if (
    compareTimestamps(targetTs, baseEndTs) > 0 &&
    verifiedSlices.length === 0 &&
    compareTimestamps(lastCoveredTs, targetTs) < 0
  ) {
    throw new Error(
      `Target timestamp ${formatOplogTimestamp(targetTs)} is newer than base backup (${formatOplogTimestamp(baseEndTs)}), but no oplog slices are available to bridge the window`,
    );
  }

  // Require exact timestamp coverage (for ISO-second targets with i=0xffffffff, coverage must reach
  // past the end of that second so writes at higher ordinals within the second are never silently missed).
  const isInsufficientCoverage = compareTimestamps(lastCoveredTs, targetTs) < 0;

  if (isInsufficientCoverage) {
    throw new Error(
      `PITR oplog coverage ends at ${formatOplogTimestamp(lastCoveredTs)}, which is before target ${formatOplogTimestamp(targetTs)}`,
    );
  }

  return {
    baseBackup: selectedBackup,
    baseManifest: selectedManifest,
    baseEndTs,
    slices: verifiedSlices,
    lastCoveredTs,
    targetTs,
  };
}

function verifySliceManifest(
  config: AppConfig,
  sliceKey: string,
  manifest: BackupManifest | null,
): Buffer[] {
  const keyRing = resolveKeyRing(config);
  const isSigned = Boolean(manifest?.signature?.hmac || manifest?.encryption?.manifestHmac);

  if (isSigned && manifest) {
    if (keyRing.length === 0) {
      throw new Error(
        `Oplog slice manifest for ${sliceKey} is signed with HMAC, but ENCRYPTION_KEY / key ring is not configured`,
      );
    }
    verifyManifest(keyRing, manifest);
  } else if (manifest?.encryption) {
    if (keyRing.length === 0) {
      throw new Error(`Oplog slice ${sliceKey} is encrypted, but ENCRYPTION_KEY is not set`);
    }
  }

  return keyRing;
}

/**
 * Zero-disk streaming reader for an archived oplog slice.
 * Verifies manifest HMAC upfront, decrypts/gunzips on the fly into `consumer(bsonStream)`,
 * and verifies SHA-256 + byte size on completion.
 */
export async function streamOplogSlice(
  { config, storage, signal }: { config: AppConfig; storage: StorageClient; signal?: AbortSignal },
  sliceKey: string,
  consumer: (gunzippedBsonStream: Readable) => Promise<void>,
): Promise<ExtractOplogSliceResult> {
  const manifest = await readManifest(storage, sliceKey);
  const keyRing = verifySliceManifest(config, sliceKey, manifest);

  const digest = new DigestStream();
  const decryptStage = manifest?.encryption ? [createDecryptor(keyRing, manifest.encryption)] : [];
  const outPass = new PassThrough({ highWaterMark: 256 * 1024 });
  const controller = linkedController(signal);

  try {
    const downloadStream = await storage.download(sliceKey, { signal: controller.signal });
    const stages = [downloadStream, digest, ...decryptStage, createGunzip(), outPass];

    await settleAll(controller, [
      runStreamPipeline(stages, { signal: controller.signal }),
      consumer(outPass),
    ]);
  } finally {
    controller.dispose();
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

export async function downloadAndExtractOplogSlice(
  { config, storage, signal }: { config: AppConfig; storage: StorageClient; signal?: AbortSignal },
  sliceKey: string,
  destBsonFile: string,
): Promise<ExtractOplogSliceResult> {
  const manifest = await readManifest(storage, sliceKey);
  const keyRing = verifySliceManifest(config, sliceKey, manifest);

  const digest = new DigestStream();
  const decryptStage = manifest?.encryption ? [createDecryptor(keyRing, manifest.encryption)] : [];

  await fs.mkdir(path.dirname(destBsonFile), { recursive: true });
  const fileHandle = await fs.open(destBsonFile, "w", 0o600);
  const writeStream = fileHandle.createWriteStream();

  try {
    const downloadStream = await storage.download(sliceKey, { signal });
    const stages = [downloadStream, digest, ...decryptStage, createGunzip(), writeStream];
    await runStreamPipeline(stages, { signal });
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
  metrics,
  signal,
  createMongoClient = (u) => new MongoClient(u),
}: StartOplogArchiverOptions): OplogArchiver {
  const intervalMs = (config.pitr?.flushIntervalSeconds ?? 60) * 1000;
  const scope = scopeOf(config);
  const lockFile = path.join(config.workDir, ".oplog.lock");
  const distLockKey = `${pitrPrefix(scope)}.locks/oplog.lock.json`;
  let stopped = false;
  let activeTickPromise: Promise<void> | null = null;
  let activeAbortController: AbortController | null = null;
  let timer: NodeJS.Timeout | null = null;

  function recordManifestMetrics(manifest: BackupManifest): void {
    const endTsStr = manifest.oplogWindow?.coverageEndTs ?? manifest.oplogWindow?.endTs;
    const endTs = endTsStr ? parseOplogTimestamp(endTsStr) : null;
    metrics?.recordPitrSliceSuccess?.(
      manifest.oplogWindow?.entries ?? 0,
      manifest.size ?? 0,
      endTs?.t ?? Math.floor(Date.now() / 1000),
      Boolean(manifest.oplogWindow?.gapDetected),
    );
  }

  async function runTick(): Promise<void> {
    if (stopped || signal?.aborted) return;
    let release: (() => Promise<void>) | null = null;
    const tickController = linkedController(signal);
    activeAbortController = new AbortController();
    const abortTick = (err: unknown) => {
      activeAbortController?.abort(err);
      tickController.abort(err);
    };

    try {
      release = await acquireHybridLock({
        file: lockFile,
        storage,
        lockKey: distLockKey,
        staleAfterMs: Math.max(120_000, intervalMs * 3),
        distributedTtlMs: Math.max(90_000, intervalMs * 2),
        logger,
        onLost: (err) => {
          logger.error({ err }, "Oplog archiver lock lease lost; aborting active slice capture");
          abortTick(err);
        },
      });
      if (!release) {
        logger.debug("Skipping PITR oplog slice tick: another instance holds the oplog lock");
        return;
      }

      // Drain all pending oplog slices up to the current majority commit point so
      // high write throughput (> maxSliceMb per flushIntervalSeconds) never falls behind.
      if (config.sharding?.enabled) {
        const { captureShardedOplogSlices } = await import("./sharding.js");
        while (!stopped && !tickController.signal.aborted) {
          const result = await captureShardedOplogSlices({
            config,
            storage,
            logger,
            signal: tickController.signal,
            createMongoClient,
          });
          if (!result) break;
          const capturedList = [
            ...(result.configsvr ? [result.configsvr] : []),
            ...Object.values(result.shards).filter((m): m is BackupManifest => Boolean(m)),
          ];
          for (const m of capturedList) {
            recordManifestMetrics(m);
          }
          if (capturedList.length === 0) break;
        }
      } else {
        let capturedAny = false;
        while (!stopped && !tickController.signal.aborted) {
          const captured = await captureOplogSlice({
            config,
            storage,
            logger,
            signal: tickController.signal,
            createMongoClient,
          });
          if (!captured) break;
          capturedAny = true;
          recordManifestMetrics(captured);
        }
        if (!capturedAny && metrics?.recordPitrCoverage && typeof storage.getJson === "function") {
          const stateObj = await storage
            .getJson<PitrStateRecord>(pitrStateKey(scope))
            .catch(() => null);
          const st =
            stateObj && "data" in stateObj ? stateObj.data : (stateObj as PitrStateRecord | null);
          const covStr = st?.coverageEndTs ?? st?.lastTs;
          const covTs = covStr ? parseOplogTimestamp(covStr) : null;
          if (covTs) {
            metrics.recordPitrCoverage(covTs.t, Boolean(st?.gapDetected));
          }
        }
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      const msgLower = err.message.toLowerCase();
      const isGapErr =
        msgLower.includes("cappedpositionlost") ||
        msgLower.includes("oplog rolled over") ||
        msgLower.includes("timeline diverged");
      metrics?.recordPitrSliceFailure?.(isGapErr);
      logger.error({ err }, "Continuous PITR oplog slice capture failed");
      if (notifier) {
        await notifier.notifyFailure("pitr-oplog", err).catch(() => {});
      }
    } finally {
      activeAbortController = null;
      tickController.dispose();
      if (release) {
        await release().catch(() => {});
      }
    }
  }

  function scheduleTick(): void {
    if (stopped || signal?.aborted || activeTickPromise) return;
    activeTickPromise = runTick().finally(() => {
      activeTickPromise = null;
    });
  }

  return {
    start(): void {
      logger.info(
        {
          flushIntervalSeconds: config.pitr?.flushIntervalSeconds ?? 60,
          maxSliceMb: config.pitr?.maxSliceMb ?? 16,
        },
        "Starting continuous PITR oplog archiver",
      );
      scheduleTick();
      timer = setInterval(() => {
        scheduleTick();
      }, intervalMs);
      timer.unref();
    },

    async stop(): Promise<void> {
      stopped = true;
      if (timer) clearInterval(timer);
      if (activeAbortController) {
        activeAbortController.abort(new Error("Oplog archiver stopping"));
      }
      if (activeTickPromise) {
        try {
          await activeTickPromise;
        } catch {}
      }
    },
  };
}
