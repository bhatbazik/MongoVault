import { Transform, type Readable, type TransformCallback } from "node:stream";
import { BSON } from "mongodb";
import type { OplogTimestamp } from "./types/oplog.js";

/**
 * MongoDB `mongodump --archive` binary format magic number (0x8199e26d LE).
 * Reference: github.com/mongodb/mongo-tools/common/archive
 */
export const ARCHIVE_MAGIC_NUMBER = 0x8199e26d;
const TERMINATOR = -1; // 0xFFFFFFFF signed int32 LE
const MAX_BSON_DOC_SIZE = 64 * 1024 * 1024; // 64 MiB defensive cap

export interface ArchiveOplogBounds {
  firstTs: OplogTimestamp | null;
  lastTs: OplogTimestamp | null;
  entries: number;
  validArchive: boolean;
  archiveTerminated: boolean;
  corrupted: boolean;
  bytesRead: number;
}

export interface ArchiveDocument {
  db: string;
  collection: string;
  rawDoc: Buffer;
}

/**
 * O(1)-amortized byte queue backed by a chunk list with head cursor,
 * eliminating O(N^2) `Buffer.concat` overhead on large streamed documents.
 */
export class ByteQueue {
  #chunks: Buffer[] = [];
  #headIdx = 0;
  #headOffset = 0;
  #totalBytes = 0;
  readonly #scratch4 = Buffer.allocUnsafe(4);

  get available(): number {
    return this.#totalBytes;
  }

  push(chunk: Buffer): void {
    if (chunk.length === 0) return;
    this.#chunks.push(chunk);
    this.#totalBytes += chunk.length;
  }

  clear(): void {
    this.#chunks = [];
    this.#headIdx = 0;
    this.#headOffset = 0;
    this.#totalBytes = 0;
  }

  #advanceChunk(): void {
    this.#headIdx++;
    this.#headOffset = 0;
    if (this.#headIdx >= 64 && this.#headIdx * 2 >= this.#chunks.length) {
      this.#chunks = this.#chunks.slice(this.#headIdx);
      this.#headIdx = 0;
    }
  }

  #fillScratch4(): boolean {
    if (this.#totalBytes < 4) return false;
    let copied = 0;
    let idx = this.#headIdx;
    let off = this.#headOffset;
    while (copied < 4 && idx < this.#chunks.length) {
      const c = this.#chunks[idx]!;
      const take = Math.min(4 - copied, c.length - off);
      c.copy(this.#scratch4, copied, off, off + take);
      copied += take;
      idx++;
      off = 0;
    }
    return true;
  }

  peekInt32LE(): number | null {
    if (this.#totalBytes < 4) return null;
    const head = this.#chunks[this.#headIdx]!;
    if (head.length - this.#headOffset >= 4) {
      return head.readInt32LE(this.#headOffset);
    }
    this.#fillScratch4();
    return this.#scratch4.readInt32LE(0);
  }

  peekUInt32LE(): number | null {
    if (this.#totalBytes < 4) return null;
    const head = this.#chunks[this.#headIdx]!;
    if (head.length - this.#headOffset >= 4) {
      return head.readUInt32LE(this.#headOffset);
    }
    this.#fillScratch4();
    return this.#scratch4.readUInt32LE(0);
  }

  readBytes(len: number): Buffer {
    if (len <= 0) return Buffer.alloc(0);
    const head = this.#chunks[this.#headIdx]!;
    if (head.length - this.#headOffset >= len) {
      const slice = head.subarray(this.#headOffset, this.#headOffset + len);
      this.#headOffset += len;
      this.#totalBytes -= len;
      if (this.#headOffset >= head.length) {
        this.#advanceChunk();
      }
      return slice;
    }
    const out = Buffer.allocUnsafe(len);
    let copied = 0;
    while (copied < len) {
      const cur = this.#chunks[this.#headIdx]!;
      const avail = cur.length - this.#headOffset;
      const take = Math.min(len - copied, avail);
      cur.copy(out, copied, this.#headOffset, this.#headOffset + take);
      copied += take;
      this.#headOffset += take;
      this.#totalBytes -= take;
      if (this.#headOffset >= cur.length) {
        this.#advanceChunk();
      }
    }
    return out;
  }

  consumeBytes(len: number): number {
    let remaining = len;
    let lastByte = -1;
    while (remaining > 0 && this.#totalBytes > 0) {
      const head = this.#chunks[this.#headIdx]!;
      const avail = head.length - this.#headOffset;
      if (avail <= remaining) {
        lastByte = head[head.length - 1]!;
        this.#totalBytes -= avail;
        remaining -= avail;
        this.#advanceChunk();
      } else {
        lastByte = head[this.#headOffset + remaining - 1]!;
        this.#headOffset += remaining;
        this.#totalBytes -= remaining;
        remaining = 0;
      }
    }
    return lastByte;
  }

  passBytes(len: number, emit: (buf: Buffer) => void): number {
    let remaining = len;
    let lastByte = -1;
    while (remaining > 0 && this.#totalBytes > 0) {
      const head = this.#chunks[this.#headIdx]!;
      const avail = head.length - this.#headOffset;
      if (avail <= remaining) {
        const slice = this.#headOffset === 0 ? head : head.subarray(this.#headOffset);
        lastByte = slice[slice.length - 1]!;
        emit(slice);
        this.#totalBytes -= avail;
        remaining -= avail;
        this.#advanceChunk();
      } else {
        const slice = head.subarray(this.#headOffset, this.#headOffset + remaining);
        lastByte = slice[slice.length - 1]!;
        emit(slice);
        this.#headOffset += remaining;
        this.#totalBytes -= remaining;
        remaining = 0;
      }
    }
    return lastByte;
  }

  drainAll(): Buffer[] {
    const out: Buffer[] = [];
    for (let i = this.#headIdx; i < this.#chunks.length; i++) {
      const c = this.#chunks[i]!;
      if (i === this.#headIdx && this.#headOffset > 0) {
        if (this.#headOffset < c.length) {
          out.push(c.subarray(this.#headOffset));
        }
      } else {
        out.push(c);
      }
    }
    this.clear();
    return out;
  }
}

function extractTsFromRawBson(rawDoc: Buffer): OplogTimestamp | null {
  try {
    const doc = BSON.deserialize(rawDoc, { promoteValues: false }) as {
      ts?: {
        getHighBits?: () => number;
        getLowBits?: () => number;
        t?: number;
        i?: number;
      };
    };
    const ts = doc.ts;
    if (!ts) return null;
    if (typeof ts.getHighBits === "function" && typeof ts.getLowBits === "function") {
      return { t: ts.getHighBits() >>> 0, i: ts.getLowBits() >>> 0 };
    }
    if (typeof ts.t === "number" && typeof ts.i === "number") {
      return { t: ts.t >>> 0, i: ts.i >>> 0 };
    }
    return null;
  } catch {
    return null;
  }
}

export function isEmbeddedOplogNamespace(db: string, collection: string): boolean {
  return db === "" && (collection === "oplog" || collection === "");
}

const TIMESERIES_BUCKETS_PREFIX = "system.buckets.";

class ArchiveNamespaceTracker {
  readonly #pendingPrelude = new Set<string>();
  readonly #pendingTimeseries = new Set<string>();
  readonly #openChunks = new Set<string>();

  static #key(db: string, collection: string): string {
    return `${db}\0${collection}`;
  }

  recordPrelude(db: string, collection: string, type?: string): void {
    const key = ArchiveNamespaceTracker.#key(db, collection);
    this.#pendingPrelude.add(key);
    if (type === "timeseries" && !collection.startsWith(TIMESERIES_BUCKETS_PREFIX)) {
      this.#pendingTimeseries.add(key);
    }
  }

  recordChunkHeader(db: string, collection: string, isEof: boolean): void {
    const key = ArchiveNamespaceTracker.#key(db, collection);
    if (isEof) {
      this.#openChunks.delete(key);
      this.#pendingPrelude.delete(key);
      if (collection.startsWith(TIMESERIES_BUCKETS_PREFIX)) {
        const logicalKey = ArchiveNamespaceTracker.#key(
          db,
          collection.slice(TIMESERIES_BUCKETS_PREFIX.length),
        );
        if (this.#pendingTimeseries.has(logicalKey)) {
          this.#pendingTimeseries.delete(logicalKey);
          this.#pendingPrelude.delete(logicalKey);
        }
      } else {
        this.#pendingTimeseries.delete(key);
      }
    } else {
      this.#openChunks.add(key);
    }
  }

  allClosed(): boolean {
    return this.#openChunks.size === 0 && this.#pendingPrelude.size === 0;
  }

  clear(): void {
    this.#pendingPrelude.clear();
    this.#pendingTimeseries.clear();
    this.#openChunks.clear();
  }
}

type TapState =
  | "MAGIC"
  | "PRELUDE_HEADER"
  | "PRELUDE_META_OR_TERM"
  | "CHUNK_HEADER"
  | "CHUNK_DOC_LEN"
  | "CHUNK_DOC_SKIP"
  | "CHUNK_OPLOG_DOC"
  | "ARCHIVE_END"
  | "PASSTHROUGH";

/**
 * Zero-allocation pass-through Transform stream that inspects an uncompressed
 * `mongodump --archive` stream on the fly to extract the exact `firstTs` and `lastTs`
 * of the embedded oplog chunk (`db === "" && (collection === "oplog" || collection === "")`).
 *
 * Every byte passes straight through untouched. Collection document bytes are skipped
 * via 4-byte length arithmetic (O(1) RAM, zero deserialization overhead).
 * If the stream is not a valid MongoDB archive or encounters mid-stream corruption,
 * it invalidates `validArchive` / marks `corrupted` and falls back to passthrough without
 * throwing from the stream.
 */
export class ArchiveOplogTap extends Transform {
  #state: TapState = "MAGIC";
  #queue = new ByteQueue();
  #pendingBytes = 0;
  #pendingDocLen = 0;
  #currentIsOplog = false;
  #currentChunkIsEof = false;
  readonly #namespaces = new ArchiveNamespaceTracker();

  #firstTs: OplogTimestamp | null = null;
  #lastTs: OplogTimestamp | null = null;
  #entries = 0;
  #validArchive = false;
  #archiveTerminated = false;
  #corrupted = false;
  #bytesRead = 0;

  get bounds(): ArchiveOplogBounds {
    return {
      firstTs: this.#firstTs,
      lastTs: this.#lastTs,
      entries: this.#entries,
      validArchive: this.#validArchive,
      archiveTerminated: this.#archiveTerminated,
      corrupted: this.#corrupted,
      bytesRead: this.#bytesRead,
    };
  }

  #failToPassthrough(): void {
    this.#state = "PASSTHROUGH";
    this.#validArchive = false;
    this.#archiveTerminated = false;
    this.#corrupted = true;
    this.#currentIsOplog = false;
    this.#currentChunkIsEof = false;
    this.#queue.clear();
    this.#namespaces.clear();
  }

  override _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    this.#bytesRead += data.length;
    try {
      if (this.#state !== "PASSTHROUGH") {
        this.#processChunk(data);
      }
      callback(null, data);
    } catch {
      // Never fail a backup stream due to tap inspection; fall back to passthrough
      this.#failToPassthrough();
      callback(null, data);
    }
  }

  override _flush(callback: TransformCallback): void {
    if (this.#bytesRead > 0 && this.#state !== "PASSTHROUGH") {
      const cleanlyEnded =
        this.#queue.available === 0 &&
        ((this.#state === "ARCHIVE_END" && this.#archiveTerminated) ||
          (this.#state === "CHUNK_HEADER" && this.#namespaces.allClosed()));
      if (cleanlyEnded) {
        this.#archiveTerminated = true;
      } else {
        this.#corrupted = true;
        this.#validArchive = false;
        this.#archiveTerminated = false;
      }
    }
    callback();
  }

  #processChunk(incoming: Buffer): void {
    let offset = 0;

    // Fast path: skipping body bytes of a non-oplog BSON document without queuing
    if (this.#state === "CHUNK_DOC_SKIP" && this.#queue.available === 0) {
      if (incoming.length < this.#pendingBytes) {
        this.#pendingBytes -= incoming.length;
        return;
      }
      if (incoming[this.#pendingBytes - 1] !== 0x00) {
        this.#failToPassthrough();
        return;
      }
      offset = this.#pendingBytes;
      this.#pendingBytes = 0;
      this.#state = "CHUNK_DOC_LEN";
      if (offset >= incoming.length) return;
    }

    const tail = offset > 0 ? incoming.subarray(offset) : incoming;
    this.#queue.push(tail);

    while (this.#queue.available > 0 && this.#state !== "PASSTHROUGH") {
      if (this.#state === "ARCHIVE_END") {
        // Unexpected trailing bytes after archive-level TERMINATOR (-1)
        this.#failToPassthrough();
        return;
      }

      if (this.#state === "MAGIC") {
        const magic = this.#queue.peekUInt32LE();
        if (magic === null) break;
        if (magic !== ARCHIVE_MAGIC_NUMBER) {
          this.#failToPassthrough();
          return;
        }
        this.#queue.consumeBytes(4);
        this.#validArchive = true;
        this.#state = "PRELUDE_HEADER";
        continue;
      }

      if (this.#state === "PRELUDE_HEADER") {
        const docLen = this.#queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          this.#failToPassthrough();
          return;
        }
        if (this.#queue.available < docLen) break;
        const lastByte = this.#queue.consumeBytes(docLen);
        if (lastByte !== 0x00) {
          this.#failToPassthrough();
          return;
        }
        this.#state = "PRELUDE_META_OR_TERM";
        continue;
      }

      if (this.#state === "PRELUDE_META_OR_TERM") {
        const nextInt = this.#queue.peekInt32LE();
        if (nextInt === null) break;
        if (nextInt === TERMINATOR) {
          this.#queue.consumeBytes(4);
          this.#state = "CHUNK_HEADER";
          continue;
        }
        if (nextInt < 5 || nextInt > MAX_BSON_DOC_SIZE) {
          this.#failToPassthrough();
          return;
        }
        if (this.#queue.available < nextInt) break;
        const metaRaw = this.#queue.readBytes(nextInt);
        if (metaRaw[nextInt - 1] !== 0x00) {
          this.#failToPassthrough();
          return;
        }
        const meta = BSON.deserialize(metaRaw, { promoteValues: true }) as {
          db?: string;
          collection?: string;
          type?: string;
        };
        this.#namespaces.recordPrelude(
          typeof meta.db === "string" ? meta.db : "",
          typeof meta.collection === "string" ? meta.collection : "",
          typeof meta.type === "string" ? meta.type : undefined,
        );
        continue;
      }

      if (this.#state === "CHUNK_HEADER") {
        const docLen = this.#queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen === TERMINATOR) {
          this.#queue.consumeBytes(4);
          this.#archiveTerminated = true;
          this.#state = "ARCHIVE_END";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          this.#failToPassthrough();
          return;
        }
        if (this.#queue.available < docLen) break;
        const hdrRaw = this.#queue.readBytes(docLen);
        if (hdrRaw[docLen - 1] !== 0x00) {
          this.#failToPassthrough();
          return;
        }

        const hdr = BSON.deserialize(hdrRaw, { promoteValues: true }) as {
          db?: string;
          collection?: string;
          EOF?: boolean;
        };
        const db = typeof hdr.db === "string" ? hdr.db : "";
        const coll = typeof hdr.collection === "string" ? hdr.collection : "";
        const isEof = Boolean(hdr.EOF);
        this.#namespaces.recordChunkHeader(db, coll, isEof);
        this.#currentChunkIsEof = isEof;
        this.#currentIsOplog = isEmbeddedOplogNamespace(db, coll) && !isEof;
        this.#state = "CHUNK_DOC_LEN";
        continue;
      }

      if (this.#state === "CHUNK_DOC_LEN") {
        const docLen = this.#queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen === TERMINATOR) {
          this.#queue.consumeBytes(4);
          this.#currentIsOplog = false;
          this.#currentChunkIsEof = false;
          this.#state = "CHUNK_HEADER";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          this.#failToPassthrough();
          return;
        }
        if (this.#currentChunkIsEof) {
          this.#failToPassthrough();
          return;
        }
        if (this.#currentIsOplog) {
          this.#pendingDocLen = docLen;
          this.#state = "CHUNK_OPLOG_DOC";
        } else {
          const remainingInQueue = this.#queue.available;
          if (remainingInQueue >= docLen) {
            const lastByte = this.#queue.consumeBytes(docLen);
            if (lastByte !== 0x00) {
              this.#failToPassthrough();
              return;
            }
          } else {
            this.#queue.consumeBytes(remainingInQueue);
            this.#pendingBytes = docLen - remainingInQueue;
            this.#state = "CHUNK_DOC_SKIP";
          }
        }
        continue;
      }

      if (this.#state === "CHUNK_DOC_SKIP") {
        const available = this.#queue.available;
        if (available < this.#pendingBytes) {
          this.#queue.consumeBytes(available);
          this.#pendingBytes -= available;
          break;
        }
        const lastByte = this.#queue.consumeBytes(this.#pendingBytes);
        if (lastByte !== 0x00) {
          this.#failToPassthrough();
          return;
        }
        this.#pendingBytes = 0;
        this.#state = "CHUNK_DOC_LEN";
        continue;
      }

      if (this.#state === "CHUNK_OPLOG_DOC") {
        if (this.#queue.available < this.#pendingDocLen) break;
        const rawDoc = this.#queue.readBytes(this.#pendingDocLen);
        if (rawDoc[this.#pendingDocLen - 1] !== 0x00) {
          this.#failToPassthrough();
          return;
        }
        this.#pendingDocLen = 0;
        this.#state = "CHUNK_DOC_LEN";

        const ts = extractTsFromRawBson(rawDoc);
        if (!ts) {
          this.#failToPassthrough();
          return;
        }
        if (!this.#firstTs) this.#firstTs = ts;
        this.#lastTs = ts;
        this.#entries++;
        continue;
      }
    }
  }
}

export interface ArchiveNamespaceFilterOptions {
  /**
   * When true, non-archive payloads (first 4 bytes != ARCHIVE_MAGIC_NUMBER or empty/sub-4-byte
   * streams) pass through untouched instead of failing, allowing unit tests with dummy archive
   * bytes to work while real `mongorestore` still rejects invalid archive magic numbers.
   * Defaults to `false`.
   */
  passthroughNonArchive?: boolean;
  /**
   * When true (default), strips conflicting `_id_` index / `idIndex` metadata from timeseries
   * collections in the archive prelude so `mongorestore --preserveUUID` on MongoDB 9.0+ does not
   * fail with `(InvalidOptions) The 'clusteredIndex' option is not supported with the 'idIndex' option`.
   */
  sanitizeTimeseriesMetadata?: boolean;
}

function sanitizeTimeseriesPreludeMeta(
  meta: Record<string, unknown>,
): Record<string, unknown> | null {
  if (typeof meta.metadata !== "string" || !meta.metadata) return null;
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(meta.metadata) as Record<string, unknown>;
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;

  const opts =
    parsed.options && typeof parsed.options === "object"
      ? (parsed.options as Record<string, unknown>)
      : null;
  const isTimeseries = meta.type === "timeseries" || Boolean(opts?.timeseries);
  if (!isTimeseries) return null;

  let changed = false;
  if (opts && "idIndex" in opts) {
    delete opts.idIndex;
    changed = true;
  }
  if (Array.isArray(parsed.indexes)) {
    const filtered = parsed.indexes.filter(
      (idx) =>
        !(idx && typeof idx === "object" && (idx as Record<string, unknown>).name === "_id_"),
    );
    if (filtered.length !== parsed.indexes.length) {
      parsed.indexes = filtered;
      changed = true;
    }
  }
  if (!changed) return null;
  return { ...meta, metadata: JSON.stringify(parsed) };
}

function compileNamespaceExcludePredicate(
  excludePatterns: string[] | ((db: string, collection: string) => boolean),
): (db: string, collection: string) => boolean {
  if (typeof excludePatterns === "function") {
    return (db: string, collection: string) => {
      if (isEmbeddedOplogNamespace(db, collection)) return false;
      return excludePatterns(db, collection);
    };
  }
  const exact = new Set<string>();
  const regexes: RegExp[] = [];
  for (const raw of excludePatterns) {
    const pattern = raw.trim();
    if (!pattern) continue;
    if (!pattern.includes("*")) {
      exact.add(pattern);
    } else {
      const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*");
      regexes.push(new RegExp(`^${escaped}$`));
    }
  }
  return (db: string, collection: string) => {
    // Embedded oplog chunk (db === "" && (collection === "oplog" || collection === "")) is never excluded
    // so its BSON stream and CRC remain intact.
    if (isEmbeddedOplogNamespace(db, collection)) return false;
    const ns = `${db}.${collection}`;
    if (exact.has(ns)) return true;
    return regexes.some((rx) => rx.test(ns));
  };
}

type FilterState =
  | "MAGIC"
  | "PRELUDE_HEADER"
  | "PRELUDE_META_OR_TERM"
  | "CHUNK_HEADER"
  | "CHUNK_DOC_LEN"
  | "CHUNK_DOC_SKIP"
  | "CHUNK_DOC_PASS"
  | "ARCHIVE_END"
  | "PASSTHROUGH";

/**
 * Streaming Transform filter over an uncompressed `mongodump --archive` stream
 * that strips excluded namespaces' prelude `CollectionMetadata` documents and collection
 * chunks (in O(1) RAM, O(N) time) while keeping all other collections and the embedded
 * oplog chunk (`db === "" && (collection === "oplog" || collection === "")`) byte-for-byte intact so CRCs remain valid.
 */
export class ArchiveNamespaceFilter extends Transform {
  readonly #isExcluded: (db: string, collection: string) => boolean;
  readonly #passthroughNonArchive: boolean;
  readonly #sanitizeTimeseriesMetadata: boolean;
  #state: FilterState = "MAGIC";
  #queue = new ByteQueue();
  #pendingBytes = 0;
  #currentExcluded = false;
  #currentChunkIsEof = false;
  #seenMagic = false;
  #archiveEnded = false;
  readonly #namespaces = new ArchiveNamespaceTracker();

  constructor(
    excludePatterns: string[] | ((db: string, collection: string) => boolean),
    options: ArchiveNamespaceFilterOptions = {},
  ) {
    super();
    this.#isExcluded = compileNamespaceExcludePredicate(excludePatterns);
    this.#passthroughNonArchive = Boolean(options.passthroughNonArchive);
    this.#sanitizeTimeseriesMetadata = options.sanitizeTimeseriesMetadata !== false;
  }

  override _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    try {
      if (this.#state === "PASSTHROUGH") {
        callback(null, data);
        return;
      }
      this.#queue.push(data);
      this.#processQueue();
      callback();
    } catch (error) {
      callback(error instanceof Error ? error : new Error(String(error)));
    }
  }

  override _flush(callback: TransformCallback): void {
    if (this.#state === "PASSTHROUGH") {
      callback();
      return;
    }
    if (!this.#seenMagic) {
      if (this.#passthroughNonArchive) {
        for (const buf of this.#queue.drainAll()) {
          this.push(buf);
        }
        callback();
        return;
      }
      callback(new Error("Truncated MongoDB archive stream in namespace filter"));
      return;
    }
    const cleanlyEnded =
      this.#queue.available === 0 &&
      ((this.#state === "ARCHIVE_END" && this.#archiveEnded) ||
        (this.#state === "CHUNK_HEADER" && this.#namespaces.allClosed()));
    if (!cleanlyEnded) {
      callback(new Error("Truncated MongoDB archive stream in namespace filter"));
      return;
    }
    callback();
  }

  #processQueue(): void {
    while (this.#queue.available > 0 && this.#state !== "PASSTHROUGH") {
      if (this.#state === "ARCHIVE_END") {
        throw new Error(
          "Corrupted MongoDB archive stream: unexpected trailing bytes after archive terminator",
        );
      }

      if (this.#state === "MAGIC") {
        const magic = this.#queue.peekUInt32LE();
        if (magic === null) break;
        if (magic !== ARCHIVE_MAGIC_NUMBER) {
          if (this.#passthroughNonArchive) {
            this.#state = "PASSTHROUGH";
            for (const buf of this.#queue.drainAll()) {
              this.push(buf);
            }
            return;
          }
          throw new Error(`Invalid MongoDB archive magic number: 0x${magic.toString(16)}`);
        }
        this.#seenMagic = true;
        this.push(this.#queue.readBytes(4));
        this.#state = "PRELUDE_HEADER";
        continue;
      }

      if (this.#state === "PRELUDE_HEADER") {
        const docLen = this.#queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive prelude header length: ${docLen}`);
        }
        if (this.#queue.available < docLen) break;
        const raw = this.#queue.readBytes(docLen);
        if (raw[docLen - 1] !== 0x00) {
          throw new Error("Corrupted archive prelude header: missing null terminator");
        }
        this.push(raw);
        this.#state = "PRELUDE_META_OR_TERM";
        continue;
      }

      if (this.#state === "PRELUDE_META_OR_TERM") {
        const nextInt = this.#queue.peekInt32LE();
        if (nextInt === null) break;
        if (nextInt === TERMINATOR) {
          this.push(this.#queue.readBytes(4));
          this.#state = "CHUNK_HEADER";
          continue;
        }
        if (nextInt < 5 || nextInt > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive prelude metadata length: ${nextInt}`);
        }
        if (this.#queue.available < nextInt) break;
        const rawMeta = this.#queue.readBytes(nextInt);
        if (rawMeta[nextInt - 1] !== 0x00) {
          throw new Error("Corrupted archive prelude metadata: missing null terminator");
        }
        const meta = BSON.deserialize(rawMeta, { promoteValues: true }) as Record<string, unknown>;
        const db = typeof meta.db === "string" ? meta.db : "";
        const coll = typeof meta.collection === "string" ? meta.collection : "";
        this.#namespaces.recordPrelude(
          db,
          coll,
          typeof meta.type === "string" ? meta.type : undefined,
        );
        if (!this.#isExcluded(db, coll)) {
          const sanitized = this.#sanitizeTimeseriesMetadata
            ? sanitizeTimeseriesPreludeMeta(meta)
            : null;
          this.push(sanitized ? Buffer.from(BSON.serialize(sanitized)) : rawMeta);
        }
        continue;
      }

      if (this.#state === "CHUNK_HEADER") {
        const docLen = this.#queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen === TERMINATOR) {
          this.push(this.#queue.readBytes(4));
          this.#archiveEnded = true;
          this.#state = "ARCHIVE_END";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive chunk header length: ${docLen}`);
        }
        if (this.#queue.available < docLen) break;
        const hdrRaw = this.#queue.readBytes(docLen);
        if (hdrRaw[docLen - 1] !== 0x00) {
          throw new Error("Corrupted archive chunk header: missing null terminator");
        }
        const hdr = BSON.deserialize(hdrRaw, { promoteValues: true }) as {
          db?: string;
          collection?: string;
          EOF?: boolean;
        };
        const db = typeof hdr.db === "string" ? hdr.db : "";
        const coll = typeof hdr.collection === "string" ? hdr.collection : "";
        const isEof = Boolean(hdr.EOF);
        this.#namespaces.recordChunkHeader(db, coll, isEof);
        this.#currentChunkIsEof = isEof;
        this.#currentExcluded = this.#isExcluded(db, coll);
        if (!this.#currentExcluded) {
          this.push(hdrRaw);
        }
        this.#state = "CHUNK_DOC_LEN";
        continue;
      }

      if (this.#state === "CHUNK_DOC_LEN") {
        const docLen = this.#queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen === TERMINATOR) {
          const term = this.#queue.readBytes(4);
          if (!this.#currentExcluded) {
            this.push(term);
          }
          this.#currentExcluded = false;
          this.#currentChunkIsEof = false;
          this.#state = "CHUNK_HEADER";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive document length: ${docLen}`);
        }
        if (this.#currentChunkIsEof) {
          throw new Error("Corrupted MongoDB archive stream: unexpected document in EOF chunk");
        }
        this.#pendingBytes = docLen;
        this.#state = this.#currentExcluded ? "CHUNK_DOC_SKIP" : "CHUNK_DOC_PASS";
        continue;
      }

      if (this.#state === "CHUNK_DOC_SKIP") {
        const avail = this.#queue.available;
        if (avail < this.#pendingBytes) {
          this.#queue.consumeBytes(avail);
          this.#pendingBytes -= avail;
          break;
        }
        const lastByte = this.#queue.consumeBytes(this.#pendingBytes);
        if (lastByte !== 0x00) {
          throw new Error("Corrupted BSON document in archive: missing null terminator");
        }
        this.#pendingBytes = 0;
        this.#state = "CHUNK_DOC_LEN";
        continue;
      }

      if (this.#state === "CHUNK_DOC_PASS") {
        const avail = this.#queue.available;
        if (avail < this.#pendingBytes) {
          this.#queue.passBytes(avail, (b) => this.push(b));
          this.#pendingBytes -= avail;
          break;
        }
        const lastByte = this.#queue.passBytes(this.#pendingBytes, (b) => this.push(b));
        if (lastByte !== 0x00) {
          throw new Error("Corrupted BSON document in archive: missing null terminator");
        }
        this.#pendingBytes = 0;
        this.#state = "CHUNK_DOC_LEN";
        continue;
      }
    }
  }
}

/**
 * Streams raw BSON documents sequentially from any Node Readable stream
 * (e.g. gunzipped oplog slice) with O(1) RAM and strict corruption checks.
 */
export async function readBsonDocumentsFromStream(
  stream: Readable,
  onRawDoc: (rawDoc: Buffer) => Promise<void> | void,
  signal?: AbortSignal,
): Promise<number> {
  const queue = new ByteQueue();
  let count = 0;

  for await (const rawChunk of stream) {
    if (signal?.aborted) {
      const err = new Error("BSON stream reading aborted");
      err.name = "AbortError";
      throw err;
    }
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
    queue.push(chunk);

    while (queue.available >= 4) {
      const docLen = queue.peekInt32LE()!;
      if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
        throw new Error(`Corrupted BSON document length (${docLen}) in stream`);
      }
      if (queue.available < docLen) break;

      const rawDoc = Buffer.from(queue.readBytes(docLen));
      if (rawDoc[docLen - 1] !== 0x00) {
        throw new Error("Corrupted BSON document: missing null terminator");
      }
      count++;
      await onRawDoc(rawDoc);
    }
  }

  if (queue.available > 0) {
    throw new Error(`Truncated BSON document at end of stream (${queue.available} trailing bytes)`);
  }

  return count;
}

/**
 * Streams documents for selected collections from an uncompressed `mongodump --archive` stream
 * with O(1) RAM and zero temporary disk files.
 *
 * Used for programmatic CSRS config.* restoration (preserving `config.system.sessions`)
 * and streaming extraction of embedded `oplog.bson` chunks.
 */
export async function streamArchiveCollections(
  stream: Readable,
  filter: (db: string, collection: string) => boolean,
  onDoc: (entry: ArchiveDocument) => Promise<void> | void,
  signal?: AbortSignal,
): Promise<number> {
  let state:
    | "MAGIC"
    | "PRELUDE_HEADER"
    | "PRELUDE_META_OR_TERM"
    | "CHUNK_HEADER"
    | "CHUNK_DOC_LEN"
    | "CHUNK_DOC_SKIP"
    | "CHUNK_DOC_READ"
    | "ARCHIVE_END" = "MAGIC";
  const queue = new ByteQueue();
  const namespaces = new ArchiveNamespaceTracker();
  let pendingBytes = 0;
  let pendingDocLen = 0;
  let currentDb = "";
  let currentCollection = "";
  let currentSelected = false;
  let currentChunkIsEof = false;
  let archiveEnded = false;
  let yielded = 0;

  for await (const rawChunk of stream) {
    if (signal?.aborted) {
      const err = new Error("Archive stream reading aborted");
      err.name = "AbortError";
      throw err;
    }
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);

    let offset = 0;
    if (state === "CHUNK_DOC_SKIP" && queue.available === 0) {
      if (chunk.length < pendingBytes) {
        pendingBytes -= chunk.length;
        continue;
      }
      if (chunk[pendingBytes - 1] !== 0x00) {
        throw new Error("Corrupted BSON document in archive: missing null terminator");
      }
      offset = pendingBytes;
      pendingBytes = 0;
      state = "CHUNK_DOC_LEN";
      if (offset >= chunk.length) continue;
    }

    const tail = offset > 0 ? chunk.subarray(offset) : chunk;
    queue.push(tail);

    while (queue.available > 0) {
      if (state === "ARCHIVE_END") {
        throw new Error(
          "Corrupted MongoDB archive stream: unexpected trailing bytes after archive terminator",
        );
      }

      if (state === "MAGIC") {
        const magic = queue.peekUInt32LE();
        if (magic === null) break;
        if (magic !== ARCHIVE_MAGIC_NUMBER) {
          throw new Error(`Invalid MongoDB archive magic number: 0x${magic.toString(16)}`);
        }
        queue.consumeBytes(4);
        state = "PRELUDE_HEADER";
        continue;
      }

      if (state === "PRELUDE_HEADER") {
        const docLen = queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive prelude header length: ${docLen}`);
        }
        if (queue.available < docLen) break;
        const lastByte = queue.consumeBytes(docLen);
        if (lastByte !== 0x00) {
          throw new Error("Corrupted archive prelude header: missing null terminator");
        }
        state = "PRELUDE_META_OR_TERM";
        continue;
      }

      if (state === "PRELUDE_META_OR_TERM") {
        const nextInt = queue.peekInt32LE();
        if (nextInt === null) break;
        if (nextInt === TERMINATOR) {
          queue.consumeBytes(4);
          state = "CHUNK_HEADER";
          continue;
        }
        if (nextInt < 5 || nextInt > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive prelude metadata length: ${nextInt}`);
        }
        if (queue.available < nextInt) break;
        const metaRaw = queue.readBytes(nextInt);
        if (metaRaw[nextInt - 1] !== 0x00) {
          throw new Error("Corrupted archive prelude metadata: missing null terminator");
        }
        const meta = BSON.deserialize(metaRaw, { promoteValues: true }) as {
          db?: string;
          collection?: string;
          type?: string;
        };
        namespaces.recordPrelude(
          typeof meta.db === "string" ? meta.db : "",
          typeof meta.collection === "string" ? meta.collection : "",
          typeof meta.type === "string" ? meta.type : undefined,
        );
        continue;
      }

      if (state === "CHUNK_HEADER") {
        const docLen = queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen === TERMINATOR) {
          queue.consumeBytes(4);
          archiveEnded = true;
          state = "ARCHIVE_END";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive chunk header length: ${docLen}`);
        }
        if (queue.available < docLen) break;
        const hdrRaw = queue.readBytes(docLen);
        if (hdrRaw[docLen - 1] !== 0x00) {
          throw new Error("Corrupted archive chunk header: missing null terminator");
        }

        const hdr = BSON.deserialize(hdrRaw, { promoteValues: true }) as {
          db?: string;
          collection?: string;
          EOF?: boolean;
        };
        currentDb = typeof hdr.db === "string" ? hdr.db : "";
        currentCollection = typeof hdr.collection === "string" ? hdr.collection : "";
        const isEof = Boolean(hdr.EOF);
        namespaces.recordChunkHeader(currentDb, currentCollection, isEof);
        currentChunkIsEof = isEof;
        currentSelected = !isEof && filter(currentDb, currentCollection);
        state = "CHUNK_DOC_LEN";
        continue;
      }

      if (state === "CHUNK_DOC_LEN") {
        const docLen = queue.peekInt32LE();
        if (docLen === null) break;
        if (docLen === TERMINATOR) {
          queue.consumeBytes(4);
          currentSelected = false;
          currentChunkIsEof = false;
          state = "CHUNK_HEADER";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive document length: ${docLen}`);
        }
        if (currentChunkIsEof) {
          throw new Error("Corrupted MongoDB archive stream: unexpected document in EOF chunk");
        }
        if (currentSelected) {
          pendingDocLen = docLen;
          state = "CHUNK_DOC_READ";
        } else {
          const remaining = queue.available;
          if (remaining >= docLen) {
            const lastByte = queue.consumeBytes(docLen);
            if (lastByte !== 0x00) {
              throw new Error("Corrupted BSON document in archive: missing null terminator");
            }
          } else {
            queue.consumeBytes(remaining);
            pendingBytes = docLen - remaining;
            state = "CHUNK_DOC_SKIP";
          }
        }
        continue;
      }

      if (state === "CHUNK_DOC_SKIP") {
        const available = queue.available;
        if (available < pendingBytes) {
          queue.consumeBytes(available);
          pendingBytes -= available;
          break;
        }
        const lastByte = queue.consumeBytes(pendingBytes);
        if (lastByte !== 0x00) {
          throw new Error("Corrupted BSON document in archive: missing null terminator");
        }
        pendingBytes = 0;
        state = "CHUNK_DOC_LEN";
        continue;
      }

      if (state === "CHUNK_DOC_READ") {
        if (queue.available < pendingDocLen) break;
        const rawDoc = Buffer.from(queue.readBytes(pendingDocLen));
        if (rawDoc[pendingDocLen - 1] !== 0x00) {
          throw new Error("Corrupted BSON document in archive: missing null terminator");
        }
        pendingDocLen = 0;
        state = "CHUNK_DOC_LEN";
        yielded++;
        await onDoc({ db: currentDb, collection: currentCollection, rawDoc });
        continue;
      }
    }
  }

  const cleanlyEnded =
    queue.available === 0 &&
    ((state === "ARCHIVE_END" && archiveEnded) ||
      (state === "CHUNK_HEADER" && namespaces.allClosed()));
  if (!cleanlyEnded) {
    throw new Error("Truncated MongoDB archive stream");
  }

  return yielded;
}
