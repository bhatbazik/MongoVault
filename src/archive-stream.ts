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
}

export interface ArchiveDocument {
  db: string;
  collection: string;
  rawDoc: Buffer;
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

type TapState =
  | "MAGIC"
  | "PRELUDE_HEADER"
  | "PRELUDE_META_OR_TERM"
  | "CHUNK_HEADER"
  | "CHUNK_DOC_LEN"
  | "CHUNK_DOC_SKIP"
  | "CHUNK_OPLOG_DOC"
  | "PASSTHROUGH";

/**
 * Zero-allocation pass-through Transform stream that inspects an uncompressed
 * `mongodump --archive` stream on the fly to extract the exact `firstTs` and `lastTs`
 * of the embedded oplog chunk (`db === "" && collection === ""`).
 *
 * Every byte passes straight through untouched. Collection document bytes are skipped
 * via 4-byte length arithmetic (O(1) RAM, zero deserialization overhead).
 * If the stream does not begin with the MongoDB archive magic number, it falls back
 * transparently to pure passthrough mode without error.
 */
export class ArchiveOplogTap extends Transform {
  #state: TapState = "MAGIC";
  #buf: Buffer = Buffer.alloc(0);
  #pendingBytes = 0;
  #pendingDocLen = 0;
  #currentIsOplog = false;

  #firstTs: OplogTimestamp | null = null;
  #lastTs: OplogTimestamp | null = null;
  #entries = 0;
  #validArchive = false;

  get bounds(): ArchiveOplogBounds {
    return {
      firstTs: this.#firstTs,
      lastTs: this.#lastTs,
      entries: this.#entries,
      validArchive: this.#validArchive,
    };
  }

  override _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    try {
      if (this.#state !== "PASSTHROUGH") {
        this.#processChunk(data);
      }
      callback(null, data);
    } catch {
      // Never fail a backup stream due to tap inspection; fall back to passthrough
      this.#state = "PASSTHROUGH";
      this.#buf = Buffer.alloc(0);
      callback(null, data);
    }
  }

  #processChunk(incoming: Buffer): void {
    let offset = 0;

    // Fast path: skipping body bytes of a non-oplog BSON document without copying
    if (this.#state === "CHUNK_DOC_SKIP" && this.#buf.length === 0) {
      if (incoming.length < this.#pendingBytes) {
        this.#pendingBytes -= incoming.length;
        return;
      }
      offset = this.#pendingBytes;
      this.#pendingBytes = 0;
      this.#state = "CHUNK_DOC_LEN";
      if (offset >= incoming.length) return;
    }

    const tail = offset > 0 ? incoming.subarray(offset) : incoming;
    this.#buf = this.#buf.length > 0 ? Buffer.concat([this.#buf, tail]) : tail;

    let pos = 0;
    const len = this.#buf.length;

    while (pos < len && this.#state !== "PASSTHROUGH") {
      if (this.#state === "MAGIC") {
        if (len - pos < 4) break;
        const magic = this.#buf.readUInt32LE(pos);
        if (magic !== ARCHIVE_MAGIC_NUMBER) {
          this.#state = "PASSTHROUGH";
          this.#buf = Buffer.alloc(0);
          return;
        }
        this.#validArchive = true;
        pos += 4;
        this.#state = "PRELUDE_HEADER";
        continue;
      }

      if (this.#state === "PRELUDE_HEADER") {
        if (len - pos < 4) break;
        const docLen = this.#buf.readInt32LE(pos);
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          this.#state = "PASSTHROUGH";
          this.#buf = Buffer.alloc(0);
          return;
        }
        if (len - pos < docLen) break;
        pos += docLen;
        this.#state = "PRELUDE_META_OR_TERM";
        continue;
      }

      if (this.#state === "PRELUDE_META_OR_TERM") {
        if (len - pos < 4) break;
        const nextInt = this.#buf.readInt32LE(pos);
        if (nextInt === TERMINATOR) {
          pos += 4;
          this.#state = "CHUNK_HEADER";
          continue;
        }
        if (nextInt < 5 || nextInt > MAX_BSON_DOC_SIZE) {
          this.#state = "PASSTHROUGH";
          this.#buf = Buffer.alloc(0);
          return;
        }
        if (len - pos < nextInt) break;
        pos += nextInt;
        continue;
      }

      if (this.#state === "CHUNK_HEADER") {
        if (len - pos < 4) break;
        const docLen = this.#buf.readInt32LE(pos);
        if (docLen === TERMINATOR) {
          pos += 4;
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          this.#state = "PASSTHROUGH";
          this.#buf = Buffer.alloc(0);
          return;
        }
        if (len - pos < docLen) break;
        const hdrRaw = this.#buf.subarray(pos, pos + docLen);
        pos += docLen;

        const hdr = BSON.deserialize(hdrRaw, { promoteValues: true }) as {
          db?: string;
          collection?: string;
          EOF?: boolean;
        };
        const db = typeof hdr.db === "string" ? hdr.db : "";
        const coll = typeof hdr.collection === "string" ? hdr.collection : "";
        this.#currentIsOplog = db === "" && coll === "" && !hdr.EOF;
        this.#state = "CHUNK_DOC_LEN";
        continue;
      }

      if (this.#state === "CHUNK_DOC_LEN") {
        if (len - pos < 4) break;
        const docLen = this.#buf.readInt32LE(pos);
        if (docLen === TERMINATOR) {
          pos += 4;
          this.#currentIsOplog = false;
          this.#state = "CHUNK_HEADER";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          this.#state = "PASSTHROUGH";
          this.#buf = Buffer.alloc(0);
          return;
        }
        if (this.#currentIsOplog) {
          this.#pendingDocLen = docLen;
          this.#state = "CHUNK_OPLOG_DOC";
        } else {
          const remainingInBuf = len - pos;
          if (remainingInBuf >= docLen) {
            pos += docLen;
          } else {
            this.#pendingBytes = docLen - remainingInBuf;
            pos = len;
            this.#state = "CHUNK_DOC_SKIP";
          }
        }
        continue;
      }

      if (this.#state === "CHUNK_DOC_SKIP") {
        const available = len - pos;
        if (available < this.#pendingBytes) {
          this.#pendingBytes -= available;
          pos = len;
          break;
        }
        pos += this.#pendingBytes;
        this.#pendingBytes = 0;
        this.#state = "CHUNK_DOC_LEN";
        continue;
      }

      if (this.#state === "CHUNK_OPLOG_DOC") {
        if (len - pos < this.#pendingDocLen) break;
        const rawDoc = this.#buf.subarray(pos, pos + this.#pendingDocLen);
        pos += this.#pendingDocLen;
        this.#pendingDocLen = 0;
        this.#state = "CHUNK_DOC_LEN";

        const ts = extractTsFromRawBson(rawDoc);
        if (ts) {
          if (!this.#firstTs) this.#firstTs = ts;
          this.#lastTs = ts;
        }
        this.#entries++;
        continue;
      }
    }

    if (pos === len) {
      this.#buf = Buffer.alloc(0);
    } else if (pos > 0) {
      // Copy remaining tail so we do not pin a large incoming buffer
      this.#buf = Buffer.from(this.#buf.subarray(pos));
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
  let buf = Buffer.alloc(0);
  let count = 0;

  for await (const rawChunk of stream) {
    if (signal?.aborted) {
      const err = new Error("BSON stream reading aborted");
      err.name = "AbortError";
      throw err;
    }
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
    buf = buf.length > 0 ? Buffer.concat([buf, chunk]) : chunk;

    let pos = 0;
    while (buf.length - pos >= 4) {
      const docLen = buf.readInt32LE(pos);
      if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
        throw new Error(`Corrupted BSON document length (${docLen}) in stream`);
      }
      if (buf.length - pos < docLen) break;

      const rawDoc = Buffer.from(buf.subarray(pos, pos + docLen));
      if (rawDoc[docLen - 1] !== 0x00) {
        throw new Error("Corrupted BSON document: missing null terminator");
      }
      pos += docLen;
      count++;
      await onRawDoc(rawDoc);
    }

    if (pos === buf.length) {
      buf = Buffer.alloc(0);
    } else if (pos > 0) {
      buf = Buffer.from(buf.subarray(pos));
    }
  }

  if (buf.length > 0) {
    throw new Error(`Truncated BSON document at end of stream (${buf.length} trailing bytes)`);
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
    | "CHUNK_DOC_READ" = "MAGIC";
  let buf = Buffer.alloc(0);
  let pendingBytes = 0;
  let pendingDocLen = 0;
  let currentDb = "";
  let currentCollection = "";
  let currentSelected = false;
  let yielded = 0;

  for await (const rawChunk of stream) {
    if (signal?.aborted) {
      const err = new Error("Archive stream reading aborted");
      err.name = "AbortError";
      throw err;
    }
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);

    let offset = 0;
    if (state === "CHUNK_DOC_SKIP" && buf.length === 0) {
      if (chunk.length < pendingBytes) {
        pendingBytes -= chunk.length;
        continue;
      }
      offset = pendingBytes;
      pendingBytes = 0;
      state = "CHUNK_DOC_LEN";
      if (offset >= chunk.length) continue;
    }

    const tail = offset > 0 ? chunk.subarray(offset) : chunk;
    buf = buf.length > 0 ? Buffer.concat([buf, tail]) : tail;

    let pos = 0;
    const len = buf.length;

    while (pos < len) {
      if (state === "MAGIC") {
        if (len - pos < 4) break;
        const magic = buf.readUInt32LE(pos);
        if (magic !== ARCHIVE_MAGIC_NUMBER) {
          throw new Error(`Invalid MongoDB archive magic number: 0x${magic.toString(16)}`);
        }
        pos += 4;
        state = "PRELUDE_HEADER";
        continue;
      }

      if (state === "PRELUDE_HEADER") {
        if (len - pos < 4) break;
        const docLen = buf.readInt32LE(pos);
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive prelude header length: ${docLen}`);
        }
        if (len - pos < docLen) break;
        pos += docLen;
        state = "PRELUDE_META_OR_TERM";
        continue;
      }

      if (state === "PRELUDE_META_OR_TERM") {
        if (len - pos < 4) break;
        const nextInt = buf.readInt32LE(pos);
        if (nextInt === TERMINATOR) {
          pos += 4;
          state = "CHUNK_HEADER";
          continue;
        }
        if (nextInt < 5 || nextInt > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive prelude metadata length: ${nextInt}`);
        }
        if (len - pos < nextInt) break;
        pos += nextInt;
        continue;
      }

      if (state === "CHUNK_HEADER") {
        if (len - pos < 4) break;
        const docLen = buf.readInt32LE(pos);
        if (docLen === TERMINATOR) {
          pos += 4;
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive chunk header length: ${docLen}`);
        }
        if (len - pos < docLen) break;
        const hdrRaw = buf.subarray(pos, pos + docLen);
        pos += docLen;

        const hdr = BSON.deserialize(hdrRaw, { promoteValues: true }) as {
          db?: string;
          collection?: string;
          EOF?: boolean;
        };
        currentDb = typeof hdr.db === "string" ? hdr.db : "";
        currentCollection = typeof hdr.collection === "string" ? hdr.collection : "";
        currentSelected = !hdr.EOF && filter(currentDb, currentCollection);
        state = "CHUNK_DOC_LEN";
        continue;
      }

      if (state === "CHUNK_DOC_LEN") {
        if (len - pos < 4) break;
        const docLen = buf.readInt32LE(pos);
        if (docLen === TERMINATOR) {
          pos += 4;
          currentSelected = false;
          state = "CHUNK_HEADER";
          continue;
        }
        if (docLen < 5 || docLen > MAX_BSON_DOC_SIZE) {
          throw new Error(`Corrupted archive document length: ${docLen}`);
        }
        if (currentSelected) {
          pendingDocLen = docLen;
          state = "CHUNK_DOC_READ";
        } else {
          const remaining = len - pos;
          if (remaining >= docLen) {
            pos += docLen;
          } else {
            pendingBytes = docLen - remaining;
            pos = len;
            state = "CHUNK_DOC_SKIP";
          }
        }
        continue;
      }

      if (state === "CHUNK_DOC_SKIP") {
        const available = len - pos;
        if (available < pendingBytes) {
          pendingBytes -= available;
          pos = len;
          break;
        }
        pos += pendingBytes;
        pendingBytes = 0;
        state = "CHUNK_DOC_LEN";
        continue;
      }

      if (state === "CHUNK_DOC_READ") {
        if (len - pos < pendingDocLen) break;
        const rawDoc = Buffer.from(buf.subarray(pos, pos + pendingDocLen));
        if (rawDoc[pendingDocLen - 1] !== 0x00) {
          throw new Error("Corrupted BSON document in archive: missing null terminator");
        }
        pos += pendingDocLen;
        pendingDocLen = 0;
        state = "CHUNK_DOC_LEN";
        yielded++;
        await onDoc({ db: currentDb, collection: currentCollection, rawDoc });
        continue;
      }
    }

    if (pos === len) {
      buf = Buffer.alloc(0);
    } else if (pos > 0) {
      buf = Buffer.from(buf.subarray(pos));
    }
  }

  if (state === "CHUNK_DOC_SKIP" || state === "CHUNK_DOC_READ" || buf.length > 0) {
    throw new Error("Truncated MongoDB archive stream");
  }

  return yielded;
}
