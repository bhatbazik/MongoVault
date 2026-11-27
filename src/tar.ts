import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";

export interface TarEntry {
  name: string;
  fullPath: string;
  size: number;
}

export interface TarExtractResult {
  extractedCount: number;
  totalBytes: number;
}

const MAX_PAX_PAYLOAD_BYTES = 1024 * 1024; // 1 MiB defensive cap

function makeAbortError(reason?: unknown): Error {
  if (reason instanceof Error) {
    if (reason.name === "AbortError") return reason;
    const err = new Error(reason.message);
    err.name = "AbortError";
    return err;
  }
  const err = new Error(reason ? String(reason) : "The operation was aborted");
  err.name = "AbortError";
  return err;
}

function splitUstarPath(cleanName: string): { prefix: string; base: string } | null {
  if (Buffer.byteLength(cleanName, "utf8") <= 100) {
    return { prefix: "", base: cleanName };
  }
  // Find a '/' split where prefix <= 155 UTF-8 bytes and base <= 100 UTF-8 bytes
  let searchFrom = cleanName.length;
  while (searchFrom > 0) {
    const slashIdx = cleanName.lastIndexOf("/", searchFrom - 1);
    if (slashIdx <= 0) break;
    const prefix = cleanName.slice(0, slashIdx);
    const base = cleanName.slice(slashIdx + 1);
    if (
      base.length > 0 &&
      Buffer.byteLength(base, "utf8") <= 100 &&
      Buffer.byteLength(prefix, "utf8") <= 155
    ) {
      return { prefix, base };
    }
    searchFrom = slashIdx;
  }
  return null;
}

function truncateUtf8(str: string, maxBytes: number): string {
  const buf = Buffer.from(str, "utf8");
  if (buf.length <= maxBytes) return str;
  // Walk back from maxBytes so we don't split a multi-byte UTF-8 sequence
  let end = maxBytes;
  while (end > 0 && (buf[end]! & 0xc0) === 0x80) {
    end--;
  }
  return buf.subarray(0, end).toString("utf8");
}

function buildRawTarHeader(
  baseName: string,
  prefix: string,
  size: number,
  mtime: number,
  typeflag: string,
): Buffer {
  const buf = Buffer.alloc(512, 0);

  buf.write(truncateUtf8(baseName, 100), 0, 100, "utf8");
  if (prefix) {
    buf.write(truncateUtf8(prefix, 155), 345, 155, "utf8");
  }

  // mode: 100..108 (0644)
  buf.write("0000644\0", 100, "ascii");
  // uid: 108..116
  buf.write("0000000\0", 108, "ascii");
  // gid: 116..124
  buf.write("0000000\0", 116, "ascii");

  // size: 124..136 (11 octal digits + null terminator, or GNU base-256 for files >= 8 GiB)
  if (size <= 0o77777777777) {
    const sizeOctal = size.toString(8).padStart(11, "0") + "\0";
    buf.write(sizeOctal, 124, "ascii");
  } else {
    buf[124] = 0x80;
    let remaining = BigInt(size);
    for (let i = 135; i >= 125; i--) {
      buf[i] = Number(remaining & 0xffn);
      remaining >>= 8n;
    }
  }

  // mtime: 136..148 (seconds since epoch in octal + null terminator)
  const mtimeOctal =
    Math.floor(mtime / 1000)
      .toString(8)
      .padStart(11, "0") + "\0";
  buf.write(mtimeOctal, 136, "ascii");

  // typeflag: 156 ('0' = regular file, 'x' = PAX extended header)
  buf.write(typeflag, 156, "ascii");

  // magic: 257..263 ('ustar\0')
  buf.write("ustar\0", 257, "ascii");
  // version: 263..265 ('00')
  buf.write("00", 263, "ascii");

  // Compute checksum over 512 bytes with chksum field treated as 8 ASCII spaces (32)
  buf.fill(32, 148, 156);
  let checksum = 0;
  for (let i = 0; i < 512; i++) {
    checksum += buf[i]!;
  }
  const chksumOctal = checksum.toString(8).padStart(6, "0") + "\0 ";
  buf.write(chksumOctal, 148, "ascii");

  return buf;
}

function buildPaxExtendedHeader(cleanName: string, mtime: number): Buffer {
  // Format: "<len> path=<cleanName>\n" where <len> includes its own decimal digits
  const tail = ` path=${cleanName}\n`;
  const tailBytes = Buffer.byteLength(tail, "utf8");
  let totalLen = tailBytes + String(tailBytes).length;
  while (String(totalLen).length + tailBytes !== totalLen) {
    totalLen = String(totalLen).length + tailBytes;
  }
  const payload = Buffer.from(`${totalLen}${tail}`, "utf8");
  const paxHeader = buildRawTarHeader("PaxHeaders/entry", "", payload.length, mtime, "x");
  const padLen = payload.length % 512 === 0 ? 0 : 512 - (payload.length % 512);
  return padLen > 0
    ? Buffer.concat([paxHeader, payload, Buffer.alloc(padLen, 0)])
    : Buffer.concat([paxHeader, payload]);
}

interface PaxHeaders {
  path?: string;
  size?: number;
}

function parsePaxHeaders(payload: Buffer): PaxHeaders {
  const result: PaxHeaders = {};
  let offset = 0;
  while (offset < payload.length) {
    const spaceIdx = payload.indexOf(0x20, offset);
    if (spaceIdx === -1) {
      throw new Error("Corrupted tar PAX header: missing record length delimiter");
    }
    const lenStr = payload.toString("ascii", offset, spaceIdx);
    if (!/^\d+$/.test(lenStr)) {
      throw new Error(`Corrupted tar PAX header: invalid record length "${lenStr}"`);
    }
    const recLen = Number(lenStr);
    const minPrefixLen = spaceIdx - offset + 1;
    if (
      !Number.isSafeInteger(recLen) ||
      recLen <= minPrefixLen ||
      offset + recLen > payload.length
    ) {
      throw new Error(
        `Corrupted tar PAX header: record length ${lenStr} out of bounds (offset=${offset}, payloadLength=${payload.length})`,
      );
    }
    if (payload[offset + recLen - 1] !== 0x0a) {
      throw new Error("Corrupted tar PAX header: record missing trailing newline");
    }
    const recordEnd = offset + recLen - 1;
    const record = payload.toString("utf8", spaceIdx + 1, recordEnd);
    const eqIdx = record.indexOf("=");
    if (eqIdx > 0) {
      const key = record.slice(0, eqIdx);
      const value = record.slice(eqIdx + 1);
      if (key === "path") {
        result.path = value;
      } else if (key === "size") {
        if (!/^\d+$/.test(value)) {
          throw new Error(`Invalid PAX size value: "${value}"`);
        }
        const parsedSize = Number(value);
        if (!Number.isSafeInteger(parsedSize) || parsedSize < 0) {
          throw new Error(`PAX size out of safe integer range: "${value}"`);
        }
        result.size = parsedSize;
      }
    }
    offset += recLen;
  }
  return result;
}

/**
 * Creates a standard 512-byte POSIX UStar tar file header, supporting paths up to 255 UTF-8 bytes via UStar prefix.
 */
export function createTarHeader(name: string, size: number, mtime: number = Date.now()): Buffer {
  const cleanName = name.replace(/\\/g, "/").replace(/^\/+/, "");
  const split = splitUstarPath(cleanName);
  if (split) {
    return buildRawTarHeader(split.base, split.prefix, size, mtime, "0");
  }
  const slashIdx = cleanName.lastIndexOf("/");
  const base = slashIdx >= 0 ? cleanName.slice(slashIdx + 1) : cleanName;
  const prefix = slashIdx > 0 ? cleanName.slice(0, slashIdx) : "";
  return buildRawTarHeader(base, prefix, size, mtime, "0");
}

/**
 * Creates either a 512-byte UStar header or a PAX extended header + UStar header
 * when the UTF-8 path exceeds UStar limits (100-byte name / 155-byte prefix).
 */
export function createTarHeaderBlock(
  name: string,
  size: number,
  mtime: number = Date.now(),
): Buffer {
  const cleanName = name.replace(/\\/g, "/").replace(/^\/+/, "");
  const split = splitUstarPath(cleanName);
  if (split) {
    return buildRawTarHeader(split.base, split.prefix, size, mtime, "0");
  }
  const paxBlock = buildPaxExtendedHeader(cleanName, mtime);
  const fallbackHeader = createTarHeader(cleanName, size, mtime);
  return Buffer.concat([paxBlock, fallbackHeader]);
}

async function writeAll(fh: fs.FileHandle, data: Buffer): Promise<void> {
  let offset = 0;
  while (offset < data.length) {
    const { bytesWritten } = await fh.write(data, offset, data.length - offset);
    if (bytesWritten <= 0) {
      throw new Error("Short write: 0 bytes written to target file");
    }
    offset += bytesWritten;
  }
}

/**
 * Generates an async stream of tar-encoded byte chunks from an array of files.
 */
export async function* generateTarStream(
  files: TarEntry[],
  signal?: AbortSignal,
  options: {
    includeEof?: boolean;
    onEntrySize?: (entry: TarEntry, actualSize: number) => void;
  } = {},
): AsyncGenerator<Buffer, void, unknown> {
  const { includeEof = true, onEntrySize } = options;
  for (const file of files) {
    if (signal?.aborted) {
      throw makeAbortError(signal.reason);
    }
    let stat;
    try {
      stat = await fs.stat(file.fullPath);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      throw new Error(
        `Physical backup file missing or unreadable during TAR streaming: ${file.name} (${msg})`,
        { cause: err },
      );
    }
    const expectedSize = stat.size;
    onEntrySize?.(file, expectedSize);
    const headerBlock = createTarHeaderBlock(file.name, expectedSize, stat.mtimeMs);
    yield headerBlock;

    // Stream file contents in 64KB chunks, strictly bounded to expectedSize
    // so live WiredTiger file growth never shifts TAR headers.
    let bytesEmitted = 0;
    if (expectedSize > 0) {
      const stream = fsSync.createReadStream(file.fullPath, { highWaterMark: 64 * 1024 });
      for await (const rawChunk of stream) {
        if (signal?.aborted) {
          stream.destroy();
          throw makeAbortError(signal.reason);
        }
        const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
        const remaining = expectedSize - bytesEmitted;
        if (chunk.length >= remaining) {
          if (remaining > 0) {
            yield chunk.subarray(0, remaining);
            bytesEmitted += remaining;
          }
          stream.destroy();
          break;
        }
        yield chunk;
        bytesEmitted += chunk.length;
      }
    }

    // Fail loudly if a file shrank/truncated during backup instead of silently zero-padding
    // corrupted WiredTiger B-tree pages.
    if (bytesEmitted < expectedSize) {
      throw new Error(
        `File ${file.name} shrank during physical backup (expected ${expectedSize} bytes, read ${bytesEmitted} bytes); refusing to zero-pad corrupted data`,
      );
    }

    // Pad file data to 512-byte block boundary
    const remainder = expectedSize % 512;
    if (remainder > 0) {
      yield Buffer.alloc(512 - remainder, 0);
    }
  }

  if (signal?.aborted) {
    throw makeAbortError(signal.reason);
  }

  // End of archive marker: two 512-byte zero blocks (1024 bytes)
  if (includeEof) {
    yield Buffer.alloc(1024, 0);
  }
}

/**
 * Wraps generateTarStream into a standard Node.js Readable stream.
 */
export function createTarPackStream(files: TarEntry[], signal?: AbortSignal): Readable {
  return Readable.from(generateTarStream(files, signal));
}

/**
 * Extracts a streaming POSIX tar archive safely to targetDir with path traversal guards,
 * PAX extended path support, full short-write handling, and truncation detection.
 */
async function processTarStream(
  stream: Readable,
  targetDir: string | null,
  signal?: AbortSignal,
): Promise<TarExtractResult> {
  const dryRun = targetDir === null;
  let resolvedRoot: string | null = null;
  if (!dryRun) {
    await fs.mkdir(targetDir, { recursive: true });
    resolvedRoot = path.resolve(targetDir);
  }

  let buffer = Buffer.alloc(0);
  let currentHeader: {
    name: string;
    size: number;
    kind: "file" | "pax" | "pax-global";
    mode?: number;
    mtimeSec?: number;
    fullPath?: string;
  } | null = null;
  let remainingBytes = 0;
  let remainingPad = 0;
  let currentFileHandle: fs.FileHandle | null = null;
  let paxChunks: Buffer[] = [];
  let pendingPax: PaxHeaders | null = null;
  let extractedCount = 0;
  let totalBytes = 0;
  let zeroBlocks = 0;

  try {
    for await (const chunk of stream) {
      if (signal?.aborted) {
        throw makeAbortError(signal.reason);
      }
      buffer = Buffer.concat([buffer, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);

      while (buffer.length > 0) {
        if (remainingPad > 0) {
          const toDiscard = Math.min(buffer.length, remainingPad);
          buffer = buffer.subarray(toDiscard);
          remainingPad -= toDiscard;
          if (remainingPad > 0) break;
        }

        if (!currentHeader) {
          if (buffer.length < 512) break; // Need full 512-byte header block

          const headerBuf = buffer.subarray(0, 512);
          buffer = buffer.subarray(512);

          // Check for 512-byte zero block (end-of-archive indicator: POSIX requires 2 blocks)
          const isZero = headerBuf.every((b) => b === 0);
          if (isZero) {
            if (pendingPax !== null) {
              throw new Error(
                "Truncated tar archive: dangling PAX extended header before end-of-archive",
              );
            }
            zeroBlocks++;
            continue;
          }

          if (zeroBlocks > 0) {
            throw new Error(
              "Corrupted tar archive: unexpected non-zero block after end-of-archive marker",
            );
          }

          // Verify tar header checksum (bytes 148..156 treated as ASCII spaces)
          const rawChksum = headerBuf
            .toString("ascii", 148, 156)
            .replace(/[\0 ]+$/, "")
            .trim();
          const expectedChksum = parseInt(rawChksum, 8);
          let computedChksum = 0;
          for (let i = 0; i < 512; i++) {
            computedChksum += i >= 148 && i < 156 ? 32 : headerBuf[i]!;
          }
          if (Number.isNaN(expectedChksum) || expectedChksum !== computedChksum) {
            throw new Error("Corrupted tar header: checksum mismatch");
          }

          const typeflag = headerBuf.toString("ascii", 156, 157);
          let size: number;
          if ((headerBuf[124]! & 0x80) !== 0) {
            // GNU base-256 encoded size
            if ((headerBuf[124]! & 0x40) !== 0) {
              throw new Error("Corrupted tar header: invalid entry size (negative base-256)");
            }
            let bigSize = BigInt(headerBuf[124]! & 0x7f);
            for (let i = 125; i < 136; i++) {
              bigSize = (bigSize << 8n) | BigInt(headerBuf[i]!);
            }
            if (bigSize > BigInt(Number.MAX_SAFE_INTEGER)) {
              throw new Error(
                "Corrupted tar header: invalid entry size (exceeds MAX_SAFE_INTEGER)",
              );
            }
            size = Number(bigSize);
          } else {
            const rawSize = headerBuf
              .toString("ascii", 124, 136)
              .replace(/[\0 ]+$/g, "")
              .trim();
            if (rawSize.length === 0) {
              size = 0;
            } else {
              if (!/^[0-7]+$/.test(rawSize)) {
                throw new Error(`Corrupted tar header: invalid octal size "${rawSize}"`);
              }
              size = parseInt(rawSize, 8);
            }
          }
          if (!Number.isSafeInteger(size) || size < 0) {
            throw new Error(`Corrupted tar header: invalid entry size (${size})`);
          }

          if (typeflag === "x") {
            if (size > MAX_PAX_PAYLOAD_BYTES) {
              throw new Error(`PAX extended header exceeds safety limit (${size} bytes)`);
            }
            currentHeader = { name: "", size, kind: "pax" };
            remainingBytes = size;
            paxChunks = [];
            continue;
          }

          if (typeflag === "g") {
            if (size > MAX_PAX_PAYLOAD_BYTES) {
              throw new Error(`PAX global header exceeds safety limit (${size} bytes)`);
            }
            if (size > 0) {
              currentHeader = { name: "", size, kind: "pax-global" };
              remainingBytes = size;
            }
            continue;
          }

          const baseName = headerBuf.toString("utf8", 0, 100).replace(/\0.*$/, "");
          const ustarPrefix = headerBuf.toString("utf8", 345, 500).replace(/\0.*$/, "");
          const paxMeta = pendingPax;
          pendingPax = null;
          const rawName = paxMeta?.path ?? (ustarPrefix ? `${ustarPrefix}/${baseName}` : baseName);
          const effectiveSize = paxMeta?.size ?? size;
          const rawMode = headerBuf
            .toString("ascii", 100, 108)
            .replace(/[\0 ]+$/g, "")
            .trim();
          const parsedMode = /^[0-7]+$/.test(rawMode) ? parseInt(rawMode, 8) & 0o777 : undefined;
          const rawMtime = headerBuf
            .toString("ascii", 136, 148)
            .replace(/[\0 ]+$/g, "")
            .trim();
          const parsedMtimeSec = /^[0-7]+$/.test(rawMtime) ? parseInt(rawMtime, 8) : undefined;

          if (typeflag !== "5" && typeflag !== "0" && typeflag !== "\0") {
            throw new Error(
              `Unsupported tar entry typeflag ${JSON.stringify(typeflag)} for entry: ${rawName || "<unnamed>"}`,
            );
          }

          if (!rawName) {
            throw new Error("Corrupted tar header: missing entry name");
          }

          // Reject traversal / absolute paths BEFORE normalization strips prefixes
          if (rawName.includes("\0")) {
            throw new Error(`Tar entry escapes target directory: ${rawName}`);
          }
          const slashNormalized = rawName.replace(/\\/g, "/");
          if (slashNormalized.startsWith("/") || /^[A-Za-z]:/.test(slashNormalized)) {
            throw new Error(`Tar entry escapes target directory: ${rawName}`);
          }
          const rawSegments = slashNormalized.split("/");
          if (rawSegments.includes("..")) {
            throw new Error(`Tar entry escapes target directory: ${rawName}`);
          }
          const safeName = path.posix.normalize(slashNormalized);
          if (!safeName || safeName === ".." || safeName.startsWith("../")) {
            throw new Error(`Tar entry escapes target directory: ${rawName}`);
          }
          if (safeName === ".") {
            if (typeflag === "5") {
              const remainder = effectiveSize % 512;
              remainingPad = effectiveSize + (remainder > 0 ? 512 - remainder : 0);
              continue;
            }
            throw new Error(`Tar entry escapes target directory: ${rawName}`);
          }

          const containmentRoot = resolvedRoot ?? path.resolve("/tar-verify-root");
          const fullPath = path.resolve(containmentRoot, safeName);
          if (
            fullPath !== containmentRoot &&
            !fullPath.startsWith(`${containmentRoot}${path.sep}`)
          ) {
            throw new Error(`Tar entry escapes target directory: ${rawName}`);
          }

          if (typeflag === "5") {
            if (!dryRun) {
              await fs.mkdir(fullPath, { recursive: true });
            }
            const remainder = effectiveSize % 512;
            remainingPad = effectiveSize + (remainder > 0 ? 512 - remainder : 0);
            continue;
          }

          currentHeader = {
            name: safeName,
            size: effectiveSize,
            kind: "file",
            mode: parsedMode,
            mtimeSec: parsedMtimeSec,
            fullPath,
          };
          remainingBytes = effectiveSize;

          if (!dryRun) {
            await fs.mkdir(path.dirname(fullPath), { recursive: true });
            currentFileHandle = await fs.open(fullPath, "w", parsedMode ?? 0o600);
          }
        }

        if (currentHeader) {
          if (remainingBytes > 0) {
            const toWrite = Math.min(buffer.length, remainingBytes);
            const slice = buffer.subarray(0, toWrite);
            if (currentHeader.kind === "pax") {
              paxChunks.push(Buffer.from(slice));
            } else if (currentHeader.kind === "file") {
              if (currentFileHandle) {
                await writeAll(currentFileHandle, slice);
              }
              totalBytes += toWrite;
            }
            buffer = buffer.subarray(toWrite);
            remainingBytes -= toWrite;
          }

          if (remainingBytes === 0) {
            if (currentHeader.kind === "pax") {
              pendingPax = parsePaxHeaders(Buffer.concat(paxChunks));
              paxChunks = [];
            } else if (currentHeader.kind === "file") {
              if (currentFileHandle) {
                await currentFileHandle.sync();
                await currentFileHandle.close();
                currentFileHandle = null;
                if (currentHeader.fullPath) {
                  if (currentHeader.mode !== undefined) {
                    await fs.chmod(currentHeader.fullPath, currentHeader.mode);
                  }
                  if (currentHeader.mtimeSec !== undefined) {
                    const mtimeDate = new Date(currentHeader.mtimeSec * 1000);
                    await fs.utimes(currentHeader.fullPath, mtimeDate, mtimeDate);
                  }
                }
              }
              extractedCount++;
            }

            const remainder = currentHeader.size % 512;
            remainingPad = remainder > 0 ? 512 - remainder : 0;
            currentHeader = null;
          } else {
            break; // Await more chunks for the current entry
          }
        }
      }
    }

    if (signal?.aborted) {
      throw makeAbortError(signal.reason);
    }
    if (remainingBytes > 0 || remainingPad > 0 || currentHeader !== null) {
      throw new Error("Truncated tar archive: unexpected end of stream mid-entry");
    }
    if (pendingPax !== null) {
      throw new Error("Truncated tar archive: dangling PAX extended header");
    }
    if (buffer.length > 0) {
      throw new Error("Truncated tar archive: trailing incomplete block at end of stream");
    }
    if (zeroBlocks < 2) {
      throw new Error("Truncated tar archive: missing end-of-archive zero blocks");
    }
  } finally {
    if (currentFileHandle) {
      await currentFileHandle.close().catch(() => {});
    }
  }

  return { extractedCount, totalBytes };
}

/**
 * Extracts a streaming POSIX tar archive safely to targetDir with path traversal guards,
 * PAX extended path/size support, strict typeflag validation, full short-write handling,
 * and 2-block EOF truncation detection.
 */
export async function extractTarStream(
  stream: Readable,
  targetDir: string,
  signal?: AbortSignal,
): Promise<TarExtractResult> {
  return processTarStream(stream, targetDir, signal);
}

/**
 * Validates a streaming POSIX tar archive in-memory (headers, checksums, PAX, typeflags,
 * path containment, entry bounds, and 2-block EOF) without writing any files to disk.
 */
export async function verifyTarStream(
  stream: Readable,
  signal?: AbortSignal,
): Promise<TarExtractResult> {
  return processTarStream(stream, null, signal);
}
