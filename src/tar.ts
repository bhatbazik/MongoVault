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

/**
 * Creates a standard 512-byte POSIX UStar tar file header.
 */
export function createTarHeader(name: string, size: number, mtime: number = Date.now()): Buffer {
  const buf = Buffer.alloc(512, 0);

  // name: 0..100 (normalized, POSIX separators)
  const normName = name.replace(/\\/g, "/").slice(0, 100);
  buf.write(normName, 0, "utf8");

  // mode: 100..108 (0644)
  buf.write("0000644\0", 100, "ascii");
  // uid: 108..116
  buf.write("0000000\0", 108, "ascii");
  // gid: 116..124
  buf.write("0000000\0", 116, "ascii");

  // size: 124..136 (11 octal digits + null terminator)
  const sizeOctal = size.toString(8).padStart(11, "0") + "\0";
  buf.write(sizeOctal, 124, "ascii");

  // mtime: 136..148 (seconds since epoch in octal + null terminator)
  const mtimeOctal =
    Math.floor(mtime / 1000)
      .toString(8)
      .padStart(11, "0") + "\0";
  buf.write(mtimeOctal, 136, "ascii");

  // typeflag: 156 ('0' = regular file)
  buf.write("0", 156, "ascii");

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

/**
 * Generates an async stream of tar-encoded byte chunks from an array of files.
 */
export async function* generateTarStream(
  files: TarEntry[],
  signal?: AbortSignal,
): AsyncGenerator<Buffer, void, unknown> {
  for (const file of files) {
    if (signal?.aborted) return;
    let stat;
    try {
      stat = await fs.stat(file.fullPath);
    } catch {
      continue;
    }
    const header = createTarHeader(file.name, stat.size, stat.mtimeMs);
    yield header;

    // Stream file contents in 64KB chunks
    const stream = fsSync.createReadStream(file.fullPath, { highWaterMark: 64 * 1024 });
    for await (const chunk of stream) {
      if (signal?.aborted) {
        stream.destroy();
        return;
      }
      yield Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    }

    // Pad file data to 512-byte block boundary
    const remainder = stat.size % 512;
    if (remainder > 0) {
      yield Buffer.alloc(512 - remainder, 0);
    }
  }

  // End of archive marker: two 512-byte zero blocks (1024 bytes)
  yield Buffer.alloc(1024, 0);
}

/**
 * Wraps generateTarStream into a standard Node.js Readable stream.
 */
export function createTarPackStream(files: TarEntry[], signal?: AbortSignal): Readable {
  return Readable.from(generateTarStream(files, signal));
}

/**
 * Extracts a streaming POSIX tar archive safely to targetDir with path traversal guards.
 */
export async function extractTarStream(
  stream: Readable,
  targetDir: string,
  signal?: AbortSignal,
): Promise<TarExtractResult> {
  await fs.mkdir(targetDir, { recursive: true });

  let buffer = Buffer.alloc(0);
  let currentHeader: { name: string; size: number } | null = null;
  let remainingBytes = 0;
  let remainingPad = 0;
  let currentFileHandle: fs.FileHandle | null = null;
  let extractedCount = 0;
  let totalBytes = 0;

  for await (const chunk of stream) {
    if (signal?.aborted) break;
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

        // Check for 512-byte zero block (end-of-archive indicator)
        const isZero = headerBuf.every((b) => b === 0);
        if (isZero) continue;

        const rawName = headerBuf.toString("utf8", 0, 100).replace(/\0.*$/, "").trim();
        if (!rawName) continue;

        // Path traversal sanitization
        const safeName = path.normalize(rawName).replace(/^(\.\.[/\\])+/, "");
        const rawSize = headerBuf.toString("ascii", 124, 136).replace(/\0.*$/, "").trim();
        const size = parseInt(rawSize, 8) || 0;

        currentHeader = { name: safeName, size };
        remainingBytes = size;

        const fullPath = path.join(targetDir, safeName);
        await fs.mkdir(path.dirname(fullPath), { recursive: true });
        currentFileHandle = await fs.open(fullPath, "w", 0o600);
      }

      if (currentHeader && currentFileHandle) {
        if (remainingBytes > 0) {
          const toWrite = Math.min(buffer.length, remainingBytes);
          const slice = buffer.subarray(0, toWrite);
          await currentFileHandle.writeFile(slice);
          buffer = buffer.subarray(toWrite);
          remainingBytes -= toWrite;
          totalBytes += toWrite;
        }

        if (remainingBytes === 0) {
          await currentFileHandle.close();
          currentFileHandle = null;
          extractedCount++;

          const remainder = currentHeader.size % 512;
          remainingPad = remainder > 0 ? 512 - remainder : 0;
          currentHeader = null;
        } else {
          break; // Await more chunks for the current file
        }
      }
    }
  }

  if (currentFileHandle) {
    await currentFileHandle.close().catch(() => {});
  }

  return { extractedCount, totalBytes };
}
