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
 * Creates a standard 512-byte POSIX UStar tar file header, supporting paths up to 255 bytes via UStar prefix.
 */
export function createTarHeader(name: string, size: number, mtime: number = Date.now()): Buffer {
  const buf = Buffer.alloc(512, 0);

  const cleanName = name.replace(/\\/g, "/").replace(/^\/+/, "");
  if (Buffer.byteLength(cleanName, "utf8") <= 100) {
    buf.write(cleanName, 0, 100, "utf8");
  } else {
    // Split into UStar prefix (up to 155 bytes at offset 345) and basename (up to 100 bytes at offset 0)
    const slashIdx = cleanName.lastIndexOf("/");
    if (slashIdx > 0) {
      const prefix = cleanName.slice(0, slashIdx).slice(0, 155);
      const base = cleanName.slice(slashIdx + 1).slice(0, 100);
      buf.write(base, 0, 100, "utf8");
      buf.write(prefix, 345, 155, "utf8");
    } else {
      buf.write(cleanName.slice(0, 100), 0, 100, "utf8");
    }
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
  options: { includeEof?: boolean } = {},
): AsyncGenerator<Buffer, void, unknown> {
  const { includeEof = true } = options;
  for (const file of files) {
    if (signal?.aborted) return;
    let stat;
    try {
      stat = await fs.stat(file.fullPath);
    } catch {
      continue;
    }
    const expectedSize = stat.size;
    const header = createTarHeader(file.name, expectedSize, stat.mtimeMs);
    yield header;

    // Stream file contents in 64KB chunks, strictly bounded to expectedSize
    // so live WiredTiger writes (file growth or truncation) never shift TAR headers.
    let bytesEmitted = 0;
    if (expectedSize > 0) {
      const stream = fsSync.createReadStream(file.fullPath, { highWaterMark: 64 * 1024 });
      for await (const rawChunk of stream) {
        if (signal?.aborted) {
          stream.destroy();
          return;
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

    // Zero-pad if the file shrank or truncated while being read during hot backup
    if (bytesEmitted < expectedSize) {
      let missing = expectedSize - bytesEmitted;
      const zeroChunkSize = 64 * 1024;
      while (missing > 0) {
        const n = Math.min(missing, zeroChunkSize);
        yield Buffer.alloc(n, 0);
        missing -= n;
      }
    }

    // Pad file data to 512-byte block boundary
    const remainder = expectedSize % 512;
    if (remainder > 0) {
      yield Buffer.alloc(512 - remainder, 0);
    }
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

        const baseName = headerBuf.toString("utf8", 0, 100).replace(/\0.*$/, "").trim();
        const ustarPrefix = headerBuf.toString("utf8", 345, 500).replace(/\0.*$/, "").trim();
        const rawName = ustarPrefix ? `${ustarPrefix}/${baseName}` : baseName;
        if (!rawName) continue;

        // Path traversal sanitization
        const safeName = path.posix
          .normalize(rawName.replace(/\\/g, "/"))
          .replace(/^(\.\.\/)+/, "")
          .replace(/^\/+/, "");
        let size: number;
        if ((headerBuf[124]! & 0x80) !== 0) {
          // GNU base-256 encoded size
          let bigSize = BigInt(headerBuf[124]! & 0x7f);
          for (let i = 125; i < 136; i++) {
            bigSize = (bigSize << 8n) | BigInt(headerBuf[i]!);
          }
          size = Number(bigSize);
        } else {
          const rawSize = headerBuf.toString("ascii", 124, 136).replace(/\0.*$/, "").trim();
          size = parseInt(rawSize, 8) || 0;
        }

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
