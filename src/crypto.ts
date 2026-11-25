import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  hkdfSync,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";
import { Transform, type TransformCallback } from "node:stream";
import type { BackupManifest } from "./types/catalog.js";
import type { EncryptionEnvelope, Encryptor, VerifyManifestOptions } from "./types/crypto.js";

export const ALGORITHM = "aes-256-gcm";
export const CHUNKED_ALGORITHM = "aes-256-gcm-chunked";
export const CHUNK_SIZE = 64 * 1024;
export const KEY_LENGTH = 32;
export const IV_LENGTH = 12;
export const TAG_LENGTH = 16;
export const MAX_FRAME_SIZE = 10 * 1024 * 1024; // 10 MB max frame size safety limit
export const MAX_FRAME_INDEX = 0xffffffff;

function decodeStrictBase64(value: string, label: string): Buffer {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length % 4 !== 0 ||
    !/^[A-Za-z0-9+/]+={0,2}$/.test(value)
  ) {
    throw new Error(`Invalid base64 encoding for ${label}`);
  }
  return Buffer.from(value, "base64");
}

export function parseKey(value: string): Buffer {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Encryption key must be ${KEY_LENGTH} bytes (base64 or hex encoded)`);
  }
  let key: Buffer;
  if (/^[0-9a-f]{64}$/i.test(value)) {
    key = Buffer.from(value, "hex");
  } else if (value.length % 4 === 0 && /^[A-Za-z0-9+/]+={0,2}$/.test(value)) {
    key = Buffer.from(value, "base64");
  } else {
    throw new Error(`Encryption key must be ${KEY_LENGTH} bytes (base64 or hex encoded)`);
  }
  if (key.length !== KEY_LENGTH) {
    throw new Error(`Encryption key must be ${KEY_LENGTH} bytes (base64 or hex encoded)`);
  }
  return key;
}

export function parseKeyRing(value?: string | string[] | null): Buffer[] {
  if (!value) return [];
  const entries = Array.isArray(value) ? value : String(value).split(",");
  return entries
    .map((k) => k.trim())
    .filter(Boolean)
    .map((k) => parseKey(k));
}

export function generateKey(): string {
  return randomBytes(KEY_LENGTH).toString("base64");
}

export function keyId(masterKey: Buffer): string {
  return createHash("sha256").update(masterKey).digest("hex").slice(0, 16);
}

export function deriveNonce(baseNonce: Buffer, frameIndex: number): Buffer {
  if (!Number.isInteger(frameIndex) || frameIndex < 0 || frameIndex > MAX_FRAME_INDEX) {
    throw new Error(`AEAD frame index out of bounds: ${frameIndex}`);
  }
  const nonce = Buffer.from(baseNonce);
  // Store 32-bit frame sequence in the last 4 bytes of 12-byte IV
  nonce.writeUInt32BE(frameIndex >>> 0, 8);
  return nonce;
}

export function computeAad(frameIndex: number, isFinal: boolean): Buffer {
  const aad = Buffer.alloc(5);
  aad.writeUInt32BE(frameIndex >>> 0, 0);
  aad.writeUInt8(isFinal ? 1 : 0, 4);
  return aad;
}

export class ChunkedAeadEncryptStream extends Transform {
  #dataKey: Buffer;
  #baseNonce: Buffer;
  #chunkSize: number;
  #buffer: Buffer[] = [];
  #bufferedBytes: number = 0;
  #frameIndex: number = 0;

  constructor(dataKey: Buffer, baseNonce: Buffer, chunkSize: number = CHUNK_SIZE) {
    super();
    this.#dataKey = dataKey;
    this.#baseNonce = baseNonce;
    this.#chunkSize = chunkSize;
  }

  override _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    try {
      if (chunk.length === 0) {
        callback();
        return;
      }
      this.#buffer.push(chunk);
      this.#bufferedBytes += chunk.length;

      if (this.#bufferedBytes >= this.#chunkSize) {
        const combined =
          this.#buffer.length === 1 && this.#buffer[0]
            ? this.#buffer[0]
            : Buffer.concat(this.#buffer);
        let offset = 0;
        while (combined.length - offset >= this.#chunkSize) {
          const slice = combined.subarray(offset, offset + this.#chunkSize);
          offset += this.#chunkSize;
          this.#encryptAndEmit(slice, false);
        }
        const remainder = combined.subarray(offset);
        this.#buffer = remainder.length ? [remainder] : [];
        this.#bufferedBytes = remainder.length;
      }
      callback();
    } catch (err) {
      callback(err as Error);
    }
  }

  override _flush(callback: TransformCallback): void {
    try {
      const remaining =
        this.#buffer.length === 1 && this.#buffer[0]
          ? this.#buffer[0]
          : Buffer.concat(this.#buffer);
      this.#encryptAndEmit(remaining, true);
      callback();
    } catch (err) {
      callback(err as Error);
    }
  }

  #encryptAndEmit(plaintext: Buffer, isFinal: boolean): void {
    const frameIdx = this.#frameIndex++;
    const nonce = deriveNonce(this.#baseNonce, frameIdx);
    const cipher = createCipheriv(ALGORITHM, this.#dataKey, nonce);
    const aad = computeAad(frameIdx, isFinal);
    cipher.setAAD(aad);

    const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    const authTag = cipher.getAuthTag();

    const header = Buffer.alloc(5 + IV_LENGTH);
    header.writeUInt32BE(ciphertext.length, 0);
    header.writeUInt8(isFinal ? 1 : 0, 4);
    nonce.copy(header, 5);

    this.push(Buffer.concat([header, ciphertext, authTag]));
  }
}

export class ChunkedAeadDecryptStream extends Transform {
  #dataKey: Buffer;
  #baseNonce: Buffer;
  #frameIndex: number = 0;
  #buffer: Buffer[] = [];
  #bufferedBytes: number = 0;
  #expectedFrameSize: number | null = null;
  #seenFinal: boolean = false;

  constructor(dataKey: Buffer, baseNonce: Buffer) {
    super();
    this.#dataKey = dataKey;
    this.#baseNonce = baseNonce;
  }

  override _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    try {
      if (this.#seenFinal && chunk.length > 0) {
        throw new Error("Unexpected trailing bytes after final AEAD frame");
      }
      if (chunk.length === 0) {
        callback();
        return;
      }
      this.#buffer.push(chunk);
      this.#bufferedBytes += chunk.length;

      while (true) {
        if (this.#expectedFrameSize === null) {
          if (this.#bufferedBytes < 5 + IV_LENGTH + TAG_LENGTH) {
            break;
          }
          let cipherLength: number;
          if (this.#buffer[0] && this.#buffer[0].length >= 4) {
            cipherLength = this.#buffer[0].readUInt32BE(0);
          } else {
            const head = Buffer.concat(this.#buffer);
            this.#buffer = [head];
            cipherLength = head.readUInt32BE(0);
          }
          if (cipherLength > MAX_FRAME_SIZE) {
            throw new Error(`AEAD frame size exceeds limit (${cipherLength} > ${MAX_FRAME_SIZE})`);
          }
          this.#expectedFrameSize = 5 + IV_LENGTH + cipherLength + TAG_LENGTH;
        }

        if (this.#bufferedBytes < this.#expectedFrameSize) {
          break;
        }

        if (this.#seenFinal) {
          throw new Error("Unexpected trailing frame after final AEAD frame");
        }

        const combined =
          this.#buffer.length === 1 && this.#buffer[0]
            ? this.#buffer[0]
            : Buffer.concat(this.#buffer);
        let offset = 0;

        while (combined.length - offset >= 5 + IV_LENGTH + TAG_LENGTH) {
          const cipherLength = combined.readUInt32BE(offset);
          if (cipherLength > MAX_FRAME_SIZE) {
            throw new Error(`AEAD frame size exceeds limit (${cipherLength} > ${MAX_FRAME_SIZE})`);
          }
          const totalFrameSize = 5 + IV_LENGTH + cipherLength + TAG_LENGTH;
          if (combined.length - offset < totalFrameSize) {
            this.#expectedFrameSize = totalFrameSize;
            break;
          }
          if (this.#seenFinal) {
            throw new Error("Unexpected trailing frame after final AEAD frame");
          }

          const flag = combined.readUInt8(offset + 4);
          if (flag !== 0 && flag !== 1) {
            throw new Error(`Invalid AEAD frame flag: ${flag}`);
          }
          const isFinal = flag === 1;
          const nonce = combined.subarray(offset + 5, offset + 5 + IV_LENGTH);
          const ciphertext = combined.subarray(
            offset + 5 + IV_LENGTH,
            offset + 5 + IV_LENGTH + cipherLength,
          );
          const authTag = combined.subarray(
            offset + 5 + IV_LENGTH + cipherLength,
            offset + totalFrameSize,
          );

          const frameIdx = this.#frameIndex;
          const expectedNonce = deriveNonce(this.#baseNonce, frameIdx);
          if (!nonce.equals(expectedNonce)) {
            throw new Error(`AEAD frame sequence corrupted or reordered at frame ${frameIdx}`);
          }

          const decipher = createDecipheriv(ALGORITHM, this.#dataKey, nonce);
          decipher.setAuthTag(authTag);
          const aad = computeAad(frameIdx, isFinal);
          decipher.setAAD(aad);

          const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);

          this.#frameIndex++;
          if (isFinal) {
            this.#seenFinal = true;
          }
          offset += totalFrameSize;
          this.#expectedFrameSize = null;

          this.push(plaintext);
        }

        const remainder = combined.subarray(offset);
        if (this.#seenFinal && remainder.length > 0) {
          throw new Error(
            `Unexpected trailing data after final AEAD frame (${remainder.length} bytes)`,
          );
        }
        this.#buffer = remainder.length ? [remainder] : [];
        this.#bufferedBytes = remainder.length;
        if (this.#expectedFrameSize !== null && this.#bufferedBytes < this.#expectedFrameSize) {
          break;
        }
      }
      callback();
    } catch (err) {
      callback(err as Error);
    }
  }

  override _flush(callback: TransformCallback): void {
    if (this.#bufferedBytes > 0) {
      return callback(
        new Error(`Incomplete AEAD frame at end of stream (${this.#bufferedBytes} trailing bytes)`),
      );
    }
    if (!this.#seenFinal) {
      return callback(new Error("Truncated AEAD stream: end of stream reached before final frame"));
    }
    callback();
  }
}

export function createEncryptor(masterKey: Buffer): Encryptor {
  const dataKey = randomBytes(KEY_LENGTH);
  const baseNonce = randomBytes(IV_LENGTH);
  const stream = new ChunkedAeadEncryptStream(dataKey, baseNonce);

  return {
    stream,
    envelope: () => ({
      algorithm: CHUNKED_ALGORITHM,
      keyId: keyId(masterKey),
      wrappedKey: wrap(masterKey, dataKey),
      iv: baseNonce.toString("base64"),
      chunkSize: CHUNK_SIZE,
    }),
  };
}

export function createDecryptor(
  masterKeys: Buffer | Buffer[],
  envelope: EncryptionEnvelope,
): Transform {
  if (!envelope || typeof envelope !== "object") {
    throw new Error("Missing or invalid encryption envelope");
  }
  if (envelope.algorithm !== CHUNKED_ALGORITHM) {
    throw new Error(`Unsupported encryption algorithm: ${envelope.algorithm}`);
  }
  const keys = Array.isArray(masterKeys) ? masterKeys : [masterKeys];
  if (keys.length === 0) {
    throw new Error("No encryption keys configured in key ring");
  }
  const matchedKey = keys.find((k) => keyId(k) === envelope.keyId);
  if (!matchedKey) {
    const configuredIds = keys.map((k) => keyId(k)).join(", ");
    throw new Error(
      `Backup was encrypted with key ${envelope.keyId}, but configured key ring has: [${configuredIds}]`,
    );
  }
  const ivBuffer = decodeStrictBase64(envelope.iv, "encryption IV");
  if (ivBuffer.length !== IV_LENGTH) {
    throw new Error(`Invalid encryption IV length: expected ${IV_LENGTH}, got ${ivBuffer.length}`);
  }
  const dataKey = unwrap(matchedKey, envelope.wrappedKey);

  return new ChunkedAeadDecryptStream(dataKey, ivBuffer);
}

export const HKDF_WRAP_INFO = "mongovault-v2-dek-wrap";
export const HKDF_HMAC_INFO = "mongovault-v2-manifest-hmac";

export function deriveWrapKey(masterKey: Buffer): Buffer {
  return Buffer.from(hkdfSync("sha256", masterKey, Buffer.alloc(0), HKDF_WRAP_INFO, KEY_LENGTH));
}

export function deriveHmacKey(masterKey: Buffer): Buffer {
  return Buffer.from(hkdfSync("sha256", masterKey, Buffer.alloc(0), HKDF_HMAC_INFO, KEY_LENGTH));
}

function wrap(masterKey: Buffer, dataKey: Buffer): string {
  const iv = randomBytes(IV_LENGTH);
  const wrapKey = deriveWrapKey(masterKey);
  const cipher = createCipheriv(ALGORITHM, wrapKey, iv);
  const encrypted = Buffer.concat([cipher.update(dataKey), cipher.final()]);
  return Buffer.concat([iv, cipher.getAuthTag(), encrypted]).toString("base64");
}

function unwrap(masterKey: Buffer, wrapped: string): Buffer {
  const buffer = decodeStrictBase64(wrapped, "wrapped data key");
  const minWrappedBytes = IV_LENGTH + TAG_LENGTH + KEY_LENGTH;
  if (buffer.length < minWrappedBytes) {
    throw new Error(
      `Invalid wrapped data key length: expected at least ${minWrappedBytes} bytes, got ${buffer.length}`,
    );
  }
  const iv = buffer.subarray(0, IV_LENGTH);
  const tag = buffer.subarray(IV_LENGTH, IV_LENGTH + TAG_LENGTH);
  const encrypted = buffer.subarray(IV_LENGTH + TAG_LENGTH);

  const derivedKey = deriveWrapKey(masterKey);
  const decipher = createDecipheriv(ALGORITHM, derivedKey, iv);
  decipher.setAuthTag(tag);
  const unwrapped = Buffer.concat([decipher.update(encrypted), decipher.final()]);
  if (unwrapped.length !== KEY_LENGTH) {
    throw new Error(
      `Invalid unwrapped data key length: expected ${KEY_LENGTH}, got ${unwrapped.length}`,
    );
  }
  return unwrapped;
}

function canonicalManifestPayload(
  manifest: BackupManifest | (Record<string, unknown> & Partial<BackupManifest>),
): string {
  const enc = manifest.encryption as
    (Record<string, unknown> & { manifestHmac?: string }) | null | undefined;
  const cleanEncryption = enc ? { ...enc } : null;
  if (cleanEncryption) {
    delete cleanEncryption.manifestHmac;
  }
  return JSON.stringify({
    version: manifest.version,
    ...("type" in manifest && manifest.type ? { type: manifest.type } : {}),
    ...("engine" in manifest && manifest.engine ? { engine: manifest.engine } : {}),
    key: manifest.key,
    database: manifest.database,
    createdAt: manifest.createdAt,
    completedAt: manifest.completedAt,
    size: manifest.size,
    sha256: manifest.sha256,
    compression: manifest.compression,
    oplog: Boolean(manifest.oplog),
    ...(manifest.oplogWindow ? { oplogWindow: manifest.oplogWindow } : {}),
    excludedCollections: manifest.excludedCollections ?? [],
    collections: manifest.collections ?? {},
    ...(manifest.schema ? { schema: manifest.schema } : {}),
    ...(manifest.physical ? { physical: manifest.physical } : {}),
    ...(manifest.sharding ? { sharding: manifest.sharding } : {}),
    ...(manifest.objectLock ? { objectLock: manifest.objectLock } : {}),
    encryption: cleanEncryption,
  });
}

export function signManifest(
  masterKey: Buffer,
  manifest: BackupManifest | Record<string, unknown>,
): string {
  const hmacKey = deriveHmacKey(masterKey);
  return createHmac("sha256", hmacKey).update(canonicalManifestPayload(manifest)).digest("hex");
}

export function verifyManifest(
  masterKeys: Buffer | Buffer[],
  manifest: BackupManifest | Record<string, unknown>,
  options: VerifyManifestOptions = {},
): boolean {
  const partial = manifest as Partial<BackupManifest>;
  const enc = partial.encryption;
  const sig = partial.signature;
  const manifestKeyStr = partial.key ?? "unknown";

  if (sig !== undefined && sig !== null) {
    if (sig.algorithm !== "HMAC-SHA256") {
      throw new Error(
        `Unsupported manifest signature algorithm for ${manifestKeyStr}: ${String(sig.algorithm)}`,
      );
    }
    if (!sig.hmac) {
      throw new Error(`Invalid manifest signature for ${manifestKeyStr}: missing hmac`);
    }
  }

  if (sig?.hmac && enc?.manifestHmac && sig.hmac !== enc.manifestHmac) {
    throw new Error(`Conflicting HMAC signatures in manifest for ${manifestKeyStr}`);
  }
  if (sig?.keyId && enc?.keyId && sig.keyId !== enc.keyId) {
    throw new Error(`Conflicting keyIds in manifest signatures for ${manifestKeyStr}`);
  }

  const keys = Array.isArray(masterKeys) ? masterKeys : [masterKeys];
  const hmacHex = sig?.hmac || enc?.manifestHmac;
  const targetKeyId = sig?.keyId || enc?.keyId;

  if (!hmacHex) {
    if (enc) {
      throw new Error(
        `Encrypted backup manifest for ${manifestKeyStr} is missing required HMAC signature`,
      );
    }
    if (options.requireSigned || (keys.length > 0 && !options.allowUnsigned)) {
      throw new Error(`Backup manifest for ${manifestKeyStr} is unsigned (missing HMAC signature)`);
    }
    return true;
  }

  if (!/^[0-9a-f]{64}$/i.test(String(hmacHex))) {
    throw new Error(
      `Manifest HMAC verification failed for ${manifestKeyStr}: malformed HMAC digest`,
    );
  }

  const matchedKey = targetKeyId
    ? keys.find((k) => keyId(k) === targetKeyId)
    : keys.find((k) => {
        try {
          const expected = Buffer.from(signManifest(k, manifest), "hex");
          const actual = Buffer.from(String(hmacHex), "hex");
          return expected.length === actual.length && timingSafeEqual(expected, actual);
        } catch {
          return false;
        }
      });

  if (!matchedKey) {
    const configuredIds = keys.map((k) => keyId(k)).join(", ");
    throw new Error(
      `Backup manifest was signed with key ${targetKeyId || "unknown"}, but configured key ring has: [${configuredIds}]`,
    );
  }

  const actual = Buffer.from(String(hmacHex), "hex");
  const expectedHkdf = Buffer.from(signManifest(matchedKey, manifest), "hex");
  if (expectedHkdf.length === actual.length && timingSafeEqual(expectedHkdf, actual)) {
    return true;
  }

  throw new Error(
    `Manifest HMAC verification failed for ${partial.key}: manifest has been tampered with`,
  );
}
