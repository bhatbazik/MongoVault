import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";
import { Transform } from "node:stream";

export const ALGORITHM = "aes-256-gcm";
export const CHUNKED_ALGORITHM = "aes-256-gcm-chunked";
export const CHUNK_SIZE = 64 * 1024;
export const KEY_LENGTH = 32;
export const IV_LENGTH = 12;
export const TAG_LENGTH = 16;
export const MAX_FRAME_SIZE = 10 * 1024 * 1024; // 10 MB max frame size safety limit
export const MAX_FRAME_INDEX = 0xffffffff;

export function parseKey(value) {
  const key = /^[0-9a-f]{64}$/i.test(value)
    ? Buffer.from(value, "hex")
    : Buffer.from(value, "base64");
  if (key.length !== KEY_LENGTH) {
    throw new Error(`Encryption key must be ${KEY_LENGTH} bytes (base64 or hex encoded)`);
  }
  return key;
}

export function parseKeyRing(value) {
  if (!value) return [];
  const entries = Array.isArray(value) ? value : String(value).split(",");
  return entries
    .map((k) => k.trim())
    .filter(Boolean)
    .map((k) => parseKey(k));
}

export function generateKey() {
  return randomBytes(KEY_LENGTH).toString("base64");
}

export function keyId(masterKey) {
  return createHash("sha256").update(masterKey).digest("hex").slice(0, 16);
}

export function deriveNonce(baseNonce, frameIndex) {
  if (!Number.isInteger(frameIndex) || frameIndex < 0 || frameIndex > MAX_FRAME_INDEX) {
    throw new Error(`AEAD frame index out of bounds: ${frameIndex}`);
  }
  const nonce = Buffer.from(baseNonce);
  // Store 32-bit frame sequence in the last 4 bytes of 12-byte IV
  nonce.writeUInt32BE(frameIndex >>> 0, 8);
  return nonce;
}

export function computeAad(frameIndex, isFinal) {
  const aad = Buffer.alloc(5);
  aad.writeUInt32BE(frameIndex >>> 0, 0);
  aad.writeUInt8(isFinal ? 1 : 0, 4);
  return aad;
}

export class ChunkedAeadEncryptStream extends Transform {
  #dataKey;
  #baseNonce;
  #chunkSize;
  #buffer = [];
  #bufferedBytes = 0;
  #frameIndex = 0;

  constructor(dataKey, baseNonce, chunkSize = CHUNK_SIZE) {
    super();
    this.#dataKey = dataKey;
    this.#baseNonce = baseNonce;
    this.#chunkSize = chunkSize;
  }

  _transform(chunk, _encoding, callback) {
    this.#buffer.push(chunk);
    this.#bufferedBytes += chunk.length;

    while (this.#bufferedBytes >= this.#chunkSize) {
      const combined = this.#buffer.length === 1 ? this.#buffer[0] : Buffer.concat(this.#buffer);
      const slice = combined.subarray(0, this.#chunkSize);
      const remainder = combined.subarray(this.#chunkSize);

      this.#buffer = remainder.length ? [remainder] : [];
      this.#bufferedBytes = remainder.length;

      this.#encryptAndEmit(slice, false);
    }
    callback();
  }

  _flush(callback) {
    const remaining = this.#buffer.length === 1 ? this.#buffer[0] : (this.#buffer.length ? Buffer.concat(this.#buffer) : Buffer.alloc(0));
    this.#encryptAndEmit(remaining, true);
    callback();
  }

  #encryptAndEmit(plaintext, isFinal) {
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
  #dataKey;
  #baseNonce;
  #frameIndex = 0;
  #buffer = [];
  #bufferedBytes = 0;
  #seenFinal = false;

  constructor(dataKey, baseNonce) {
    super();
    this.#dataKey = dataKey;
    this.#baseNonce = baseNonce;
  }

  _transform(chunk, _encoding, callback) {
    this.#buffer.push(chunk);
    this.#bufferedBytes += chunk.length;

    try {
      while (this.#bufferedBytes >= 5 + IV_LENGTH + TAG_LENGTH) {
        const combined = this.#buffer.length === 1 ? this.#buffer[0] : Buffer.concat(this.#buffer);
        const cipherLength = combined.readUInt32BE(0);
        if (cipherLength > MAX_FRAME_SIZE) {
          throw new Error(`AEAD frame size exceeds limit (${cipherLength} > ${MAX_FRAME_SIZE})`);
        }
        const totalFrameSize = 5 + IV_LENGTH + cipherLength + TAG_LENGTH;

        if (combined.length < totalFrameSize) {
          this.#buffer = [combined];
          this.#bufferedBytes = combined.length;
          break;
        }

        const isFinal = combined.readUInt8(4) === 1;
        const nonce = combined.subarray(5, 5 + IV_LENGTH);
        const ciphertext = combined.subarray(5 + IV_LENGTH, 5 + IV_LENGTH + cipherLength);
        const authTag = combined.subarray(5 + IV_LENGTH + cipherLength, totalFrameSize);

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

        const remainder = combined.subarray(totalFrameSize);
        this.#buffer = remainder.length ? [remainder] : [];
        this.#bufferedBytes = remainder.length;

        this.push(plaintext);
      }
      callback();
    } catch (err) {
      callback(err);
    }
  }

  _flush(callback) {
    if (this.#bufferedBytes > 0) {
      return callback(
        new Error(`Incomplete AEAD frame at end of stream (${this.#bufferedBytes} trailing bytes)`),
      );
    }
    if (!this.#seenFinal) {
      return callback(
        new Error("Truncated AEAD stream: end of stream reached before final frame"),
      );
    }
    callback();
  }
}

export function createEncryptor(masterKey) {
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

export function createDecryptor(masterKeys, envelope) {
  if (envelope.algorithm !== ALGORITHM && envelope.algorithm !== CHUNKED_ALGORITHM) {
    throw new Error(`Unsupported encryption algorithm: ${envelope.algorithm}`);
  }
  const keys = Array.isArray(masterKeys) ? masterKeys : [masterKeys];
  const matchedKey = keys.find((k) => keyId(k) === envelope.keyId);
  if (!matchedKey) {
    const configuredIds = keys.map((k) => keyId(k)).join(", ");
    throw new Error(
      `Backup was encrypted with key ${envelope.keyId}, but configured key ring has: [${configuredIds}]`,
    );
  }
  const dataKey = unwrap(matchedKey, envelope.wrappedKey);
  const ivBuffer = Buffer.from(envelope.iv, "base64");

  if (envelope.algorithm === CHUNKED_ALGORITHM) {
    return new ChunkedAeadDecryptStream(dataKey, ivBuffer);
  }

  // Backwards compatibility: legacy single-block AES-256-GCM
  const decipher = createDecipheriv(ALGORITHM, dataKey, ivBuffer);
  if (envelope.authTag) {
    decipher.setAuthTag(Buffer.from(envelope.authTag, "base64"));
  }
  return decipher;
}

function wrap(masterKey, dataKey) {
  const iv = randomBytes(IV_LENGTH);
  const cipher = createCipheriv(ALGORITHM, masterKey, iv);
  const encrypted = Buffer.concat([cipher.update(dataKey), cipher.final()]);
  return Buffer.concat([iv, cipher.getAuthTag(), encrypted]).toString("base64");
}

function unwrap(masterKey, wrapped) {
  const buffer = Buffer.from(wrapped, "base64");
  const iv = buffer.subarray(0, IV_LENGTH);
  const tag = buffer.subarray(IV_LENGTH, IV_LENGTH + TAG_LENGTH);
  const encrypted = buffer.subarray(IV_LENGTH + TAG_LENGTH);
  const decipher = createDecipheriv(ALGORITHM, masterKey, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(encrypted), decipher.final()]);
}

function canonicalManifestPayload(manifest) {
  const cleanEncryption = manifest.encryption ? { ...manifest.encryption } : null;
  if (cleanEncryption) {
    delete cleanEncryption.manifestHmac;
  }
  return JSON.stringify({
    version: manifest.version,
    ...(manifest.type ? { type: manifest.type } : {}),
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
    encryption: cleanEncryption,
  });
}

export function signManifest(masterKey, manifest) {
  return createHmac("sha256", masterKey)
    .update(canonicalManifestPayload(manifest))
    .digest("hex");
}

export function verifyManifest(masterKeys, manifest) {
  if (!manifest?.encryption?.manifestHmac) return true;
  const keys = Array.isArray(masterKeys) ? masterKeys : [masterKeys];
  const matchedKey = keys.find((k) => keyId(k) === manifest.encryption.keyId);
  if (!matchedKey) {
    const configuredIds = keys.map((k) => keyId(k)).join(", ");
    throw new Error(
      `Backup was encrypted with key ${manifest.encryption.keyId}, but configured key ring has: [${configuredIds}]`,
    );
  }
  const expected = Buffer.from(signManifest(matchedKey, manifest), "hex");
  const actual = Buffer.from(String(manifest.encryption.manifestHmac), "hex");
  if (expected.length !== actual.length || !timingSafeEqual(expected, actual)) {
    throw new Error(`Manifest HMAC verification failed for ${manifest.key}: manifest has been tampered with`);
  }
  return true;
}
