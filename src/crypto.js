import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
} from "node:crypto";
import { Transform } from "node:stream";

export const ALGORITHM = "aes-256-gcm";
export const CHUNKED_ALGORITHM = "aes-256-gcm-chunked";
export const CHUNK_SIZE = 64 * 1024;
export const KEY_LENGTH = 32;
export const IV_LENGTH = 12;
export const TAG_LENGTH = 16;

export function parseKey(value) {
  const key = /^[0-9a-f]{64}$/i.test(value)
    ? Buffer.from(value, "hex")
    : Buffer.from(value, "base64");
  if (key.length !== KEY_LENGTH) {
    throw new Error(`Encryption key must be ${KEY_LENGTH} bytes (base64 or hex encoded)`);
  }
  return key;
}

export function generateKey() {
  return randomBytes(KEY_LENGTH).toString("base64");
}

export function keyId(masterKey) {
  return createHash("sha256").update(masterKey).digest("hex").slice(0, 16);
}

export function deriveNonce(baseNonce, frameIndex) {
  const nonce = Buffer.from(baseNonce);
  // Store 32-bit frame sequence in the last 4 bytes of 12-byte IV
  nonce.writeUInt32BE(frameIndex >>> 0, 8);
  return nonce;
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
    const nonce = deriveNonce(this.#baseNonce, this.#frameIndex++);
    const cipher = createCipheriv(ALGORITHM, this.#dataKey, nonce);
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

        const expectedNonce = deriveNonce(this.#baseNonce, this.#frameIndex);
        if (!nonce.equals(expectedNonce)) {
          throw new Error(`AEAD frame sequence corrupted or reordered at frame ${this.#frameIndex}`);
        }

        const decipher = createDecipheriv(ALGORITHM, this.#dataKey, nonce);
        decipher.setAuthTag(authTag);
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
    if (!this.#seenFinal && this.#frameIndex > 0) {
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

export function createDecryptor(masterKey, envelope) {
  if (envelope.algorithm !== ALGORITHM && envelope.algorithm !== CHUNKED_ALGORITHM) {
    throw new Error(`Unsupported encryption algorithm: ${envelope.algorithm}`);
  }
  if (envelope.keyId !== keyId(masterKey)) {
    throw new Error(
      `Backup was encrypted with key ${envelope.keyId}, but the configured key is ${keyId(masterKey)}`,
    );
  }
  const dataKey = unwrap(masterKey, envelope.wrappedKey);
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
