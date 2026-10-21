import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";

const ALGORITHM = "aes-256-gcm";
const KEY_LENGTH = 32;
const IV_LENGTH = 12;
const TAG_LENGTH = 16;

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

export function createEncryptor(masterKey) {
  const dataKey = randomBytes(KEY_LENGTH);
  const iv = randomBytes(IV_LENGTH);
  const cipher = createCipheriv(ALGORITHM, dataKey, iv);

  return {
    stream: cipher,
    envelope: () => ({
      algorithm: ALGORITHM,
      keyId: keyId(masterKey),
      wrappedKey: wrap(masterKey, dataKey),
      iv: iv.toString("base64"),
      authTag: cipher.getAuthTag().toString("base64"),
    }),
  };
}

export function createDecryptor(masterKey, envelope) {
  if (envelope.algorithm !== ALGORITHM) {
    throw new Error(`Unsupported encryption algorithm: ${envelope.algorithm}`);
  }
  if (envelope.keyId !== keyId(masterKey)) {
    throw new Error(
      `Backup was encrypted with key ${envelope.keyId}, but the configured key is ${keyId(masterKey)}`,
    );
  }
  const dataKey = unwrap(masterKey, envelope.wrappedKey);
  const decipher = createDecipheriv(ALGORITHM, dataKey, Buffer.from(envelope.iv, "base64"));
  decipher.setAuthTag(Buffer.from(envelope.authTag, "base64"));
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
