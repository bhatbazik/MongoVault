import { Buffer } from "node:buffer";
import { createCipheriv, randomBytes } from "node:crypto";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { describe, expect, it } from "vitest";
import {
  CHUNKED_ALGORITHM,
  createDecryptor,
  createEncryptor,
  generateKey,
  keyId,
  parseKey,
} from "../src/crypto.js";

describe("crypto", () => {
  it("generates a 32-byte base64 key", () => {
    const raw = generateKey();
    const parsed = parseKey(raw);
    expect(parsed.length).toBe(32);
  });

  it("parses hex and base64 encoded keys", () => {
    const key = Buffer.alloc(32, 7);
    expect(parseKey(key.toString("hex"))).toEqual(key);
    expect(parseKey(key.toString("base64"))).toEqual(key);
  });

  it("rejects invalid key lengths", () => {
    expect(() => parseKey("short")).toThrow("Encryption key must be 32 bytes");
  });

  it("encrypts and decrypts stream payloads with chunked AEAD frames", async () => {
    const masterKey = parseKey(generateKey());
    const payload = Buffer.from("Lorem ipsum dolor sit amet, secret mongo database content");

    const encryptor = createEncryptor(masterKey);
    const encryptedChunks = [];

    await pipeline(
      Readable.from(payload),
      encryptor.stream,
      async function* (source) {
        for await (const chunk of source) {
          encryptedChunks.push(chunk);
          yield chunk;
        }
      },
    );

    const envelope = encryptor.envelope();
    expect(envelope.keyId).toBe(keyId(masterKey));
    expect(envelope.algorithm).toBe(CHUNKED_ALGORITHM);
    expect(envelope.chunkSize).toBe(64 * 1024);

    const decipher = createDecryptor(masterKey, envelope);
    const decryptedChunks = [];

    await pipeline(
      Readable.from(Buffer.concat(encryptedChunks)),
      decipher,
      async function* (source) {
        for await (const chunk of source) {
          decryptedChunks.push(chunk);
          yield chunk;
        }
      },
    );

    expect(Buffer.concat(decryptedChunks)).toEqual(payload);
  });

  it("fails decryption immediately if frame data is tampered with", async () => {
    const masterKey = parseKey(generateKey());
    const encryptor = createEncryptor(masterKey);
    const chunks = [];

    await pipeline(
      Readable.from(Buffer.from("tamper test payload")),
      encryptor.stream,
      async function* (source) {
        for await (const chunk of source) chunks.push(chunk);
      },
    );

    const envelope = encryptor.envelope();
    const encrypted = Buffer.concat(chunks);
    // Tamper with the last byte (authTag) of the frame
    encrypted[encrypted.length - 1] ^= 0xff;

    const decipher = createDecryptor(masterKey, envelope);
    await expect(
      pipeline(Readable.from(encrypted), decipher, async function* (s) {
        for await (const c of s) yield c;
      }),
    ).rejects.toThrow();
  });

  it("detects truncated stream missing final frame", async () => {
    const masterKey = parseKey(generateKey());
    const encryptor = createEncryptor(masterKey);
    const chunks = [];

    await pipeline(
      Readable.from(Buffer.alloc(100_000, "a")),
      encryptor.stream,
      async function* (source) {
        for await (const chunk of source) chunks.push(chunk);
      },
    );

    const envelope = encryptor.envelope();
    const fullEncrypted = Buffer.concat(chunks);
    // Cut off the second frame (truncation)
    const truncated = fullEncrypted.subarray(0, 65536 + 5 + 12 + 16);

    const decipher = createDecryptor(masterKey, envelope);
    await expect(
      pipeline(Readable.from(truncated), decipher, async function* (s) {
        for await (const c of s) yield c;
      }),
    ).rejects.toThrow(/Truncated AEAD stream/);
  });

  it("supports backwards compatibility with legacy single-block aes-256-gcm backups", async () => {
    const masterKey = parseKey(generateKey());
    const dataKey = randomBytes(32);
    const iv = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", dataKey, iv);
    const payload = Buffer.from("legacy database backup content");
    const ciphertext = Buffer.concat([cipher.update(payload), cipher.final()]);
    const authTag = cipher.getAuthTag();

    // Wrap dataKey with masterKey using AES-GCM
    const wrapIv = randomBytes(12);
    const wrapCipher = createCipheriv("aes-256-gcm", masterKey, wrapIv);
    const wrappedEnc = Buffer.concat([wrapCipher.update(dataKey), wrapCipher.final()]);
    const wrappedKey = Buffer.concat([wrapIv, wrapCipher.getAuthTag(), wrappedEnc]).toString("base64");

    const legacyEnvelope = {
      algorithm: "aes-256-gcm",
      keyId: keyId(masterKey),
      wrappedKey,
      iv: iv.toString("base64"),
      authTag: authTag.toString("base64"),
    };

    const decipher = createDecryptor(masterKey, legacyEnvelope);
    const decrypted = [];
    await pipeline(
      Readable.from(ciphertext),
      decipher,
      async function* (source) {
        for await (const chunk of source) decrypted.push(chunk);
      },
    );

    expect(Buffer.concat(decrypted)).toEqual(payload);
  });

  it("fails decryption if keyId does not match master key", async () => {
    const keyA = parseKey(generateKey());
    const keyB = parseKey(generateKey());
    const encryptor = createEncryptor(keyA);

    await pipeline(Readable.from(Buffer.from("data")), encryptor.stream, async function* (s) {
      for await (const c of s) yield c;
    });

    const envelope = encryptor.envelope();
    expect(() => createDecryptor(keyB, envelope)).toThrow("Backup was encrypted with key");
  });

  it("authenticates frame header and prevents truncation by tampering with isFinal flag", async () => {
    const masterKey = parseKey(generateKey());
    const payload = randomBytes(64 * 1024 * 3 + 100);
    const encryptor = createEncryptor(masterKey);
    const chunks = [];

    await pipeline(
      Readable.from(payload),
      encryptor.stream,
      async function* (source) {
        for await (const chunk of source) chunks.push(chunk);
      },
    );

    const envelope = encryptor.envelope();
    const fullCiphertext = Buffer.concat(chunks);
    const frameSize = 5 + 12 + 64 * 1024 + 16;
    // Keep first 2 frames and flip frame #2's isFinal byte to 1
    const truncated = Buffer.from(fullCiphertext.subarray(0, frameSize * 2));
    truncated.writeUInt8(1, frameSize + 4);

    const decipher = createDecryptor(masterKey, envelope);
    await expect(
      pipeline(Readable.from(truncated), decipher, async function* (s) {
        for await (const c of s) yield c;
      }),
    ).rejects.toThrow();
  });

  it("supports key rotation with key ring array", async () => {
    const oldKey = parseKey(generateKey());
    const newKey = parseKey(generateKey());
    const encryptor = createEncryptor(oldKey);

    await pipeline(Readable.from(Buffer.from("data")), encryptor.stream, async function* (s) {
      for await (const c of s) yield c;
    });

    const envelope = encryptor.envelope();
    // Decryptor provided with key ring [newKey, oldKey]
    const decipher = createDecryptor([newKey, oldKey], envelope);
    expect(decipher).toBeDefined();
  });
});
