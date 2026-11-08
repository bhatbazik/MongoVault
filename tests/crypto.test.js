import { Buffer } from "node:buffer";
import { createHmac, randomBytes } from "node:crypto";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { describe, expect, it } from "vitest";
import {
  CHUNKED_ALGORITHM,
  createDecryptor,
  createEncryptor,
  deriveHmacKey,
  deriveNonce,
  deriveWrapKey,
  generateKey,
  keyId,
  parseKey,
  signManifest,
  verifyManifest,
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

    await pipeline(Readable.from(payload), encryptor.stream, async function* (source) {
      for await (const chunk of source) {
        encryptedChunks.push(chunk);
        yield chunk;
      }
    });

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

  it("rejects non-chunked encryption algorithms", () => {
    const masterKey = parseKey(generateKey());
    const invalidEnvelope = {
      algorithm: "aes-256-gcm",
      keyId: keyId(masterKey),
      wrappedKey: "dummy",
      iv: Buffer.alloc(12).toString("base64"),
    };
    expect(() => createDecryptor(masterKey, invalidEnvelope)).toThrow(
      "Unsupported encryption algorithm: aes-256-gcm",
    );
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

    await pipeline(Readable.from(payload), encryptor.stream, async function* (source) {
      for await (const chunk of source) chunks.push(chunk);
    });

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

  it("rejects out-of-bounds frame indices in deriveNonce to prevent nonce wrap-around", () => {
    const baseNonce = randomBytes(12);
    expect(() => deriveNonce(baseNonce, -1)).toThrow("AEAD frame index out of bounds");
    expect(() => deriveNonce(baseNonce, 0x100000000)).toThrow("AEAD frame index out of bounds");
  });

  it("signs and verifies manifest HMAC and detects tampering", () => {
    const masterKey = parseKey(generateKey());
    const manifest = {
      version: 2,
      key: "mongodb/prod/2026-03-10/20260310T020000Z.archive.gz.enc",
      database: "prod",
      createdAt: "2026-03-10T02:00:00.000Z",
      completedAt: "2026-03-10T02:01:00.000Z",
      size: 1024,
      sha256: "abc123",
      compression: "gzip",
      oplog: false,
      excludedCollections: [],
      collections: { users: 50 },
      encryption: {
        algorithm: CHUNKED_ALGORITHM,
        keyId: keyId(masterKey),
        wrappedKey: "wrapped",
        iv: "iv",
        chunkSize: 65536,
      },
    };

    manifest.encryption.manifestHmac = signManifest(masterKey, manifest);
    expect(verifyManifest(masterKey, manifest)).toBe(true);

    // Tamper with oplog flag
    const tampered = structuredClone(manifest);
    tampered.oplog = true;
    expect(() => verifyManifest(masterKey, tampered)).toThrow("Manifest HMAC verification failed");
  });

  it("derives distinct sub-keys via HKDF for wrapping and HMAC signing (domain separation)", () => {
    const masterKey = parseKey(generateKey());
    const wrapKey = deriveWrapKey(masterKey);
    const hmacKey = deriveHmacKey(masterKey);

    expect(wrapKey.length).toBe(32);
    expect(hmacKey.length).toBe(32);
    expect(wrapKey).not.toEqual(masterKey);
    expect(hmacKey).not.toEqual(masterKey);
    expect(wrapKey).not.toEqual(hmacKey);
  });

  it("rejects manifests signed with raw masterKey or invalid signature without HKDF", () => {
    const masterKey = parseKey(generateKey());
    const manifest = {
      version: 2,
      key: "mongodb/prod/2026-03-10/invalid-hmac.archive.gz.enc",
      database: "prod",
      createdAt: "2026-03-10T02:00:00.000Z",
      completedAt: "2026-03-10T02:01:00.000Z",
      size: 1024,
      sha256: "abc123raw",
      compression: "gzip",
      oplog: false,
      excludedCollections: [],
      collections: {},
      encryption: {
        algorithm: CHUNKED_ALGORITHM,
        keyId: keyId(masterKey),
        wrappedKey: "wrapped",
        iv: "iv",
        chunkSize: 65536,
      },
    };

    // Sign directly with raw masterKey (which must be rejected because HKDF derivation is strictly required)
    const cleanEncryption = { ...manifest.encryption };
    const canonicalPayload = JSON.stringify({
      version: manifest.version,
      key: manifest.key,
      database: manifest.database,
      createdAt: manifest.createdAt,
      completedAt: manifest.completedAt,
      size: manifest.size,
      sha256: manifest.sha256,
      compression: manifest.compression,
      oplog: false,
      excludedCollections: [],
      collections: {},
      encryption: cleanEncryption,
    });
    manifest.encryption.manifestHmac = createHmac("sha256", masterKey)
      .update(canonicalPayload)
      .digest("hex");

    expect(() => verifyManifest(masterKey, manifest)).toThrow("Manifest HMAC verification failed");
  });
});
