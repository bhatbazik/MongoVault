import { Buffer } from "node:buffer";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { describe, expect, it } from "vitest";
import {
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

  it("encrypts and decrypts stream payloads with envelope metadata", async () => {
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
    expect(envelope.algorithm).toBe("aes-256-gcm");

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

  it("fails decryption if auth tag is modified", async () => {
    const masterKey = parseKey(generateKey());
    const encryptor = createEncryptor(masterKey);
    const chunks = [];

    await pipeline(
      Readable.from(Buffer.from("tamper test")),
      encryptor.stream,
      async function* (source) {
        for await (const chunk of source) chunks.push(chunk);
      },
    );

    const envelope = encryptor.envelope();
    const badTag = Buffer.from(envelope.authTag, "base64");
    badTag[0] ^= 0xff;
    envelope.authTag = badTag.toString("base64");

    const decipher = createDecryptor(masterKey, envelope);
    await expect(
      pipeline(Readable.from(Buffer.concat(chunks)), decipher, async function* (s) {
        for await (const c of s) yield c;
      }),
    ).rejects.toThrow();
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
});
