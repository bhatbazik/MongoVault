import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { describe, expect, it } from "vitest";
import { DigestStream } from "../src/digest.js";

describe("digest", () => {
  it("computes SHA-256 and tracks bytes in stream without modifying payload", async () => {
    const data = Buffer.from("hello world streaming test 12345");
    const expectedHash = createHash("sha256").update(data).digest("hex");

    const digest = new DigestStream();
    const collected = [];

    await pipeline(
      Readable.from(data),
      digest,
      async function* (source) {
        for await (const chunk of source) {
          collected.push(chunk);
          yield chunk;
        }
      },
    );

    expect(Buffer.concat(collected)).toEqual(data);
    expect(digest.bytes).toBe(data.length);
    expect(digest.digest()).toBe(expectedHash);
  });
});
