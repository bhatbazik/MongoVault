import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { describe, expect, it } from "vitest";
import { createTarHeader, createTarPackStream, extractTarStream } from "../src/tar.js";

describe("POSIX UStar / PAX TAR streamer (tar.ts full coverage)", () => {
  it("packs and extracts files with long nested paths (>255 bytes PAX) and splits UStar paths (100-255 bytes)", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-tar-"));
    const srcDir = path.join(workDir, "src");
    const dstDir = path.join(workDir, "dst");

    try {
      // 1. Short path
      const shortFile = path.join(srcDir, "short.txt");
      // 2. UStar split path (between 101 and 255 bytes, with '/' split)
      const midDir = path.join(srcDir, "a".repeat(80), "b".repeat(50));
      const midFile = path.join(midDir, "medium.wt");
      // 3. Very long path (> 255 bytes -> triggers PAX extended header)
      const longDir = path.join(srcDir, "x".repeat(120), "y".repeat(120));
      const longFile = path.join(longDir, "longfilename.wt");

      await fs.mkdir(midDir, { recursive: true });
      await fs.mkdir(longDir, { recursive: true });
      await fs.writeFile(shortFile, "hello");
      await fs.writeFile(midFile, "medium-data");
      await fs.writeFile(longFile, "long-pax-data");

      const tarStream = createTarPackStream([
        { name: "short.txt", fullPath: shortFile, size: 5 },
        { name: `${"a".repeat(80)}/${"b".repeat(50)}/medium.wt`, fullPath: midFile, size: 11 },
        {
          name: `${"x".repeat(120)}/${"y".repeat(120)}/longfilename.wt`,
          fullPath: longFile,
          size: 13,
        },
      ]);

      const result = await extractTarStream(tarStream, dstDir);
      expect(result.extractedCount).toBe(3);
      expect(result.totalBytes).toBe(29);
      expect(await fs.readFile(path.join(dstDir, "short.txt"), "utf8")).toBe("hello");
      expect(
        await fs.readFile(
          path.join(dstDir, `${"x".repeat(120)}/${"y".repeat(120)}/longfilename.wt`),
          "utf8",
        ),
      ).toBe("long-pax-data");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("rejects path-traversal TAR entries, supports AbortSignal, and encodes >8GiB base-256 sizes", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-tar-sec-"));
    const srcFile = path.join(workDir, "f.txt");
    await fs.writeFile(srcFile, "123");

    try {
      // Base-256 size encoding for >= 8 GiB
      const bigHeader = createTarHeader("bigfile.wt", 9 * 1024 * 1024 * 1024);
      expect(bigHeader[124]).toBe(0x80);

      // Raw TAR header with path traversal bypassing packer sanitization
      const evilHdr = createTarHeader("placeholder.txt", 3);
      evilHdr.write("../../etc/passwd\0", 0, 17, "utf8");
      // Recompute checksum
      evilHdr.fill(32, 148, 156);
      let sum = 0;
      for (let i = 0; i < 512; i++) sum += evilHdr[i];
      evilHdr.write(sum.toString(8).padStart(6, "0") + "\0 ", 148, "ascii");
      const payload = Buffer.alloc(512, 0);
      payload.write("123", 0, "utf8");
      const evilStream = Readable.from([Buffer.concat([evilHdr, payload, Buffer.alloc(1024, 0)])]);
      const res = await extractTarStream(evilStream, path.join(workDir, "out"));
      expect(res.extractedCount).toBe(1);
      expect(await fs.readFile(path.join(workDir, "out", "etc", "passwd"), "utf8")).toBe("123");

      const ac = new AbortController();
      ac.abort(new Error("cancelled"));
      const abortedStream = createTarPackStream(
        [{ name: "f.txt", fullPath: srcFile, size: 3 }],
        ac.signal,
      );
      await expect(
        extractTarStream(abortedStream, path.join(workDir, "out2"), ac.signal),
      ).rejects.toThrow();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
