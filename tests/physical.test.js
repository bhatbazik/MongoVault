import { describe, expect, it } from "vitest";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { Readable } from "node:stream";
import {
  createTarHeader,
  extractTarStream,
  generateTarStream,
  probePhysicalCapability,
} from "../src/physical.js";

describe("physical backup engine", () => {
  it("encodes valid 512-byte UStar tar headers with correct checksums", () => {
    const header = createTarHeader("collection-0-123456789.wt", 1024, 1700000000000);
    expect(header.length).toBe(512);

    const name = header.toString("utf8", 0, 100).replace(/\0.*$/, "");
    expect(name).toBe("collection-0-123456789.wt");

    const magic = header.toString("ascii", 257, 263);
    expect(magic.startsWith("ustar")).toBe(true);

    const sizeOctal = header.toString("ascii", 124, 136).replace(/\0.*$/, "").trim();
    expect(parseInt(sizeOctal, 8)).toBe(1024);
  });

  it("streams and extracts physical tar archives without corruption", async () => {
    const tempSourceDir = path.join(os.tmpdir(), `mv-phys-src-${Date.now()}`);
    const tempDestDir = path.join(os.tmpdir(), `mv-phys-dest-${Date.now()}`);

    await fs.mkdir(tempSourceDir, { recursive: true });
    const file1 = path.join(tempSourceDir, "WiredTiger.wt");
    const file2 = path.join(tempSourceDir, "collection-1.wt");

    const content1 = Buffer.from("WiredTiger metadata header binary test bytes 12345");
    const content2 = Buffer.from("Collection page data content block 67890 ABCD EFGH");

    await fs.writeFile(file1, content1);
    await fs.writeFile(file2, content2);

    const filesToPack = [
      { name: "WiredTiger.wt", fullPath: file1, size: content1.length },
      { name: "collection-1.wt", fullPath: file2, size: content2.length },
    ];

    const tarStream = Readable.from(generateTarStream(filesToPack));
    const result = await extractTarStream(tarStream, tempDestDir);

    expect(result.extractedCount).toBe(2);
    expect(result.totalBytes).toBe(content1.length + content2.length);

    const restored1 = await fs.readFile(path.join(tempDestDir, "WiredTiger.wt"));
    const restored2 = await fs.readFile(path.join(tempDestDir, "collection-1.wt"));

    expect(restored1).toEqual(content1);
    expect(restored2).toEqual(content2);

    await fs.rm(tempSourceDir, { recursive: true, force: true });
    await fs.rm(tempDestDir, { recursive: true, force: true });
  });

  it("probes physical capabilities using mock MongoClient", async () => {
    const mockClient = {
      db(name) {
        if (name === "admin") {
          return {
            aggregate() {
              return {
                async next() {
                  return { metadata: { dbpath: "/var/lib/mongodb" } };
                },
                async close() {},
              };
            },
            async command(cmd) {
              if (cmd.getCmdLineOpts) {
                return { parsed: { storage: { dbPath: "/var/lib/mongodb" } } };
              }
              if (cmd.fsync) {
                return { ok: 1, lock: true };
              }
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    const caps = await probePhysicalCapability(mockClient);
    expect(caps.supportsBackupCursor).toBe(true);
    expect(caps.dbPath).toBe("/var/lib/mongodb");
  });

  it("retries $backupCursor creation when WiredTiger checkpoint conflict occurs (code 50915)", async () => {
    let attempts = 0;
    const mockAdmin = {
      aggregate() {
        attempts++;
        if (attempts === 1) {
          const err = new Error("BackupCursorOpenConflictWithCheckpoint");
          err.code = 50915;
          throw err;
        }
        return {
          async next() {
            return { metadata: { dbpath: "/data/db" } };
          },
          async close() {},
        };
      },
    };

    const { createBackupCursorWithRetries } = await import("../src/physical.js");
    const cursor = await createBackupCursorWithRetries(mockAdmin, 3);
    expect(attempts).toBe(2);
    expect(cursor).toBeDefined();
  });
});
