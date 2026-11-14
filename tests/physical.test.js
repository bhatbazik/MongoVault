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

  it("trims dbPath prefix from absolute $backupCursor filenames", async () => {
    const { trimDbPathPrefix } = await import("../src/physical.js");
    expect(trimDbPathPrefix("/var/lib/mongodb/WiredTiger.wt", "/var/lib/mongodb")).toBe(
      "WiredTiger.wt",
    );
    expect(
      trimDbPathPrefix("/var/lib/mongodb/journal/WiredTigerLog.0000000001", "/var/lib/mongodb/"),
    ).toBe("journal/WiredTigerLog.0000000001");
    expect(trimDbPathPrefix("collection-0-123.wt", "/var/lib/mongodb")).toBe("collection-0-123.wt");
  });

  it("executes full physical backup and restore with $backupCursor and $backupCursorExtend", async () => {
    const { runPhysicalBackup, restorePhysicalBackup } = await import("../src/physical.js");
    const { createFilesystemStorage } = await import("../src/storage.js");

    const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "mv-phys-e2e-"));
    const sourceDbPath = path.join(tempRoot, "source-db");
    const restoreDbPath = path.join(tempRoot, "restore-db");
    const bucketDir = path.join(tempRoot, "bucket");

    await fs.mkdir(path.join(sourceDbPath, "journal"), { recursive: true });
    await fs.writeFile(path.join(sourceDbPath, "WiredTiger.wt"), "WT_ROOT_DATA");
    await fs.writeFile(
      path.join(sourceDbPath, "journal", "WiredTigerLog.0000000001"),
      "WT_JOURNAL_DATA",
    );

    const storage = createFilesystemStorage({ basePath: bucketDir });
    const mockLogger = {
      child() {
        return this;
      },
      info() {},
      warn() {},
      error() {},
      debug() {},
    };

    const mockClient = {
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            aggregate(pipeline) {
              if (pipeline[0]?.$backupCursor) {
                const docs = [
                  {
                    metadata: {
                      backupId: "b-1",
                      dbpath: sourceDbPath,
                      checkpointTimestamp: { t: 1774092000, i: 1 },
                    },
                  },
                  {
                    filename: path.join(sourceDbPath, "WiredTiger.wt"),
                  },
                ];
                let idx = 0;
                return {
                  async next() {
                    return docs[idx++] ?? null;
                  },
                  async *[Symbol.asyncIterator]() {
                    while (idx < docs.length) {
                      yield docs[idx++];
                    }
                  },
                  async close() {},
                };
              }
              if (pipeline[0]?.$backupCursorExtend) {
                const jDocs = [
                  {
                    filename: path.join(sourceDbPath, "journal", "WiredTigerLog.0000000001"),
                  },
                ];
                return {
                  async *[Symbol.asyncIterator]() {
                    for (const d of jDocs) yield d;
                  },
                  async close() {},
                };
              }
              throw new Error("Unexpected pipeline");
            },
            async command() {
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    const config = {
      mongo: { uri: "mongodb://localhost:27017", database: "*" },
      storage: { prefix: "phys-test" },
      encryptionKey: Buffer.alloc(32, 9),
    };

    const backupRes = await runPhysicalBackup({
      config,
      storage,
      logger: mockLogger,
      dbPath: sourceDbPath,
      createMongoClient: () => mockClient,
    });

    expect(backupRes.filesCount).toBe(2);
    expect(backupRes.manifest.physical?.cursorType).toBe("$backupCursor");
    expect(backupRes.manifest.oplogWindow?.endTs).toBe("1774092000:1");

    const restoreRes = await restorePhysicalBackup({
      config,
      storage,
      logger: mockLogger,
      targetDbPath: restoreDbPath,
      ref: "latest",
    });

    expect(restoreRes.restoredFiles).toBe(2);
    const restoredWt = await fs.readFile(path.join(restoreDbPath, "WiredTiger.wt"), "utf8");
    const restoredJournal = await fs.readFile(
      path.join(restoreDbPath, "journal", "WiredTigerLog.0000000001"),
      "utf8",
    );
    expect(restoredWt).toBe("WT_ROOT_DATA");
    expect(restoredJournal).toBe("WT_JOURNAL_DATA");

    await fs.rm(tempRoot, { recursive: true, force: true });
  });

  it("bounds TAR stream output when live WiredTiger files grow or shrink during hot backup", async () => {
    const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-tar-live-"));
    const destDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-tar-dest-"));
    try {
      const growingFile = path.join(tempDir, "collection-grow.wt");
      const shrinkingFile = path.join(tempDir, "collection-shrink.wt");
      const tailFile = path.join(tempDir, "WiredTiger.wt");

      await fs.writeFile(growingFile, Buffer.alloc(100, 0x41));
      await fs.writeFile(shrinkingFile, Buffer.alloc(200, 0x42));
      await fs.writeFile(tailFile, Buffer.from("TAIL_OK"));

      const entries = [
        { name: "collection-grow.wt", fullPath: growingFile, size: 100 },
        { name: "collection-shrink.wt", fullPath: shrinkingFile, size: 200 },
        { name: "WiredTiger.wt", fullPath: tailFile, size: 7 },
      ];

      // Mutate files after stat/header creation by intercepting generator steps
      const gen = generateTarStream(entries);
      const chunks = [];
      let step = 0;
      for await (const chunk of gen) {
        step++;
        if (step === 1) {
          // Right after header for growingFile is emitted (100 bytes), append 500 extra bytes on disk!
          await fs.appendFile(growingFile, Buffer.alloc(500, 0x58));
          // And truncate shrinkingFile from 200 bytes down to 50 bytes before its header/read!
        }
        if (step === 4) {
          // Right after header for shrinkingFile is emitted (after step 1=hdr1, 2=data1, 3=pad1, 4=hdr2),
          // truncate shrinkingFile on disk from 200 down to 50 bytes before its stream is read!
          await fs.truncate(shrinkingFile, 50);
        }
        chunks.push(chunk);
      }

      const result = await extractTarStream(Readable.from(Buffer.concat(chunks)), destDir);
      expect(result.extractedCount).toBe(3);

      const extractedGrow = await fs.readFile(path.join(destDir, "collection-grow.wt"));
      const extractedShrink = await fs.readFile(path.join(destDir, "collection-shrink.wt"));
      const extractedTail = await fs.readFile(path.join(destDir, "WiredTiger.wt"), "utf8");

      expect(extractedGrow.length).toBe(100);
      expect(extractedShrink.length).toBe(200);
      expect(extractedTail).toBe("TAIL_OK");
    } finally {
      await fs.rm(tempDir, { recursive: true, force: true });
      await fs.rm(tempDestDirIfAny(destDir), { recursive: true, force: true });
    }

    function tempDestDirIfAny(d) {
      return d;
    }
  });
});
