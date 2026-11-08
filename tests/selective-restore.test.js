import pino from "pino";
import { PassThrough, Readable } from "node:stream";
import { describe, expect, it } from "vitest";
import { restoreBackup } from "../src/restore.js";

describe("selective restore (namespace filtering)", () => {
  const config = {
    mongo: {
      uri: "mongodb://localhost:27017",
      database: "*",
      parallelCollections: 2,
    },
    storage: {
      prefix: "mongodb",
    },
    workDir: "/tmp/selective-restore-test",
  };

  it("passes nsInclude and nsExclude to mongorestore options", async () => {
    let capturedOptions = null;
    const mockTools = {
      async mongorestore(opts) {
        capturedOptions = opts;
        const stdin = new PassThrough();
        return {
          stdin,
          done: Promise.resolve({ restored: 150, failed: 0 }),
        };
      },
    };

    const mockStorage = {
      async head() {
        return { size: 1000, lastModified: new Date() };
      },
      async getJson() {
        return {
          data: {
            key: "mongodb/cluster/2026-03-10/backup.archive.gz",
            database: "*",
            size: 15,
            sha256: "7cb3e37a8c71a287586d099b9c310df90eac69d7f99c0042af4ce67ebc504c22",
          },
        };
      },
      async download() {
        return Readable.from([Buffer.from("archive-content")]);
      },
    };

    const logger = pino({ level: "silent" });

    // Restore with selective inclusion and exclusion
    await restoreBackup(
      { config, storage: mockStorage, logger, tools: mockTools },
      {
        ref: "mongodb/cluster/2026-03-10/backup.archive.gz",
        uri: "mongodb://localhost:27017",
        nsInclude: "production.users,production.orders*",
        nsExclude: "production.audit_log",
        preVerify: false,
      },
    );

    expect(capturedOptions).not.toBeNull();
    expect(capturedOptions.nsInclude).toBe("production.users,production.orders*");
    expect(capturedOptions.nsExclude).toBe("production.audit_log");
  });

  it("rejects oplog replay when selective namespace filters are present", async () => {
    const mockStorage = {
      async head() {
        return { size: 1000, lastModified: new Date() };
      },
      async getJson() {
        return {
          data: {
            key: "mongodb/cluster/2026-03-10/backup.archive.gz",
            database: "*",
          },
        };
      },
    };
    const logger = pino({ level: "silent" });

    await expect(
      restoreBackup(
        { config, storage: mockStorage, logger },
        {
          ref: "mongodb/cluster/2026-03-10/backup.archive.gz",
          uri: "mongodb://localhost:27017",
          nsInclude: "production.users",
          oplogReplay: true,
          preVerify: false,
        },
      ),
    ).rejects.toThrow("Cannot use --oplog-replay");
  });

  it("performs upfront pre-verification when preVerify: true is specified without drop", async () => {
    let mongorestoreCalled = false;
    const mockTools = {
      async mongorestore() {
        mongorestoreCalled = true;
        return {
          stdin: new PassThrough(),
          done: Promise.resolve({ restored: 10, failed: 0 }),
        };
      },
    };

    const mockStorage = {
      async head() {
        return { size: 100, lastModified: new Date() };
      },
      async getJson() {
        return {
          data: {
            key: "mongodb/cluster/2026-03-10/corrupt.archive.gz",
            database: "*",
            size: 100,
            sha256: "expected-sha256",
          },
        };
      },
      async download() {
        // Return corrupt content that fails gzip / checksum
        return Readable.from([Buffer.from("invalid-corrupted-bytes")]);
      },
    };

    const logger = pino({ level: "silent" });

    // With preVerify: true, it should reject during upfront verification before mongorestore is ever invoked
    await expect(
      restoreBackup(
        { config, storage: mockStorage, logger, tools: mockTools },
        {
          ref: "mongodb/cluster/2026-03-10/corrupt.archive.gz",
          uri: "mongodb://localhost:27017",
          drop: false,
          preVerify: true,
        },
      ),
    ).rejects.toThrow(/could not be read back|Checksum mismatch/i);

    expect(mongorestoreCalled).toBe(false);
  });
});
