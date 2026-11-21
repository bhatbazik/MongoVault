import { Readable } from "node:stream";
import { BSON } from "mongodb";
import { describe, expect, it } from "vitest";
import { ARCHIVE_MAGIC_NUMBER, streamArchiveCollections } from "../src/archive-stream.js";
import { ConfigError, loadConfig } from "../src/config.js";
import { formatStatus } from "../src/status.js";

function int32LE(n) {
  const b = Buffer.alloc(4);
  b.writeInt32LE(n, 0);
  return b;
}

function uint32LE(n) {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n >>> 0, 0);
  return b;
}

describe("Additional Edge-Case Coverage (archive-stream, config, status)", () => {
  it("archive-stream handles CHUNK_DOC_SKIP across split chunks, EOF, and corrupted/truncated errors", async () => {
    const parts = [];
    parts.push(uint32LE(ARCHIVE_MAGIC_NUMBER));
    parts.push(Buffer.from(BSON.serialize({ version: "0.1" })));
    parts.push(int32LE(-1));

    // Ignored collection (exercises CHUNK_DOC_SKIP across 2-byte split chunks)
    parts.push(Buffer.from(BSON.serialize({ db: "skipdb", collection: "big" })));
    parts.push(Buffer.from(BSON.serialize({ _id: 1, data: "x".repeat(40) })));
    parts.push(int32LE(-1));

    // Selected collection
    parts.push(Buffer.from(BSON.serialize({ db: "keepdb", collection: "users" })));
    parts.push(Buffer.from(BSON.serialize({ _id: 2, name: "alice" })));
    parts.push(int32LE(-1));
    parts.push(int32LE(-1));

    const full = Buffer.concat(parts);
    // Feed in tiny 7-byte chunks to force CHUNK_DOC_SKIP & CHUNK_DOC_READ splitting
    const tinyChunks = [];
    for (let i = 0; i < full.length; i += 7) {
      tinyChunks.push(full.subarray(i, i + 7));
    }

    const docs = [];
    const count = await streamArchiveCollections(
      Readable.from(tinyChunks),
      (db) => db === "keepdb",
      async ({ db, collection, rawDoc }) => {
        docs.push({ db, collection, doc: BSON.deserialize(rawDoc) });
      },
    );
    expect(count).toBe(1);
    expect(docs[0].doc.name).toBe("alice");

    // Truncated stream error
    await expect(
      streamArchiveCollections(
        Readable.from([full.subarray(0, full.length - 10)]),
        () => true,
        async () => {},
      ),
    ).rejects.toThrow(/Truncated/);

    // Invalid magic number
    await expect(
      streamArchiveCollections(
        Readable.from([Buffer.from([1, 2, 3, 4])]),
        () => true,
        async () => {},
      ),
    ).rejects.toThrow(/Invalid MongoDB archive magic/);
  });

  it("config.ts validates S3, filesystem, encryption, oplog, and PITR constraints", () => {
    expect(() =>
      loadConfig({
        MONGODB_URI: "mongodb://localhost:27017",
        MONGODB_DATABASE: "mydb",
        MONGODB_OPLOG: "true",
        PITR_ENABLED: "true",
        STORAGE_TYPE: "s3",
      }),
    ).toThrow(ConfigError);

    expect(() =>
      loadConfig({
        MONGODB_URI: "mongodb://localhost:27017",
        MONGODB_DATABASE: "*",
        MONGODB_EXCLUDE_COLLECTIONS: "logs",
        STORAGE_TYPE: "filesystem",
        STORAGE_PATH: "",
        ENCRYPTION_KEY: "bad",
        ENCRYPTION_KEY_RING: "bad",
      }),
    ).toThrow(ConfigError);
  });

  it("formatStatus renders disconnected, active lock, PITR gaps, ObjectLock, sharding, and recentBackups", () => {
    const text = formatStatus({
      cluster: { connected: false, error: "timeout" },
      storage: {
        type: "s3",
        destination: "s3://bkt/mv",
        accessible: false,
        error: "AccessDenied",
        totalBackups: 1,
        totalSizeBytes: 2048,
        latestBackup: {
          createdAt: "2026-01-01T00:00:00Z",
          sizeBytes: 2048,
          encrypted: true,
          oplog: false,
        },
      },
      pitr: {
        enabled: true,
        flushIntervalSeconds: 30,
        slicesCount: 2,
        earliestTs: "10:1",
        latestTs: "20:1",
        timelineContinuous: false,
        gapWarning: "Gap detected between 12:1 and 15:1",
      },
      operation: {
        running: true,
        lockType: "hybrid",
        holder: "host-1",
        acquiredAt: "2026-01-01T00:00:00Z",
        expiresAt: "2026-01-01T00:05:00Z",
      },
      engine: "physical",
      objectLock: { enabled: true, mode: "COMPLIANCE", retentionDays: 30 },
      sharding: { isSharded: true },
      recentBackups: [{ key: "mv/b1.tar.gz", createdAt: "2026-01-01T00:00:00Z", size: 2048 }],
    });

    expect(text).toContain("FAILED (timeout)");
    expect(text).toContain("UNREACHABLE (AccessDenied)");
    expect(text).toContain("WARNING: Gap detected");
    expect(text).toContain("ACTIVE (hybrid lock)");
    expect(text).toContain("COMPLIANCE");
    expect(text).toContain("Active (Multi-Shard Coordinated)");
    expect(text).toContain("mv/b1.tar.gz");
  });
});
