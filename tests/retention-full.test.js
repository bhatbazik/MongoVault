import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";
import { describe, expect, it, vi } from "vitest";
import { writeManifest } from "../src/catalog.js";
import { oplogSliceKey } from "../src/oplog.js";
import { deleteBackup, pruneOplogSlices } from "../src/retention.js";
import { createFilesystemStorage } from "../src/storage.js";

function makeLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
}

describe("Retention & Cascade Delete (retention.ts full coverage)", () => {
  it("deleteBackup protects PITR segment anchors, supports --force/--dry-run, and cascades sharded .cluster keys", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-ret-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const logger = makeLogger();
    const config = {
      storage: { prefix: "mv" },
      mongo: { database: "*" },
      schedule: { timezone: "UTC" },
    };

    try {
      const b1 = "mv/_cluster/2026-01-01T00-00-00.cluster";
      const cfgKey = "mv/configsvr/b1.archive.gz";
      const s1Key = "mv/shards/rs1/b1.archive.gz";
      const brKey = "mv/shards/rs1/_bridging/b1.oplog.gz";

      await storage.upload(b1, Buffer.from("cluster-meta"));
      await storage.upload(cfgKey, Buffer.from("cfg"));
      await storage.upload(s1Key, Buffer.from("shard1"));
      await storage.upload(brKey, Buffer.from("bridge"));

      await writeManifest(storage, {
        version: 1,
        type: "sharded-cluster-backup",
        key: b1,
        database: "*",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:01:00Z",
        durationMs: 1000,
        size: 12,
        sha256: "abc",
        compression: "gzip",
        encryption: null,
        oplogWindow: { startTs: "100:0", endTs: "100:1" },
        sharding: {
          clusterTime: "100:1",
          configsvr: { backupKey: cfgKey, status: "success" },
          shards: {
            rs1: { backupKey: s1Key, bridgingSliceKeys: [brKey], status: "success" },
          },
        },
        collections: {},
      });

      // Add a PITR slice anchored to b1
      const sl1 = oplogSliceKey(
        { prefix: "mv", database: "*" },
        { t: 100, i: 1 },
        { t: 200, i: 1 },
      );
      await storage.upload(sl1, Buffer.from("slice1"));
      await writeManifest(storage, {
        version: 1,
        type: "oplog-slice",
        key: sl1,
        database: "*",
        createdAt: "2026-01-01T00:02:00Z",
        completedAt: "2026-01-01T00:02:01Z",
        durationMs: 100,
        size: 6,
        sha256: "s1",
        compression: "gzip",
        encryption: null,
        oplogWindow: { startTs: "100:1", endTs: "200:1" },
        collections: {},
      });

      // Refuses to delete sole PITR anchor without --force
      await expect(deleteBackup({ config, storage, logger }, { key: b1 })).rejects.toThrow(
        /active base snapshot anchoring PITR/,
      );

      // Dry-run with --force preserves files
      const dryRes = await deleteBackup(
        { config, storage, logger },
        { key: b1, force: true, dryRun: true },
      );
      expect(dryRes.deleted).toBe(false);
      expect(await storage.head(s1Key)).not.toBeNull();

      // Actual delete with --force cascades to configsvr, shard, and bridging keys
      const delRes = await deleteBackup({ config, storage, logger }, { key: b1, force: true });
      expect(delRes.deleted).toBe(true);
      expect(await storage.head(b1)).toBeNull();
      expect(await storage.head(cfgKey)).toBeNull();
      expect(await storage.head(s1Key)).toBeNull();
      expect(await storage.head(brKey)).toBeNull();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("pruneOplogSlices handles sharded discovery, ObjectLock warnings, and dryRun", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-prune-oplog-"));
    const storage = createFilesystemStorage({ basePath: workDir });
    const logger = makeLogger();
    const config = {
      storage: { prefix: "mv" },
      mongo: { database: "*" },
      sharding: { enabled: true },
      schedule: { timezone: "UTC" },
    };

    try {
      const cfgSlice = oplogSliceKey(
        { prefix: "mv/configsvr", database: "*" },
        { t: 50, i: 0 },
        { t: 100, i: 1 },
      );
      const s1Slice = oplogSliceKey(
        { prefix: "mv/shards/rs1", database: "*" },
        { t: 50, i: 0 },
        { t: 100, i: 1 },
      );
      await storage.upload(cfgSlice, Buffer.from("c"));
      await storage.upload(s1Slice, Buffer.from("s"));
      await writeManifest(storage, {
        version: 1,
        type: "oplog-slice",
        key: cfgSlice,
        database: "*",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:00:01Z",
        durationMs: 1,
        size: 1,
        sha256: "a",
        compression: "gzip",
        encryption: null,
        oplogWindow: { startTs: "50:0", endTs: "100:1" },
        collections: {},
      });
      await writeManifest(storage, {
        version: 1,
        type: "oplog-slice",
        key: s1Slice,
        database: "*",
        createdAt: "2026-01-01T00:00:00Z",
        completedAt: "2026-01-01T00:00:01Z",
        durationMs: 1,
        size: 1,
        sha256: "b",
        compression: "gzip",
        encryption: null,
        oplogWindow: { startTs: "50:0", endTs: "100:1" },
        collections: {},
      });

      const dry = await pruneOplogSlices(
        { config, storage, logger },
        { olderThan: "200:0", dryRun: true },
      );
      expect(dry.deletedSlices.length).toBe(2);
      expect(await storage.head(cfgSlice)).not.toBeNull();

      // ObjectLock error during pruneOplogSlices logs warning instead of crashing
      const origRemoveMany = storage.removeMany.bind(storage);
      storage.removeMany = vi.fn(async () => {
        throw new Error("AccessDenied: S3 Object Lock WORM retention period active");
      });
      await pruneOplogSlices({ config, storage, logger }, { olderThan: "200:0" });
      expect(logger.warn).toHaveBeenCalled();

      // Real delete
      storage.removeMany = origRemoveMany;
      const actual = await pruneOplogSlices({ config, storage, logger }, { olderThan: "200:0" });
      expect(actual.deletedSlices.length).toBe(2);
      expect(await storage.head(cfgSlice)).toBeNull();
      expect(await storage.head(s1Slice)).toBeNull();

      // ObjectLock error on deleteBackup throws informative error
      await expect(
        deleteBackup(
          {
            config: { ...config, sharding: { enabled: false } },
            storage: {
              ...storage,
              list: storage.list.bind(storage),
              getJson: vi.fn(async () => ({
                data: {
                  version: 1,
                  key: "mv/_cluster/b.gz",
                  database: "*",
                  createdAt: "2026-01-01T00:00:00Z",
                  completedAt: "2026-01-01T00:00:01Z",
                  durationMs: 1,
                  size: 1,
                  sha256: "x",
                  compression: "gzip",
                  encryption: null,
                  collections: {},
                },
              })),
              head: vi.fn(async () => ({ size: 1 })),
              removeMany: vi.fn(async () => {
                throw new Error("Object Lock retention period active");
              }),
            },
            logger,
          },
          { key: "mv/_cluster/b.gz", force: true },
        ),
      ).rejects.toThrow(/S3 WORM Compliance Object Lock/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
