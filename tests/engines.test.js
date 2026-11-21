import { describe, expect, it, vi } from "vitest";

vi.mock("../src/backup.js", () => ({
  runBackup: vi.fn(async () => ({ key: "logical.archive.gz", size: 100 })),
}));
vi.mock("../src/restore.js", () => ({
  restoreBackup: vi.fn(async () => ({ restored: true })),
}));
vi.mock("../src/physical.js", () => ({
  runPhysicalBackup: vi.fn(async () => ({ manifest: { key: "phys.tar.gz", size: 200 } })),
  restorePhysicalBackup: vi.fn(async () => ({ restoredFiles: 5 })),
}));
vi.mock("../src/sharding.js", () => ({
  runShardedClusterBackup: vi.fn(async () => ({ manifest: { key: "shard.cluster", size: 300 } })),
  restoreShardedCluster: vi.fn(async () => ({ shardsRestored: 2 })),
}));

import {
  EngineRegistry,
  LogicalBackupEngine,
  PhysicalBackupEngine,
  ShardedBackupEngine,
} from "../src/engines.js";

const baseConfig = {
  engine: "logical",
  mongo: { uri: "mongodb://localhost:27017", database: "app" },
  sharding: { enabled: false },
};

describe("EngineRegistry & Backup Engines", () => {
  it("resolves backup and restore engines properly", () => {
    expect(EngineRegistry.resolveForBackup({ config: baseConfig }).name).toBe("logical");
    expect(EngineRegistry.resolveForBackup({ engine: "physical", config: baseConfig }).name).toBe(
      "physical",
    );
    expect(EngineRegistry.resolveForBackup({ sharding: true, config: baseConfig }).name).toBe(
      "sharded",
    );
    expect(
      EngineRegistry.resolveForBackup({
        config: { ...baseConfig, sharding: { enabled: true } },
      }).name,
    ).toBe("sharded");

    expect(EngineRegistry.resolveForRestore({ config: baseConfig }).name).toBe("logical");
    expect(
      EngineRegistry.resolveForRestore({ dbPath: "/var/lib/mongo", config: baseConfig }).name,
    ).toBe("physical");
    expect(
      EngineRegistry.resolveForRestore({
        key: "db/2026.physical.tar.gz.enc",
        config: baseConfig,
      }).name,
    ).toBe("physical");
    expect(
      EngineRegistry.resolveForRestore({
        key: "cluster/2026.cluster.enc",
        config: baseConfig,
      }).name,
    ).toBe("sharded");
    expect(EngineRegistry.resolveForRestore({ sharding: true, config: baseConfig }).name).toBe(
      "sharded",
    );
    expect(() => EngineRegistry.get("nonexistent")).toThrow(/Unknown backup engine/);
  });

  it("executes backup and restore via Logical, Physical, and Sharded engines", async () => {
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const storage = {};

    const logical = new LogicalBackupEngine();
    expect((await logical.backup({ config: baseConfig, storage, logger })).key).toBe(
      "logical.archive.gz",
    );
    expect(await logical.restore({ config: baseConfig, storage, logger })).toEqual({
      restored: true,
    });

    const physical = new PhysicalBackupEngine();
    expect(
      (await physical.backup({ config: baseConfig, storage, logger, dbPath: "/data" })).key,
    ).toBe("phys.tar.gz");
    await expect(physical.restore({ config: baseConfig, storage, logger })).rejects.toThrow(
      /dbPath.*required/,
    );
    expect(
      await physical.restore({ config: baseConfig, storage, logger, dbPath: "/data" }),
    ).toEqual({ restoredFiles: 5 });

    const sharded = new ShardedBackupEngine();
    expect((await sharded.backup({ config: baseConfig, storage, logger })).key).toBe(
      "shard.cluster",
    );
    expect(await sharded.restore({ config: baseConfig, storage, logger })).toEqual({
      shardsRestored: 2,
    });

    const custom = {
      name: "custom-test",
      backup: vi.fn(),
      restore: vi.fn(),
    };
    EngineRegistry.register(custom);
    expect(EngineRegistry.get("custom-test")).toBe(custom);
  });
});
