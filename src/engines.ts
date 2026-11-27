import { runBackup } from "./backup.js";
import { isClusterScope } from "./catalog.js";
import { runPhysicalBackup, restorePhysicalBackup } from "./physical.js";
import { restoreBackup } from "./restore.js";
import { runShardedClusterBackup, restoreShardedCluster } from "./sharding.js";
import type { BackupManifest } from "./types/catalog.js";
import type { AppConfig } from "./types/config.js";
import type { Logger } from "./types/logger.js";
import type { StorageClient } from "./types/storage.js";

export interface BackupEngineOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  dbPath?: string;
}

export interface RestoreEngineOptions {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  ref?: string;
  uri?: string;
  database?: string;
  dbPath?: string;
  nsInclude?: string;
  nsExclude?: string;
  drop?: boolean;
  preVerify?: boolean;
  oplogReplay?: boolean;
  oplogLimit?: string;
  pitr?: string;
  parallelCollections?: number;
}

/**
 * Interface for backup and restore execution engines.
 */
export interface BackupEngineStrategy {
  readonly name: string;
  backup(options: BackupEngineOptions): Promise<BackupManifest>;
  restore(options: RestoreEngineOptions): Promise<unknown>;
}

export class LogicalBackupEngine implements BackupEngineStrategy {
  readonly name = "logical";

  async backup(options: BackupEngineOptions): Promise<BackupManifest> {
    return runBackup({
      config: options.config,
      storage: options.storage,
      logger: options.logger,
      signal: options.signal,
    });
  }

  async restore(options: RestoreEngineOptions): Promise<unknown> {
    if (options.dbPath) {
      throw new Error(
        "Logical restore does not support --db-path; use the physical engine instead",
      );
    }
    const targetUri = options.uri || options.config.mongo.uri;
    return restoreBackup(
      {
        config: options.config,
        storage: options.storage,
        logger: options.logger,
        signal: options.signal,
      },
      {
        ref: options.ref || "latest",
        uri: targetUri,
        database: options.database,
        nsInclude: options.nsInclude,
        nsExclude: options.nsExclude,
        drop: options.drop,
        preVerify: options.preVerify,
        oplogReplay: options.oplogReplay,
        oplogLimit: options.oplogLimit,
        pitr: options.pitr,
        parallelCollections: options.parallelCollections,
      },
    );
  }
}

export class PhysicalBackupEngine implements BackupEngineStrategy {
  readonly name = "physical";

  async backup(options: BackupEngineOptions): Promise<BackupManifest> {
    const res = await runPhysicalBackup({
      config: options.config,
      storage: options.storage,
      logger: options.logger,
      signal: options.signal,
      dbPath: options.dbPath,
      fsyncLockFallback: options.config.physical?.allowFsyncLock,
      allowPrimaryFsyncLock: options.config.physical?.allowPrimaryFsyncLock,
      fsyncLockTimeoutMs: options.config.physical?.fsyncLockTimeoutMs,
    });
    return res.manifest;
  }

  async restore(options: RestoreEngineOptions): Promise<unknown> {
    if (
      options.nsInclude ||
      options.nsExclude ||
      options.database ||
      options.oplogReplay !== undefined ||
      options.oplogLimit
    ) {
      throw new Error(
        "Physical restore does not support namespace filters, --database, --oplog-replay, or --oplog-limit",
      );
    }
    if (!options.dbPath) {
      throw new Error("Target WiredTiger dbPath (--db-path) is required for physical restore");
    }
    return restorePhysicalBackup({
      config: options.config,
      storage: options.storage,
      logger: options.logger,
      targetDbPath: options.dbPath,
      ref: options.ref || "latest",
      pitr: options.pitr,
      signal: options.signal,
    });
  }
}

/**
 * Sharded cluster backup engine (Experimental).
 * Coordinates multi-shard cluster backups across mongos, config servers, and shard replica sets.
 */
export class ShardedBackupEngine implements BackupEngineStrategy {
  readonly name = "sharded";

  async backup(options: BackupEngineOptions): Promise<BackupManifest> {
    const res = await runShardedClusterBackup({
      config: options.config,
      storage: options.storage,
      logger: options.logger,
      signal: options.signal,
    });
    return res.manifest;
  }

  async restore(options: RestoreEngineOptions): Promise<unknown> {
    if (
      options.dbPath ||
      options.nsInclude ||
      options.nsExclude ||
      (options.database && !isClusterScope(options.database)) ||
      options.oplogReplay !== undefined ||
      options.oplogLimit
    ) {
      throw new Error(
        "Sharded cluster restore does not support --db-path, selective namespace filters, --oplog-replay, or --oplog-limit",
      );
    }
    return restoreShardedCluster({
      config: options.config,
      storage: options.storage,
      logger: options.logger,
      ref: options.ref || "latest",
      pitr: options.pitr,
      signal: options.signal,
      drop: options.drop,
      preVerify: options.preVerify,
    });
  }
}

/**
 * Registry of available backup and restore engines.
 */
export class EngineRegistry {
  private static readonly engines = new Map<string, BackupEngineStrategy>([
    ["logical", new LogicalBackupEngine()],
    ["physical", new PhysicalBackupEngine()],
    ["sharded", new ShardedBackupEngine()],
  ]);

  static register(engine: BackupEngineStrategy): void {
    this.engines.set(engine.name, engine);
  }

  static get(name: string): BackupEngineStrategy {
    const engine = this.engines.get(name);
    if (!engine) {
      throw new Error(
        `Unknown backup engine: "${name}". Supported engines: ${[...this.engines.keys()].join(", ")}`,
      );
    }
    return engine;
  }

  /**
   * Resolves the appropriate engine based on CLI parameters and configuration flags.
   */
  static resolveForBackup(params: {
    engine?: string;
    sharding?: boolean;
    config: AppConfig;
  }): BackupEngineStrategy {
    if (params.engine) {
      const engine = this.get(params.engine);
      if ((params.sharding || params.config.sharding?.enabled) && params.engine !== "sharded") {
        throw new Error(
          `Conflicting backup options: explicit engine "${params.engine}" cannot be used with sharding enabled`,
        );
      }
      return engine;
    }
    if (params.sharding || params.config.sharding?.enabled) {
      return this.get("sharded");
    }
    return this.get(params.config.engine || "logical");
  }

  /**
   * Resolves the appropriate engine based on CLI restore parameters.
   */
  static resolveForRestore(params: {
    engine?: string;
    key?: string;
    dbPath?: string;
    sharding?: boolean;
    config: AppConfig;
  }): BackupEngineStrategy {
    const key = params.key || "";
    const keyIsPhysical = /\.physical\.tar\.gz(\.enc)?$/.test(key);
    const keyIsSharded = /\.cluster(\.enc)?$/.test(key);
    const keyIsLogical = /\.archive\.gz(\.enc)?$/.test(key);

    if (params.engine) {
      const engine = this.get(params.engine);
      if (params.engine !== "physical" && (params.dbPath || keyIsPhysical)) {
        throw new Error(
          `Conflicting restore options: explicit engine "${params.engine}" is incompatible with physical restore target/archive`,
        );
      }
      if (params.engine !== "sharded" && (params.sharding || keyIsSharded)) {
        throw new Error(
          `Conflicting restore options: explicit engine "${params.engine}" is incompatible with sharded restore target/archive`,
        );
      }
      if (params.engine !== "logical" && keyIsLogical) {
        throw new Error(
          `Conflicting restore options: explicit engine "${params.engine}" is incompatible with logical archive key "${key}"`,
        );
      }
      return engine;
    }

    if (params.dbPath && params.sharding) {
      throw new Error("Conflicting restore options: --db-path and --sharding cannot both be set");
    }
    if (params.dbPath && (keyIsSharded || keyIsLogical)) {
      throw new Error(
        `Conflicting restore options: --db-path is incompatible with non-physical archive key "${key}"`,
      );
    }
    if (params.sharding && (keyIsPhysical || keyIsLogical)) {
      throw new Error(
        `Conflicting restore options: --sharding is incompatible with non-sharded archive key "${key}"`,
      );
    }

    if (keyIsPhysical || params.dbPath) {
      return this.get("physical");
    }
    if (keyIsSharded || params.sharding) {
      return this.get("sharded");
    }
    if (keyIsLogical) {
      return this.get("logical");
    }
    if (params.config.sharding?.enabled) {
      return this.get("sharded");
    }
    return this.get(params.config.engine || "logical");
  }
}
