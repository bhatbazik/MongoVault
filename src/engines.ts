import { runBackup } from "./backup.js";
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
    });
    return res.manifest;
  }

  async restore(options: RestoreEngineOptions): Promise<unknown> {
    if (!options.dbPath) {
      throw new Error("Target WiredTiger dbPath (--db-path) is required for physical restore");
    }
    return restorePhysicalBackup({
      config: options.config,
      storage: options.storage,
      logger: options.logger,
      targetDbPath: options.dbPath,
      ref: options.ref || "latest",
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
    return restoreShardedCluster({
      config: options.config,
      storage: options.storage,
      logger: options.logger,
      ref: options.ref || "latest",
      signal: options.signal,
      drop: options.drop,
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
      return this.get(params.engine);
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
    if (
      params.dbPath ||
      params.engine === "physical" ||
      /\.physical\.tar\.gz(\.enc)?$/.test(params.key || "")
    ) {
      return this.get("physical");
    }
    if (
      params.sharding ||
      params.engine === "sharded" ||
      /\.cluster(\.enc)?$/.test(params.key || "") ||
      params.config.sharding?.enabled
    ) {
      return this.get("sharded");
    }
    return this.get(params.engine || params.config.engine || "logical");
  }
}
