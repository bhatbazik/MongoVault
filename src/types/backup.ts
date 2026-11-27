import type { BackupManifest } from "./catalog.js";
import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { StorageClient } from "./storage.js";
import type { BackupTools } from "./tools.js";

export interface BackupContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  signal?: AbortSignal;
  tools?: BackupTools;
  /** Optional lock-fencing assertion checked before critical upload/manifest transitions. */
  assertLockValid?: () => void;
}

export type RunBackupResult = BackupManifest;
