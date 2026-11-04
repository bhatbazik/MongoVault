import type { AppConfig } from "./config.js";
import type { Logger } from "./logger.js";
import type { MetricsRegistry } from "./metrics.js";
import type { Notifier } from "./notify.js";
import type { StorageClient } from "./storage.js";

export interface SchedulerContext {
  config: AppConfig;
  storage: StorageClient;
  logger: Logger;
  notifier: Notifier;
  metrics: MetricsRegistry;
}

export interface SchedulerState {
  status: string;
  lastRunAt: string | null;
  lastSuccessAt: string | null;
  lastError: string | null;
  lastManifest: unknown;
  lastDrillAt: string | null;
  uptimeSeconds?: number;
  isShuttingDown?: boolean;
}

export interface Scheduler {
  getState: () => SchedulerState;
  triggerBackup: () => Promise<void>;
  triggerDrill: () => Promise<void>;
  start: () => void;
  stop: () => Promise<void>;
}
