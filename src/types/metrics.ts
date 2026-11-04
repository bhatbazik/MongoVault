import type { Logger } from "./logger.js";

export interface DaemonState {
  status: string;
  lastRunAt: string | null;
  lastSuccessAt: string | null;
  lastError: string | null;
  lastManifest: unknown;
  lastDrillAt: string | null;
  uptimeSeconds?: number;
  isShuttingDown?: boolean;
}

export interface MetricsRegistry {
  recordBackupSuccess: (database: string, sizeBytes: number, durationMs: number) => void;
  recordBackupFailure: (database: string) => void;
  recordDrillSuccess: () => void;
  recordDrillFailure: () => void;
  getMetrics: () => Promise<string>;
  contentType: string;
}

export interface HttpServerOptions {
  port: number;
  metrics: MetricsRegistry;
  getState?: () => DaemonState;
  logger: Logger;
}

export interface HttpServerInstance {
  close: () => Promise<void>;
}
