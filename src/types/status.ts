import type { BackupSummary } from "./catalog.js";

export interface ClusterStatus {
  connected: boolean;
  version?: string;
  setName?: string;
  role?: "primary" | "secondary" | "arbiter" | "standalone" | "mongos" | "unknown";
  primaryHost?: string;
  me?: string;
  hosts?: string[];
  readPreference?: string;
  oplog?: {
    available: boolean;
    oldestTs?: string;
    newestTs?: string;
    retentionHours?: number;
  };
  error?: string;
}

export interface StorageStatus {
  type: "s3" | "filesystem";
  destination: string;
  prefix: string;
  accessible: boolean;
  totalBackups: number;
  totalSizeBytes: number;
  latestBackup?: {
    key: string;
    createdAt: string;
    sizeBytes: number;
    encrypted: boolean;
    oplog: boolean;
  };
  error?: string;
}

export interface PitrStatus {
  enabled: boolean;
  activeArchiver: boolean;
  flushIntervalSeconds: number;
  slicesCount: number;
  earliestTs?: string;
  latestTs?: string;
  timelineContinuous: boolean;
  gapWarning?: string;
}

export interface OperationLockStatus {
  running: boolean;
  lockType?: "local" | "distributed" | "hybrid";
  holder?: string;
  acquiredAt?: string;
  expiresAt?: string;
}

export interface SystemStatus {
  timestamp: string;
  cluster: ClusterStatus;
  storage: StorageStatus;
  pitr: PitrStatus;
  operation: OperationLockStatus;
  recentBackups: BackupSummary[];
  engine?: "logical" | "physical" | "snapshot";
  objectLock?: {
    enabled: boolean;
    mode?: string;
    retentionDays?: number;
  };
  sharding?: {
    isSharded: boolean;
    shardsCount?: number;
    shards?: string[];
    balancerEnabled?: boolean;
  };
}
