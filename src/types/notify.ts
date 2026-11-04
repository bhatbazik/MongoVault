import type { BackupManifest } from "./catalog.js";

export interface EmailOptions {
  subject: string;
  text: string;
}

export interface NotificationFailureContext {
  database?: string;
  [key: string]: unknown;
}

export interface Notifier {
  notifySuccess: (event: string, data: BackupManifest | Record<string, unknown>) => Promise<void>;
  notifyFailure: (
    event: string,
    error: Error,
    context?: NotificationFailureContext,
  ) => Promise<void>;
}
