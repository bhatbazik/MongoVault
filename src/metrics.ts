import http from "node:http";
import client from "prom-client";
import type { HttpServerInstance, HttpServerOptions, MetricsRegistry } from "./types/metrics.js";

export function createMetricsRegistry(): MetricsRegistry {
  const register = new client.Registry();
  client.collectDefaultMetrics({ register, prefix: "mongodb_backup_" });

  const backupRunsTotal = new client.Counter({
    name: "mongodb_backup_runs_total",
    help: "Total count of backup executions",
    labelNames: ["status", "database"],
    registers: [register],
  });

  const backupDurationSeconds = new client.Histogram({
    name: "mongodb_backup_duration_seconds",
    help: "Backup run duration in seconds",
    labelNames: ["database"],
    buckets: [1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600],
    registers: [register],
  });

  const backupSizeBytes = new client.Gauge({
    name: "mongodb_backup_size_bytes",
    help: "Size of the last generated backup archive in bytes",
    labelNames: ["database"],
    registers: [register],
  });

  const backupLastSuccessTimestamp = new client.Gauge({
    name: "mongodb_backup_last_success_timestamp_seconds",
    help: "Unix timestamp in seconds of the most recent successful backup",
    labelNames: ["database"],
    registers: [register],
  });

  const drillRunsTotal = new client.Counter({
    name: "mongodb_backup_drill_runs_total",
    help: "Total count of restore drill runs",
    labelNames: ["status"],
    registers: [register],
  });

  const drillLastSuccessTimestamp = new client.Gauge({
    name: "mongodb_backup_drill_last_success_timestamp_seconds",
    help: "Unix timestamp in seconds of the most recent successful restore drill",
    registers: [register],
  });

  const pitrSlicesTotal = new client.Counter({
    name: "mongodb_backup_pitr_slices_total",
    help: "Total count of PITR oplog slice capture executions",
    labelNames: ["status"],
    registers: [register],
  });

  const pitrSliceEntriesTotal = new client.Counter({
    name: "mongodb_backup_pitr_slice_entries_total",
    help: "Total number of oplog entries archived into PITR slices",
    registers: [register],
  });

  const pitrSliceBytesTotal = new client.Counter({
    name: "mongodb_backup_pitr_slice_bytes_total",
    help: "Total compressed/encrypted bytes uploaded for PITR slices",
    registers: [register],
  });

  const pitrLastSliceTimestamp = new client.Gauge({
    name: "mongodb_backup_pitr_last_slice_timestamp_seconds",
    help: "Oplog Unix timestamp in seconds covered by the most recent archived PITR slice",
    registers: [register],
  });

  const pitrLagSeconds = new client.Gauge({
    name: "mongodb_backup_pitr_lag_seconds",
    help: "Current PITR archive lag in seconds relative to wall clock",
    registers: [register],
  });

  const pitrGapDetected = new client.Gauge({
    name: "mongodb_backup_pitr_gap_detected",
    help: "Whether a PITR oplog timeline gap has been detected (1 = gap, 0 = continuous)",
    registers: [register],
  });

  return {
    recordBackupSuccess(database: string, sizeBytes: number, durationMs: number): void {
      backupRunsTotal.inc({ status: "success", database });
      backupDurationSeconds.observe({ database }, durationMs / 1000);
      backupSizeBytes.set({ database }, sizeBytes);
      backupLastSuccessTimestamp.set({ database }, Math.floor(Date.now() / 1000));
    },

    recordBackupFailure(database: string): void {
      backupRunsTotal.inc({ status: "failure", database });
    },

    recordDrillSuccess(): void {
      drillRunsTotal.inc({ status: "success" });
      drillLastSuccessTimestamp.set(Math.floor(Date.now() / 1000));
    },

    recordDrillFailure(): void {
      drillRunsTotal.inc({ status: "failure" });
    },

    recordPitrSliceSuccess(
      entries: number,
      sizeBytes: number,
      endEpochSeconds: number,
      gapDetected = false,
    ): void {
      pitrSlicesTotal.inc({ status: "success" });
      if (entries > 0) pitrSliceEntriesTotal.inc(entries);
      if (sizeBytes > 0) pitrSliceBytesTotal.inc(sizeBytes);
      pitrLastSliceTimestamp.set(endEpochSeconds);
      pitrLagSeconds.set(Math.max(0, Math.floor(Date.now() / 1000) - endEpochSeconds));
      pitrGapDetected.set(gapDetected ? 1 : 0);
    },

    recordPitrSliceFailure(gapDetected = false): void {
      pitrSlicesTotal.inc({ status: "failure" });
      if (gapDetected) {
        pitrGapDetected.set(1);
      }
    },

    recordPitrCoverage(coverageEndEpochSeconds: number, gapDetected = false): void {
      pitrLastSliceTimestamp.set(coverageEndEpochSeconds);
      pitrLagSeconds.set(Math.max(0, Math.floor(Date.now() / 1000) - coverageEndEpochSeconds));
      pitrGapDetected.set(gapDetected ? 1 : 0);
    },

    async getMetrics(): Promise<string> {
      return register.metrics();
    },

    contentType: register.contentType,
  };
}

export function startHttpServer({
  port,
  metrics,
  getState,
  logger,
}: HttpServerOptions): HttpServerInstance | null {
  if (!port || port <= 0) return null;

  const server = http.createServer(async (req, res) => {
    try {
      const pathname = new URL(req.url || "/", "http://localhost").pathname;
      if (req.method === "GET" && (pathname === "/healthz" || pathname === "/ready")) {
        const state = getState ? getState() : { status: "ok" };
        const isHealthy =
          state.status !== "error" && !("isShuttingDown" in state && Boolean(state.isShuttingDown));
        res.writeHead(isHealthy ? 200 : 503, { "Content-Type": "application/json" });
        res.end(JSON.stringify(state));
        return;
      }

      if (req.method === "GET" && pathname === "/metrics") {
        const data = await metrics.getMetrics();
        res.writeHead(200, { "Content-Type": metrics.contentType });
        res.end(data);
        return;
      }

      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Not Found");
    } catch (error) {
      logger.error({ err: error }, "HTTP request handling error");
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "text/plain" });
      }
      if (!res.writableEnded) {
        res.end("Internal Server Error");
      }
    }
  });

  server.on("error", (error) => {
    logger.error({ err: error, port }, "HTTP metrics and health server error");
  });

  server.listen(port, () => {
    logger.info({ port }, "HTTP metrics and health server listening");
  });

  return {
    close(): Promise<void> {
      return new Promise((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections?.();
      });
    },
  };
}
