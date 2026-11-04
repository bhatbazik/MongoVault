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
      if (req.method === "GET" && (req.url === "/healthz" || req.url === "/ready")) {
        const state = getState ? getState() : { status: "ok" };
        const isHealthy =
          state.status !== "error" && !("isShuttingDown" in state && Boolean(state.isShuttingDown));
        res.writeHead(isHealthy ? 200 : 503, { "Content-Type": "application/json" });
        res.end(JSON.stringify(state));
        return;
      }

      if (req.method === "GET" && req.url === "/metrics") {
        const data = await metrics.getMetrics();
        res.writeHead(200, { "Content-Type": metrics.contentType });
        res.end(data);
        return;
      }

      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Not Found");
    } catch (error) {
      logger.error({ err: error }, "HTTP request handling error");
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end("Internal Server Error");
    }
  });

  server.listen(port, () => {
    logger.info({ port }, "HTTP metrics and health server listening");
  });

  return {
    close(): Promise<void> {
      return new Promise((resolve) => server.close(() => resolve()));
    },
  };
}
