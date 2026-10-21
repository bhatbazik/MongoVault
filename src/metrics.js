import http from "node:http";
import client from "prom-client";

export function createMetricsRegistry() {
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
    recordBackupSuccess(database, sizeBytes, durationMs) {
      backupRunsTotal.inc({ status: "success", database });
      backupDurationSeconds.observe({ database }, durationMs / 1000);
      backupSizeBytes.set({ database }, sizeBytes);
      backupLastSuccessTimestamp.set({ database }, Math.floor(Date.now() / 1000));
    },

    recordBackupFailure(database) {
      backupRunsTotal.inc({ status: "failure", database });
    },

    recordDrillSuccess() {
      drillRunsTotal.inc({ status: "success" });
      drillLastSuccessTimestamp.set(Math.floor(Date.now() / 1000));
    },

    recordDrillFailure() {
      drillRunsTotal.inc({ status: "failure" });
    },

    async getMetrics() {
      return register.metrics();
    },

    contentType: register.contentType,
  };
}

export function startHttpServer({ port, metrics, getState, logger }) {
  if (!port || port <= 0) return null;

  const server = http.createServer(async (req, res) => {
    try {
      if (req.method === "GET" && (req.url === "/healthz" || req.url === "/ready")) {
        const state = getState ? getState() : { status: "ok" };
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify(state));
      }

      if (req.method === "GET" && req.url === "/metrics") {
        const data = await metrics.getMetrics();
        res.writeHead(200, { "Content-Type": metrics.contentType });
        return res.end(data);
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
    close() {
      return new Promise((resolve) => server.close(resolve));
    },
  };
}
