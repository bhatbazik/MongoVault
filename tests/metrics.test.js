import { describe, expect, it, vi } from "vitest";
import { createMetricsRegistry, startHttpServer } from "../src/metrics.js";

function noopLogger() {
  return {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: () => noopLogger(),
  };
}

describe("Metrics Registry & HTTP Health/Metrics Server", () => {
  it("records backup and drill metrics and exports prometheus format", async () => {
    const metrics = createMetricsRegistry();
    metrics.recordBackupSuccess("appdb", 1024 * 1024, 2500);
    metrics.recordBackupFailure("appdb");
    metrics.recordDrillSuccess();
    metrics.recordDrillFailure();
    metrics.recordPitrSliceSuccess(42, 8192, Math.floor(Date.now() / 1000) - 5, false);
    metrics.recordPitrSliceFailure(true);
    metrics.recordPitrCoverage(Math.floor(Date.now() / 1000), false);

    const output = await metrics.getMetrics();
    expect(metrics.contentType).toContain("text/plain");
    expect(output).toContain('mongodb_backup_runs_total{status="success",database="appdb"} 1');
    expect(output).toContain('mongodb_backup_runs_total{status="failure",database="appdb"} 1');
    expect(output).toContain('mongodb_backup_size_bytes{database="appdb"} 1048576');
    expect(output).toContain('mongodb_backup_drill_runs_total{status="success"} 1');
    expect(output).toContain('mongodb_backup_drill_runs_total{status="failure"} 1');
    expect(output).toContain('mongodb_backup_pitr_slices_total{status="success"} 1');
    expect(output).toContain('mongodb_backup_pitr_slices_total{status="failure"} 1');
    expect(output).toContain("mongodb_backup_pitr_slice_entries_total 42");
    expect(output).toContain("mongodb_backup_pitr_slice_bytes_total 8192");
    expect(output).toContain("mongodb_backup_pitr_gap_detected 0");
    expect(output).toContain("mongodb_backup_pitr_lag_seconds");
  });

  it("returns null when port is 0 or falsy", () => {
    const metrics = createMetricsRegistry();
    expect(startHttpServer({ port: 0, metrics, logger: noopLogger() })).toBeNull();
  });

  it("serves /healthz, /ready, /metrics, 404, 503 when shutting down, and 500 on errors", async () => {
    const metrics = createMetricsRegistry();
    const logger = noopLogger();
    let state = { status: "idle", isShuttingDown: false };

    // Use an ephemeral port by finding one or picking a high port
    const port = 39123 + Math.floor(Math.random() * 1000);
    const server = startHttpServer({
      port,
      metrics,
      getState: () => state,
      logger,
    });

    expect(server).not.toBeNull();

    // Give server a moment to bind
    await new Promise((r) => setTimeout(r, 100));

    try {
      const healthRes = await fetch(`http://127.0.0.1:${port}/healthz`);
      expect(healthRes.status).toBe(200);
      expect(await healthRes.json()).toEqual({ status: "idle", isShuttingDown: false });

      state = { status: "idle", isShuttingDown: true };
      const readyRes = await fetch(`http://127.0.0.1:${port}/ready`);
      expect(readyRes.status).toBe(503);

      const metricsRes = await fetch(`http://127.0.0.1:${port}/metrics`);
      expect(metricsRes.status).toBe(200);
      expect(await metricsRes.text()).toContain("mongodb_backup_");

      const notFoundRes = await fetch(`http://127.0.0.1:${port}/unknown`);
      expect(notFoundRes.status).toBe(404);

      // Test 500 error path when getState throws
      state = null;
      const errServerPort = port + 1;
      const errServer = startHttpServer({
        port: errServerPort,
        metrics,
        getState: () => {
          throw new Error("state failure");
        },
        logger,
      });
      await new Promise((r) => setTimeout(r, 80));
      try {
        const errRes = await fetch(`http://127.0.0.1:${errServerPort}/healthz`);
        expect(errRes.status).toBe(500);
      } finally {
        await errServer?.close();
      }
    } finally {
      await server?.close();
    }
  });
});
