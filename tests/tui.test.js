import { describe, expect, it, vi } from "vitest";
import { CommandRegistry } from "../src/commands/index.js";
import { TuiCommand, renderDashboardFrame } from "../src/commands/tui.js";
import {
  badge,
  createSpinner,
  padVisibleEnd,
  padVisibleStart,
  renderBanner,
  renderBar,
  renderErrorBox,
  renderKeyValue,
  renderPanel,
  renderTable,
  stripAnsi,
  style,
  supportsColor,
  truncateVisible,
  visibleLength,
} from "../src/tui.js";

describe("CLI TUI design system (tui.ts)", () => {
  it("respects NO_COLOR and FORCE_COLOR and computes ANSI-stripped widths accurately", () => {
    const prevNo = process.env.NO_COLOR;
    const prevForce = process.env.FORCE_COLOR;
    try {
      process.env.NO_COLOR = "1";
      delete process.env.FORCE_COLOR;
      expect(supportsColor({ isTTY: true })).toBe(false);

      delete process.env.NO_COLOR;
      process.env.FORCE_COLOR = "1";
      expect(supportsColor({ isTTY: false })).toBe(true);
    } finally {
      if (prevNo === undefined) delete process.env.NO_COLOR;
      else process.env.NO_COLOR = prevNo;
      if (prevForce === undefined) delete process.env.FORCE_COLOR;
      else process.env.FORCE_COLOR = prevForce;
    }

    const colored = style.green("hello", true);
    expect(stripAnsi(colored)).toBe("hello");
    expect(visibleLength(colored)).toBe(5);
    expect(visibleLength(padVisibleEnd(colored, 10))).toBe(10);
    expect(visibleLength(padVisibleStart(colored, 10))).toBe(10);
    expect(truncateVisible("abcdefghij", 5)).toBe("abcd…");
  });

  it("renders banners, badges, panels, tables, progress bars, and error boxes cleanly", () => {
    const banner = stripAnsi(renderBanner("Diagnostics", true));
    expect(banner).toContain("MongoVault");
    expect(banner).toContain("Diagnostics");

    expect(stripAnsi(badge("PASS", "success", true))).toBe("✓ PASS");
    expect(stripAnsi(badge("FAIL", "error", true))).toBe("✗ FAIL");
    expect(stripAnsi(badge("WARN", "warn", true))).toBe("⚠ WARN");

    const kv = renderKeyValue(
      [
        { label: "Engine", value: "LOGICAL", tone: "info" },
        { label: "Size", value: "42.0 MB" },
      ],
      { indent: 1, color: false },
    );
    const panel = renderPanel(kv, {
      title: "Backup Summary",
      subtitle: "ok",
      tone: "success",
      color: false,
    });
    expect(panel).toContain("╭─");
    expect(panel).toContain("Backup Summary");
    expect(panel).toContain("Engine:");
    expect(panel).toContain("42.0 MB");
    expect(panel).toContain("╰─");

    const tbl = renderTable(
      [
        { header: "Key", maxWidth: 12 },
        { header: "Count", align: "right" },
      ],
      [
        ["very-long-backup-key.gz", "120"],
        ["short.gz", "5"],
      ],
      { color: false },
    );
    expect(tbl).toContain("Key");
    expect(tbl).toContain("very-long-b…");
    expect(tbl).toContain("120");

    expect(stripAnsi(renderBar(0.5, 10, false))).toBe("█████░░░░░");

    const errBox = stripAnsi(
      renderErrorBox("Backup Failed", new Error("disk full"), { showStack: true, color: false }),
    );
    expect(errBox).toContain("Backup Failed");
    expect(errBox).toContain("disk full");
  });

  it("handles TTY spinner updates, succeed, fail, and non-TTY no-op cleanly", () => {
    const writes = [];
    const fakeStream = {
      isTTY: true,
      write: (chunk) => {
        writes.push(chunk);
        return true;
      },
    };
    const spinner = createSpinner("Uploading", { enabled: true, stream: fakeStream });
    spinner.update("Verifying");
    spinner.succeed("Done");
    expect(writes.some((w) => w.includes("Uploading"))).toBe(true);
    expect(writes.some((w) => w.includes("Verifying"))).toBe(true);
    expect(writes.some((w) => stripAnsi(w).includes("✓ Done"))).toBe(true);

    const failWrites = [];
    const failSpinner = createSpinner("Restoring", {
      enabled: true,
      stream: { isTTY: true, write: (c) => failWrites.push(c) },
    });
    failSpinner.fail("Aborted");
    expect(failWrites.some((w) => stripAnsi(w).includes("✗ Aborted"))).toBe(true);

    // Disabled spinner should not write anything
    const silentWrites = [];
    const silentSpinner = createSpinner("Silent", {
      enabled: false,
      stream: { isTTY: false, write: (c) => silentWrites.push(c) },
    });
    silentSpinner.update("Still silent");
    silentSpinner.succeed("Finished");
    expect(silentWrites).toEqual([]);
  });
});

describe("Interactive TUI Control Center (commands/tui.ts)", () => {
  const baseState = {
    activeTab: "overview",
    status: {
      timestamp: "2026-11-24T12:00:00Z",
      cluster: {
        connected: true,
        setName: "rs0",
        role: "primary",
        version: "9.0.8",
        me: "mongo1:27017",
        oplog: { available: true, oldestTs: "100:1", newestTs: "500:1", retentionHours: 24 },
      },
      storage: {
        type: "s3",
        destination: "s3://backups/mongodb",
        prefix: "mongodb",
        accessible: true,
        totalBackups: 2,
        totalSizeBytes: 2048,
      },
      pitr: {
        enabled: true,
        activeArchiver: true,
        flushIntervalSeconds: 60,
        slicesCount: 3,
        earliestTs: "100:1",
        latestTs: "500:1",
        timelineContinuous: true,
      },
      operation: { running: false },
      recentBackups: [
        {
          key: "mongodb/app/2026-11-24/b1.archive.gz.enc",
          createdAt: "2026-11-24T10:00:00Z",
          size: 1024,
          encrypted: true,
          complete: true,
        },
      ],
      engine: "logical",
    },
    backups: [
      {
        key: "mongodb/app/2026-11-24/b1.archive.gz.enc",
        createdAt: "2026-11-24T10:00:00Z",
        size: 1024,
        encrypted: true,
        complete: true,
      },
    ],
    slices: [
      {
        key: "mongodb/app_pitr/2026-11-24/s1.oplog.bson.gz.enc",
        startTs: { t: 100, i: 1 },
        endTs: { t: 200, i: 1 },
        size: 512,
        complete: true,
      },
    ],
    checks: {
      allPassed: true,
      checks: [
        { name: "MongoDB Connectivity", passed: true, message: "Connected" },
        { name: "Client-Side Encryption", passed: true, message: "AES-256-GCM verified" },
      ],
    },
    selectedBackupIdx: 0,
    selectedSliceIdx: 0,
    inspectedManifest: {
      key: "mongodb/app/2026-11-24/b1.archive.gz.enc",
      manifest: {
        key: "mongodb/app/2026-11-24/b1.archive.gz.enc",
        engine: "logical",
        database: "*",
        durationMs: 2500,
        sha256: "deadbeef",
        collections: { "shop.users": 42 },
        oplogWindow: { startTs: "100:1", endTs: "110:1" },
        schema: {
          collections: { "shop.users": { indexes: [{ name: "_id_", key: { _id: 1 } }] } },
          views: [],
        },
      },
    },
    busyMessage: null,
    toast: { text: "Telemetry refreshed", tone: "success" },
    lastRefreshedAt: new Date("2026-11-24T12:00:00Z"),
    timezone: "UTC",
  };

  it("renders all 4 dashboard tabs cleanly without ANSI leakage when color is disabled", () => {
    expect(CommandRegistry.has("tui")).toBe(true);
    expect(CommandRegistry.has("ui")).toBe(true);

    const overview = renderDashboardFrame(
      { ...baseState, activeTab: "overview" },
      { color: false },
    );
    expect(overview).toContain("Control Center");
    expect(overview).toContain("Replica Set [rs0]");
    expect(overview).toContain("Recent Backups");

    const backups = renderDashboardFrame({ ...baseState, activeTab: "backups" }, { color: false });
    expect(backups).toContain("Backup Archives");
    expect(backups).toContain("Selected Backup Details");
    expect(backups).toContain("deadbeef");

    const pitr = renderDashboardFrame({ ...baseState, activeTab: "pitr" }, { color: false });
    expect(pitr).toContain("PITR Oplog Slices");
    expect(pitr).toContain("100:1");

    const checks = renderDashboardFrame({ ...baseState, activeTab: "checks" }, { color: false });
    expect(checks).toContain("System Preflight Verification");
    expect(checks).toContain("2/2 passed");
  });

  it("falls back cleanly to JSON output when --json is passed to TuiCommand", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    try {
      const cmd = new TuiCommand();
      await cmd.execute({
        config: {
          workDir: "/tmp/mv-tui-test",
          mongo: {
            uri: "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200&connectTimeoutMS=200",
            database: "app",
            oplog: false,
            excludeCollections: [],
          },
          pitr: { enabled: false, flushIntervalSeconds: 60, maxSliceMb: 16 },
          storage: { type: "filesystem", bucket: "/tmp", prefix: "mv" },
          schedule: { timezone: "UTC" },
        },
        storage: {
          bucket: "/tmp",
          async *list() {},
          async getJson() {
            return null;
          },
        },
        logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
        notifier: {},
        metrics: {},
        values: { json: true },
      });
      expect(logSpy).toHaveBeenCalledTimes(1);
      const parsed = JSON.parse(logSpy.mock.calls[0][0]);
      expect(parsed).toHaveProperty("cluster");
      expect(parsed).toHaveProperty("storage");
    } finally {
      logSpy.mockRestore();
    }
  });
});
