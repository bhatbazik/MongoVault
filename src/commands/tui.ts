import path from "node:path";
import readline from "node:readline";
import { databasePrefix, listBackups, scopeOf } from "../catalog.js";
import { runPreflightCheck, type PreflightCheckResult } from "../check.js";
import { EngineRegistry } from "../engines.js";
import { formatBytes, formatDate, formatDuration } from "../format.js";
import { acquireHybridLock } from "../lock.js";
import { formatOplogTimestamp, listOplogSlices } from "../oplog.js";
import { describeBackup, verifyBackup } from "../restore.js";
import { formatStatus, getStatus } from "../status.js";
import {
  badge,
  getTerminalWidth,
  padVisibleEnd,
  renderBanner,
  renderKeyValue,
  renderPanel,
  renderTable,
  style,
  type KeyValueRow,
  type TuiTone,
} from "../tui.js";
import type { BackupManifest, BackupSummary } from "../types/catalog.js";
import type { OplogSlice } from "../types/oplog.js";
import type { SystemStatus } from "../types/status.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export type TuiTab = "overview" | "backups" | "pitr" | "checks";

const TAB_ORDER: TuiTab[] = ["overview", "backups", "pitr", "checks"];

export interface TuiDashboardState {
  activeTab: TuiTab;
  status: SystemStatus | null;
  backups: BackupSummary[];
  slices: OplogSlice[];
  checks: PreflightCheckResult | null;
  selectedBackupIdx: number;
  selectedSliceIdx: number;
  inspectedManifest: { key: string; manifest: BackupManifest | null } | null;
  busyMessage: string | null;
  toast: { text: string; tone: TuiTone } | null;
  lastRefreshedAt: Date | null;
  timezone?: string;
}

export function renderDashboardFrame(
  state: TuiDashboardState,
  options: { color?: boolean; width?: number } = {},
): string {
  const c = options.color;
  const width = options.width ?? getTerminalWidth(108, 72);
  const out: string[] = [];

  // 1. Top Banner + Live Pills
  const clusterPill = !state.status
    ? badge("Loading…", "muted", c)
    : state.status.cluster.connected
      ? badge(
          state.status.cluster.setName
            ? `${state.status.cluster.setName} (${(state.status.cluster.role || "node").toUpperCase()})`
            : `MongoDB v${state.status.cluster.version || "?"}`,
          "success",
          c,
        )
      : badge("Cluster Offline", "error", c);

  const pitrPill = !state.status
    ? ""
    : !state.status.pitr.enabled
      ? badge("PITR Off", "muted", c)
      : state.status.pitr.timelineContinuous
        ? badge("PITR Continuous", "success", c)
        : badge("PITR Gap", "warn", c);

  const refreshStr = state.lastRefreshedAt
    ? style.dim(`updated ${formatDate(state.lastRefreshedAt, state.timezone)}`, c)
    : style.dim("initializing…", c);

  out.push(`${renderBanner("Control Center", c)}    ${clusterPill}  ${pitrPill}`);
  out.push(style.dim(`  ${refreshStr}`, c));
  out.push("");

  // 2. Navigation Tabs
  const tabs: Array<{ id: TuiTab; key: string; label: string }> = [
    { id: "overview", key: "1", label: "Overview" },
    { id: "backups", key: "2", label: `Backups (${state.backups.length})` },
    { id: "pitr", key: "3", label: `PITR Slices (${state.slices.length})` },
    { id: "checks", key: "4", label: "Preflight Checks" },
  ];
  const tabBar = tabs
    .map((t) => {
      const text = `[${t.key}] ${t.label}`;
      return t.id === state.activeTab
        ? style.bold(style.brightGreen(`▸ ${text}`, c), c)
        : style.dim(`  ${text}`, c);
    })
    .join("   ");
  out.push(tabBar);
  out.push(style.gray("─".repeat(width), c));
  out.push("");

  // 3. Active Tab Content
  if (state.activeTab === "overview") {
    if (!state.status) {
      out.push(
        renderPanel([`  ${style.dim("Loading live cluster and storage telemetry…", c)}`], {
          title: "Overview",
          width,
          color: c,
        }),
      );
    } else {
      const s = state.status;
      const summaryRows: KeyValueRow[] = [
        {
          label: "Cluster",
          value: s.cluster.connected
            ? `${s.cluster.setName ? `Replica Set [${s.cluster.setName}]` : s.cluster.role === "mongos" ? "Sharded Cluster (mongos)" : "Standalone"} · MongoDB v${s.cluster.version}`
            : `FAILED (${s.cluster.error || "unreachable"})`,
          tone: s.cluster.connected ? "success" : "error",
        },
        ...(s.cluster.oplog?.available
          ? [
              {
                label: "Oplog Window",
                value: `${s.cluster.oplog.oldestTs} -> ${s.cluster.oplog.newestTs} (${s.cluster.oplog.retentionHours}h)`,
              },
            ]
          : []),
        {
          label: "Storage",
          value: `${s.storage.type.toUpperCase()} · ${s.storage.destination} · ${s.storage.totalBackups} backups (${formatBytes(s.storage.totalSizeBytes)})`,
          tone: s.storage.accessible ? "info" : "error",
        },
        {
          label: "PITR",
          value: s.pitr.enabled
            ? `${s.pitr.slicesCount} slices (${s.pitr.flushIntervalSeconds}s flush) · ${s.pitr.timelineContinuous ? "continuous" : `GAP: ${s.pitr.gapWarning}`}`
            : "Disabled",
          tone: !s.pitr.enabled ? "muted" : s.pitr.timelineContinuous ? "success" : "warn",
        },
        {
          label: "Active Lock",
          value: s.operation.running
            ? `ACTIVE (${s.operation.lockType}, holder: ${s.operation.holder || "unknown"})`
            : "Idle",
          tone: s.operation.running ? "info" : "muted",
        },
        {
          label: "Engine / WORM",
          value: `${(s.engine || "logical").toUpperCase()} · Object Lock: ${s.objectLock?.enabled ? `${s.objectLock.mode} (${s.objectLock.retentionDays}d)` : "Off"}`,
        },
      ];
      out.push(
        renderPanel(renderKeyValue(summaryRows, { indent: 1, color: c }), {
          title: "Live Cluster & Backup Overview",
          tone: s.cluster.connected && s.storage.accessible ? "brand" : "error",
          width,
          color: c,
        }),
      );

      if (s.recentBackups && s.recentBackups.length > 0) {
        out.push("");
        const tbl = renderTable(
          [
            { header: "Recent Backup", maxWidth: Math.max(36, width - 48) },
            { header: "Created" },
            { header: "Size", align: "right" },
            { header: "Crypto" },
          ],
          s.recentBackups.map((b) => [
            b.key,
            formatDate(b.createdAt, state.timezone),
            formatBytes(b.size || 0),
            b.encrypted ? badge("AES-256", "success", c) : style.dim("plain", c),
          ]),
          { indent: 1, color: c },
        );
        out.push(
          renderPanel(tbl.split("\n"), {
            title: "Recent Backups",
            subtitle: `latest ${s.recentBackups.length}`,
            width,
            color: c,
          }),
        );
      }
    }
  } else if (state.activeTab === "backups") {
    if (state.backups.length === 0) {
      out.push(
        renderPanel([`  ${style.dim("No backups found in configured storage prefix.", c)}`], {
          title: "Backups",
          tone: "muted",
          width,
          color: c,
        }),
      );
    } else {
      const pageSize = 12;
      const start = Math.max(
        0,
        Math.min(
          state.selectedBackupIdx - Math.floor(pageSize / 2),
          state.backups.length - pageSize,
        ),
      );
      const visible = state.backups.slice(start, start + pageSize);
      const tbl = renderTable(
        [
          { header: " ", maxWidth: 2 },
          { header: "Backup Key", maxWidth: Math.max(34, width - 54) },
          { header: "Created" },
          { header: "Size", align: "right" },
          { header: "Crypto" },
          { header: "Status" },
        ],
        visible.map((b, i) => {
          const absIdx = start + i;
          const selected = absIdx === state.selectedBackupIdx;
          const marker = selected ? style.brightGreen("▸", c) : " ";
          const keyText = selected ? style.bold(style.cyan(b.key, c), c) : b.key;
          return [
            marker,
            keyText,
            formatDate(b.createdAt, state.timezone),
            formatBytes(b.size),
            b.encrypted ? badge("AES-256", "success", c) : style.dim("plain", c),
            b.complete ? badge("complete", "success", c) : badge("unverified", "warn", c),
          ];
        }),
        { indent: 1, color: c },
      );
      out.push(
        renderPanel(tbl.split("\n"), {
          title: "Backup Archives",
          subtitle: `${state.selectedBackupIdx + 1}/${state.backups.length} · ↑/↓ select · Enter inspect · v verify`,
          tone: "info",
          width,
          color: c,
        }),
      );

      if (state.inspectedManifest) {
        const { key, manifest } = state.inspectedManifest;
        const detailLines = manifest
          ? renderKeyValue(
              [
                { label: "Key", value: manifest.key },
                {
                  label: "Engine / DB",
                  value: `${(manifest.engine || "logical").toUpperCase()} · ${manifest.database} · ${formatDuration(manifest.durationMs)}`,
                },
                { label: "SHA-256", value: manifest.sha256 },
                {
                  label: "Collections",
                  value: `${Object.keys(manifest.collections || {}).length} collections (${Object.values(
                    manifest.collections || {},
                  )
                    .reduce((s, n) => s + n, 0)
                    .toLocaleString()} docs)`,
                },
                ...(manifest.oplogWindow
                  ? [
                      {
                        label: "Oplog Window",
                        value: `${manifest.oplogWindow.startTs} -> ${manifest.oplogWindow.endTs}`,
                        tone: "info" as const,
                      },
                    ]
                  : []),
                ...(manifest.schema
                  ? [
                      {
                        label: "Schema",
                        value: `${Object.values(manifest.schema.collections || {}).reduce(
                          (acc, col) => acc + (col.indexes?.length ?? 0),
                          0,
                        )} indexes · ${Object.values(manifest.schema.collections || {}).filter((col) => Boolean(col.options?.validator)).length} validators · ${manifest.schema.views?.length ?? 0} views`,
                        tone: "success" as const,
                      },
                    ]
                  : []),
              ],
              { indent: 1, color: c },
            )
          : [`  ${badge(`No manifest found for ${key}`, "warn", c)}`];
        out.push("");
        out.push(
          renderPanel(detailLines, {
            title: "Selected Backup Details",
            tone: "brand",
            width,
            color: c,
          }),
        );
      }
    }
  } else if (state.activeTab === "pitr") {
    if (state.slices.length === 0) {
      out.push(
        renderPanel([`  ${style.dim("No PITR oplog slices archived in configured prefix.", c)}`], {
          title: "PITR Oplog Timeline",
          tone: "muted",
          width,
          color: c,
        }),
      );
    } else {
      const pageSize = 14;
      const start = Math.max(
        0,
        Math.min(state.selectedSliceIdx - Math.floor(pageSize / 2), state.slices.length - pageSize),
      );
      const visible = state.slices.slice(start, start + pageSize);
      const tbl = renderTable(
        [
          { header: " ", maxWidth: 2 },
          { header: "Slice Key", maxWidth: Math.max(32, width - 50) },
          { header: "Start TS" },
          { header: "End TS" },
          { header: "Size", align: "right" },
        ],
        visible.map((s, i) => {
          const absIdx = start + i;
          const selected = absIdx === state.selectedSliceIdx;
          return [
            selected ? style.brightGreen("▸", c) : " ",
            selected ? style.bold(style.cyan(s.key, c), c) : s.key,
            formatOplogTimestamp(s.startTs),
            formatOplogTimestamp(s.endTs),
            formatBytes(s.size),
          ];
        }),
        { indent: 1, color: c },
      );
      const totalSliceBytes = state.slices.reduce((acc, s) => acc + (s.size || 0), 0);
      const pitrTone: TuiTone =
        state.status?.pitr.timelineContinuous === false ? "warn" : "success";
      const lines = [
        ...tbl.split("\n"),
        "",
        ` ${style.dim(`Total: ${state.slices.length} slices · ${formatBytes(totalSliceBytes)}`)}`,
      ];
      if (state.status?.pitr.gapWarning) {
        lines.push(` ${badge(`Gap Warning: ${state.status.pitr.gapWarning}`, "warn", c)}`);
      }
      out.push(
        renderPanel(lines, {
          title: "PITR Oplog Slices",
          subtitle: `${state.selectedSliceIdx + 1}/${state.slices.length}`,
          tone: pitrTone,
          width,
          color: c,
        }),
      );
    }
  } else if (state.activeTab === "checks") {
    if (!state.checks) {
      out.push(
        renderPanel(
          [`  ${style.dim("Press [c] or [r] to run live system preflight checks.", c)}`],
          {
            title: "Preflight Checks",
            tone: "muted",
            width,
            color: c,
          },
        ),
      );
    } else {
      const passedCount = state.checks.checks.filter((ch) => ch.passed).length;
      const nameWidth = state.checks.checks.reduce((m, ch) => Math.max(m, ch.name.length), 24);
      const lines = state.checks.checks.map((ch) => {
        const isWarn = ch.passed && ch.message.includes("WARNING:");
        const b = !ch.passed
          ? badge("FAIL", "error", c)
          : isWarn
            ? badge("WARN", "warn", c)
            : badge("PASS", "success", c);
        return ` ${padVisibleEnd(b, 8)} ${padVisibleEnd(style.bold(ch.name, c), nameWidth)}  ${ch.message}`;
      });
      out.push(
        renderPanel(lines, {
          title: "System Preflight Verification",
          subtitle: `${passedCount}/${state.checks.checks.length} passed`,
          tone: state.checks.allPassed ? "success" : "error",
          width,
          color: c,
        }),
      );
    }
  }

  // 4. Busy / Toast Bar + Controls Footer
  out.push("");
  if (state.busyMessage) {
    out.push(`  ${badge(state.busyMessage, "info", c)}`);
  } else if (state.toast) {
    out.push(`  ${badge(state.toast.text, state.toast.tone, c)}`);
  }

  out.push(style.gray("─".repeat(width), c));
  out.push(
    style.dim(
      "  [1-4 / Tab] Switch View   [↑/↓] Navigate   [Enter] Inspect   [v] Verify   [b] Backup   [c] Checks   [r] Refresh   [q] Quit",
      c,
    ),
  );

  return out.join("\n");
}

export class TuiCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, values } = ctx;
    const scope = scopeOf(config);

    // Non-interactive / JSON fallback so pipes & scripts never hang
    if (values.json || !process.stdin.isTTY || !process.stdout.isTTY) {
      const status = await getStatus({ config, storage, logger });
      if (values.json) {
        console.log(JSON.stringify(status, null, 2));
      } else {
        console.log(`\n${formatStatus(status)}`);
      }
      return;
    }

    const state: TuiDashboardState = {
      activeTab: "overview",
      status: null,
      backups: [],
      slices: [],
      checks: null,
      selectedBackupIdx: 0,
      selectedSliceIdx: 0,
      inspectedManifest: null,
      busyMessage: "Loading cluster & storage telemetry…",
      toast: null,
      lastRefreshedAt: null,
      timezone: config.schedule.timezone,
    };

    let closed = false;
    let operationRunning = false;

    const draw = () => {
      if (closed) return;
      process.stdout.write("\x1b[H\x1b[2J");
      process.stdout.write(`${renderDashboardFrame(state, { color: true })}\n`);
    };

    const refreshAll = async (includeChecks = false) => {
      if (operationRunning || closed) return;
      operationRunning = true;
      state.busyMessage = includeChecks
        ? "Refreshing telemetry & running preflight checks…"
        : "Refreshing telemetry…";
      draw();
      try {
        let storageError: string | null = null;
        const [status, backups, slices, checks] = await Promise.all([
          getStatus({ config, storage, logger }),
          listBackups(storage, scope).catch((err) => {
            storageError = err instanceof Error ? err.message : String(err);
            return [] as BackupSummary[];
          }),
          listOplogSlices(storage, scope).catch((err) => {
            storageError = storageError ?? (err instanceof Error ? err.message : String(err));
            return [] as OplogSlice[];
          }),
          includeChecks ? runPreflightCheck({ config, storage }) : Promise.resolve(state.checks),
        ]);
        state.status = status;
        state.backups = backups;
        state.slices = slices;
        state.checks = checks;
        state.selectedBackupIdx = Math.min(
          state.selectedBackupIdx,
          Math.max(0, backups.length - 1),
        );
        state.selectedSliceIdx = Math.min(state.selectedSliceIdx, Math.max(0, slices.length - 1));
        state.lastRefreshedAt = new Date();
        state.toast = storageError
          ? { text: `Storage warning: ${storageError}`, tone: "warn" }
          : { text: "Telemetry refreshed", tone: "success" };
      } catch (err) {
        state.toast = {
          text: `Refresh failed: ${err instanceof Error ? err.message : String(err)}`,
          tone: "error",
        };
      } finally {
        state.busyMessage = null;
        operationRunning = false;
        draw();
      }
    };

    const inspectSelected = async () => {
      const selected = state.backups[state.selectedBackupIdx];
      if (!selected || operationRunning) return;
      operationRunning = true;
      state.busyMessage = `Loading manifest for ${selected.key}…`;
      draw();
      try {
        const res = await describeBackup({ config, storage, logger }, selected.key);
        state.inspectedManifest = { key: res.key, manifest: res.manifest };
        state.toast = { text: `Loaded metadata for ${selected.key}`, tone: "info" };
      } catch (err) {
        state.toast = {
          text: `Inspect failed: ${err instanceof Error ? err.message : String(err)}`,
          tone: "error",
        };
      } finally {
        state.busyMessage = null;
        operationRunning = false;
        draw();
      }
    };

    const verifySelected = async () => {
      const selected = state.backups[state.selectedBackupIdx];
      if (!selected || operationRunning) return;
      operationRunning = true;
      state.busyMessage = `Streaming & verifying SHA-256 for ${selected.key}…`;
      draw();
      try {
        const res = await verifyBackup({ config, storage, logger }, selected.key);
        state.toast = {
          text: `Verified ${res.key} (${formatBytes(res.size)}, SHA-256 OK)`,
          tone: "success",
        };
      } catch (err) {
        state.toast = {
          text: `Verification FAILED: ${err instanceof Error ? err.message : String(err)}`,
          tone: "error",
        };
      } finally {
        state.busyMessage = null;
        operationRunning = false;
        draw();
      }
    };

    let activeBackupController: AbortController | null = null;
    const runManualBackup = async () => {
      if (operationRunning) return;
      operationRunning = true;
      state.busyMessage = `Running ${config.engine || "logical"} backup…`;
      draw();
      const controller = new AbortController();
      activeBackupController = controller;
      let releaseLock: (() => Promise<void>) | null = null;
      try {
        releaseLock = await acquireHybridLock({
          file: path.join(config.workDir, ".backup.lock"),
          storage,
          lockKey: `${databasePrefix(scope)}.locks/backup.lock.json`,
          staleAfterMs: Math.max(config.timeoutMs + 60_000, 120_000),
          logger,
          onLost: (err) => {
            controller.abort(err ?? new Error("Lost backup lock"));
          },
        });
        if (!releaseLock) {
          state.toast = { text: "Backup skipped: another backup lock is held", tone: "warn" };
          return;
        }
        const engine = EngineRegistry.resolveForBackup({ config });
        const manifest = await engine.backup({
          config,
          storage,
          logger,
          signal: controller.signal,
        });
        state.toast = {
          text: `Backup complete: ${manifest.key} (${formatBytes(manifest.size)} in ${formatDuration(manifest.durationMs)})`,
          tone: "success",
        };
      } catch (err) {
        state.toast = {
          text: `Backup FAILED: ${err instanceof Error ? err.message : String(err)}`,
          tone: "error",
        };
      } finally {
        activeBackupController = null;
        await releaseLock?.().catch(() => {});
        state.busyMessage = null;
        operationRunning = false;
        await refreshAll(false);
      }
    };

    // Enter alternate screen buffer & hide cursor
    process.stdout.write("\x1b[?1049h\x1b[?25l");
    readline.emitKeypressEvents(process.stdin);
    const prevRawMode = process.stdin.isTTY ? process.stdin.isRaw : false;
    if (process.stdin.isTTY) {
      process.stdin.setRawMode(true);
    }
    process.stdin.resume();

    await new Promise<void>((resolve) => {
      const autoRefreshTimer = setInterval(() => {
        void refreshAll(false);
      }, 15_000);
      autoRefreshTimer.unref();

      const cleanup = () => {
        if (closed) return;
        closed = true;
        clearInterval(autoRefreshTimer);
        activeBackupController?.abort(new Error("TUI exiting"));
        process.stdin.off("keypress", onKeypress);
        process.stdin.off("error", cleanup);
        process.stdin.off("end", cleanup);
        process.stdout.off("resize", draw);
        process.off("SIGINT", onTerm);
        process.off("SIGTERM", onTerm);
        process.off("SIGHUP", onTerm);
        if (process.stdin.isTTY) {
          process.stdin.setRawMode(Boolean(prevRawMode));
        }
        process.stdin.pause();
        process.stdout.write("\x1b[?25h\x1b[?1049l");
        resolve();
      };
      const onTerm = () => cleanup();

      const onKeypress = (_str: string, key: readline.Key) => {
        if (closed) return;
        if (key.ctrl && key.name === "c") {
          cleanup();
          return;
        }
        if (key.name === "q" || key.name === "escape") {
          cleanup();
          return;
        }

        if (key.name === "1") {
          state.activeTab = "overview";
          draw();
          return;
        }
        if (key.name === "2") {
          state.activeTab = "backups";
          draw();
          return;
        }
        if (key.name === "3") {
          state.activeTab = "pitr";
          draw();
          return;
        }
        if (key.name === "4") {
          state.activeTab = "checks";
          if (!state.checks) {
            void refreshAll(true);
          } else {
            draw();
          }
          return;
        }
        if (key.name === "tab" || key.name === "right") {
          const idx = TAB_ORDER.indexOf(state.activeTab);
          state.activeTab = TAB_ORDER[(idx + 1) % TAB_ORDER.length]!;
          if (state.activeTab === "checks" && !state.checks) {
            void refreshAll(true);
          } else {
            draw();
          }
          return;
        }
        if (key.name === "left") {
          const idx = TAB_ORDER.indexOf(state.activeTab);
          state.activeTab = TAB_ORDER[(idx - 1 + TAB_ORDER.length) % TAB_ORDER.length]!;
          draw();
          return;
        }
        if (key.name === "up" || key.name === "k") {
          if (state.activeTab === "backups" && state.selectedBackupIdx > 0) {
            state.selectedBackupIdx--;
            state.inspectedManifest = null;
            draw();
          } else if (state.activeTab === "pitr" && state.selectedSliceIdx > 0) {
            state.selectedSliceIdx--;
            draw();
          }
          return;
        }
        if (key.name === "down" || key.name === "j") {
          if (state.activeTab === "backups" && state.selectedBackupIdx < state.backups.length - 1) {
            state.selectedBackupIdx++;
            state.inspectedManifest = null;
            draw();
          } else if (
            state.activeTab === "pitr" &&
            state.selectedSliceIdx < state.slices.length - 1
          ) {
            state.selectedSliceIdx++;
            draw();
          }
          return;
        }
        if (key.name === "return" || key.name === "d") {
          if (state.activeTab === "backups") {
            void inspectSelected();
          }
          return;
        }
        if (key.name === "v") {
          if (state.activeTab === "backups") {
            void verifySelected();
          }
          return;
        }
        if (key.name === "b") {
          void runManualBackup();
          return;
        }
        if (key.name === "c") {
          state.activeTab = "checks";
          void refreshAll(true);
          return;
        }
        if (key.name === "r") {
          void refreshAll(state.activeTab === "checks");
        }
      };

      process.stdin.on("keypress", onKeypress);
      process.stdin.on("error", cleanup);
      process.stdin.on("end", cleanup);
      process.stdout.on("resize", draw);
      process.on("SIGINT", onTerm);
      process.on("SIGTERM", onTerm);
      process.on("SIGHUP", onTerm);

      draw();
      void refreshAll(false);
    });
  }
}
