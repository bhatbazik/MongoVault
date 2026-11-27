import path from "node:path";
import { databasePrefix, scopeOf } from "../catalog.js";
import { acquireHybridLock } from "../lock.js";
import { pitrPrefix } from "../oplog.js";
import { renderErrorBox } from "../tui.js";
import type { ReleaseLockFn } from "../types/lock.js";
import type { CliCommandContext } from "./types.js";

export type CommandLockKind = "backup" | "oplog";

export interface CommandLockHandle {
  release: ReleaseLockFn;
  controller: AbortController;
}

/**
 * Acquire the hybrid (local PID + distributed lease) lock that guards mutating CLI commands
 * against racing backups / the PITR archiver. Exits with code 1 when the lock is busy.
 * The returned controller aborts when the lease is lost.
 */
export async function acquireCommandLock(
  ctx: CliCommandContext,
  kind: CommandLockKind,
  title: string,
): Promise<CommandLockHandle> {
  const { config, storage, logger } = ctx;
  const controller = new AbortController();
  const scope = scopeOf(config);
  const file = path.join(config.workDir, kind === "oplog" ? ".oplog.lock" : ".backup.lock");
  const lockKey =
    kind === "oplog"
      ? `${pitrPrefix(scope)}.locks/oplog.lock.json`
      : `${databasePrefix(scope)}.locks/backup.lock.json`;
  const release = await acquireHybridLock({
    file,
    storage,
    lockKey,
    staleAfterMs: Math.max(config.timeoutMs + 60_000, 120_000),
    logger,
    onLost: (err) => {
      logger.error({ err }, `Lost ${kind} lock during CLI command; aborting`);
      controller.abort(err ?? new Error(`Lost ${kind} lock`));
    },
  });
  if (!release) {
    console.error(
      renderErrorBox(
        title,
        new Error(`Another ${kind} operation is in progress (failed to acquire ${kind} lock)`),
      ),
    );
    process.exit(1);
  }
  return { release, controller };
}

/** Abort the controller on SIGINT/SIGTERM so `finally` blocks can release locks. */
export function abortOnTerminationSignal(controller: AbortController): () => void {
  const handler = (sig: NodeJS.Signals) => {
    controller.abort(new Error(`Received ${sig}`));
    process.exitCode = 130;
  };
  const onInt = () => handler("SIGINT");
  const onTerm = () => handler("SIGTERM");
  process.once("SIGINT", onInt);
  process.once("SIGTERM", onTerm);
  return () => {
    process.removeListener("SIGINT", onInt);
    process.removeListener("SIGTERM", onTerm);
  };
}

const READ_PREFERENCES = new Set([
  "primary",
  "primaryPreferred",
  "secondary",
  "secondaryPreferred",
  "nearest",
]);

/** Validate --parallel / --read-preference before touching MongoDB or storage. */
export function validateCommonFlags(values: {
  parallel?: string;
  "read-preference"?: string;
}): string | null {
  if (values.parallel !== undefined) {
    const raw = String(values.parallel).trim();
    const n = Number(raw);
    if (!/^\d+$/.test(raw) || !Number.isInteger(n) || n < 1 || n > 32) {
      return `--parallel must be an integer between 1 and 32 (got "${raw}")`;
    }
  }
  const rp = values["read-preference"];
  if (rp !== undefined && !READ_PREFERENCES.has(rp)) {
    return `--read-preference must be one of ${[...READ_PREFERENCES].join(", ")} (got "${rp}")`;
  }
  return null;
}
