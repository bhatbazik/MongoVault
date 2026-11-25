import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { findFreePort } from "./cluster-harness.js";

const PROJECT_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const DIST_INDEX = path.join(PROJECT_ROOT, "dist/index.js");
const LOCK_WORKER = path.join(PROJECT_ROOT, "tests/e2e/helpers/lock-worker.mjs");
const CLOCK_SKEW_PRELOAD = path.join(PROJECT_ROOT, "tests/e2e/helpers/clock-skew-preload.mjs");

const trackedChildren = new Set();
const autoTempDirs = new Set();

function buildChildEnv(extraEnv = {}, workDir, clockSkewMs) {
  const nodeDir = path.dirname(process.execPath);
  const currentPath = process.env.PATH || "";
  const env = {
    ...process.env,
    ...extraEnv,
    PATH: currentPath.startsWith(nodeDir) ? currentPath : `${nodeDir}:${currentPath}`,
    WORK_DIR: extraEnv.WORK_DIR ?? workDir,
  };
  if (clockSkewMs !== undefined && clockSkewMs !== 0) {
    env.MONGOVAULT_CLOCK_SKEW_MS = String(clockSkewMs);
  }
  return env;
}

function buildNodeArgs(scriptPath, scriptArgs, clockSkewMs) {
  const args = [];
  if (clockSkewMs !== undefined && clockSkewMs !== 0) {
    args.push("--import", CLOCK_SKEW_PRELOAD);
  }
  args.push(scriptPath, ...scriptArgs);
  return args;
}

async function resolveIsolatedWorkDir(workDir) {
  if (workDir) {
    await fs.mkdir(workDir, { recursive: true });
    return { dir: workDir, autoCreated: false };
  }
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-cli-"));
  autoTempDirs.add(dir);
  return { dir, autoCreated: true };
}

function trackChild(proc) {
  trackedChildren.add(proc);
  proc.once("close", () => trackedChildren.delete(proc));
  proc.once("error", () => trackedChildren.delete(proc));
}

async function terminateProcess(proc, gracefulMs = 5_000) {
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
  const exitPromise = new Promise((resolve) => {
    proc.once("close", resolve);
    proc.once("error", resolve);
  });
  try {
    proc.kill("SIGTERM");
  } catch {
    return;
  }
  await Promise.race([exitPromise, new Promise((r) => setTimeout(r, gracefulMs))]);
  if (proc.exitCode === null && proc.signalCode === null) {
    try {
      proc.kill("SIGKILL");
    } catch {}
    await exitPromise;
  }
}

export async function cleanupAllChildren() {
  const children = [...trackedChildren];
  await Promise.all(
    children.map(async (proc) => {
      if (proc.exitCode === null && proc.signalCode === null) {
        const exitPromise = new Promise((resolve) => {
          proc.once("close", resolve);
          proc.once("error", resolve);
        });
        try {
          proc.kill("SIGKILL");
        } catch {}
        await exitPromise;
      }
      trackedChildren.delete(proc);
    }),
  );
  const dirs = [...autoTempDirs];
  autoTempDirs.clear();
  await Promise.all(dirs.map((d) => fs.rm(d, { recursive: true, force: true }).catch(() => {})));
}

export async function runCliCommand(
  args = [],
  { env = {}, workDir, cwd, timeoutMs = 60_000, clockSkewMs, parseJson = false } = {},
) {
  const { dir: isolatedDir, autoCreated } = await resolveIsolatedWorkDir(workDir);
  const effectiveCwd = cwd ?? isolatedDir;
  const childEnv = buildChildEnv(env, isolatedDir, clockSkewMs);
  const nodeArgs = buildNodeArgs(DIST_INDEX, args, clockSkewMs);

  const startedAt = Date.now();
  const proc = spawn(process.execPath, nodeArgs, {
    cwd: effectiveCwd,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });
  trackChild(proc);

  let stdout = "";
  let stderr = "";
  proc.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });
  proc.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  let timedOut = false;
  let timer = null;
  let killTimer = null;

  try {
    const { exitCode, signal } = await new Promise((resolve, reject) => {
      timer = setTimeout(() => {
        timedOut = true;
        try {
          proc.kill("SIGTERM");
        } catch {}
        killTimer = setTimeout(() => {
          if (proc.exitCode === null && proc.signalCode === null) {
            try {
              proc.kill("SIGKILL");
            } catch {}
          }
        }, 3_000);
      }, timeoutMs);

      proc.once("error", reject);
      proc.once("close", (code, sig) => resolve({ exitCode: code, signal: sig }));
    });

    if (timedOut) {
      throw new Error(
        `CLI command timed out after ${timeoutMs}ms: node ${args.join(" ")}\nstdout:\n${stdout}\nstderr:\n${stderr}`,
      );
    }

    return {
      exitCode,
      signal,
      stdout,
      stderr,
      json: parseJson ? JSON.parse(stdout) : null,
      durationMs: Date.now() - startedAt,
    };
  } finally {
    if (timer) clearTimeout(timer);
    if (killTimer) clearTimeout(killTimer);
    if (proc.exitCode === null && proc.signalCode === null) {
      try {
        proc.kill("SIGKILL");
      } catch {}
    }
    if (autoCreated) {
      autoTempDirs.delete(isolatedDir);
      await fs.rm(isolatedDir, { recursive: true, force: true }).catch(() => {});
    }
  }
}

export async function startDaemon({
  env = {},
  workDir,
  cwd,
  httpPort,
  clockSkewMs,
  readyTimeoutMs = 30_000,
} = {}) {
  const { dir: isolatedDir } = await resolveIsolatedWorkDir(workDir);
  const port = httpPort ?? (await findFreePort());
  if (!port || port <= 0) {
    throw new Error(`Invalid daemon HTTP port reserved: ${port}`);
  }

  const childEnv = buildChildEnv(
    {
      ...env,
      HTTP_PORT: String(port),
    },
    isolatedDir,
    clockSkewMs,
  );
  const nodeArgs = buildNodeArgs(DIST_INDEX, ["daemon"], clockSkewMs);

  const proc = spawn(process.execPath, nodeArgs, {
    cwd: cwd ?? isolatedDir,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });
  trackChild(proc);

  let stdout = "";
  let stderr = "";
  proc.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });
  proc.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  const baseUrl = `http://127.0.0.1:${port}`;
  const exitPromise = new Promise((resolve) => {
    proc.once("close", (code, sig) => resolve({ exitCode: code, signal: sig }));
  });

  const deadline = Date.now() + readyTimeoutMs;
  let ready = false;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null || proc.signalCode !== null) {
      throw new Error(
        `Daemon exited early (code=${proc.exitCode}, signal=${proc.signalCode})\nstdout:\n${stdout}\nstderr:\n${stderr}`,
      );
    }
    try {
      const res = await fetch(`${baseUrl}/ready`);
      if (res.status === 200) {
        ready = true;
        break;
      }
    } catch {
      // Not listening yet
    }
    await new Promise((r) => setTimeout(r, 150));
  }

  if (!ready) {
    await terminateProcess(proc, 2_000);
    throw new Error(
      `Daemon did not become ready on ${baseUrl}/ready within ${readyTimeoutMs}ms\nstdout:\n${stdout}\nstderr:\n${stderr}`,
    );
  }

  return {
    proc,
    pid: proc.pid ?? null,
    port,
    baseUrl,
    workDir: isolatedDir,
    getStdout: () => stdout,
    getStderr: () => stderr,
    async getHealth() {
      const res = await fetch(`${baseUrl}/healthz`);
      return { status: res.status, body: await res.json() };
    },
    async getReady() {
      const res = await fetch(`${baseUrl}/ready`);
      return { status: res.status, body: await res.json() };
    },
    async getMetrics() {
      const res = await fetch(`${baseUrl}/metrics`);
      return { status: res.status, text: await res.text() };
    },
    signal(sig = "SIGTERM") {
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill(sig);
      }
    },
    async waitForExit(timeoutMs = 15_000) {
      const result = await Promise.race([
        exitPromise,
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error(`Daemon did not exit within ${timeoutMs}ms`)),
            timeoutMs,
          ),
        ),
      ]);
      return result;
    },
    async stop() {
      await terminateProcess(proc, 10_000);
    },
  };
}

export async function spawnLockWorker(options = {}) {
  const { workDir, cwd, clockSkewMs, env = {}, ...workerConfig } = options;
  const { dir: isolatedDir } = await resolveIsolatedWorkDir(workDir);
  const childEnv = buildChildEnv(env, isolatedDir, clockSkewMs);
  const configArg = JSON.stringify(workerConfig);
  const nodeArgs = buildNodeArgs(LOCK_WORKER, ["--config", configArg], clockSkewMs);

  const proc = spawn(process.execPath, nodeArgs, {
    cwd: cwd ?? isolatedDir,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });
  trackChild(proc);

  let stdout = "";
  let stderr = "";
  let lineBuffer = "";
  const events = [];
  const waiters = [];

  const notifyWaiters = (ev) => {
    for (let i = waiters.length - 1; i >= 0; i--) {
      const w = waiters[i];
      if (!w.name || ev.event === w.name) {
        clearTimeout(w.timer);
        waiters.splice(i, 1);
        w.resolve(ev);
      }
    }
  };

  proc.stdout.on("data", (chunk) => {
    const str = chunk.toString();
    stdout += str;
    lineBuffer += str;
    let idx;
    while ((idx = lineBuffer.indexOf("\n")) !== -1) {
      const line = lineBuffer.slice(0, idx).trim();
      lineBuffer = lineBuffer.slice(idx + 1);
      if (!line) continue;
      try {
        const parsed = JSON.parse(line);
        events.push(parsed);
        notifyWaiters(parsed);
      } catch {
        // Ignore non-JSON output
      }
    }
  });

  proc.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  const exitPromise = new Promise((resolve) => {
    proc.once("close", (code, sig) => {
      if (lineBuffer.trim()) {
        try {
          const parsed = JSON.parse(lineBuffer.trim());
          events.push(parsed);
          notifyWaiters(parsed);
        } catch {}
        lineBuffer = "";
      }
      for (let i = waiters.length - 1; i >= 0; i--) {
        const w = waiters[i];
        clearTimeout(w.timer);
        waiters.splice(i, 1);
        w.reject(
          new Error(
            `lock-worker exited (code=${code}, sig=${sig}) before event "${w.name}"\nstdout:\n${stdout}\nstderr:\n${stderr}`,
          ),
        );
      }
      resolve({ exitCode: code, signal: sig });
    });
  });

  return {
    proc,
    pid: proc.pid ?? null,
    events,
    getStdout: () => stdout,
    getStderr: () => stderr,
    waitForEvent(name, timeoutMs = 10_000) {
      const existing = events.find((e) => !name || e.event === name);
      if (existing) return Promise.resolve(existing);
      if (proc.exitCode !== null || proc.signalCode !== null) {
        return Promise.reject(
          new Error(
            `lock-worker already exited (code=${proc.exitCode}) before event "${name}"\nstdout:\n${stdout}\nstderr:\n${stderr}`,
          ),
        );
      }
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const idx = waiters.findIndex((w) => w.timer === timer);
          if (idx !== -1) waiters.splice(idx, 1);
          reject(
            new Error(
              `Timed out waiting for lock-worker event "${name}" after ${timeoutMs}ms\nstdout:\n${stdout}\nstderr:\n${stderr}`,
            ),
          );
        }, timeoutMs);
        waiters.push({ name, resolve, reject, timer });
      });
    },
    async waitForExit(timeoutMs = 10_000) {
      return Promise.race([
        exitPromise,
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error(`lock-worker did not exit within ${timeoutMs}ms`)),
            timeoutMs,
          ),
        ),
      ]);
    },
    async kill(sig = "SIGKILL") {
      if (proc.exitCode !== null || proc.signalCode !== null) {
        return exitPromise;
      }
      proc.kill(sig);
      if (sig === "SIGSTOP" || sig === "SIGCONT") return null;
      return exitPromise;
    },
    async stop() {
      await terminateProcess(proc, 5_000);
    },
  };
}
