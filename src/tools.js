import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { formatDuration } from "./format.js";

const KILL_GRACE_MS = 10_000;
const TAIL_LINES = 40;
const DUMPED = /(?:done dumping|dumped)\s+["']?([^.\s"']+)["']?\.["']?(.+?)["']?\s+\(([\d,]+)\s+documents?\)/i;
const RESTORED = /([\d,]+)\s+document\(s\)\s+restored\s+successfully\.\s+([\d,]+)\s+document\(s\)\s+failed/i;

export function isClusterDatabase(database) {
  return !database || database === "*" || database === "all";
}

export function parseDumpLine(line) {
  const match = DUMPED.exec(line);
  return match
    ? {
        database: match[1],
        collection: match[2],
        documents: Number(match[3].replace(/,/g, "")),
      }
    : null;
}

export function parseRestoreLine(line) {
  const match = RESTORED.exec(line);
  return match
    ? {
        restored: Number(match[1].replace(/,/g, "")),
        failed: Number(match[2].replace(/,/g, "")),
      }
    : null;
}

export function runTool(command, args, { stdio, signal, timeoutMs, onLine = () => {} }) {
  const child = spawn(command, args, { stdio });
  const tail = [];

  readline.createInterface({ input: child.stderr, crlfDelay: Infinity }).on("line", (line) => {
    tail.push(line);
    if (tail.length > TAIL_LINES) tail.shift();
    onLine(line);
  });

  const done = new Promise((resolve, reject) => {
    let failure = null;

    const terminate = (reason) => {
      if (failure || child.exitCode !== null || child.signalCode !== null) return;
      failure = reason;
      child.kill("SIGTERM");
      setTimeout(() => child.kill("SIGKILL"), KILL_GRACE_MS).unref();
    };

    const timer = timeoutMs
      ? setTimeout(
          () => terminate(new Error(`${command} timed out after ${formatDuration(timeoutMs)}`)),
          timeoutMs,
        )
      : null;

    const onAbort = () => terminate(signal.reason ?? new Error(`${command} was aborted`));
    if (signal?.aborted) onAbort();
    else signal?.addEventListener("abort", onAbort, { once: true });

    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };

    child.once("error", (error) => {
      cleanup();
      reject(
        error.code === "ENOENT"
          ? new Error(`${command} not found. Install MongoDB Database Tools and add them to PATH.`)
          : error,
      );
    });

    child.once("close", (code, exitSignal) => {
      cleanup();
      if (failure) return reject(failure);
      if (code === 0) return resolve();
      const output = tail.length ? `\n${tail.join("\n")}` : "";
      reject(new Error(`${command} exited with ${code ?? exitSignal}${output}`));
    });
  });

  return { child, done };
}

export async function mongodump({
  uri,
  database,
  excludeCollections = [],
  parallelCollections,
  oplog = false,
  workDir,
  signal,
  timeoutMs,
  logger,
  command = "mongodump",
}) {
  const isCluster = isClusterDatabase(database);
  if (oplog && !isCluster) {
    throw new Error("--oplog mode is only supported on full cluster dumps (database: * or all)");
  }
  if (isCluster && excludeCollections.length > 0) {
    throw new Error("--db is required when --excludeCollection is specified");
  }

  const { file: configFile, cleanup: cleanupConfig } = await writeUriConfig(workDir, uri);
  const args = [`--config=${configFile}`];

  if (!isCluster) {
    args.push(`--db=${database}`);
  }
  for (const name of excludeCollections) {
    args.push(`--excludeCollection=${name}`);
  }
  if (parallelCollections && parallelCollections > 0) {
    args.push(`--numParallelCollections=${parallelCollections}`);
  }
  if (oplog) {
    args.push("--oplog");
  }

  args.push("--archive", "--gzip");
  const collections = {};

  const { child, done } = runTool(command, args, {
    stdio: ["ignore", "pipe", "pipe"],
    signal,
    timeoutMs,
    onLine: (line) => {
      logger?.debug({ tool: "mongodump" }, line);
      const dumped = parseDumpLine(line);
      if (dumped) {
        const key = isCluster
          ? `${dumped.database}.${dumped.collection}`
          : dumped.collection;
        collections[key] = dumped.documents;
      }
    },
  });

  return {
    stdout: child.stdout,
    done: done.finally(cleanupConfig).then(() => ({ collections })),
  };
}

export async function mongorestore({
  uri,
  workDir,
  nsInclude,
  nsFrom,
  nsTo,
  drop = false,
  oplogReplay = false,
  oplogLimit,
  parallelCollections,
  signal,
  timeoutMs,
  logger,
  command = "mongorestore",
}) {
  if (oplogLimit && !oplogReplay) {
    throw new Error("cannot use --oplogLimit without --oplogReplay enabled");
  }
  if (oplogReplay && (nsInclude || (nsFrom && nsTo))) {
    throw new Error("cannot use --oplogReplay with namespace includes or renames specified");
  }

  const { file: configFile, cleanup: cleanupConfig } = await writeUriConfig(workDir, uri);
  const args = [`--config=${configFile}`, "--archive", "--gzip"];
  if (nsInclude) args.push(`--nsInclude=${nsInclude}`);
  if (nsFrom && nsTo) args.push(`--nsFrom=${nsFrom}`, `--nsTo=${nsTo}`);
  if (drop) args.push("--drop");
  if (oplogReplay) args.push("--oplogReplay");
  if (oplogLimit) args.push(`--oplogLimit=${oplogLimit}`);
  if (parallelCollections && parallelCollections > 0) {
    args.push(`--numParallelCollections=${parallelCollections}`);
  }
  let summary = null;

  const { child, done } = runTool(command, args, {
    stdio: ["pipe", "ignore", "pipe"],
    signal,
    timeoutMs,
    onLine: (line) => {
      logger?.debug({ tool: "mongorestore" }, line);
      summary = parseRestoreLine(line) ?? summary;
    },
  });

  return {
    stdin: child.stdin,
    done: done
      .finally(cleanupConfig)
      .then(() => {
        if (summary?.failed > 0) {
          throw new Error(`mongorestore failed to restore ${summary.failed} documents`);
        }
        return summary ?? { restored: 0, failed: 0 };
      }),
  };
}

export async function replayOplogFile({
  uri,
  workDir,
  oplogFile,
  oplogLimit,
  signal,
  timeoutMs,
  logger,
  command = "mongorestore",
}) {
  if (!oplogFile) {
    throw new Error("oplogFile is required for replayOplogFile");
  }
  const { file: configFile, cleanup: cleanupConfig } = await writeUriConfig(workDir, uri);
  const emptyDir = path.join(workDir, `.empty-${randomUUID()}`);
  await fs.mkdir(emptyDir, { recursive: true });

  const args = [`--config=${configFile}`, "--oplogReplay", `--oplogFile=${oplogFile}`];
  if (oplogLimit) {
    args.push(`--oplogLimit=${oplogLimit}`);
  }
  args.push(`--dir=${emptyDir}`);

  const { done } = runTool(command, args, {
    stdio: ["ignore", "ignore", "pipe"],
    signal,
    timeoutMs,
    onLine: (line) => {
      logger?.debug({ tool: "mongorestore-oplog" }, line);
    },
  });

  try {
    await done;
  } finally {
    await Promise.allSettled([
      cleanupConfig(),
      fs.rm(emptyDir, { recursive: true, force: true }),
    ]);
  }
}

async function writeUriConfig(workDir, uri) {
  await fs.mkdir(workDir, { recursive: true });
  const file = path.join(workDir, `.${randomUUID()}.yaml`);
  await fs.writeFile(file, `uri: ${JSON.stringify(uri)}\n`, { mode: 0o600 });

  const onExit = () => {
    try {
      fsSync.rmSync(file, { force: true });
    } catch {}
  };
  process.once("exit", onExit);

  const cleanup = async () => {
    process.removeListener("exit", onExit);
    await fs.rm(file, { force: true });
  };

  return { file, cleanup };
}
