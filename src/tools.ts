import { spawn, type StdioOptions } from "node:child_process";
import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { formatDuration } from "./format.js";
import type {
  DatabaseToolsStatus,
  DumpLineResult,
  MongodumpOptions,
  MongodumpResult,
  MongorestoreOptions,
  MongorestoreResult,
  ReplayOplogFileOptions,
  RestoreLineResult,
  RunToolOptions,
  RunToolResult,
  ToolVersionInfo,
} from "./types/tools.js";

const KILL_GRACE_MS = 10_000;
const TAIL_LINES = 40;
const DUMPED =
  /(?:done dumping|dumped)\s+["']?([^.\s"']+)["']?\.["']?(.+?)["']?\s+\(([\d,]+)\s+documents?\)/i;
const RESTORED =
  /([\d,]+)\s+document\(s\)\s+restored\s+successfully\.\s+([\d,]+)\s+document\(s\)\s+failed/i;

export function isClusterDatabase(database?: string | null): boolean {
  return !database || database === "*" || database === "all";
}

export function parseDumpLine(line: string): DumpLineResult | null {
  const match = DUMPED.exec(line);
  if (!match || !match[1] || !match[2] || !match[3]) {
    return null;
  }
  return {
    database: match[1],
    collection: match[2],
    documents: Number(match[3].replace(/,/g, "")),
  };
}

export function parseRestoreLine(line: string): RestoreLineResult | null {
  const match = RESTORED.exec(line);
  if (!match || !match[1] || !match[2]) {
    return null;
  }
  return {
    restored: Number(match[1].replace(/,/g, "")),
    failed: Number(match[2].replace(/,/g, "")),
  };
}

export function runTool(
  command: string,
  args: string[],
  { stdio, signal, timeoutMs, onLine = () => {} }: RunToolOptions,
): RunToolResult {
  const child = spawn(command, args, { stdio: stdio as StdioOptions });
  const tail: string[] = [];

  if (child.stderr) {
    readline.createInterface({ input: child.stderr, crlfDelay: Infinity }).on("line", (line) => {
      tail.push(line);
      if (tail.length > TAIL_LINES) tail.shift();
      onLine(line);
    });
  }

  const done = new Promise<void>((resolve, reject) => {
    let failure: unknown = null;

    const terminate = (reason: unknown) => {
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

    const onAbort = () => terminate(signal?.reason ?? new Error(`${command} was aborted`));
    if (signal?.aborted) onAbort();
    else signal?.addEventListener("abort", onAbort, { once: true });

    const cleanup = () => {
      if (timer) clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };

    child.once("error", (error: Error & { code?: string }) => {
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
  readPreference,
  workDir,
  signal,
  timeoutMs,
  logger,
  command = "mongodump",
}: MongodumpOptions): Promise<MongodumpResult> {
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
  if (readPreference) {
    args.push(`--readPreference=${readPreference}`);
  }
  if (oplog) {
    args.push("--oplog");
  }

  args.push("--archive", "--gzip");
  const collections: Record<string, number> = {};

  const { child, done } = runTool(command, args, {
    stdio: ["ignore", "pipe", "pipe"],
    signal,
    timeoutMs,
    onLine: (line) => {
      logger?.debug({ tool: "mongodump" }, line);
      const dumped = parseDumpLine(line);
      if (dumped) {
        const key = isCluster ? `${dumped.database}.${dumped.collection}` : dumped.collection;
        collections[key] = dumped.documents;
      }
    },
  });

  return {
    stdout: child.stdout!,
    done: done.finally(cleanupConfig).then(() => ({ collections })),
  };
}

export async function mongorestore({
  uri,
  workDir,
  nsInclude,
  nsExclude,
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
}: MongorestoreOptions): Promise<MongorestoreResult> {
  if (oplogLimit && !oplogReplay) {
    throw new Error("cannot use --oplogLimit without --oplogReplay enabled");
  }
  if (oplogReplay && (nsInclude || nsExclude || (nsFrom && nsTo))) {
    throw new Error("cannot use --oplogReplay with namespace includes or renames specified");
  }

  const { file: configFile, cleanup: cleanupConfig } = await writeUriConfig(workDir, uri);
  const args = [`--config=${configFile}`, "--archive", "--gzip", "--stopOnError"];
  if (nsInclude) args.push(`--nsInclude=${nsInclude}`);
  if (nsExclude) args.push(`--nsExclude=${nsExclude}`);
  if (nsFrom && nsTo) args.push(`--nsFrom=${nsFrom}`, `--nsTo=${nsTo}`);
  if (drop) args.push("--drop");
  if (oplogReplay) args.push("--oplogReplay");
  if (oplogLimit) args.push(`--oplogLimit=${oplogLimit}`);
  if (parallelCollections && parallelCollections > 0) {
    args.push(`--numParallelCollections=${parallelCollections}`);
  }
  let summary: RestoreLineResult | null = null;

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
    stdin: child.stdin!,
    done: done.finally(cleanupConfig).then(() => {
      if (summary && summary.failed > 0) {
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
}: ReplayOplogFileOptions): Promise<void> {
  if (!oplogFile) {
    throw new Error("oplogFile is required for replayOplogFile");
  }
  const { file: configFile, cleanup: cleanupConfig } = await writeUriConfig(workDir, uri);
  const emptyDir = path.join(workDir, `.empty-${randomUUID()}`);
  await fs.mkdir(emptyDir, { recursive: true });

  const args = [
    `--config=${configFile}`,
    "--stopOnError",
    "--oplogReplay",
    `--oplogFile=${oplogFile}`,
  ];
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
    await Promise.allSettled([cleanupConfig(), fs.rm(emptyDir, { recursive: true, force: true })]);
  }
}

async function writeUriConfig(
  workDir: string,
  uri: string,
): Promise<{ file: string; cleanup: () => Promise<void> }> {
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

export async function checkTool(command: string): Promise<ToolVersionInfo> {
  return new Promise((resolve) => {
    const child = spawn(command, ["--version"], { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr?.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.once("error", (error: Error & { code?: string }) => {
      resolve({
        available: false,
        error: error.code === "ENOENT" ? `${command} not found in PATH` : error.message,
      });
    });
    child.once("close", (code) => {
      if (code === 0) {
        const text = stdout || stderr;
        const match = /version\s+([v\d.]+)/i.exec(text);
        resolve({
          available: true,
          version: match ? match[1] : text.trim().split("\n")[0],
        });
      } else {
        resolve({
          available: false,
          error: `${command} exited with code ${code}: ${stderr || stdout}`,
        });
      }
    });
  });
}

export async function checkDatabaseTools(): Promise<DatabaseToolsStatus> {
  const [dump, restore] = await Promise.all([checkTool("mongodump"), checkTool("mongorestore")]);
  return {
    mongodump: dump,
    mongorestore: restore,
  };
}
