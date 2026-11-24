import { spawn, type StdioOptions } from "node:child_process";
import { randomUUID } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import type { Readable } from "node:stream";
import { createGzip } from "node:zlib";
import { ArchiveOplogTap } from "./archive-stream.js";
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
const DUMPED = /(?:done dumping|dumped)\s+(.+?)\s+\(([\d,]+)\s+documents?\)/i;
const RESTORED =
  /([\d,]+)\s+document\(s\)\s+restored\s+successfully\.\s+([\d,]+)\s+document\(s\)\s+failed/i;

export function isClusterDatabase(database?: string | null): boolean {
  return !database || database === "*" || database === "all";
}

export function parseDumpLine(line: string): DumpLineResult | null {
  const match = DUMPED.exec(line);
  if (!match || !match[1] || !match[2]) {
    return null;
  }
  let ns = match[1].trim();
  // Strip outer Go %#q backticks or quotes used by mongodb-database-tools >= 100.14 (`db.coll`, "db.coll")
  if (
    ns.length >= 2 &&
    ((ns.startsWith("`") && ns.endsWith("`")) ||
      (ns.startsWith('"') && ns.endsWith('"')) ||
      (ns.startsWith("'") && ns.endsWith("'")))
  ) {
    ns = ns.slice(1, -1);
  }
  const dot = ns.indexOf(".");
  if (dot <= 0 || dot === ns.length - 1) {
    return null;
  }
  const stripOuterQuotes = (value: string) => value.replace(/^["'`]|["'`]$/g, "");
  const database = stripOuterQuotes(ns.slice(0, dot));
  const collection = stripOuterQuotes(ns.slice(dot + 1));
  if (!database || !collection) {
    return null;
  }
  return {
    database,
    collection,
    documents: Number(match[2].replace(/,/g, "")),
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
  let rl: readline.Interface | null = null;
  let stderrClosed: Promise<void> = Promise.resolve();

  if (child.stderr) {
    rl = readline.createInterface({ input: child.stderr, crlfDelay: Infinity });
    rl.on("line", (line) => {
      tail.push(line);
      if (tail.length > TAIL_LINES) tail.shift();
      onLine(line);
    });
    stderrClosed = new Promise<void>((resolve) => {
      rl!.once("close", resolve);
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
      rl?.close();
      void stderrClosed.then(() => {
        if (failure) return reject(failure);
        if (code === 0) return resolve();
        const output = tail.length ? `\n${tail.join("\n")}` : "";
        reject(new Error(`${command} exited with ${code ?? exitSignal}${output}`));
      });
    });
  });

  return { child, done };
}

function uriHasReadPreference(uri: string): boolean {
  const queryIdx = uri.indexOf("?");
  if (queryIdx === -1) return false;
  return /(?:^|&)readPreference=/i.test(uri.slice(queryIdx + 1));
}

function splitNamespacePatterns(raw?: string): string[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
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
  if (readPreference && !uriHasReadPreference(uri)) {
    args.push(`--readPreference=${readPreference}`);
  }
  if (oplog) {
    // Emit an uncompressed archive from mongodump so ArchiveOplogTap can inspect the
    // exact embedded oplog bounds in O(1) RAM before gzipping the stream for upload.
    args.push("--oplog", "--archive");
  } else {
    args.push("--archive", "--gzip");
  }

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

  if (!oplog) {
    return {
      stdout: child.stdout!,
      done: done.finally(cleanupConfig).then(() => ({ collections })),
    };
  }

  const tap = new ArchiveOplogTap();
  const gzip = createGzip();
  const rawOut = child.stdout!;

  rawOut.on("error", (err) => tap.destroy(err));
  tap.on("error", (err) => gzip.destroy(err));
  gzip.on("close", () => {
    if (!tap.readableEnded && !tap.destroyed) {
      tap.destroy();
    }
    if (!rawOut.readableEnded && !rawOut.destroyed) {
      rawOut.destroy();
    }
  });
  rawOut.pipe(tap).pipe(gzip);

  const tapFinished = new Promise<void>((resolve) => {
    if (tap.readableEnded || tap.destroyed) {
      resolve();
      return;
    }
    tap.once("end", resolve);
    tap.once("close", resolve);
  });

  return {
    stdout: gzip as Readable,
    done: done.finally(cleanupConfig).then(async () => {
      await tapFinished;
      const bounds = tap.bounds;
      return {
        collections,
        oplogStartTs: bounds.firstTs,
        oplogEndTs: bounds.lastTs,
        oplogEntries: bounds.entries,
      };
    }),
  };
}

export async function mongorestore({
  uri,
  workDir,
  nsInclude,
  nsExclude,
  systemNsExclude = [],
  nsFrom,
  nsTo,
  drop = false,
  preserveUUID,
  bypassDocumentValidation = true,
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
  for (const pattern of splitNamespacePatterns(nsInclude)) {
    args.push(`--nsInclude=${pattern}`);
  }
  for (const pattern of splitNamespacePatterns(nsExclude)) {
    args.push(`--nsExclude=${pattern}`);
  }
  for (const pattern of systemNsExclude) {
    if (pattern.trim().length > 0) {
      args.push(`--nsExclude=${pattern.trim()}`);
    }
  }
  if (nsFrom && nsTo) args.push(`--nsFrom=${nsFrom}`, `--nsTo=${nsTo}`);
  if (drop) args.push("--drop");
  if (drop && !(nsFrom && nsTo) && preserveUUID !== false) {
    args.push("--preserveUUID");
  }
  if (bypassDocumentValidation !== false) {
    args.push("--bypassDocumentValidation");
  }
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
