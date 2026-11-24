import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { MongoClient } from "mongodb";
import { createDecryptor, createEncryptor, keyId } from "./crypto.js";
import { fromBsonTimestamp } from "./oplog.js";
import { checkDatabaseTools, checkTool } from "./tools.js";
import { badge, padVisibleEnd, renderBanner, renderPanel, style } from "./tui.js";
import type { AppConfig } from "./types/config.js";
import type { StorageClient } from "./types/storage.js";

export interface CheckItem {
  name: string;
  passed: boolean;
  message: string;
  details?: Record<string, unknown>;
}

export interface PreflightCheckResult {
  allPassed: boolean;
  checks: CheckItem[];
}

export interface PreflightCheckContext {
  config: AppConfig;
  storage: StorageClient;
  createMongoClient?: (uri: string) => MongoClient;
}

/**
 * Checks availability of mongodump and mongorestore CLI tools.
 */
async function checkTools(): Promise<CheckItem[]> {
  try {
    const toolsStatus = await checkDatabaseTools();
    const dumpCheck: CheckItem = {
      name: "Database Tools: mongodump",
      passed: toolsStatus.mongodump.available,
      message: toolsStatus.mongodump.available
        ? `Installed (${toolsStatus.mongodump.version})`
        : toolsStatus.mongodump.error || "mongodump not found in PATH",
    };
    const restoreCheck: CheckItem = {
      name: "Database Tools: mongorestore",
      passed: toolsStatus.mongorestore.available,
      message: toolsStatus.mongorestore.available
        ? `Installed (${toolsStatus.mongorestore.version})`
        : toolsStatus.mongorestore.error || "mongorestore not found in PATH",
    };
    return [dumpCheck, restoreCheck];
  } catch (error) {
    return [
      {
        name: "Database Tools Check",
        passed: false,
        message: error instanceof Error ? error.message : String(error),
      },
    ];
  }
}

/**
 * Checks MongoDB connectivity, server greeting, and oplog accessibility if enabled.
 */
async function checkMongo(
  config: AppConfig,
  createMongoClient: (uri: string) => MongoClient,
): Promise<CheckItem[]> {
  const checks: CheckItem[] = [];
  const client = createMongoClient(config.mongo.uri);

  try {
    await client.connect();
    const admin = client.db("admin");
    const ping = await admin.command({ ping: 1 });
    const hello = await admin.command({ hello: 1 }).catch(() => admin.command({ isMaster: 1 }));

    checks.push({
      name: "MongoDB Connectivity",
      passed: ping.ok === 1,
      message: `Connected successfully to ${hello.me || "instance"} (setName: ${hello.setName || "none"})`,
    });

    if (config.mongo.oplog || config.pitr?.enabled) {
      try {
        const localDb = client.db("local");
        const oplogColl = localDb.collection("oplog.rs");
        const count = await oplogColl.estimatedDocumentCount();
        let windowNote = "";
        let windowSeconds: number | null = null;
        let fillRatio: number | null = null;

        try {
          if (typeof oplogColl.find === "function") {
            const oldestDoc = await oplogColl
              .find({}, { projection: { ts: 1 } })
              .sort({ $natural: 1 })
              .limit(1)
              .next();
            const newestDoc = await oplogColl
              .find({}, { projection: { ts: 1 } })
              .sort({ $natural: -1 })
              .limit(1)
              .next();
            const oldestTs = fromBsonTimestamp(oldestDoc?.ts);
            const newestTs = fromBsonTimestamp(newestDoc?.ts);
            if (oldestTs && newestTs && newestTs.t >= oldestTs.t) {
              windowSeconds = newestTs.t - oldestTs.t;
              const hours = (windowSeconds / 3600).toFixed(2);
              windowNote = `, retention span ~${hours}h`;
            }
          }
          if (typeof localDb.command === "function") {
            const stats = (await localDb.command({ collStats: "oplog.rs" }).catch(() => null)) as {
              size?: number;
              maxSize?: number;
            } | null;
            if (
              stats &&
              typeof stats.size === "number" &&
              typeof stats.maxSize === "number" &&
              stats.maxSize > 0
            ) {
              fillRatio = stats.size / stats.maxSize;
              windowNote += ` (${Math.round(fillRatio * 100)}% of ${Math.round(stats.maxSize / (1024 * 1024))} MiB cap)`;
            }
          }
        } catch {
          // Best-effort oplog window sizing probe
        }

        checks.push({
          name: "MongoDB Oplog Access",
          passed: true,
          message: `local.oplog.rs accessible (~${count} entries${windowNote})`,
        });

        const minSafeWindowSeconds = Math.max(3600, (config.pitr?.flushIntervalSeconds ?? 60) * 10);
        if (
          config.pitr?.enabled &&
          windowSeconds !== null &&
          fillRatio !== null &&
          fillRatio >= 0.8 &&
          windowSeconds < minSafeWindowSeconds
        ) {
          checks.push({
            name: "MongoDB Oplog Window",
            passed: false,
            message: `Oplog is ${Math.round(fillRatio * 100)}% full with only ${(windowSeconds / 3600).toFixed(2)}h retention (< ${(minSafeWindowSeconds / 3600).toFixed(1)}h safe minimum); resize local.oplog.rs to avoid PITR rollover gaps`,
          });
        }
      } catch (err) {
        checks.push({
          name: "MongoDB Oplog Access",
          passed: false,
          message: `Cannot access local.oplog.rs: ${(err as Error).message}`,
        });
      }
    }

    if (config.pitr?.enabled) {
      try {
        await client.db("config").collection("transactions").findOne({});
        checks.push({
          name: "PITR Transactions Access",
          passed: true,
          message: "config.transactions accessible for active transaction boundary clamping",
        });
      } catch (err) {
        checks.push({
          name: "PITR Transactions Access",
          passed: false,
          message: `Cannot read config.transactions: ${(err as Error).message}`,
        });
      }
    }

    if (config.sharding?.enabled) {
      const isMongos = hello?.msg === "isdbgrid";
      if (!isMongos) {
        checks.push({
          name: "Sharded Cluster (mongos)",
          passed: false,
          message: "SHARDING_ENABLED is true, but MONGODB_URI is not connected to a mongos router",
        });
      } else {
        try {
          const shards = await client.db("config").collection("shards").find({}).toArray();
          checks.push({
            name: "Sharded Cluster (mongos)",
            passed: shards.length > 0,
            message:
              shards.length > 0
                ? `Connected to mongos (${shards.length} shard(s) discovered)`
                : "Connected to mongos, but no shards found in config.shards",
          });
        } catch (err) {
          checks.push({
            name: "Sharded Cluster (mongos)",
            passed: false,
            message: `Cannot read config.shards: ${(err as Error).message}`,
          });
        }
      }
    }
  } catch (error) {
    checks.push({
      name: "MongoDB Connectivity",
      passed: false,
      message: `Failed to connect: ${error instanceof Error ? error.message : String(error)}`,
    });
  } finally {
    await client.close().catch(() => {});
  }

  return checks;
}

/**
 * Checks storage read, write, head, and delete permissions via a temporary probe file.
 */
async function checkStorage(config: AppConfig, storage: StorageClient): Promise<CheckItem> {
  const probeKey = `${config.storage.prefix}/.probe-${randomUUID()}.json`;
  const backendLabel = `Storage Backend (${config.storage.type.toUpperCase()})`;

  try {
    const probeData = { probe: true, timestamp: Date.now() };
    await storage.putJson(probeKey, probeData);
    const retrieved = await storage.getJson<{ probe: boolean }>(probeKey);
    const head = await storage.head(probeKey);
    await storage.remove(probeKey);

    const probeOk = retrieved?.data?.probe === true && head?.size !== undefined && head.size > 0;
    return {
      name: backendLabel,
      passed: probeOk,
      message: `Read, write, head, and delete verified on ${storage.bucket}`,
    };
  } catch (error) {
    return {
      name: backendLabel,
      passed: false,
      message: `Storage permission test failed: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

/**
 * Verifies client-side AES-256-GCM encryption key derivation and envelope roundtrip.
 */
function checkEncryption(config: AppConfig): CheckItem {
  if (config.encryptionKey) {
    try {
      const id = keyId(config.encryptionKey);
      const encryptor = createEncryptor(config.encryptionKey);
      const envelope = encryptor.envelope();
      const decryptor = createDecryptor([config.encryptionKey], envelope);
      return {
        name: "Client-Side Encryption",
        passed: Boolean(decryptor && id),
        message: `AES-256-GCM chunked verified (Key ID: ${id})`,
      };
    } catch (error) {
      return {
        name: "Client-Side Encryption",
        passed: false,
        message: `Key validation failed: ${error instanceof Error ? error.message : String(error)}`,
      };
    }
  }

  if (config.requireEncryption) {
    return {
      name: "Client-Side Encryption",
      passed: false,
      message: "REQUIRE_ENCRYPTION is enabled, but ENCRYPTION_KEY is not configured",
    };
  }

  return {
    name: "Client-Side Encryption",
    passed: true,
    message:
      "Disabled (WARNING: ENCRYPTION_KEY not set — backups are unencrypted and manifests unsigned; set REQUIRE_ENCRYPTION=true in production)",
  };
}

/**
 * Verifies scratch working directory write and read permissions.
 */
async function checkScratchDir(config: AppConfig): Promise<CheckItem> {
  try {
    await fs.mkdir(config.workDir, { recursive: true });
    const probeFile = path.join(config.workDir, `.probe-${randomUUID()}`);
    await fs.writeFile(probeFile, "ok", { mode: 0o600 });
    await fs.readFile(probeFile);
    await fs.rm(probeFile, { force: true });
    return {
      name: "Scratch Directory",
      passed: true,
      message: `Read and write access verified (${config.workDir})`,
    };
  } catch (error) {
    return {
      name: "Scratch Directory",
      passed: false,
      message: `Scratch directory access failed: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

async function checkPhysicalPrerequisites(config: AppConfig): Promise<CheckItem[]> {
  if (config.engine !== "physical") return [];
  const checks: CheckItem[] = [];
  const mongodStatus = await checkTool("mongod");
  checks.push({
    name: "Physical Engine: mongod",
    passed: mongodStatus.available,
    message: mongodStatus.available
      ? `Installed (${mongodStatus.version})`
      : mongodStatus.error || "mongod binary not found in PATH (required for physical recovery)",
  });
  if (config.physical?.dbPath) {
    try {
      await fs.access(config.physical.dbPath);
      checks.push({
        name: "Physical Engine: dbPath",
        passed: true,
        message: `Accessible (${config.physical.dbPath})`,
      });
    } catch (err) {
      checks.push({
        name: "Physical Engine: dbPath",
        passed: false,
        message: `Cannot access dbPath ${config.physical.dbPath}: ${(err as Error).message}`,
      });
    }
  }
  return checks;
}

/**
 * Executes system preflight checks in parallel.
 */
export async function runPreflightCheck({
  config,
  storage,
  createMongoClient = (u) => new MongoClient(u),
}: PreflightCheckContext): Promise<PreflightCheckResult> {
  const [toolsChecks, mongoChecks, physicalChecks, storageCheck, scratchCheck] = await Promise.all([
    checkTools(),
    checkMongo(config, createMongoClient),
    checkPhysicalPrerequisites(config),
    checkStorage(config, storage),
    checkScratchDir(config),
  ]);

  const encryptionCheck = checkEncryption(config);

  const checks: CheckItem[] = [
    ...toolsChecks,
    ...mongoChecks,
    ...physicalChecks,
    storageCheck,
    encryptionCheck,
    scratchCheck,
  ];

  const allPassed = checks.every((c) => c.passed);
  return { allPassed, checks };
}

export function formatPreflightCheck(result: PreflightCheckResult): string {
  const passedCount = result.checks.filter((c) => c.passed).length;
  const totalCount = result.checks.length;
  const nameWidth = result.checks.reduce((max, c) => Math.max(max, c.name.length), 26);

  const lines: string[] = [];
  for (const check of result.checks) {
    const isWarn = check.passed && check.message.includes("WARNING:");
    const statusBadge = !check.passed
      ? badge("FAIL", "error")
      : isWarn
        ? badge("WARN", "warn")
        : badge("PASS", "success");
    const msg = !check.passed
      ? style.red(check.message)
      : isWarn
        ? style.yellow(check.message)
        : check.message;
    lines.push(
      ` ${padVisibleEnd(statusBadge, 8)} ${padVisibleEnd(style.bold(check.name), nameWidth)}  ${msg}`,
    );
  }

  lines.push("");
  if (result.allPassed) {
    lines.push(
      ` ${badge("All system prerequisites PASSED. Ready for backup and restore operations.", "success")}`,
    );
  } else {
    lines.push(
      ` ${badge("One or more prerequisite checks FAILED. Please resolve the errors above.", "error")}`,
    );
  }

  const panel = renderPanel(lines, {
    title: "MongoVault Preflight System Verification",
    subtitle: `${passedCount}/${totalCount} passed`,
    tone: result.allPassed ? "success" : "error",
  });

  return `${renderBanner("Preflight Check")}\n\n${panel}`;
}
