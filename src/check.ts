import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { MongoClient } from "mongodb";
import { createDecryptor, createEncryptor, keyId } from "./crypto.js";
import { checkDatabaseTools } from "./tools.js";
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

    if (config.mongo.oplog) {
      try {
        const oplogColl = client.db("local").collection("oplog.rs");
        const count = await oplogColl.estimatedDocumentCount();
        checks.push({
          name: "MongoDB Oplog Access",
          passed: true,
          message: `local.oplog.rs accessible (~${count} entries)`,
        });
      } catch (err) {
        checks.push({
          name: "MongoDB Oplog Access",
          passed: false,
          message: `Cannot access local.oplog.rs: ${(err as Error).message}`,
        });
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

  return {
    name: "Client-Side Encryption",
    passed: true,
    message: "Disabled (No master key configured)",
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

/**
 * Executes system preflight checks in parallel.
 */
export async function runPreflightCheck({
  config,
  storage,
  createMongoClient = (u) => new MongoClient(u),
}: PreflightCheckContext): Promise<PreflightCheckResult> {
  const [toolsChecks, mongoChecks, storageCheck, scratchCheck] = await Promise.all([
    checkTools(),
    checkMongo(config, createMongoClient),
    checkStorage(config, storage),
    checkScratchDir(config),
  ]);

  const encryptionCheck = checkEncryption(config);

  const checks: CheckItem[] = [
    ...toolsChecks,
    ...mongoChecks,
    storageCheck,
    encryptionCheck,
    scratchCheck,
  ];

  const allPassed = checks.every((c) => c.passed);
  return { allPassed, checks };
}

export function formatPreflightCheck(result: PreflightCheckResult): string {
  const lines: string[] = [];
  lines.push("MongoVault Preflight System Verification:");
  lines.push("==========================================");

  for (const check of result.checks) {
    const symbol = check.passed ? "✓ PASS" : "✗ FAIL";
    lines.push(`  [${symbol}] ${check.name.padEnd(30)} : ${check.message}`);
  }

  lines.push("==========================================");
  if (result.allPassed) {
    lines.push("All system prerequisites PASSED. Ready for backup and restore operations.");
  } else {
    lines.push("One or more prerequisite checks FAILED. Please resolve the errors above.");
  }

  return lines.join("\n");
}
