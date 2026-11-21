import { EventEmitter } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";

const { mockSpawn } = vi.hoisted(() => ({
  mockSpawn: vi.fn(),
}));

vi.mock("node:child_process", () => ({
  spawn: mockSpawn,
}));

import {
  checkDatabaseTools,
  checkTool,
  mongodump,
  mongorestore,
  replayOplogFile,
  runTool,
} from "../src/tools.js";

function createFakeChild({
  exitCode = 0,
  stdoutData = "",
  stderrLines = [],
  emitError = null,
} = {}) {
  const child = new EventEmitter();
  child.stdin = new PassThrough();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.exitCode = null;
  child.signalCode = null;
  child.kill = vi.fn((sig) => {
    child.signalCode = sig;
    setTimeout(() => child.emit("close", null, sig), 5);
  });

  setTimeout(() => {
    if (emitError) {
      child.emit("error", emitError);
      return;
    }
    for (const line of stderrLines) {
      child.stderr.write(`${line}\n`);
    }
    child.stderr.end();
    if (stdoutData) {
      child.stdout.write(stdoutData);
    }
    child.stdout.end();
    child.exitCode = exitCode;
    child.emit("close", exitCode, null);
  }, 5);

  return child;
}

describe("Database Tools runner (tools.ts full coverage)", () => {
  afterEach(() => {
    mockSpawn.mockReset();
  });

  it("runTool handles success, non-zero exit, ENOENT, abort, and timeout", async () => {
    mockSpawn.mockImplementationOnce(() =>
      createFakeChild({ exitCode: 0, stderrLines: ["line 1", "line 2"] }),
    );
    const onLine = vi.fn();
    await runTool("echo", ["hi"], { stdio: ["ignore", "pipe", "pipe"], onLine }).done;
    expect(onLine).toHaveBeenCalledTimes(2);

    mockSpawn.mockImplementationOnce(() =>
      createFakeChild({ exitCode: 2, stderrLines: ["fatal error"] }),
    );
    await expect(
      runTool("mongodump", [], { stdio: ["ignore", "pipe", "pipe"] }).done,
    ).rejects.toThrow(/exited with 2/);

    const enoent = Object.assign(new Error("not found"), { code: "ENOENT" });
    mockSpawn.mockImplementationOnce(() => createFakeChild({ emitError: enoent }));
    await expect(
      runTool("missingbin", [], { stdio: ["ignore", "pipe", "pipe"] }).done,
    ).rejects.toThrow(/not found\. Install MongoDB Database Tools/);

    // Already aborted
    const ac = new AbortController();
    ac.abort(new Error("user cancelled"));
    mockSpawn.mockImplementationOnce(() => {
      const longChild = new EventEmitter();
      longChild.stdin = new PassThrough();
      longChild.stdout = new PassThrough();
      longChild.stderr = new PassThrough();
      longChild.exitCode = null;
      longChild.signalCode = null;
      longChild.kill = vi.fn((sig) => {
        longChild.signalCode = sig;
        setTimeout(() => longChild.emit("close", null, sig), 5);
      });
      return longChild;
    });
    await expect(
      runTool("mongodump", [], { stdio: ["ignore", "pipe", "pipe"], signal: ac.signal }).done,
    ).rejects.toThrow("user cancelled");

    // Timeout
    mockSpawn.mockImplementationOnce(() => {
      const timeoutChild = new EventEmitter();
      timeoutChild.stdin = new PassThrough();
      timeoutChild.stdout = new PassThrough();
      timeoutChild.stderr = new PassThrough();
      timeoutChild.exitCode = null;
      timeoutChild.signalCode = null;
      timeoutChild.kill = vi.fn((sig) => {
        timeoutChild.signalCode = sig;
        setTimeout(() => timeoutChild.emit("close", null, sig), 5);
      });
      return timeoutChild;
    });
    await expect(
      runTool("mongodump", [], { stdio: ["ignore", "pipe", "pipe"], timeoutMs: 15 }).done,
    ).rejects.toThrow(/timed out/);
  });

  it("mongodump validates options and runs both standard and --oplog streams", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-tools-"));
    try {
      await expect(
        mongodump({ uri: "mongodb://localhost", database: "mydb", oplog: true, workDir }),
      ).rejects.toThrow(/--oplog mode is only supported on full cluster/);

      await expect(
        mongodump({
          uri: "mongodb://localhost",
          database: "*",
          excludeCollections: ["logs"],
          workDir,
        }),
      ).rejects.toThrow(/--db is required/);

      // Non-oplog single DB dump
      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({
          exitCode: 0,
          stdoutData: "raw-archive",
          stderrLines: ["done dumping mydb.users (42 documents)"],
        }),
      );
      const res1 = await mongodump({
        uri: "mongodb://localhost:27017",
        database: "mydb",
        excludeCollections: ["tmp"],
        parallelCollections: 4,
        readPreference: "secondaryPreferred",
        workDir,
      });
      const chunks1 = [];
      for await (const c of res1.stdout) chunks1.push(c);
      const done1 = await res1.done;
      expect(done1.collections).toEqual({ users: 42 });

      // Cluster dump with --oplog (ArchiveOplogTap + gzip)
      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({
          exitCode: 0,
          stdoutData: Buffer.alloc(0),
          stderrLines: ["done dumping app.orders (10 documents)"],
        }),
      );
      const res2 = await mongodump({
        uri: "mongodb://localhost:27017/?readPreference=primary",
        database: "*",
        oplog: true,
        workDir,
      });
      for await (const _ of res2.stdout) {
        // drain
      }
      const done2 = await res2.done;
      expect(done2.collections).toEqual({ "app.orders": 10 });
      expect(done2.oplogEntries).toBe(0);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("mongorestore validates flags, parses restored summary, and fails on failed docs", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-tools-res-"));
    try {
      await expect(
        mongorestore({
          uri: "mongodb://localhost",
          workDir,
          oplogLimit: "100:1",
          oplogReplay: false,
        }),
      ).rejects.toThrow(/--oplogLimit without --oplogReplay/);

      await expect(
        mongorestore({
          uri: "mongodb://localhost",
          workDir,
          oplogReplay: true,
          nsInclude: "app.*",
        }),
      ).rejects.toThrow(/--oplogReplay with namespace includes/);

      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({
          exitCode: 0,
          stderrLines: ["15 document(s) restored successfully. 0 document(s) failed."],
        }),
      );
      const r1 = await mongorestore({
        uri: "mongodb://localhost",
        workDir,
        nsInclude: "app.users, app.orders",
        nsExclude: "app.logs",
        systemNsExclude: ["admin.system.version"],
        drop: true,
        parallelCollections: 2,
      });
      r1.stdin.end();
      expect(await r1.done).toEqual({ restored: 15, failed: 0 });

      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({
          exitCode: 0,
          stderrLines: ["10 document(s) restored successfully. 2 document(s) failed."],
        }),
      );
      const r2 = await mongorestore({
        uri: "mongodb://localhost",
        workDir,
        nsFrom: "a.*",
        nsTo: "b.*",
        bypassDocumentValidation: false,
      });
      r2.stdin.end();
      await expect(r2.done).rejects.toThrow(/failed to restore 2 documents/);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("replayOplogFile and checkTool / checkDatabaseTools handle all branches", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-tools-rep-"));
    try {
      await expect(
        replayOplogFile({ uri: "mongodb://localhost", workDir, oplogFile: "" }),
      ).rejects.toThrow(/oplogFile is required/);

      mockSpawn.mockImplementationOnce(() => createFakeChild({ exitCode: 0 }));
      await replayOplogFile({
        uri: "mongodb://localhost",
        workDir,
        oplogFile: "/tmp/oplog.bson",
        oplogLimit: "100:1",
      });

      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({ exitCode: 0, stdoutData: "mongodump version 100.9.4\n" }),
      );
      const ok = await checkTool("mongodump");
      expect(ok).toEqual({ available: true, version: "100.9.4" });

      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({ exitCode: 1, stderrLines: ["bad flag"] }),
      );
      const fail = await checkTool("mongodump");
      expect(fail.available).toBe(false);

      const enoent = Object.assign(new Error("no bin"), { code: "ENOENT" });
      mockSpawn.mockImplementationOnce(() => createFakeChild({ emitError: enoent }));
      const missing = await checkTool("missing");
      expect(missing).toEqual({ available: false, error: "missing not found in PATH" });

      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({ exitCode: 0, stdoutData: "100.9.4" }),
      );
      mockSpawn.mockImplementationOnce(() =>
        createFakeChild({ exitCode: 0, stdoutData: "100.9.4" }),
      );
      const all = await checkDatabaseTools();
      expect(all.mongodump.available).toBe(true);
      expect(all.mongorestore.available).toBe(true);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
