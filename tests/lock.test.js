import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { acquireLock, readLock } from "../src/lock.js";

describe("lock", () => {
  let tmpDir;
  let lockFile;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "lock-test-"));
    lockFile = path.join(tmpDir, ".backup.lock");
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  it("acquires lock and releases via returned callback", async () => {
    const release = await acquireLock(lockFile, { staleAfterMs: 60_000 });
    expect(release).toBeTypeOf("function");

    const content = await readLock(lockFile);
    expect(content.pid).toBe(process.pid);
    expect(content.token).toBeTypeOf("string");

    const secondAcquisition = await acquireLock(lockFile, { staleAfterMs: 60_000 });
    expect(secondAcquisition).toBeNull();

    await release();
    expect(await readLock(lockFile)).toBeNull();

    const thirdAcquisition = await acquireLock(lockFile, { staleAfterMs: 60_000 });
    expect(thirdAcquisition).toBeTypeOf("function");
    await thirdAcquisition();
  });

  it("does not delete lock if another process acquired it", async () => {
    const releaseA = await acquireLock(lockFile, { staleAfterMs: 60_000 });

    await fs.writeFile(
      lockFile,
      JSON.stringify({ pid: process.pid, token: "other-token", acquiredAt: new Date().toISOString() }),
    );

    await releaseA();
    const remaining = await readLock(lockFile);
    expect(remaining?.token).toBe("other-token");
  });

  it("recovers from stale lock when PID is dead", async () => {
    await fs.writeFile(
      lockFile,
      JSON.stringify({ pid: 9999999, token: "dead-token", acquiredAt: new Date().toISOString() }),
    );

    const release = await acquireLock(lockFile, { staleAfterMs: 60_000 });
    expect(release).toBeTypeOf("function");
    await release();
  });
});
