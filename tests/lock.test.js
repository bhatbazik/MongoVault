import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  acquireDistributedLock,
  acquireHybridLock,
  acquireLock,
  readLock,
} from "../src/lock.js";

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

  it("supports distributed S3 locking and contention", async () => {
    const s3State = new Map();
    const mockStorage = {
      async acquireLock(key, { holder, ttlMs }) {
        const current = s3State.get(key);
        if (current && current.expiresAt > Date.now()) return null;
        const token = "dist-token-123";
        s3State.set(key, { token, holder, expiresAt: Date.now() + ttlMs });
        return token;
      },
      async refreshLock(key, token, ttlMs) {
        const current = s3State.get(key);
        if (current?.token === token) {
          current.expiresAt = Date.now() + ttlMs;
          return true;
        }
        return false;
      },
      async releaseLock(key, token) {
        const current = s3State.get(key);
        if (current?.token === token) s3State.delete(key);
      },
    };

    const release1 = await acquireDistributedLock(mockStorage, "prefix/.locks/db.lock.json", {
      ttlMs: 60_000,
      heartbeatIntervalMs: 10_000,
    });
    expect(release1).toBeTypeOf("function");

    // Second acquisition should fail while first is active
    const release2 = await acquireDistributedLock(mockStorage, "prefix/.locks/db.lock.json", {
      ttlMs: 60_000,
    });
    expect(release2).toBeNull();

    await release1();

    // Now second acquisition succeeds
    const release3 = await acquireDistributedLock(mockStorage, "prefix/.locks/db.lock.json", {
      ttlMs: 60_000,
    });
    expect(release3).toBeTypeOf("function");
    await release3();
  });

  it("acquires hybrid local and distributed lock", async () => {
    const s3State = new Map();
    const mockStorage = {
      async acquireLock(key, { holder, ttlMs }) {
        const current = s3State.get(key);
        if (current && current.expiresAt > Date.now()) return null;
        const token = "hybrid-token";
        s3State.set(key, { token, holder, expiresAt: Date.now() + ttlMs });
        return token;
      },
      async releaseLock(key) {
        s3State.delete(key);
      },
    };

    const release = await acquireHybridLock({
      file: lockFile,
      storage: mockStorage,
      lockKey: "prefix/.locks/test.lock.json",
      staleAfterMs: 60_000,
    });
    expect(release).toBeTypeOf("function");

    expect(await readLock(lockFile)).not.toBeNull();
    expect(s3State.get("prefix/.locks/test.lock.json")).toBeDefined();

    await release();
    expect(await readLock(lockFile)).toBeNull();
    expect(s3State.get("prefix/.locks/test.lock.json")).toBeUndefined();
  });
});
