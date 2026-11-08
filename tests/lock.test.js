import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { acquireDistributedLock, acquireHybridLock, acquireLock, readLock } from "../src/lock.js";

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
      JSON.stringify({
        pid: process.pid,
        token: "other-token",
        acquiredAt: new Date().toISOString(),
      }),
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

  it("releases local lock and rethrows if distributed lock fails with storage error", async () => {
    const failingStorage = {
      async acquireLock() {
        throw new Error("S3 network failure");
      },
    };

    await expect(
      acquireHybridLock({
        file: lockFile,
        storage: failingStorage,
        lockKey: "prefix/.locks/test.lock.json",
        staleAfterMs: 60_000,
      }),
    ).rejects.toThrow("S3 network failure");

    expect(await readLock(lockFile)).toBeNull();
  });

  it("does not steal lock while owning process is alive and active even after staleAfterMs elapsed", async () => {
    // Acquire lock with short staleAfterMs and rapid heartbeat
    const release = await acquireLock(lockFile, {
      staleAfterMs: 50,
      heartbeatIntervalMs: 15,
    });
    expect(release).toBeTypeOf("function");

    // Wait past staleAfterMs while heartbeat keeps updating mtime
    await new Promise((r) => setTimeout(r, 80));

    // Another process attempting to acquire must NOT be able to steal the lock
    const secondAcquisition = await acquireLock(lockFile, { staleAfterMs: 50 });
    expect(secondAcquisition).toBeNull();

    await release();
    expect(await readLock(lockFile)).toBeNull();
  });

  it("triggers onLost callback when distributed lease renewal returns false", async () => {
    let leaseRenewalsAllowed = true;
    let lostError = null;

    const mockStorage = {
      async acquireLock() {
        return "token-xyz";
      },
      async refreshLock() {
        return leaseRenewalsAllowed;
      },
      async releaseLock() {},
    };

    const release = await acquireDistributedLock(mockStorage, "test/lock.json", {
      ttlMs: 200,
      heartbeatIntervalMs: 20,
      onLost: (err) => {
        lostError = err;
      },
    });

    expect(release).toBeTypeOf("function");

    // Simulate another node stealing the lock so refresh returns false
    leaseRenewalsAllowed = false;

    // Wait for heartbeat tick
    await new Promise((r) => setTimeout(r, 60));

    expect(lostError).not.toBeNull();
    expect(lostError.message).toMatch(/lease renewal was rejected/i);

    await release();
  });

  it("triggers onLost callback when distributed lease renewals throw past ttlMs", async () => {
    let lostError = null;

    const mockStorage = {
      async acquireLock() {
        return "token-abc";
      },
      async refreshLock() {
        throw new Error("S3 503 SlowDown");
      },
      async releaseLock() {},
    };

    const release = await acquireDistributedLock(mockStorage, "test/lock.json", {
      ttlMs: 50,
      heartbeatIntervalMs: 15,
      onLost: (err) => {
        lostError = err;
      },
    });

    expect(release).toBeTypeOf("function");

    // Wait until TTL expires without successful heartbeat
    await new Promise((r) => setTimeout(r, 90));

    expect(lostError).not.toBeNull();
    expect(lostError.message).toMatch(/lease expired/i);

    await release();
  });

  it("triggers onLost callback when local lock file is modified or removed", async () => {
    let lostError = null;

    const release = await acquireLock(lockFile, {
      staleAfterMs: 60_000,
      heartbeatIntervalMs: 20,
      onLost: (err) => {
        lostError = err;
      },
    });

    expect(release).toBeTypeOf("function");

    // Delete the lock file behind the owner's back
    await fs.rm(lockFile, { force: true });

    // Wait for local heartbeat tick
    await new Promise((r) => setTimeout(r, 60));

    expect(lostError).not.toBeNull();
    expect(lostError.message).toMatch(/lock file was modified or removed/i);

    await release();
  });
});
