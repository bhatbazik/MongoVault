import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { describe, expect, it } from "vitest";
import { createFilesystemStorage } from "../src/storage.js";

describe("createFilesystemStorage", () => {
  it("uploads, heads, downloads, lists, and manages locks", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "fs-storage-test-"));
    const storage = createFilesystemStorage({ basePath: tmpDir });

    try {
      // 1. Upload from buffer
      await storage.upload("sub/file1.txt", Buffer.from("hello filesystem storage"));
      const head1 = await storage.head("sub/file1.txt");
      expect(head1).not.toBeNull();
      expect(head1?.size).toBe(Buffer.byteLength("hello filesystem storage"));

      // 2. Upload from stream
      const stream = Readable.from(["chunk 1 ", "chunk 2"]);
      await storage.upload("sub/file2.txt", stream);
      const head2 = await storage.head("sub/file2.txt");
      expect(head2?.size).toBe(15);

      // 3. Download
      const dlStream = await storage.download("sub/file1.txt");
      const chunks = [];
      for await (const chunk of dlStream) {
        chunks.push(Buffer.from(chunk));
      }
      expect(Buffer.concat(chunks).toString("utf8")).toBe("hello filesystem storage");

      // 4. JSON
      await storage.putJson("data.json", { message: "ok", count: 42 });
      const jsonRes = await storage.getJson("data.json");
      expect(jsonRes?.data).toEqual({ message: "ok", count: 42 });
      expect(jsonRes?.etag).toBeTruthy();

      // 5. List with prefix
      const listed = [];
      for await (const item of storage.list("sub/")) {
        listed.push(item.key);
      }
      expect(listed.sort()).toEqual(["sub/file1.txt", "sub/file2.txt"]);

      // 6. Lock acquisition, refresh, and release
      const token = await storage.acquireLock("locks/op.lock", { holder: "test:1", ttlMs: 5000 });
      expect(token).toBeTruthy();

      // Second acquire while valid should fail
      const token2 = await storage.acquireLock("locks/op.lock", {
        holder: "test:2",
        ttlMs: 5000,
      });
      expect(token2).toBeNull();

      // Refresh
      const refreshed = await storage.refreshLock("locks/op.lock", token, 10000);
      expect(refreshed).toBe(true);

      // Release
      await storage.releaseLock("locks/op.lock", token);
      const headLock = await storage.head("locks/op.lock");
      expect(headLock).toBeNull();

      // 7. Remove
      await storage.remove("sub/file1.txt");
      expect(await storage.head("sub/file1.txt")).toBeNull();
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("enforces atomic compare-and-set mutual exclusion between concurrent callers", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "fs-storage-lock-race-"));
    const storage1 = createFilesystemStorage({ basePath: tmpDir });
    const storage2 = createFilesystemStorage({ basePath: tmpDir });
    const storage3 = createFilesystemStorage({ basePath: tmpDir });

    try {
      // 3 processes concurrently try to acquire the same lock key
      const results = await Promise.all([
        storage1.acquireLock("shared.lock", { holder: "node-1", ttlMs: 10_000 }),
        storage2.acquireLock("shared.lock", { holder: "node-2", ttlMs: 10_000 }),
        storage3.acquireLock("shared.lock", { holder: "node-3", ttlMs: 10_000 }),
      ]);

      const winners = results.filter((token) => token !== null);
      // Strict mutual exclusion: exactly ONE caller must acquire the lock
      expect(winners).toHaveLength(1);

      // Other 2 must have failed
      const losers = results.filter((token) => token === null);
      expect(losers).toHaveLength(2);

      // Now test atomic stale lock takeover
      // Artificially expire the winner's lock
      const lockPath = path.join(tmpDir, "shared.lock");
      const record = JSON.parse(await fs.readFile(lockPath, "utf8"));
      record.expiresAt = Date.now() - 5000;
      await fs.writeFile(lockPath, JSON.stringify(record, null, 2));

      // Now node-2 and node-3 race to steal the expired lock
      const stealResults = await Promise.all([
        storage2.acquireLock("shared.lock", { holder: "node-2", ttlMs: 10_000 }),
        storage3.acquireLock("shared.lock", { holder: "node-3", ttlMs: 10_000 }),
      ]);

      const stealWinners = stealResults.filter((token) => token !== null);
      expect(stealWinners).toHaveLength(1);
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    }
  });
});
