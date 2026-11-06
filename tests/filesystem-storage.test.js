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
});
