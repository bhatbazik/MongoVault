import { Readable } from "node:stream";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { mockSend, mockUploadDone, mockUploadAbort } = vi.hoisted(() => ({
  mockSend: vi.fn(),
  mockUploadDone: vi.fn(async () => {}),
  mockUploadAbort: vi.fn(async () => {}),
}));

vi.mock("@aws-sdk/client-s3", () => {
  class S3Client {
    send = mockSend;
  }
  class PutObjectCommand {
    constructor(input) {
      this.input = input;
    }
  }
  class GetObjectCommand {
    constructor(input) {
      this.input = input;
    }
  }
  class HeadObjectCommand {
    constructor(input) {
      this.input = input;
    }
  }
  class DeleteObjectCommand {
    constructor(input) {
      this.input = input;
    }
  }
  class DeleteObjectsCommand {
    constructor(input) {
      this.input = input;
    }
  }
  async function* paginateListObjectsV2(_cfg, _params) {
    const res = await mockSend({ __paginate: true });
    yield res;
  }
  return {
    S3Client,
    PutObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    paginateListObjectsV2,
  };
});

vi.mock("@aws-sdk/lib-storage", () => {
  class Upload {
    constructor(opts) {
      this.opts = opts;
    }
    on = vi.fn();
    done = mockUploadDone;
    abort = mockUploadAbort;
  }
  return { Upload };
});

import { computeS3UploadParams, createStorage, S3StorageClient } from "../src/storage.js";

describe("S3StorageClient & Storage Factory", () => {
  beforeEach(() => {
    mockSend.mockReset();
    mockUploadDone.mockClear();
    mockUploadAbort.mockClear();
  });

  it("computeS3UploadParams scales partSize for 5 TiB and clamps queueSize on large parts", () => {
    const small = computeS3UploadParams(32 * 1024 * 1024, 4, 100 * 1024 * 1024);
    expect(small).toEqual({ partSize: 32 * 1024 * 1024, queueSize: 4 });

    const huge = computeS3UploadParams(32 * 1024 * 1024, 4, 2 * 1024 ** 4); // 2 TiB
    expect(huge.partSize).toBeGreaterThanOrEqual(128 * 1024 * 1024);
    expect(huge.queueSize).toBe(1);
  });

  it("createStorage selects filesystem or S3 appropriately", () => {
    const fsClient = createStorage({ type: "filesystem", path: "/tmp/mv-test" });
    expect(fsClient.bucket).toContain("mv-test");

    const s3Client = createStorage({
      type: "s3",
      region: "us-east-1",
      bucket: "bkt",
      endpoint: "http://127.0.0.1:9000",
      objectLock: { enabled: true, mode: "COMPLIANCE", retentionDays: 7, legalHold: true },
    });
    expect(s3Client).toBeInstanceOf(S3StorageClient);
    expect(s3Client.bucket).toBe("bkt");
  });

  it("upload, head, putJson, getJson, list, remove, removeMany", async () => {
    const s3 = new S3StorageClient({
      region: "us-east-1",
      bucket: "bkt",
      objectLock: { enabled: true, mode: "GOVERNANCE", retentionDays: 5, legalHold: true },
    });

    const ac = new AbortController();
    ac.abort();
    await s3.upload("app/b1.gz", Buffer.from("abc"), {
      signal: ac.signal,
      onProgress: vi.fn(),
      expectedSizeBytes: 1024,
    });
    expect(mockUploadDone).toHaveBeenCalledTimes(1);
    expect(mockUploadAbort).toHaveBeenCalled();

    // head 200 and 404
    mockSend.mockResolvedValueOnce({
      ContentLength: 123,
      LastModified: new Date(),
      Metadata: { a: "1" },
    });
    expect((await s3.head("k"))?.size).toBe(123);

    const notFound = Object.assign(new Error("404"), { name: "NoSuchKey" });
    mockSend.mockRejectedValueOnce(notFound);
    expect(await s3.head("missing")).toBeNull();

    // putJson mutable vs immutable
    mockSend.mockClear();
    mockSend.mockResolvedValue({});
    await s3.putJson("app/state.json", { ok: true });
    expect(mockSend.mock.calls[0][0].input.ObjectLockMode).toBeUndefined();

    await s3.putJson("app/b1.manifest.json", { ok: true });
    expect(mockSend.mock.calls[1][0].input.ObjectLockMode).toBe("GOVERNANCE");

    // getJson 200 & 404
    mockSend.mockResolvedValueOnce({
      Body: { transformToString: async () => '{"v":1}' },
      ETag: '"etag1"',
    });
    expect(await s3.getJson("k")).toEqual({ data: { v: 1 }, etag: "etag1" });

    mockSend.mockRejectedValueOnce(notFound);
    expect(await s3.getJson("missing")).toBeNull();

    // list
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "app/1", Size: 10, LastModified: new Date() }],
    });
    const listed = [];
    for await (const obj of s3.list("app/")) listed.push(obj);
    expect(listed.length).toBe(1);

    // remove & removeMany
    mockSend.mockResolvedValueOnce({});
    await s3.remove("app/1");

    await s3.removeMany([]);
    mockSend.mockResolvedValueOnce({ Errors: [] });
    await s3.removeMany(["app/1", "app/2"]);

    mockSend.mockResolvedValueOnce({
      Errors: [{ Key: "app/1", Message: "AccessDenied" }],
    });
    await expect(s3.removeMany(["app/1"])).rejects.toThrow(/S3 batch delete failed/);
  });

  it("download handles direct stream and Range-resumable stream across disconnects", async () => {
    const s3 = new S3StorageClient({ region: "us-east-1", bucket: "bkt" });

    // Direct stream when no ETag
    mockSend.mockResolvedValueOnce({ Body: Readable.from([Buffer.from("direct")]) });
    const s1 = await s3.download("k1");
    const c1 = [];
    for await (const ch of s1) c1.push(ch);
    expect(Buffer.concat(c1).toString()).toBe("direct");

    // Resumable stream: first stream emits 5 bytes then errors, second emits remaining 5 bytes
    async function* part1() {
      yield Buffer.from("hello");
      throw new Error("ECONNRESET");
    }
    async function* part2() {
      yield Buffer.from("world");
    }
    mockSend
      .mockResolvedValueOnce({ Body: Readable.from(part1()), ETag: '"e1"', ContentLength: 10 })
      .mockResolvedValueOnce({ Body: Readable.from(part2()), ETag: '"e1"', ContentLength: 5 });

    const s2 = await s3.download("k2");
    const c2 = [];
    for await (const ch of s2) c2.push(ch);
    expect(Buffer.concat(c2).toString()).toBe("helloworld");
  });

  it("distributed lock acquire, refresh, and release handle preconditions and 501 fallback", async () => {
    const s3 = new S3StorageClient({ region: "us-east-1", bucket: "bkt" });
    const notFound = Object.assign(new Error("404"), { name: "NoSuchKey" });
    const precond = Object.assign(new Error("412"), { name: "PreconditionFailed" });
    const notImpl = Object.assign(new Error("501"), { name: "NotImplemented" });

    // acquire: fresh lock
    mockSend.mockRejectedValueOnce(notFound).mockResolvedValueOnce({});
    const token = await s3.acquireLock("lock.json", { holder: "h1", ttlMs: 10000 });
    expect(typeof token).toBe("string");

    // acquire: active lock returns null
    mockSend.mockResolvedValueOnce({
      Body: {
        transformToString: async () =>
          JSON.stringify({ token: "t", expiresAt: Date.now() + 60000 }),
      },
      ETag: '"e1"',
    });
    expect(await s3.acquireLock("lock.json", { holder: "h2", ttlMs: 10000 })).toBeNull();

    // acquire: precondition failed returns null
    mockSend.mockRejectedValueOnce(notFound).mockRejectedValueOnce(precond);
    expect(await s3.acquireLock("lock.json", { holder: "h3", ttlMs: 10000 })).toBeNull();

    // refresh: success & precondition fail
    mockSend
      .mockResolvedValueOnce({
        Body: {
          transformToString: async () => JSON.stringify({ token, expiresAt: Date.now() + 5000 }),
        },
        ETag: '"e1"',
      })
      .mockResolvedValueOnce({});
    expect(await s3.refreshLock("lock.json", token, 10000)).toBe(true);

    mockSend
      .mockResolvedValueOnce({
        Body: {
          transformToString: async () => JSON.stringify({ token, expiresAt: Date.now() + 5000 }),
        },
        ETag: '"e1"',
      })
      .mockRejectedValueOnce(precond);
    expect(await s3.refreshLock("lock.json", token, 10000)).toBe(false);

    // release: 501 fallback to unconditional remove
    mockSend
      .mockResolvedValueOnce({
        Body: {
          transformToString: async () => JSON.stringify({ token, expiresAt: Date.now() + 5000 }),
        },
        ETag: '"e1"',
      })
      .mockRejectedValueOnce(notImpl)
      .mockResolvedValueOnce({});
    await s3.releaseLock("lock.json", token);
    expect(mockSend).toHaveBeenCalledTimes(12);
  });
});
