import { describe, expect, it } from "vitest";
import { deleteBackup, prune } from "../src/retention.js";

describe("S3 Object Lock & WORM Compliance", () => {
  it("prevents manual deletion of backups protected under S3 WORM Object Lock", async () => {
    const mockStorage = {
      bucket: "worm-bucket",
      async *list() {
        yield { key: "mongodb/prod/2026-03-10/20260310T020000Z.archive.gz", size: 1000 };
      },
      async head(_key) {
        return { size: 1000, metadata: {} };
      },
      async getJson() {
        return null;
      },
      async remove(_key) {
        const error = new Error("Object is locked under S3 WORM Compliance Mode");
        error.name = "AccessDenied";
        throw error;
      },
    };

    const mockLogger = {
      child() {
        return this;
      },
      info() {},
      debug() {},
      warn() {},
      error() {},
    };

    const config = {
      mongo: { database: "prod" },
      storage: { prefix: "mongodb", bucket: "worm-bucket" },
      schedule: { timezone: "UTC" },
    };

    await expect(
      deleteBackup(
        { config, storage: mockStorage, logger: mockLogger },
        { key: "mongodb/prod/2026-03-10/20260310T020000Z.archive.gz", force: true },
      ),
    ).rejects.toThrow(/protected by active S3 WORM/i);
  });

  it("safely skips WORM protected objects during retention prune runs", async () => {
    const warnings = [];
    const mockStorage = {
      bucket: "worm-bucket",
      async *list() {
        yield {
          key: "mongodb/prod/2026-03-01/20260301T020000Z.archive.gz.manifest.json",
          size: 100,
        };
        yield {
          key: "mongodb/prod/2026-03-01/20260301T020000Z.archive.gz",
          size: 5000,
        };
        yield {
          key: "mongodb/prod/2026-03-10/20260310T020000Z.archive.gz.manifest.json",
          size: 100,
        };
        yield {
          key: "mongodb/prod/2026-03-10/20260310T020000Z.archive.gz",
          size: 5000,
        };
      },
      async remove(_key) {
        const err = new Error("Object Lock active retention period has not expired");
        err.name = "AccessDenied";
        throw err;
      },
    };

    const mockLogger = {
      child() {
        return this;
      },
      info() {},
      debug() {},
      warn(obj, msg) {
        warnings.push({ obj, msg });
      },
      error() {},
    };

    const config = {
      mongo: { database: "prod" },
      storage: { prefix: "mongodb", bucket: "worm-bucket" },
      schedule: { timezone: "UTC" },
      retention: { keepLast: 1 },
    };

    const result = await prune({ config, storage: mockStorage, logger: mockLogger });
    expect(result.remove.length).toBeGreaterThan(0);
    expect(warnings.length).toBeGreaterThan(0);
    expect(warnings[0].msg).toMatch(/S3 WORM Object Lock/);
  });
});
