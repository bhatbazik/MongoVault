import pino from "pino";
import { describe, expect, it } from "vitest";
import { planRetention, prune } from "../src/retention.js";

describe("retention", () => {
  function makeBackup(iso) {
    return {
      key: `mongodb/prod/${iso.slice(0, 10)}/${iso.replace(/[-:]/g, "")}.archive.gz`,
      createdAt: new Date(iso),
    };
  }

  it("always keeps at least the newest backup", () => {
    const b1 = makeBackup("2026-01-01T00:00:00Z");
    const plan = planRetention([b1], { keepLast: 0 }, "UTC");
    expect(plan.keep).toContain(b1);
    expect(plan.remove).toHaveLength(0);
  });

  it("keeps configured count of daily, weekly and monthly backups", () => {
    const backups = [
      makeBackup("2026-03-01T02:00:00Z"),
      makeBackup("2026-03-01T14:00:00Z"),
      makeBackup("2026-03-02T02:00:00Z"),
      makeBackup("2026-03-03T02:00:00Z"),
      makeBackup("2026-03-10T02:00:00Z"),
      makeBackup("2026-03-17T02:00:00Z"),
      makeBackup("2026-04-01T02:00:00Z"),
    ];

    const plan = planRetention(
      backups,
      { keepLast: 2, keepDaily: 5, keepWeekly: 2, keepMonthly: 2 },
      "UTC",
    );

    expect(plan.keep.map((b) => b.createdAt.toISOString())).toEqual(
      expect.arrayContaining([
        "2026-04-01T02:00:00.000Z",
        "2026-03-17T02:00:00.000Z",
        "2026-03-10T02:00:00.000Z",
        "2026-03-03T02:00:00.000Z",
        "2026-03-02T02:00:00.000Z",
      ]),
    );

    expect(plan.remove.map((b) => b.createdAt.toISOString())).toContain("2026-03-01T02:00:00.000Z");
  });

  it("uses batch deletion (removeMany) during prune execution", async () => {
    const deletedKeys = [];
    const mockStorage = {
      async *list() {
        yield { key: "mongodb/prod/2026-03-01/20260301T000000Z.archive.gz", size: 100 };
        yield {
          key: "mongodb/prod/2026-03-01/20260301T000000Z.archive.gz.manifest.json",
          size: 10,
        };
        yield { key: "mongodb/prod/2026-03-02/20260302T000000Z.archive.gz", size: 100 };
        yield {
          key: "mongodb/prod/2026-03-02/20260302T000000Z.archive.gz.manifest.json",
          size: 10,
        };
      },
      async removeMany(keys) {
        deletedKeys.push(...keys);
      },
    };

    const config = {
      mongo: { database: "prod" },
      storage: { prefix: "mongodb" },
      schedule: { timezone: "UTC" },
      retention: { keepLast: 1 },
    };

    const logger = pino({ level: "silent" });
    const result = await prune({ config, storage: mockStorage, logger });

    expect(result.keep).toHaveLength(1);
    expect(result.remove).toHaveLength(1);
    expect(deletedKeys).toContain("mongodb/prod/2026-03-01/20260301T000000Z.archive.gz");
    expect(deletedKeys).toContain(
      "mongodb/prod/2026-03-01/20260301T000000Z.archive.gz.manifest.json",
    );
  });
});
