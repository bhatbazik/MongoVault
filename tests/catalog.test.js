import { describe, expect, it } from "vitest";
import {
  archiveKey,
  databasePrefix,
  listBackups,
  manifestKey,
  parseCreatedAt,
  resolveBackup,
} from "../src/catalog.js";

describe("catalog", () => {
  const scope = { prefix: "mongodb", database: "production" };

  it("builds canonical database prefix and archive key", () => {
    expect(databasePrefix(scope)).toBe("mongodb/production/");

    const date = new Date("2026-05-10T14:30:00Z");
    const key = archiveKey(scope, date, { encrypted: false });
    expect(key).toBe("mongodb/production/2026-05-10/20260510T143000Z.archive.gz");

    const encKey = archiveKey(scope, date, { encrypted: true });
    expect(encKey).toBe("mongodb/production/2026-05-10/20260510T143000Z.archive.gz.enc");
    expect(manifestKey(key)).toBe(`${key}.manifest.json`);
  });

  it("parses creation timestamp from object key", () => {
    const key = "mongodb/production/2026-05-10/20260510T143000Z.archive.gz";
    const parsed = parseCreatedAt(key);
    expect(parsed.toISOString()).toBe("2026-05-10T14:30:00.000Z");
  });

  it("lists and sorts backups, resolving latest complete", async () => {
    const objects = [
      { key: "mongodb/production/2026-05-10/20260510T100000Z.archive.gz", size: 100 },
      { key: "mongodb/production/2026-05-10/20260510T100000Z.archive.gz.manifest.json", size: 10 },
      { key: "mongodb/production/2026-05-10/20260510T120000Z.archive.gz", size: 200 },
      { key: "mongodb/production/2026-05-10/20260510T120000Z.archive.gz.manifest.json", size: 10 },
      { key: "mongodb/production/2026-05-10/20260510T140000Z.archive.gz", size: 150 },
    ];

    const fakeStorage = {
      async *list() {
        for (const obj of objects) yield obj;
      },
    };

    const backups = await listBackups(fakeStorage, scope);
    expect(backups.length).toBe(3);
    expect(backups[0].key).toContain("140000Z");
    expect(backups[0].complete).toBe(false);
    expect(backups[1].complete).toBe(true);

    const latest = await resolveBackup(fakeStorage, scope, "latest");
    expect(latest).toBe("mongodb/production/2026-05-10/20260510T120000Z.archive.gz");
  });
});
