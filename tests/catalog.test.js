import { describe, expect, it } from "vitest";
import {
  archiveKey,
  databasePrefix,
  listBackups,
  manifestKey,
  parseCreatedAt,
  resolveBackup,
  writeManifest,
} from "../src/catalog.js";
import { generateKey, parseKey, verifyManifest } from "../src/crypto.js";

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

  it("signs unencrypted manifests via top-level signature property", async () => {
    const key = parseKey(generateKey());
    const saved = new Map();
    const fakeStorage = {
      async putJson(k, v) {
        saved.set(k, v);
      },
    };

    const manifest = {
      version: 2,
      key: "mongodb/production/2026-05-10/20260510T120000Z.archive.gz",
      database: "production",
      createdAt: "2026-05-10T12:00:00.000Z",
      completedAt: "2026-05-10T12:05:00.000Z",
      durationMs: 300000,
      size: 1024,
      sha256: "testsha",
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: { users: 50 },
    };

    await writeManifest(fakeStorage, manifest, key);

    const written = saved.get(manifestKey(manifest.key));
    expect(written).toBeDefined();
    expect(written.signature).toBeDefined();
    expect(written.signature.algorithm).toBe("HMAC-SHA256");
    expect(written.signature.hmac).toBeTypeOf("string");

    // verifyManifest validates the written manifest
    expect(verifyManifest(key, written)).toBe(true);
  });
});
