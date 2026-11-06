import { describe, expect, it } from "vitest";
import { generateDiagnosticBundle } from "../src/diagnostic.js";

describe("diagnostic", () => {
  const config = {
    mongo: {
      uri: "mongodb://admin:superSecretPassword@mongo.production:27017/admin",
      database: "production",
      excludeCollections: [],
      oplog: true,
      parallelCollections: 2,
      readPreference: "secondaryPreferred",
    },
    storage: {
      type: "s3",
      bucket: "prod-vault-backups",
      prefix: "mongodb",
    },
    workDir: "/tmp/diagnostic-work",
  };

  it("generates sanitized diagnostic bundle and masks database credentials", async () => {
    const mockClient = {
      async connect() {},
      async close() {},
      db() {
        return {
          async command(cmd) {
            if (cmd.hello) return { isWritablePrimary: true, me: "mongo.production:27017" };
            if (cmd.buildInfo) return { version: "7.0.8" };
            return { ok: 1 };
          },
        };
      },
    };

    const mockStorage = {
      bucket: "prod-vault-backups",
      async *list() {
        yield { key: "mongodb/production/2026-03-10/20260310T020000Z.archive.gz", size: 100 };
        yield {
          key: "mongodb/production/2026-03-10/20260310T020000Z.archive.gz.manifest.json",
          size: 10,
        };
      },
      async getJson(k) {
        if (k.endsWith(".manifest.json")) {
          return { data: { key: k.replace(".manifest.json", ""), database: "production" } };
        }
        return null;
      },
    };

    const bundle = await generateDiagnosticBundle({
      config,
      storage: mockStorage,
      createMongoClient: () => mockClient,
    });

    expect(bundle.generatedAt).toBeTruthy();
    expect(bundle.system.nodeVersion).toBe(process.version);
    expect(bundle.system.platform).toBe(process.platform);

    // Verify URI password masking
    expect(bundle.database.maskedUri).not.toContain("superSecretPassword");
    expect(bundle.database.maskedUri).toContain("//***:***@");
    expect(bundle.database.connected).toBe(true);

    expect(bundle.storage.type).toBe("s3");
    expect(bundle.storage.totalBackups).toBe(1);
    expect(bundle.recentManifests).toHaveLength(1);
  });
});
