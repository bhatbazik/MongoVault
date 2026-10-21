import { describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";
import { generateKey } from "../src/crypto.js";

describe("config", () => {
  const minimalValidEnv = {
    MONGODB_URI: "mongodb://localhost:27017",
    MONGODB_DATABASE: "testdb",
    S3_BUCKET: "my-bucket",
    S3_ACCESS_KEY_ID: "key-id",
    S3_SECRET_ACCESS_KEY: "secret-key",
    S3_ENDPOINT: "https://s3.example.com",
  };

  it("loads minimal valid configuration with sensible defaults", () => {
    const config = loadConfig(minimalValidEnv);
    expect(config.mongo.database).toBe("testdb");
    expect(config.mongo.oplog).toBe(false);
    expect(config.mongo.parallelCollections).toBe(2);
    expect(config.storage.bucket).toBe("my-bucket");
    expect(config.storage.partSizeMb).toBe(32);
    expect(config.storage.queueSize).toBe(2);
    expect(config.schedule.cron).toBe("0 2 * * *");
    expect(config.schedule.timezone).toBe("UTC");
    expect(config.encryptionKey).toBeNull();
  });

  it("supports Cloudflare R2 endpoint derivation from account ID", () => {
    const { S3_ENDPOINT, ...rest } = minimalValidEnv;
    const config = loadConfig({ ...rest, R2_ACCOUNT_ID: "acct123" });
    expect(config.storage.endpoint).toBe("https://acct123.r2.cloudflarestorage.com");
    expect(config.storage.region).toBe("auto");
  });

  it("throws ConfigError on missing required variables", () => {
    expect(() => loadConfig({})).toThrow("Invalid configuration");
  });

  it("parses valid encryption key and rejects malformed", () => {
    const validKey = generateKey();
    const config = loadConfig({ ...minimalValidEnv, ENCRYPTION_KEY: validKey });
    expect(config.encryptionKey.length).toBe(32);

    expect(() => loadConfig({ ...minimalValidEnv, ENCRYPTION_KEY: "invalid-key" })).toThrow();
  });
});
