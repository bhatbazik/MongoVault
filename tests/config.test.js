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

  it("loads native AWS S3 configuration without S3_ENDPOINT", () => {
    const { S3_ENDPOINT, ...nativeAwsEnv } = minimalValidEnv;
    const config = loadConfig({ ...nativeAwsEnv, S3_REGION: "eu-west-2" });
    expect(config.storage.endpoint).toBeUndefined();
    expect(config.storage.region).toBe("eu-west-2");
    expect(config.storage.bucket).toBe("my-bucket");
  });

  it("defaults MONGODB_DATABASE to * for cluster-wide backup when omitted", () => {
    const { MONGODB_DATABASE, ...clusterEnv } = minimalValidEnv;
    const config = loadConfig(clusterEnv);
    expect(config.mongo.database).toBe("*");
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

  it("loads filesystem storage configuration without requiring S3 credentials", () => {
    const fsEnv = {
      MONGODB_URI: "mongodb://localhost:27017",
      MONGODB_DATABASE: "testdb",
      STORAGE_TYPE: "filesystem",
      STORAGE_PATH: "/var/backups/mongodb",
      MONGODB_READ_PREFERENCE: "secondary",
    };
    const config = loadConfig(fsEnv);
    expect(config.storage.type).toBe("filesystem");
    expect(config.storage.path).toBe("/var/backups/mongodb");
    expect(config.storage.bucket).toBe("/var/backups/mongodb");
    expect(config.mongo.readPreference).toBe("secondary");
  });

  it("rejects filesystem storage if STORAGE_PATH is omitted", () => {
    const fsEnv = {
      MONGODB_URI: "mongodb://localhost:27017",
      MONGODB_DATABASE: "testdb",
      STORAGE_TYPE: "filesystem",
    };
    expect(() => loadConfig(fsEnv)).toThrow("STORAGE_PATH is required");
  });

  it("rejects MONGODB_OPLOG with single database scope", () => {
    expect(() =>
      loadConfig({
        ...minimalValidEnv,
        MONGODB_DATABASE: "my_db",
        MONGODB_OPLOG: "true",
      }),
    ).toThrow("MONGODB_OPLOG can only be used with full cluster backup");
  });

  it("rejects MONGODB_EXCLUDE_COLLECTIONS with cluster-wide database scope", () => {
    expect(() =>
      loadConfig({
        ...minimalValidEnv,
        MONGODB_DATABASE: "*",
        MONGODB_EXCLUDE_COLLECTIONS: "sessions,logs",
      }),
    ).toThrow("MONGODB_EXCLUDE_COLLECTIONS cannot be used with full cluster backup");
  });

  it("parses and deduplicates ENCRYPTION_KEY_RING for key rotation", () => {
    const k1 = "a".repeat(64);
    const k2 = "b".repeat(64);
    const config = loadConfig({
      ...minimalValidEnv,
      ENCRYPTION_KEY: k1,
      ENCRYPTION_KEY_RING: `${k2}, ${k1}`,
    });
    expect(config.encryptionKeyRing.length).toBe(2);
  });
});
