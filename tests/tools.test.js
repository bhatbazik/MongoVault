import os from "node:os";
import { describe, expect, it } from "vitest";
import {
  isClusterDatabase,
  mongodump,
  mongorestore,
  parseDumpLine,
  parseRestoreLine,
} from "../src/tools.js";

describe("tools parsers", () => {
  it("parses mongodump completion line with standard, quoted, and spaced collection formats", () => {
    const line1 = "2026-03-10T12:00:00.000+0000\tdone dumping production.users (15420 documents)";
    expect(parseDumpLine(line1)).toEqual({
      database: "production",
      collection: "users",
      documents: 15420,
    });

    const line2 = `done dumping "production"."orders" (1,500,200 documents)`;
    expect(parseDumpLine(line2)).toEqual({
      database: "production",
      collection: "orders",
      documents: 1500200,
    });

    const line3 = `done dumping "production"."audit logs.v2" (42 documents)`;
    expect(parseDumpLine(line3)).toEqual({
      database: "production",
      collection: "audit logs.v2",
      documents: 42,
    });
  });

  it("parses mongorestore completion line with commas and counts", () => {
    const line = "2026-03-10T12:00:00.000+0000\t15,420 document(s) restored successfully. 0 document(s) failed to restore.";
    expect(parseRestoreLine(line)).toEqual({
      restored: 15420,
      failed: 0,
    });
  });

  it("identifies cluster-wide database scope", () => {
    expect(isClusterDatabase("*")).toBe(true);
    expect(isClusterDatabase("all")).toBe(true);
    expect(isClusterDatabase("")).toBe(true);
    expect(isClusterDatabase(null)).toBe(true);
    expect(isClusterDatabase("production")).toBe(false);
  });

  it("enforces official mongodump and mongorestore flag constraints", async () => {
    await expect(
      mongodump({
        uri: "mongodb://localhost:27017",
        database: "single_db",
        oplog: true,
        workDir: os.tmpdir(),
      }),
    ).rejects.toThrow("--oplog mode is only supported on full cluster dumps");

    await expect(
      mongodump({
        uri: "mongodb://localhost:27017",
        database: "*",
        excludeCollections: ["sessions"],
        workDir: os.tmpdir(),
      }),
    ).rejects.toThrow("--db is required when --excludeCollection is specified");

    await expect(
      mongorestore({
        uri: "mongodb://localhost:27017",
        workDir: os.tmpdir(),
        oplogReplay: false,
        oplogLimit: "1774092000:1",
      }),
    ).rejects.toThrow("cannot use --oplogLimit without --oplogReplay enabled");

    await expect(
      mongorestore({
        uri: "mongodb://localhost:27017",
        workDir: os.tmpdir(),
        oplogReplay: true,
        nsFrom: "prod.*",
        nsTo: "staging.*",
      }),
    ).rejects.toThrow("cannot use --oplogReplay with namespace includes or renames specified");
  });
});
