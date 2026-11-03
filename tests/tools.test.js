import { describe, expect, it } from "vitest";
import { isClusterDatabase, parseDumpLine, parseRestoreLine } from "../src/tools.js";

describe("tools parsers", () => {
  it("parses mongodump completion line with standard and quoted formats", () => {
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
});
