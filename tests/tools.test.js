import { describe, expect, it } from "vitest";
import { parseDumpLine, parseRestoreLine } from "../src/tools.js";

describe("tools parsers", () => {
  it("parses mongodump completion line", () => {
    const line = "2026-03-10T12:00:00.000+0000\tdone dumping production.users (15420 documents)";
    expect(parseDumpLine(line)).toEqual({
      database: "production",
      collection: "users",
      documents: 15420,
    });
  });

  it("parses mongorestore completion line", () => {
    const line = "2026-03-10T12:00:00.000+0000\t15420 document(s) restored successfully. 0 document(s) failed to restore.";
    expect(parseRestoreLine(line)).toEqual({
      restored: 15420,
      failed: 0,
    });
  });
});
