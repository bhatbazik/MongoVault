import { describe, expect, it } from "vitest";
import { formatBytes, formatDate, formatDuration } from "../src/format.js";

describe("format", () => {
  it("formats bytes accurately", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(1024)).toBe("1.0 KB");
    expect(formatBytes(10 * 1024 * 1024)).toBe("10.0 MB");
    expect(formatBytes(1.5 * 1024 * 1024 * 1024)).toBe("1.5 GB");
  });

  it("formats duration accurately", () => {
    expect(formatDuration(400)).toBe("0s");
    expect(formatDuration(45_000)).toBe("45s");
    expect(formatDuration(125_000)).toBe("2m 5s");
    expect(formatDuration(3665_000)).toBe("1h 1m");
  });

  it("formats date in target timezone", () => {
    const date = new Date("2026-03-15T12:00:00Z");
    const formattedUtc = formatDate(date, "UTC");
    expect(formattedUtc).toBe("2026-03-15 12:00:00");

    const formattedIst = formatDate(date, "Asia/Kolkata");
    expect(formattedIst).toBe("2026-03-15 17:30:00");
  });
});
