import { PassThrough, Readable, Writable } from "node:stream";
import { describe, expect, it } from "vitest";
import { linkedController, settleAll } from "../src/abort.js";
import { databasePrefix, scopeOf } from "../src/catalog.js";
import { formatBytes, formatDate, formatDuration } from "../src/format.js";
import { createLogger } from "../src/logger.js";
import { runStreamPipeline } from "../src/stream.js";

describe("Core utilities (abort, stream, format, logger, catalog)", () => {
  describe("abort.ts", () => {
    it("linkedController forwards parent abort, supports already-aborted parent, and dispose", () => {
      const parent1 = new AbortController();
      parent1.abort("pre-aborted");
      const linked1 = linkedController(parent1.signal);
      expect(linked1.signal.aborted).toBe(true);
      expect(linked1.signal.reason).toBe("pre-aborted");

      const parent2 = new AbortController();
      const linked2 = linkedController(parent2.signal);
      expect(linked2.signal.aborted).toBe(false);
      parent2.abort("later");
      expect(linked2.signal.aborted).toBe(true);
      expect(linked2.signal.reason).toBe("later");

      const parent3 = new AbortController();
      const linked3 = linkedController(parent3.signal);
      linked3.dispose();
      parent3.abort("ignored");
      expect(linked3.signal.aborted).toBe(false);

      linked3.abort("manual");
      expect(linked3.signal.aborted).toBe(true);
      expect(linked3.signal.reason).toBe("manual");
    });

    it("settleAll resolves all tasks and aborts controller if any rejects", async () => {
      const ctrl1 = linkedController();
      const res = await settleAll(ctrl1, [Promise.resolve(1), Promise.resolve("ok")]);
      expect(res).toEqual([1, "ok"]);
      expect(ctrl1.signal.aborted).toBe(false);

      const ctrl2 = linkedController();
      await expect(
        settleAll(ctrl2, [Promise.resolve(1), Promise.reject(new Error("boom"))]),
      ).rejects.toThrow("boom");
      expect(ctrl2.signal.aborted).toBe(true);
    });
  });

  describe("stream.ts", () => {
    it("runs variadic pipelines filtering falsy stages and supports AbortSignal", async () => {
      const chunks = [];
      const src = Readable.from(["hello", " ", "world"]);
      const pass = new PassThrough();
      const dst = new Writable({
        write(chunk, _enc, cb) {
          chunks.push(chunk.toString());
          cb();
        },
      });

      await runStreamPipeline([src, null, false, pass, undefined, dst]);
      expect(chunks.join("")).toBe("hello world");

      await expect(runStreamPipeline([Readable.from(["a"])])).rejects.toThrow(
        /requires at least 2 active stages/,
      );

      const ac = new AbortController();
      ac.abort(new Error("aborted"));
      await expect(
        runStreamPipeline([Readable.from(["a"]), new PassThrough()], { signal: ac.signal }),
      ).rejects.toThrow();
    });
  });

  describe("format.ts", () => {
    it("formats bytes, durations, and dates including invalid dates", () => {
      expect(formatBytes(0)).toBe("0 B");
      expect(formatBytes(-5)).toBe("0 B");
      expect(formatBytes(Number.NaN)).toBe("0 B");
      expect(formatBytes(512)).toBe("512 B");
      expect(formatBytes(1024 * 1024 * 2.5)).toBe("2.5 MB");

      expect(formatDuration(5000)).toBe("5s");
      expect(formatDuration(125000)).toBe("2m 5s");
      expect(formatDuration(3665000)).toBe("1h 1m");

      expect(formatDate("not-a-date")).toBe("not-a-date");
      expect(formatDate(new Date("2026-01-15T10:20:30Z"), "UTC")).toBe("2026-01-15 10:20:30");
    });
  });

  describe("logger.ts", () => {
    it("creates json and pretty pino loggers", () => {
      const jsonLogger = createLogger({ level: "debug", pretty: false });
      expect(typeof jsonLogger.info).toBe("function");
      jsonLogger.info("test json log");

      const prettyLogger = createLogger({ level: "warn", pretty: true });
      expect(typeof prettyLogger.warn).toBe("function");
    });
  });

  describe("catalog.ts edge cases", () => {
    it("handles databasePrefix and scopeOf", () => {
      expect(
        scopeOf({
          storage: { prefix: "mv" },
          mongo: { database: "mydb" },
        }),
      ).toEqual({ prefix: "mv", database: "mydb" });
      expect(databasePrefix({ prefix: "", database: "*" })).toBe("_cluster/");
    });
  });
});
