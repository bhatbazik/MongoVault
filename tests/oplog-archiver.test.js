import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { incrementOplogTimestamp, parsePitrTarget, startOplogArchiver } from "../src/oplog.js";

describe("Oplog Archiver & Timestamp Helpers (oplog.ts)", () => {
  it("parsePitrTarget & incrementOplogTimestamp handle ts:inc, ISO dates, and invalid values", () => {
    expect(parsePitrTarget("1700000000:5")).toEqual({ t: 1700000000, i: 5 });
    expect(parsePitrTarget("2026-01-01T00:00:10Z")).toEqual({
      t: Math.floor(new Date("2026-01-01T00:00:10Z").getTime() / 1000),
      i: 0xffffffff,
    });
    expect(() => parsePitrTarget("invalid-date")).toThrow();

    expect(incrementOplogTimestamp({ t: 100, i: 1 })).toEqual({ t: 100, i: 2 });
    expect(incrementOplogTimestamp({ t: 100, i: 0xffffffff })).toEqual({ t: 101, i: 0 });
  });

  it("startOplogArchiver starts, runs ticks, notifies on failure, and stops cleanly", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-archiver-"));
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const notifier = { notifyFailure: vi.fn(async () => {}) };
    const storage = {
      acquireLock: vi.fn(async () => "tok1"),
      refreshLock: vi.fn(async () => true),
      releaseLock: vi.fn(async () => {}),
    };

    try {
      const archiver = startOplogArchiver({
        config: {
          workDir,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          pitr: { enabled: true, flushIntervalSeconds: 60, maxSliceMb: 16 },
        },
        storage,
        logger,
        notifier,
        createMongoClient: () => ({
          connect: vi.fn(async () => {
            throw new Error("mongo unreachable");
          }),
          close: vi.fn(async () => {}),
        }),
      });

      archiver.start();
      await vi.waitFor(() =>
        expect(notifier.notifyFailure).toHaveBeenCalledWith("pitr-oplog", expect.any(Error)),
      );
      await archiver.stop();

      expect(storage.releaseLock).toHaveBeenCalled();

      // When lock is held by another process
      const lockedStorage = {
        acquireLock: vi.fn(async () => null),
        releaseLock: vi.fn(async () => {}),
      };
      const archiver2 = startOplogArchiver({
        config: {
          workDir,
          storage: { prefix: "mv" },
          mongo: { uri: "mongodb://localhost:27017", database: "*" },
          pitr: { enabled: true, flushIntervalSeconds: 60 },
        },
        storage: lockedStorage,
        logger,
      });
      archiver2.start();
      await vi.waitFor(() => expect(logger.debug).toHaveBeenCalled());
      await archiver2.stop();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
