import { describe, expect, it } from "vitest";
import { createSnapshotDriver, runCloudSnapshot } from "../src/snapshot.js";

describe("cloud volume snapshot engine", () => {
  it("creates and reports snapshot status with cloud snapshot driver", async () => {
    const driver = createSnapshotDriver({
      provider: "aws-ebs",
      volumeId: "vol-0a1b2c3d4e5f",
      region: "us-east-1",
    });

    const res = await driver.createSnapshot({
      volumeId: "vol-0a1b2c3d4e5f",
      description: "Test snapshot",
    });

    expect(res.snapshotId.startsWith("snap-")).toBe(true);
    expect(res.status).toBe("completed");

    const status = await driver.getSnapshotStatus(res.snapshotId);
    expect(status.status).toBe("completed");
  });

  it("orchestrates cloud snapshot under MongoDB fsyncLock", async () => {
    let locked = false;
    let unlocked = false;

    const mockClient = {
      async connect() {},
      async close() {},
      db(name) {
        if (name === "admin") {
          return {
            async command(cmd) {
              if (cmd.fsync) {
                locked = true;
                return { ok: 1 };
              }
              if (cmd.fsyncUnlock) {
                unlocked = true;
                return { ok: 1 };
              }
              if (cmd.replSetGetStatus) {
                return {
                  optimes: {
                    lastCommittedOpTime: { ts: { t: 1774092000, i: 1 } },
                  },
                };
              }
              return { ok: 1 };
            },
          };
        }
        return {};
      },
    };

    let uploadedManifest = null;
    const mockStorage = {
      bucket: "test-bucket",
      async putJson(key, val) {
        uploadedManifest = val;
      },
    };

    const mockLogger = {
      child() {
        return this;
      },
      info() {},
      debug() {},
      warn() {},
      error() {},
    };

    const config = {
      mongo: { uri: "mongodb://localhost:27017", database: "prod" },
      storage: { region: "us-east-1", prefix: "mongodb" },
      snapshot: { provider: "aws-ebs", volumeId: "vol-0123456789" },
    };

    const mockDriver = {
      async createSnapshot({ volumeId }) {
        return { snapshotId: `snap-${volumeId}`, status: "completed" };
      },
      async getSnapshotStatus() {
        return { status: "completed" };
      },
    };

    const result = await runCloudSnapshot({
      config,
      storage: mockStorage,
      logger: mockLogger,
      driver: mockDriver,
      volumeId: "vol-0123456789",
      createMongoClient: () => mockClient,
    });

    expect(result.snapshot.snapshotId).toBe("snap-vol-0123456789");
    expect(result.snapshot.provider).toBe("aws-ebs");
    expect(result.manifest.engine).toBe("snapshot");
    expect(locked).toBe(true);
    expect(unlocked).toBe(true);
    expect(uploadedManifest).toBeDefined();
    expect(uploadedManifest.snapshot.volumeId).toBe("vol-0123456789");

    const { restoreCloudSnapshot } = await import("../src/snapshot.js");
    mockStorage.getJson = async () => ({ data: uploadedManifest });
    const restoreRes = await restoreCloudSnapshot({
      config,
      storage: mockStorage,
      logger: mockLogger,
      ref: result.manifest.key,
    });
    expect(restoreRes.snapshotId).toBe("snap-vol-0123456789");
    expect(restoreRes.newVolumeId).toBe("vol-restored-snap-vol-0123456789");
  });
});
