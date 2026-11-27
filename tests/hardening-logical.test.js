import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { PassThrough, Readable } from "node:stream";
import { gunzipSync, gzipSync } from "node:zlib";
import { BSON, Timestamp } from "mongodb";
import { afterEach, describe, expect, it, vi } from "vitest";

const { spawnOverride, mongoClientOverride, shardingOverride, physicalOverride } = vi.hoisted(
  () => ({
    spawnOverride: { fn: null },
    mongoClientOverride: { factory: null },
    shardingOverride: { restoreShardedCluster: null },
    physicalOverride: { restorePhysicalBackup: null },
  }),
);

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    spawn: (...args) => (spawnOverride.fn ? spawnOverride.fn(...args) : actual.spawn(...args)),
  };
});

vi.mock("mongodb", async (importOriginal) => {
  const actual = await importOriginal();
  class PatchedMongoClient {
    constructor(uri, opts) {
      if (mongoClientOverride.factory) {
        return mongoClientOverride.factory(uri, opts);
      }
      return new actual.MongoClient(uri, opts);
    }
  }
  return {
    ...actual,
    MongoClient: PatchedMongoClient,
  };
});

vi.mock("../src/sharding.js", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    restoreShardedCluster: (...args) =>
      shardingOverride.restoreShardedCluster
        ? shardingOverride.restoreShardedCluster(...args)
        : actual.restoreShardedCluster(...args),
  };
});

vi.mock("../src/physical.js", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    restorePhysicalBackup: (...args) =>
      physicalOverride.restorePhysicalBackup
        ? physicalOverride.restorePhysicalBackup(...args)
        : actual.restorePhysicalBackup(...args),
  };
});

import {
  ARCHIVE_MAGIC_NUMBER,
  ArchiveNamespaceFilter,
  ArchiveOplogTap,
  readBsonDocumentsFromStream,
  streamArchiveCollections,
} from "../src/archive-stream.js";
import { runBackup } from "../src/backup.js";
import { archiveKey, manifestKey, scopeOf, writeManifest } from "../src/catalog.js";
import { createEncryptor, generateKey, parseKey } from "../src/crypto.js";
import { EngineRegistry, PhysicalBackupEngine, ShardedBackupEngine } from "../src/engines.js";
import { getOplogBounds, toBsonTimestamp, uploadOplogSlice } from "../src/oplog.js";
import {
  CLUSTER_SYSTEM_NS_EXCLUDE,
  IntegrityError,
  describeBackup,
  restoreBackup,
  restorePointInTime,
  verifyBackup,
} from "../src/restore.js";
import { runStreamPipeline } from "../src/stream.js";
import {
  checkTool,
  mongodump,
  mongorestore,
  replayOplogFile,
  runTool,
  uriHasReadPreference,
  withUriReadPreference,
} from "../src/tools.js";

function int32LE(n) {
  const b = Buffer.alloc(4);
  b.writeInt32LE(n, 0);
  return b;
}

function uint32LE(n) {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n >>> 0, 0);
  return b;
}

function buildSyntheticArchive(chunks, { includePreludeMeta = true } = {}) {
  const parts = [];
  parts.push(uint32LE(ARCHIVE_MAGIC_NUMBER));
  parts.push(Buffer.from(BSON.serialize({ concurrent_collections: 1, version: "0.1" })));
  if (includePreludeMeta) {
    for (const chunk of chunks) {
      if (chunk.db || chunk.collection) {
        parts.push(
          Buffer.from(
            BSON.serialize({
              db: chunk.db,
              collection: chunk.collection,
              metadata: "{}",
              type: "collection",
            }),
          ),
        );
      }
    }
  }
  parts.push(int32LE(-1));
  for (const chunk of chunks) {
    parts.push(Buffer.from(BSON.serialize({ db: chunk.db, collection: chunk.collection })));
    for (const doc of chunk.docs) {
      parts.push(Buffer.from(BSON.serialize(doc)));
    }
    parts.push(int32LE(-1));
  }
  parts.push(int32LE(-1));
  return Buffer.concat(parts);
}

function makeMemoryStorage() {
  const blobs = new Map();
  const jsons = new Map();
  return {
    blobs,
    jsons,
    bucket: "mem",
    async upload(key, body, opts) {
      if (Buffer.isBuffer(body)) {
        blobs.set(key, body);
        opts?.onProgress?.({ loaded: body.length });
        return;
      }
      if (opts?.signal?.aborted) {
        throw (
          opts.signal.reason ?? Object.assign(new Error("Upload aborted"), { name: "AbortError" })
        );
      }
      const onAbort = () => {
        if (typeof body.destroy === "function") {
          body.destroy(
            opts.signal.reason ??
              Object.assign(new Error("Upload aborted"), { name: "AbortError" }),
          );
        }
      };
      opts?.signal?.addEventListener("abort", onAbort, { once: true });
      try {
        const chunks = [];
        for await (const c of body) chunks.push(Buffer.from(c));
        const buf = Buffer.concat(chunks);
        blobs.set(key, buf);
        opts?.onProgress?.({ loaded: buf.length });
      } finally {
        opts?.signal?.removeEventListener("abort", onAbort);
      }
    },
    async head(key) {
      const b = blobs.get(key);
      return b ? { size: b.length, lastModified: new Date() } : null;
    },
    async download(key) {
      const b = blobs.get(key);
      if (!b) throw new Error(`NoSuchKey: ${key}`);
      return Readable.from([b]);
    },
    async putJson(key, val) {
      jsons.set(key, val);
    },
    async getJson(key) {
      return jsons.has(key) ? { data: jsons.get(key), etag: "1" } : null;
    },
    async remove(key) {
      blobs.delete(key);
    },
    async *list(prefix) {
      for (const [key, b] of blobs.entries()) {
        if (key.startsWith(prefix)) {
          yield { key, size: b.length, lastModified: new Date() };
        }
      }
      for (const key of jsons.keys()) {
        if (key.startsWith(prefix)) {
          yield { key, size: 100, lastModified: new Date() };
        }
      }
    },
  };
}

function makeLogger() {
  const logger = {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    child: () => logger,
  };
  return logger;
}

function makeBaseConfig(overrides = {}) {
  return {
    workDir: os.tmpdir(),
    timeoutMs: 30_000,
    storage: { prefix: "mv" },
    schedule: { timezone: "UTC" },
    mongo: {
      uri: "mongodb://localhost:27017/?replicaSet=rs0",
      database: "*",
      oplog: true,
      excludeCollections: [],
      parallelCollections: 2,
      readPreference: "secondaryPreferred",
    },
    pitr: { enabled: false },
    ...overrides,
  };
}

async function encryptBuffer(rawKey, buf) {
  const enc = createEncryptor(rawKey);
  const out = new PassThrough();
  const chunks = [];
  out.on("data", (c) => chunks.push(c));
  await runStreamPipeline([Readable.from([buf]), enc.stream, out]);
  return { data: Buffer.concat(chunks), envelope: enc.envelope() };
}

function createFakeChild({
  exitCode = 0,
  stdoutData = null,
  stderrLines = [],
  emitError = null,
} = {}) {
  const child = new EventEmitter();
  child.stdin = new PassThrough();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.exitCode = null;
  child.signalCode = null;
  child.kill = vi.fn((sig) => {
    child.signalCode = sig;
    setTimeout(() => child.emit("close", null, sig), 5);
  });

  setTimeout(() => {
    if (emitError) {
      child.emit("error", emitError);
      return;
    }
    for (const line of stderrLines) {
      child.stderr.write(`${line}\n`);
    }
    child.stderr.end();
    if (stdoutData !== null) {
      child.stdout.write(stdoutData);
    }
    child.stdout.end();
    child.exitCode = exitCode;
    child.emit("close", exitCode, null);
  }, 5);

  return child;
}

describe("M3a logical-engine hardening regression & fault-injection suite", () => {
  afterEach(() => {
    spawnOverride.fn = null;
    mongoClientOverride.factory = null;
    shardingOverride.restoreShardedCluster = null;
    physicalOverride.restorePhysicalBackup = null;
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("[F-LOG-001] filters excluded system namespaces from cluster archive streams during oplog replay and passes expanded systemNsExclude when oplog is off", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const config = makeBaseConfig();

    const excludedNamespaces = [
      ["admin", "system.version"],
      ["admin", "system.keys"],
      ["config", "system.sessions"],
      ["config", "system.preimages"],
      ["config", "actionlog"],
      ["config", "changelog"],
      ["config", "migrations"],
    ];

    const rawArchive = buildSyntheticArchive([
      ...excludedNamespaces.map(([db, collection], idx) => ({
        db,
        collection,
        docs: [{ _id: `sys-${idx}` }],
      })),
      {
        db: "app",
        collection: "users",
        docs: [{ _id: "u1", name: "alice" }],
      },
      {
        db: "",
        collection: "",
        docs: [
          {
            ts: new Timestamp({ t: 1700000001, i: 1 }),
            op: "i",
            ns: "app.users",
            o: { _id: "u2" },
          },
        ],
      },
    ]);
    const gz = gzipSync(rawArchive);
    const key = "mv/_cluster/2026-03-10/20260310T100000Z.archive.gz";
    await storage.upload(key, gz);
    await writeManifest(storage, {
      version: 2,
      key,
      database: "*",
      createdAt: "2026-03-10T10:00:00.000Z",
      completedAt: "2026-03-10T10:00:05.000Z",
      durationMs: 5000,
      size: gz.length,
      sha256: createHash("sha256").update(gz).digest("hex"),
      compression: "gzip",
      encryption: null,
      oplog: true,
      excludedCollections: [],
      collections: { "app.users": 1 },
    });

    // (a) Full-cluster restore with drop: true and oplogReplay: true
    const capturedStdinChunks = [];
    const mockToolsA = {
      async mongorestore() {
        const stdin = new PassThrough();
        const done = new Promise((resolve, reject) => {
          stdin.on("data", (c) => capturedStdinChunks.push(Buffer.from(c)));
          stdin.on("end", () => resolve({ restored: 1, failed: 0 }));
          stdin.on("error", reject);
        });
        return { stdin, done };
      },
    };

    await restoreBackup(
      { config, storage, logger, tools: mockToolsA },
      { ref: key, uri: "mongodb://localhost:27017", drop: true, preVerify: false },
    );

    const gunzipped = gunzipSync(Buffer.concat(capturedStdinChunks));
    const seen = [];
    await streamArchiveCollections(
      Readable.from([gunzipped]),
      () => true,
      async ({ db, collection }) => {
        seen.push(db === "" && collection === "" ? "<oplog>" : `${db}.${collection}`);
      },
    );

    expect(seen).toContain("app.users");
    expect(seen).toContain("<oplog>");
    for (const [db, coll] of excludedNamespaces) {
      expect(seen).not.toContain(`${db}.${coll}`);
    }

    // (b) Full-cluster restore with oplog: false, drop: false, nsExclude: "app.logs"
    let capturedOptsB = null;
    const mockToolsB = {
      async mongorestore(opts) {
        capturedOptsB = opts;
        const stdin = new PassThrough();
        stdin.resume();
        return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
      },
    };

    await restoreBackup(
      { config, storage, logger, tools: mockToolsB },
      {
        ref: key,
        uri: "mongodb://localhost:27017",
        drop: false,
        oplogReplay: false,
        nsExclude: "app.logs",
        preVerify: false,
      },
    );

    expect(capturedOptsB.systemNsExclude).toEqual(
      expect.arrayContaining([
        ...CLUSTER_SYSTEM_NS_EXCLUDE,
        "admin.system.keys",
        "config.system.preimages",
        "config.actionlog",
        "config.changelog",
        "config.migrations",
      ]),
    );
  });

  it("[F-LOG-002] aligns getOplogBounds URI with config.mongo.readPreference and clamps oplogWindow.startTs to preOplog.newestTs", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const config = makeBaseConfig({
      mongo: {
        uri: "mongodb://localhost:27017/?replicaSet=rs0",
        database: "*",
        oplog: true,
        excludeCollections: [],
        parallelCollections: 2,
        readPreference: "secondaryPreferred",
      },
    });

    let capturedPreUri = null;
    const manifest = await runBackup({
      config,
      storage,
      logger,
      tools: {
        getOplogBounds: vi.fn(async (uri, opts) => {
          if (opts?.phase === "pre") capturedPreUri = uri;
          return { oldestTs: { t: 100, i: 1 }, newestTs: { t: 190, i: 1 } };
        }),
        mongodump: vi.fn(async () => ({
          stdout: Readable.from([Buffer.from("fake-archive")]),
          done: Promise.resolve({
            collections: { "app.users": 1 },
            oplogStartTs: { t: 205, i: 1 },
            oplogEndTs: { t: 210, i: 1 },
            oplogEntries: 2,
          }),
        })),
      },
    });

    expect(capturedPreUri).toMatch(/[?&]readPreference=secondaryPreferred(?:&|$)/);
    expect(manifest.oplogWindow?.startTs).toBe("190:1");
  });

  it("[F-LOG-003] invalidates ArchiveOplogTap bounds on corrupted chunk header and rejects corrupt non-empty mongodump streams in runBackup", async () => {
    // Build valid magic + prelude + corrupted chunk header (> 64 MiB doc len) + embedded oplog
    const corruptBytes = Buffer.concat([
      uint32LE(ARCHIVE_MAGIC_NUMBER),
      Buffer.from(BSON.serialize({ concurrent_collections: 1, version: "0.1" })),
      int32LE(-1),
      int32LE(100 * 1024 * 1024), // > 64 MiB MAX_BSON_DOC_SIZE
      Buffer.from(BSON.serialize({ db: "", collection: "" })),
      Buffer.from(
        BSON.serialize({
          ts: new Timestamp({ t: 100, i: 1 }),
          op: "i",
          ns: "app.users",
          o: { _id: 1 },
        }),
      ),
      int32LE(-1),
      int32LE(-1),
    ]);

    // (a) Direct ArchiveOplogTap check
    const tap = new ArchiveOplogTap();
    Readable.from([corruptBytes]).pipe(tap);
    for await (const _chunk of tap) {
      // drain
    }
    expect(tap.bounds.validArchive).toBe(false);

    // (b) runBackup piped through real mongodump + ArchiveOplogTap with fake child emitting corruptBytes
    spawnOverride.fn = () =>
      createFakeChild({
        exitCode: 0,
        stdoutData: corruptBytes,
        stderrLines: ["done dumping app.users (1 documents)"],
      });

    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog003-"));
    try {
      const config = makeBaseConfig({ workDir });
      await expect(
        runBackup({
          config,
          storage,
          logger,
          tools: {
            mongodump,
            getOplogBounds: vi.fn(async () => ({
              oldestTs: { t: 90, i: 1 },
              newestTs: { t: 100, i: 1 },
            })),
          },
        }),
      ).rejects.toThrow(/corrupt|truncated|archive/i);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-004] streamArchiveCollections rejects 0-byte, boundary-truncated, and trailing-byte archives", async () => {
    const preludeHdr = Buffer.from(BSON.serialize({ concurrent_collections: 1, version: "0.1" }));
    const metaDoc = Buffer.from(
      BSON.serialize({ db: "app", collection: "users", metadata: "{}", type: "collection" }),
    );
    const chunkHdr = Buffer.from(BSON.serialize({ db: "app", collection: "users" }));
    const dataDoc = Buffer.from(BSON.serialize({ _id: 1 }));

    const noop = async () => {};
    const allFilter = () => true;

    // (a) 0-byte stream
    await expect(streamArchiveCollections(Readable.from([]), allFilter, noop)).rejects.toThrow();

    // (b) Truncated right after prelude header (PRELUDE_META_OR_TERM, buf.length === 0)
    await expect(
      streamArchiveCollections(
        Readable.from([Buffer.concat([uint32LE(ARCHIVE_MAGIC_NUMBER), preludeHdr])]),
        allFilter,
        noop,
      ),
    ).rejects.toThrow();

    // (c) Truncated right after a doc (CHUNK_DOC_LEN, buf.length === 0)
    await expect(
      streamArchiveCollections(
        Readable.from([
          Buffer.concat([
            uint32LE(ARCHIVE_MAGIC_NUMBER),
            preludeHdr,
            metaDoc,
            int32LE(-1),
            chunkHdr,
            dataDoc,
          ]),
        ]),
        allFilter,
        noop,
      ),
    ).rejects.toThrow();

    // (d) Truncated before final -1 (CHUNK_HEADER, buf.length === 0)
    await expect(
      streamArchiveCollections(
        Readable.from([
          Buffer.concat([
            uint32LE(ARCHIVE_MAGIC_NUMBER),
            preludeHdr,
            metaDoc,
            int32LE(-1),
            chunkHdr,
            dataDoc,
            int32LE(-1),
          ]),
        ]),
        allFilter,
        noop,
      ),
    ).rejects.toThrow();

    // (e) Trailing bytes after final -1
    await expect(
      streamArchiveCollections(
        Readable.from([
          Buffer.concat([
            buildSyntheticArchive([{ db: "app", collection: "users", docs: [{ _id: 1 }] }]),
            Buffer.from([0x01, 0x02, 0x03, 0x04]),
          ]),
        ]),
        allFilter,
        noop,
      ),
    ).rejects.toThrow();
  });

  it("[F-LOG-005] selective PITR enforces base oplogWindow.startTs lower bound and skips pre-backup destructive deletes in both replay paths", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog005-"));
    try {
      const storage = makeMemoryStorage();
      const logger = makeLogger();
      const config = makeBaseConfig({ workDir });
      const scope = scopeOf(config);

      const baseGz = gzipSync(
        buildSyntheticArchive([{ db: "shop", collection: "users", docs: [{ _id: "u1" }] }]),
      );
      const baseKey = archiveKey(scope, new Date("2026-03-10T10:00:00Z"), { encrypted: false });
      await storage.upload(baseKey, Readable.from([baseGz]));
      await writeManifest(storage, {
        version: 2,
        engine: "logical",
        key: baseKey,
        database: "*",
        createdAt: "2026-03-10T10:00:00.000Z",
        completedAt: "2026-03-10T10:01:00.000Z",
        durationMs: 1000,
        size: baseGz.length,
        sha256: createHash("sha256").update(baseGz).digest("hex"),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "100:1", endTs: "110:1" },
        excludedCollections: [],
        collections: { "shop.users": 1 },
      });

      const sliceDocs = [
        { ts: toBsonTimestamp({ t: 95, i: 1 }), op: "d", ns: "shop.users", o: { _id: "victim" } },
        { ts: toBsonTimestamp({ t: 100, i: 1 }), op: "i", ns: "shop.users", o: { _id: "at100" } },
        { ts: toBsonTimestamp({ t: 120, i: 1 }), op: "i", ns: "shop.users", o: { _id: "at120" } },
      ].map((d) => Buffer.from(BSON.serialize(d)));

      await uploadOplogSlice({
        config,
        storage,
        logger,
        rawDocs: sliceDocs,
        startTs: { t: 90, i: 1 },
        endTs: { t: 150, i: 1 },
      });

      // Path 1: filterOplogSliceFile + replayOplogFile
      const replayedTimestamps = [];
      await restoreBackup(
        {
          config,
          storage,
          logger,
          tools: {
            async mongorestore() {
              const stdin = new PassThrough();
              stdin.resume();
              return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
            },
            async replayOplogFile(opts) {
              const buf = await fs.readFile(opts.oplogFile);
              let off = 0;
              while (off < buf.length) {
                const len = buf.readInt32LE(off);
                const doc = BSON.deserialize(buf.subarray(off, off + len), {
                  promoteValues: false,
                });
                replayedTimestamps.push(`${doc.ts.getHighBits()}:${doc.ts.getLowBits()}`);
                off += len;
              }
            },
          },
        },
        {
          uri: "mongodb://localhost:27017",
          database: "shop",
          pitr: "140:1",
          preVerify: false,
        },
      );
      expect(replayedTimestamps).toEqual(["100:1", "120:1"]);

      // Path 2: production OplogApplier path (OplogApplier strips `ts` from cleanOp before applyOps)
      const appliedOps = [];
      const idToTs = { victim: "95:1", at100: "100:1", at120: "120:1" };
      const makeClient = () => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command(cmd) {
                if (Array.isArray(cmd?.applyOps)) {
                  for (const entry of cmd.applyOps) {
                    appliedOps.push({
                      op: entry.op,
                      ts: entry.ts
                        ? `${entry.ts.getHighBits()}:${entry.ts.getLowBits()}`
                        : idToTs[entry.o?._id],
                    });
                  }
                }
                return { ok: 1 };
              },
            };
          }
          return {
            collection() {
              return {
                async findOne() {
                  return null;
                },
              };
            },
            listCollections() {
              return {
                async toArray() {
                  return [];
                },
              };
            },
            async dropDatabase() {},
          };
        },
      });
      mongoClientOverride.factory = makeClient;

      await restoreBackup(
        {
          config,
          storage,
          logger,
          createMongoClient: makeClient,
          tools: {
            async mongorestore() {
              const stdin = new PassThrough();
              stdin.resume();
              return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
            },
          },
        },
        {
          uri: "mongodb://localhost:27017",
          database: "shop",
          pitr: "140:1",
          preVerify: false,
        },
      );
      expect(appliedOps).toEqual([
        { op: "i", ts: "100:1" },
        { op: "i", ts: "120:1" },
      ]);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-006] rejects stripped manifest signatures, unencrypted/unsigned backups under requireEncryption, and --drop without checksum", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const encKeyStr = generateKey();
    const encKey = parseKey(encKeyStr);

    // (a) Encrypted backup with signature and manifestHmac stripped
    const rawGz = gzipSync(
      buildSyntheticArchive([{ db: "app", collection: "users", docs: [{ _id: 1 }] }]),
    );
    const { data: encData, envelope } = await encryptBuffer(encKey, rawGz);
    const encKeyPath = "mv/_cluster/2026-03-10/20260310T100000Z.archive.gz.enc";
    await storage.upload(encKeyPath, encData);
    await writeManifest(
      storage,
      {
        version: 2,
        key: encKeyPath,
        database: "*",
        createdAt: "2026-03-10T10:00:00.000Z",
        completedAt: "2026-03-10T10:00:05.000Z",
        durationMs: 1000,
        size: encData.length,
        sha256: createHash("sha256").update(encData).digest("hex"),
        compression: "gzip",
        encryption: envelope,
        oplog: false,
        excludedCollections: [],
        collections: { "app.users": 1 },
      },
      encKeyStr,
    );

    const mKey = manifestKey(encKeyPath);
    const storedManifest = (await storage.getJson(mKey)).data;
    const stripped = structuredClone(storedManifest);
    delete stripped.signature;
    delete stripped.encryption.manifestHmac;
    await storage.putJson(mKey, stripped);

    const encConfig = makeBaseConfig({ encryptionKey: encKeyStr, encryptionKeyRing: [encKeyStr] });
    const mockRestoreTools = {
      mongorestore: vi.fn(async () => {
        const stdin = new PassThrough();
        stdin.resume();
        return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
      }),
    };

    await expect(
      describeBackup({ config: encConfig, storage, logger }, encKeyPath),
    ).rejects.toThrow(IntegrityError);
    await expect(verifyBackup({ config: encConfig, storage, logger }, encKeyPath)).rejects.toThrow(
      IntegrityError,
    );
    await expect(
      restoreBackup(
        { config: encConfig, storage, logger, tools: mockRestoreTools },
        { ref: encKeyPath, uri: "mongodb://localhost:27017", preVerify: false },
      ),
    ).rejects.toThrow(IntegrityError);

    // (b) requireEncryption: true with unsigned/unencrypted manifest and missing manifest
    const plainKey = "mv/_cluster/2026-03-10/20260310T110000Z.archive.gz";
    await storage.upload(plainKey, rawGz);
    await writeManifest(storage, {
      version: 2,
      key: plainKey,
      database: "*",
      createdAt: "2026-03-10T11:00:00.000Z",
      completedAt: "2026-03-10T11:00:05.000Z",
      durationMs: 1000,
      size: rawGz.length,
      sha256: createHash("sha256").update(rawGz).digest("hex"),
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: { "app.users": 1 },
    });

    const noManifestKey = "mv/_cluster/2026-03-10/20260310T113000Z.archive.gz";
    await storage.upload(noManifestKey, rawGz);

    const reqEncConfig = makeBaseConfig({
      encryptionKey: encKeyStr,
      encryptionKeyRing: [encKeyStr],
      requireEncryption: true,
    });

    await expect(
      describeBackup({ config: reqEncConfig, storage, logger }, plainKey),
    ).rejects.toThrow(IntegrityError);
    await expect(verifyBackup({ config: reqEncConfig, storage, logger }, plainKey)).rejects.toThrow(
      IntegrityError,
    );
    await expect(
      restoreBackup(
        { config: reqEncConfig, storage, logger, tools: mockRestoreTools },
        { ref: plainKey, uri: "mongodb://localhost:27017", preVerify: false },
      ),
    ).rejects.toThrow(IntegrityError);

    await expect(
      describeBackup({ config: reqEncConfig, storage, logger }, noManifestKey),
    ).rejects.toThrow(IntegrityError);
    await expect(
      verifyBackup({ config: reqEncConfig, storage, logger }, noManifestKey),
    ).rejects.toThrow(IntegrityError);
    await expect(
      restoreBackup(
        { config: reqEncConfig, storage, logger, tools: mockRestoreTools },
        { ref: noManifestKey, uri: "mongodb://localhost:27017", preVerify: false },
      ),
    ).rejects.toThrow(IntegrityError);

    // (c) drop: true, preVerify: false on backup with no manifest/checksum
    const mongorestoreSpy = vi.fn(async () => {
      const stdin = new PassThrough();
      stdin.resume();
      return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
    });
    await expect(
      restoreBackup(
        {
          config: makeBaseConfig(),
          storage,
          logger,
          tools: { mongorestore: mongorestoreSpy },
        },
        {
          ref: noManifestKey,
          uri: "mongodb://localhost:27017",
          drop: true,
          preVerify: false,
        },
      ),
    ).rejects.toThrow(IntegrityError);
    expect(mongorestoreSpy).not.toHaveBeenCalled();
  });

  it("[F-LOG-007] drops post-backup collections before Stage 1 PITR only after preVerify succeeds and includes timeseries system.buckets in selective PITR", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog007-"));
    try {
      const storage = makeMemoryStorage();
      const logger = makeLogger();
      const config = makeBaseConfig({ workDir });
      const scope = scopeOf(config);

      const baseGz = gzipSync(
        buildSyntheticArchive([{ db: "shop", collection: "metrics", docs: [{ _id: 1 }] }]),
      );
      const baseKey = archiveKey(scope, new Date("2026-03-10T10:00:00Z"), { encrypted: false });
      await storage.upload(baseKey, Readable.from([baseGz]));
      const baseManifest = {
        version: 2,
        engine: "logical",
        key: baseKey,
        database: "*",
        createdAt: "2026-03-10T10:00:00.000Z",
        completedAt: "2026-03-10T10:01:00.000Z",
        durationMs: 1000,
        size: baseGz.length,
        sha256: createHash("sha256").update(baseGz).digest("hex"),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "100:1", endTs: "110:1" },
        excludedCollections: [],
        collections: { "shop.metrics": 1 },
      };
      await writeManifest(storage, baseManifest);

      const sliceDocs = [
        {
          ts: toBsonTimestamp({ t: 120, i: 1 }),
          op: "i",
          ns: "shop.system.buckets.metrics",
          o: { _id: "b1", control: {}, data: {} },
        },
      ].map((d) => Buffer.from(BSON.serialize(d)));

      await uploadOplogSlice({
        config,
        storage,
        logger,
        rawDocs: sliceDocs,
        startTs: { t: 100, i: 1 },
        endTs: { t: 150, i: 1 },
        previousEndTs: { t: 100, i: 1 },
      });

      // (a) Track order of preVerify, drop, and Stage 1 mongorestore + verify system DBs untouched
      const events = [];
      const droppedTargets = [];
      const makeTrackedClient = () => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command(cmd) {
                if (cmd?.listDatabases) {
                  return {
                    databases: [
                      { name: "admin" },
                      { name: "local" },
                      { name: "config" },
                      { name: "shop" },
                      { name: "postBackupDb" },
                    ],
                  };
                }
                return { ok: 1 };
              },
            };
          }
          return {
            async dropDatabase() {
              events.push(`drop:${name}`);
              droppedTargets.push(name);
            },
            listCollections() {
              return {
                async toArray() {
                  return [{ name: "metrics" }, { name: "extra_post_backup" }];
                },
              };
            },
            collection(coll) {
              return {
                async drop() {
                  events.push(`drop:${name}.${coll}`);
                  droppedTargets.push(`${name}.${coll}`);
                },
                async findOne() {
                  return null;
                },
              };
            },
          };
        },
      });
      mongoClientOverride.factory = makeTrackedClient;

      await restoreBackup(
        {
          config,
          storage,
          logger,
          createMongoClient: makeTrackedClient,
          tools: {
            async mongorestore() {
              events.push("mongorestore");
              const stdin = new PassThrough();
              stdin.resume();
              return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
            },
            async replayOplogFile() {
              events.push("replayOplogFile");
            },
          },
        },
        {
          uri: "mongodb://localhost:27017",
          pitr: "140:1",
          drop: true,
          preVerify: true,
        },
      );

      expect(
        droppedTargets.some((t) => t === "postBackupDb" || t.startsWith("postBackupDb.")),
      ).toBe(true);
      expect(droppedTargets).not.toContain("admin");
      expect(droppedTargets).not.toContain("local");
      expect(droppedTargets).not.toContain("config");
      const firstDropIdx = events.findIndex((e) => e.startsWith("drop:"));
      const mongorestoreIdx = events.indexOf("mongorestore");
      expect(firstDropIdx).toBeGreaterThanOrEqual(0);
      expect(firstDropIdx).toBeLessThan(mongorestoreIdx);

      // Verify drop does NOT run when preVerify fails on corrupt base backup
      const droppedOnCorrupt = [];
      const makeCorruptCheckClient = () => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command(cmd) {
                if (cmd?.listDatabases) {
                  return { databases: [{ name: "shop" }] };
                }
                return { ok: 1 };
              },
            };
          }
          return {
            async dropDatabase() {
              droppedOnCorrupt.push(name);
            },
            listCollections() {
              return {
                async toArray() {
                  return [{ name: "metrics" }];
                },
              };
            },
            collection(coll) {
              return {
                async drop() {
                  droppedOnCorrupt.push(`${name}.${coll}`);
                },
              };
            },
          };
        },
      });
      mongoClientOverride.factory = makeCorruptCheckClient;

      const origSha = baseManifest.sha256;
      await writeManifest(storage, { ...baseManifest, sha256: "0".repeat(64) });
      await expect(
        restoreBackup(
          {
            config,
            storage,
            logger,
            createMongoClient: makeCorruptCheckClient,
            tools: {
              mongorestore: vi.fn(async () => {
                const stdin = new PassThrough();
                stdin.resume();
                return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
              }),
            },
          },
          { uri: "mongodb://localhost:27017", pitr: "140:1", drop: true, preVerify: true },
        ),
      ).rejects.toThrow(IntegrityError);
      expect(droppedOnCorrupt).toEqual([]);
      await writeManifest(storage, { ...baseManifest, sha256: origSha });

      // (b) Selective PITR with nsInclude: "shop.metrics" preserves shop.system.buckets.metrics
      const replayedNamespaces = [];
      await restoreBackup(
        {
          config,
          storage,
          logger,
          tools: {
            async mongorestore() {
              const stdin = new PassThrough();
              stdin.resume();
              return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
            },
            async replayOplogFile(opts) {
              const buf = await fs.readFile(opts.oplogFile);
              let off = 0;
              while (off < buf.length) {
                const len = buf.readInt32LE(off);
                const doc = BSON.deserialize(buf.subarray(off, off + len), {
                  promoteValues: false,
                });
                replayedNamespaces.push(doc.ns);
                off += len;
              }
            },
          },
        },
        {
          uri: "mongodb://localhost:27017",
          nsInclude: "shop.metrics",
          pitr: "140:1",
          preVerify: false,
        },
      );
      expect(replayedNamespaces).toContain("shop.system.buckets.metrics");

      // Also verify OplogApplier path receives shop.system.buckets.metrics
      const applierNamespaces = [];
      const makeApplierClient = () => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command(cmd) {
                if (Array.isArray(cmd?.applyOps)) {
                  for (const entry of cmd.applyOps) {
                    applierNamespaces.push(entry.ns);
                  }
                }
                return { ok: 1 };
              },
            };
          }
          return {
            listCollections() {
              return {
                async toArray() {
                  return [];
                },
              };
            },
            collection() {
              return {
                async findOne() {
                  return null;
                },
              };
            },
          };
        },
      });
      mongoClientOverride.factory = makeApplierClient;

      await restoreBackup(
        {
          config,
          storage,
          logger,
          createMongoClient: makeApplierClient,
          tools: {
            async mongorestore() {
              const stdin = new PassThrough();
              stdin.resume();
              return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
            },
          },
        },
        {
          uri: "mongodb://localhost:27017",
          nsInclude: "shop.metrics",
          pitr: "140:1",
          preVerify: false,
        },
      );
      expect(applierNamespaces).toContain("shop.system.buckets.metrics");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-008] checks fencing before writeManifest, skips storage.remove on lock-loss abort, and avoids archiveKey same-ms collisions", async () => {
    // (a) Abort signal inside storage.head() simulating lock loss after settleAll
    const storageA = makeMemoryStorage();
    const logger = makeLogger();
    const config = makeBaseConfig({ mongo: { ...makeBaseConfig().mongo, oplog: false } });
    const ac = new AbortController();

    const origHead = storageA.head.bind(storageA);
    storageA.head = vi.fn(async (k) => {
      ac.abort(new Error("Distributed backup lock was lost during backup"));
      return origHead(k);
    });
    const putJsonSpy = vi.spyOn(storageA, "putJson");
    const removeSpy = vi.spyOn(storageA, "remove");

    await expect(
      runBackup({
        config,
        storage: storageA,
        logger,
        signal: ac.signal,
        assertLockValid: () => {
          if (ac.signal.aborted) throw ac.signal.reason;
        },
        tools: {
          mongodump: vi.fn(async () => ({
            stdout: Readable.from([Buffer.from("archive-payload")]),
            done: Promise.resolve({ collections: { "app.users": 1 } }),
          })),
        },
      }),
    ).rejects.toThrow(/lock|abort/i);

    expect(putJsonSpy).not.toHaveBeenCalled();
    expect(removeSpy).not.toHaveBeenCalled();

    // (b) Two runBackup calls at identical Date.now() produce distinct manifest.key values
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-03-10T12:00:00.123Z"));

    const storageB = makeMemoryStorage();
    const toolsB = {
      mongodump: vi.fn(async () => ({
        stdout: Readable.from([Buffer.from("archive-payload")]),
        done: Promise.resolve({ collections: { "app.users": 1 } }),
      })),
    };
    const m1 = await runBackup({ config, storage: storageB, logger, tools: toolsB });
    const m2 = await runBackup({ config, storage: storageB, logger, tools: toolsB });

    expect(m1.key).not.toBe(m2.key);
  });

  it("[F-LOG-009] passes oplogLimit to Stage 1 mongorestore even when PITR plan has slices and replayedBaseOplog is true", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog009-"));
    try {
      const storage = makeMemoryStorage();
      const logger = makeLogger();
      const config = makeBaseConfig({ workDir });
      const scope = scopeOf(config);

      const baseGz = gzipSync(
        buildSyntheticArchive([{ db: "app", collection: "users", docs: [{ _id: 1 }] }]),
      );
      const baseKey = archiveKey(scope, new Date("2026-03-10T10:00:00Z"), { encrypted: false });
      await storage.upload(baseKey, Readable.from([baseGz]));
      await writeManifest(storage, {
        version: 2,
        engine: "logical",
        key: baseKey,
        database: "*",
        createdAt: "2026-03-10T10:00:00.000Z",
        completedAt: "2026-03-10T10:01:00.000Z",
        durationMs: 1000,
        size: baseGz.length,
        sha256: createHash("sha256").update(baseGz).digest("hex"),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "100:1", endTs: "110:1" },
        excludedCollections: [],
        collections: { "app.users": 1 },
      });

      const sliceDoc = Buffer.from(
        BSON.serialize({
          ts: toBsonTimestamp({ t: 120, i: 1 }),
          op: "i",
          ns: "app.users",
          o: { _id: 2 },
        }),
      );
      await uploadOplogSlice({
        config,
        storage,
        logger,
        rawDocs: [sliceDoc],
        startTs: { t: 110, i: 2 },
        endTs: { t: 130, i: 1 },
        previousEndTs: { t: 110, i: 1 },
      });

      let capturedStage1Opts = null;
      await restoreBackup(
        {
          config,
          storage,
          logger,
          tools: {
            async mongorestore(opts) {
              capturedStage1Opts = opts;
              const stdin = new PassThrough();
              stdin.resume();
              return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
            },
            async replayOplogFile() {},
          },
        },
        {
          uri: "mongodb://localhost:27017",
          pitr: "125:4",
          preVerify: false,
        },
      );

      expect(capturedStage1Opts?.oplogReplay).toBe(true);
      expect(capturedStage1Opts?.oplogLimit).toBe("125:5");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-010] pins resolved key when verifying ref='latest' so concurrent newer backup cannot bypass verification of corrupt target", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const config = makeBaseConfig();

    const b1Key = "mv/_cluster/2026-03-10/20260310T100000Z.archive.gz";
    const b2Key = "mv/_cluster/2026-03-10/20260310T110000Z.archive.gz";

    const validGz = gzipSync(
      buildSyntheticArchive([{ db: "app", collection: "users", docs: [{ _id: 1 }] }]),
    );
    const corruptGz = Buffer.from("corrupted-not-even-gzip");

    // B1 is corrupt (manifest records validGz sha256, stored blob is corruptGz)
    await storage.upload(b1Key, corruptGz);
    await writeManifest(storage, {
      version: 2,
      key: b1Key,
      database: "*",
      createdAt: "2026-03-10T10:00:00.000Z",
      completedAt: "2026-03-10T10:00:05.000Z",
      durationMs: 1000,
      size: validGz.length,
      sha256: createHash("sha256").update(validGz).digest("hex"),
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: { "app.users": 1 },
    });

    // B2 is valid
    await storage.upload(b2Key, validGz);
    await writeManifest(storage, {
      version: 2,
      key: b2Key,
      database: "*",
      createdAt: "2026-03-10T11:00:00.000Z",
      completedAt: "2026-03-10T11:00:05.000Z",
      durationMs: 1000,
      size: validGz.length,
      sha256: createHash("sha256").update(validGz).digest("hex"),
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: { "app.users": 1 },
    });

    let listCallCount = 0;
    storage.list = async function* () {
      listCallCount++;
      if (listCallCount === 1) {
        yield {
          key: b1Key,
          size: corruptGz.length,
          lastModified: new Date("2026-03-10T10:00:00Z"),
        };
        yield {
          key: manifestKey(b1Key),
          size: 100,
          lastModified: new Date("2026-03-10T10:00:00Z"),
        };
      } else {
        yield { key: b2Key, size: validGz.length, lastModified: new Date("2026-03-10T11:00:00Z") };
        yield {
          key: manifestKey(b2Key),
          size: 100,
          lastModified: new Date("2026-03-10T11:00:00Z"),
        };
        yield {
          key: b1Key,
          size: corruptGz.length,
          lastModified: new Date("2026-03-10T10:00:00Z"),
        };
        yield {
          key: manifestKey(b1Key),
          size: 100,
          lastModified: new Date("2026-03-10T10:00:00Z"),
        };
      }
    };

    const mongorestoreSpy = vi.fn(async () => {
      const stdin = new PassThrough();
      stdin.resume();
      return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
    });

    await expect(
      restoreBackup(
        { config, storage, logger, tools: { mongorestore: mongorestoreSpy } },
        { ref: "latest", uri: "mongodb://localhost:27017", drop: true },
      ),
    ).rejects.toThrow(IntegrityError);
    expect(mongorestoreSpy).not.toHaveBeenCalled();
  });

  it("[F-LOG-011] forwards preVerify in ShardedBackupEngine, rejects unsupported flags, respects explicit engine precedence, and rejects conflicts", async () => {
    const logger = makeLogger();
    const storage = makeMemoryStorage();
    const baseCfg = {
      engine: "logical",
      mongo: { uri: "mongodb://localhost:27017", database: "app" },
      sharding: { enabled: false },
    };

    const shardedRestoreSpy = vi.fn(async () => ({ shardsRestored: 2 }));
    const physicalRestoreSpy = vi.fn(async () => ({ restoredFiles: 5 }));
    shardingOverride.restoreShardedCluster = shardedRestoreSpy;
    physicalOverride.restorePhysicalBackup = physicalRestoreSpy;

    // (a) ShardedBackupEngine.restore forwards preVerify: true
    const sharded = new ShardedBackupEngine();
    await sharded.restore({ config: baseCfg, storage, logger, preVerify: true, drop: false });
    expect(shardedRestoreSpy).toHaveBeenCalledWith(expect.objectContaining({ preVerify: true }));

    // (b) PhysicalBackupEngine.restore & ShardedBackupEngine.restore reject unsupported flags
    const physical = new PhysicalBackupEngine();
    for (const badOpts of [
      { nsInclude: "app.users" },
      { nsExclude: "app.logs" },
      { database: "app" },
      { oplogReplay: true },
      { oplogLimit: "100:1" },
    ]) {
      await expect(
        physical.restore({ config: baseCfg, storage, logger, dbPath: "/data/db", ...badOpts }),
      ).rejects.toThrow();
      await expect(
        sharded.restore({ config: baseCfg, storage, logger, ...badOpts }),
      ).rejects.toThrow();
    }
    await expect(
      sharded.restore({ config: baseCfg, storage, logger, dbPath: "/data/db" }),
    ).rejects.toThrow();

    // (c) Explicit engine: "logical" takes precedence over config.sharding.enabled: true
    expect(
      EngineRegistry.resolveForRestore({
        engine: "logical",
        key: "s1.archive.gz",
        config: { ...baseCfg, sharding: { enabled: true } },
      }).name,
    ).toBe("logical");

    // (d) Conflicting explicit engine vs sharding / dbPath / key suffix
    expect(() =>
      EngineRegistry.resolveForBackup({
        engine: "logical",
        sharding: true,
        config: baseCfg,
      }),
    ).toThrow();
    expect(() =>
      EngineRegistry.resolveForBackup({
        engine: "logical",
        config: { ...baseCfg, sharding: { enabled: true } },
      }),
    ).toThrow();
    expect(() =>
      EngineRegistry.resolveForRestore({
        engine: "logical",
        dbPath: "/data/db",
        config: baseCfg,
      }),
    ).toThrow();
    expect(() =>
      EngineRegistry.resolveForRestore({
        engine: "logical",
        key: "backup.physical.tar.gz",
        config: baseCfg,
      }),
    ).toThrow();
    expect(() =>
      EngineRegistry.resolveForRestore({
        engine: "logical",
        key: "backup.cluster",
        config: baseCfg,
      }),
    ).toThrow();
    expect(() =>
      EngineRegistry.resolveForRestore({
        engine: "sharded",
        key: "backup.archive.gz",
        config: baseCfg,
      }),
    ).toThrow();
    expect(() =>
      EngineRegistry.resolveForRestore({
        dbPath: "/data/db",
        sharding: true,
        config: baseCfg,
      }),
    ).toThrow();
  });

  it("[F-LOG-012] cleans up URI config .yaml files and exit listeners on sync spawn/mkdir errors and uses 0o700 mode for workDir", async () => {
    const rootTmp = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog012-"));
    const subWorkDir = path.join(rootTmp, "work");
    const baselineExitListeners = process.listenerCount("exit");
    const beforeListeners = process.rawListeners("exit");

    try {
      spawnOverride.fn = () => {
        throw new Error("EAGAIN: sync spawn failed");
      };

      await expect(
        mongodump({ uri: "mongodb://localhost:27017", database: "*", workDir: subWorkDir }),
      ).rejects.toThrow(/EAGAIN/);
      await expect(
        mongorestore({ uri: "mongodb://localhost:27017", workDir: subWorkDir }),
      ).rejects.toThrow(/EAGAIN/);
      await expect(
        replayOplogFile({
          uri: "mongodb://localhost:27017",
          workDir: subWorkDir,
          oplogFile: path.join(rootTmp, "oplog.bson"),
        }),
      ).rejects.toThrow(/EAGAIN/);

      // fs.mkdir failure on emptyDir inside replayOplogFile
      spawnOverride.fn = null;
      const origMkdir = fs.mkdir.bind(fs);
      vi.spyOn(fs, "mkdir").mockImplementation(async (target, opts) => {
        if (String(target).includes(".empty-")) {
          throw new Error("ENOSPC: failed creating emptyDir");
        }
        return origMkdir(target, opts);
      });
      await expect(
        replayOplogFile({
          uri: "mongodb://localhost:27017",
          workDir: subWorkDir,
          oplogFile: path.join(rootTmp, "oplog.bson"),
        }),
      ).rejects.toThrow(/ENOSPC/);

      const stat = await fs.stat(subWorkDir);
      expect(stat.mode & 0o777).toBe(0o700);

      const remainingFiles = await fs.readdir(subWorkDir);
      const yamlFiles = remainingFiles.filter((f) => f.endsWith(".yaml"));
      expect(yamlFiles).toEqual([]);
      expect(process.listenerCount("exit")).toBe(baselineExitListeners);
    } finally {
      // Remove any exit listeners leaked by buggy src/ so process stays clean
      const currentListeners = process.rawListeners("exit");
      for (const l of currentListeners) {
        if (!beforeListeners.includes(l)) {
          process.removeListener("exit", l);
        }
      }
      await fs.rm(rootTmp, { recursive: true, force: true });
    }
  });

  it("[F-LOG-013] clears SIGKILL grace timer on clean exit, closes readline on ENOENT, and times out checkTool on hung child", async () => {
    // (a) Fake timers: child exits 5ms after SIGTERM -> SIGKILL must NOT be sent at 10s
    vi.useFakeTimers();
    const sigChild = new EventEmitter();
    sigChild.stdin = new PassThrough();
    sigChild.stdout = new PassThrough();
    sigChild.stderr = new PassThrough();
    sigChild.exitCode = null;
    sigChild.signalCode = null;
    sigChild.kill = vi.fn((sig) => {
      if (sig === "SIGTERM") {
        sigChild.signalCode = sig;
        setTimeout(() => {
          sigChild.stderr.end();
          sigChild.emit("close", null, sig);
        }, 5);
      }
    });
    spawnOverride.fn = () => sigChild;

    const ac = new AbortController();
    const { done: doneA } = runTool("mongodump", [], {
      stdio: ["ignore", "pipe", "pipe"],
      signal: ac.signal,
    });
    const settledA = doneA.catch((e) => e);
    ac.abort(new Error("user abort"));
    await vi.advanceTimersByTimeAsync(15_000);
    const errA = await settledA;
    expect(errA).toBeInstanceOf(Error);
    expect(sigChild.kill).toHaveBeenCalledWith("SIGTERM");
    expect(sigChild.kill).not.toHaveBeenCalledWith("SIGKILL");

    vi.useRealTimers();

    // (b) ENOENT error closes readline interface
    const origCreateInterface = readline.createInterface.bind(readline);
    let rlCloseSpy = null;
    vi.spyOn(readline, "createInterface").mockImplementation((opts) => {
      const rl = origCreateInterface(opts);
      rlCloseSpy = vi.spyOn(rl, "close");
      return rl;
    });

    const enoentErr = Object.assign(new Error("spawn missing ENOENT"), { code: "ENOENT" });
    spawnOverride.fn = () => createFakeChild({ emitError: enoentErr });
    await expect(
      runTool("missingbin", [], { stdio: ["ignore", "pipe", "pipe"] }).done,
    ).rejects.toThrow(/not found/);
    expect(rlCloseSpy).not.toBeNull();
    expect(rlCloseSpy).toHaveBeenCalled();

    // (c) Hung child in checkTool times out cleanly instead of hanging
    const hungChild = new EventEmitter();
    hungChild.stdout = new PassThrough();
    hungChild.stderr = new PassThrough();
    hungChild.kill = vi.fn((sig) => {
      hungChild.emit("close", null, sig);
    });
    spawnOverride.fn = () => hungChild;

    const checkResult = await Promise.race([
      checkTool("hung-mongodump", 50),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("checkTool hung without timing out")), 300),
      ),
    ]);
    expect(checkResult.available).toBe(false);
    expect(checkResult.error).toMatch(/timed out/i);
  });

  it("[F-LOG-014] aborts hung pre-backup MongoClient calls promptly and logs warnings on non-abort metadata errors", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const encKey = generateKey();
    const config = makeBaseConfig({
      encryptionKey: encKey,
      mongo: { ...makeBaseConfig().mongo, oplog: false },
    });

    // Part 1: Hung MongoClient.connect() aborts promptly on signal (< 500ms) and closes client
    const closeSpies = [];
    mongoClientOverride.factory = () => {
      let pendingTimer = null;
      const closeSpy = vi.fn(async () => {
        if (pendingTimer) {
          clearTimeout(pendingTimer);
          pendingTimer = null;
        }
      });
      closeSpies.push(closeSpy);
      return {
        connect: () =>
          new Promise((_resolve, reject) => {
            pendingTimer = setTimeout(() => reject(new Error("serverSelectionTimeout")), 1_500);
          }),
        close: closeSpy,
        db: () => ({ command: async () => ({}) }),
      };
    };

    // Fallback spawn in case untouched src swallows the timeout and proceeds to mongodump
    const gz = gzipSync(
      buildSyntheticArchive([{ db: "app", collection: "users", docs: [{ _id: 1 }] }]),
    );
    spawnOverride.fn = () =>
      createFakeChild({
        exitCode: 0,
        stdoutData: gz,
        stderrLines: ["done dumping app.users (1 documents)"],
      });

    const ac = new AbortController();
    setTimeout(() => ac.abort(new Error("backup cancelled by operator")), 20);
    const t0 = Date.now();
    await expect(runBackup({ config, storage, logger, signal: ac.signal })).rejects.toThrow(
      /abort|cancel/i,
    );
    const elapsed = Date.now() - t0;
    expect(elapsed).toBeLessThan(500);
    expect(closeSpies.some((s) => s.mock.calls.length > 0)).toBe(true);

    // Part 2: Non-abort error during metadata/size estimation logs logger.warn
    logger.warn.mockClear();
    mongoClientOverride.factory = () => ({
      connect: async () => {
        throw new Error("metadata query auth failed");
      },
      close: async () => {},
      db: () => ({ command: async () => ({}) }),
    });
    spawnOverride.fn = () =>
      createFakeChild({
        exitCode: 0,
        stdoutData: gz,
        stderrLines: ["done dumping app.users (1 documents)"],
      });

    await runBackup({ config, storage, logger });
    expect(logger.warn).toHaveBeenCalled();
  });

  it("[F-LOG-015] aborts Stage 2 MongoClient.connect() promptly on signal and adds --writeConcern=majority when w= is absent", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog015-"));
    try {
      // (a) Stage 2 restorePointInTime MongoClient.connect() aborts promptly on signal
      const storage = makeMemoryStorage();
      const logger = makeLogger();
      const config = makeBaseConfig({ workDir });
      const scope = scopeOf(config);

      const baseGz = gzipSync(
        buildSyntheticArchive([{ db: "app", collection: "users", docs: [{ _id: 1 }] }]),
      );
      const baseKey = archiveKey(scope, new Date("2026-03-10T10:00:00Z"), { encrypted: false });
      await storage.upload(baseKey, Readable.from([baseGz]));
      await writeManifest(storage, {
        version: 2,
        engine: "logical",
        key: baseKey,
        database: "*",
        createdAt: "2026-03-10T10:00:00.000Z",
        completedAt: "2026-03-10T10:01:00.000Z",
        durationMs: 1000,
        size: baseGz.length,
        sha256: createHash("sha256").update(baseGz).digest("hex"),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "100:1", endTs: "110:1" },
        excludedCollections: [],
        collections: { "app.users": 1 },
      });

      const sliceDoc = Buffer.from(
        BSON.serialize({
          ts: toBsonTimestamp({ t: 120, i: 1 }),
          op: "i",
          ns: "app.users",
          o: { _id: 2 },
        }),
      );
      await uploadOplogSlice({
        config,
        storage,
        logger,
        rawDocs: [sliceDoc],
        startTs: { t: 110, i: 2 },
        endTs: { t: 130, i: 1 },
        previousEndTs: { t: 110, i: 1 },
      });

      let timer = null;
      const closeSpy = vi.fn(async () => {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
      });
      const hungFactory = () => ({
        connect: () =>
          new Promise((_resolve, reject) => {
            timer = setTimeout(() => reject(new Error("connect timed out")), 1_500);
          }),
        close: closeSpy,
        db: () => ({}),
      });
      mongoClientOverride.factory = hungFactory;

      const ac = new AbortController();
      setTimeout(() => ac.abort(new Error("pitr cancelled")), 10);
      const t0 = Date.now();
      await expect(
        restoreBackup(
          {
            config,
            storage,
            logger,
            signal: ac.signal,
            createMongoClient: hungFactory,
            tools: {
              async mongorestore() {
                const stdin = new PassThrough();
                stdin.resume();
                return { stdin, done: Promise.resolve({ restored: 1, failed: 0 }) };
              },
            },
          },
          { uri: "mongodb://localhost:27017", pitr: "125:1", preVerify: false },
        ),
      ).rejects.toThrow(/abort|cancel/i);
      const elapsed = Date.now() - t0;
      expect(elapsed).toBeLessThan(500);
      expect(closeSpy).toHaveBeenCalled();

      // (b) mongorestore adds --writeConcern=majority unless w= is already specified in URI
      const spawnedCalls = [];
      spawnOverride.fn = (cmd, args) => {
        spawnedCalls.push({ cmd, args });
        return createFakeChild({
          exitCode: 0,
          stderrLines: ["1 document(s) restored successfully. 0 document(s) failed."],
        });
      };

      const r1 = await mongorestore({ uri: "mongodb://localhost:27017", workDir });
      r1.stdin.end();
      await r1.done;
      expect(spawnedCalls[0].args).toContain("--writeConcern=majority");

      const r2 = await mongorestore({ uri: "mongodb://localhost:27017/?w=1", workDir });
      r2.stdin.end();
      await r2.done;
      expect(spawnedCalls[1].args).not.toContain("--writeConcern=majority");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-016] documents and enforces explicit CLUSTER_SYSTEM_NS_EXCLUDE without user/role temp-collection swap", async () => {
    expect(CLUSTER_SYSTEM_NS_EXCLUDE).toEqual(
      expect.arrayContaining([
        "admin.system.version",
        "admin.system.keys",
        "config.system.sessions",
        "config.system.preimages",
        "config.transactions",
        "config.rangeDeletions",
        "config.shards",
        "config.version",
        "config.locks",
        "config.lockpings",
        "config.mongos",
        "config.actionlog",
        "config.changelog",
        "config.migrations",
        "config.cache.*",
        "config.shard.catalog.*",
      ]),
    );
    expect(CLUSTER_SYSTEM_NS_EXCLUDE).not.toContain("admin.system.users");
    expect(CLUSTER_SYSTEM_NS_EXCLUDE).not.toContain("admin.system.roles");

    // Verify ArchiveNamespaceFilter preserves embedded oplog & user collections while stripping excluded namespaces
    const rawArchive = buildSyntheticArchive([
      { db: "admin", collection: "system.keys", docs: [{ _id: "k1" }] },
      { db: "config", collection: "system.preimages", docs: [{ _id: "p1" }] },
      { db: "config", collection: "actionlog", docs: [{ _id: "a1" }] },
      { db: "config", collection: "changelog", docs: [{ _id: "c1" }] },
      { db: "config", collection: "migrations", docs: [{ _id: "m1" }] },
      { db: "app", collection: "items", docs: [{ _id: "i1" }] },
      {
        db: "",
        collection: "",
        docs: [{ ts: new Timestamp({ t: 100, i: 1 }), op: "i", ns: "app.items", o: { _id: "i2" } }],
      },
    ]);

    const filter = new ArchiveNamespaceFilter(CLUSTER_SYSTEM_NS_EXCLUDE);
    const outChunks = [];
    Readable.from([rawArchive]).pipe(filter);
    for await (const c of filter) outChunks.push(c);

    const kept = [];
    await streamArchiveCollections(
      Readable.from([Buffer.concat(outChunks)]),
      () => true,
      async ({ db, collection }) => {
        kept.push(db === "" && collection === "" ? "<oplog>" : `${db}.${collection}`);
      },
    );
    expect(kept).toEqual(["app.items", "<oplog>"]);

    // Verify mongorestore never passes --restoreDbUsersAndRoles
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog016-"));
    try {
      let spawnedArgs = [];
      spawnOverride.fn = (_cmd, args) => {
        spawnedArgs = args;
        return createFakeChild({
          exitCode: 0,
          stderrLines: ["0 document(s) restored successfully. 0 document(s) failed."],
        });
      };
      const res = await mongorestore({
        uri: "mongodb://localhost:27017",
        workDir,
        systemNsExclude: CLUSTER_SYSTEM_NS_EXCLUDE,
      });
      res.stdin.end();
      await res.done;
      expect(spawnedArgs).not.toContain("--restoreDbUsersAndRoles");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-017] rejects unpaired nsFrom/nsTo and whitespace or comma-only nsInclude/nsExclude in mongorestore", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-flog017-"));
    try {
      const spawnSpy = vi.fn();
      spawnOverride.fn = spawnSpy;

      await expect(
        mongorestore({ uri: "mongodb://localhost:27017", workDir, nsFrom: "a.*" }),
      ).rejects.toThrow(/nsFrom and nsTo/);
      await expect(
        mongorestore({ uri: "mongodb://localhost:27017", workDir, nsTo: "b.*" }),
      ).rejects.toThrow(/nsFrom and nsTo/);
      await expect(
        mongorestore({ uri: "mongodb://localhost:27017", workDir, nsFrom: "a.*", nsTo: "   " }),
      ).rejects.toThrow(/nsFrom and nsTo/);
      await expect(
        mongorestore({ uri: "mongodb://localhost:27017", workDir, nsInclude: " ,  " }),
      ).rejects.toThrow(/nsInclude/);
      await expect(
        mongorestore({ uri: "mongodb://localhost:27017", workDir, nsExclude: "  " }),
      ).rejects.toThrow(/nsExclude/);

      expect(spawnSpy).not.toHaveBeenCalled();
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-018] invalidates ArchiveOplogTap on missing 0x00 BSON terminator and processes large chunked docs linearly", async () => {
    // (a) Non-0x00 terminator inside prelude header invalidates ArchiveOplogTap
    const valid = buildSyntheticArchive([
      { db: "app", collection: "users", docs: [{ _id: 1 }] },
      {
        db: "",
        collection: "",
        docs: [{ ts: new Timestamp({ t: 100, i: 1 }), op: "i", ns: "app.users", o: { _id: 2 } }],
      },
    ]);
    const preludeLen = valid.readInt32LE(4);
    const badTerm = Buffer.from(valid);
    badTerm[4 + preludeLen - 1] = 0x42; // corrupt 0x00 terminator

    const tapBad = new ArchiveOplogTap();
    Readable.from([badTerm]).pipe(tapBad);
    for await (const _c of tapBad) {
      // drain
    }
    expect(tapBad.bounds.validArchive).toBe(false);
    expect(tapBad.bounds.corrupted).toBe(true);
    expect(tapBad.bounds.archiveTerminated).toBe(false);

    // (b) Multi-MiB document split into 4 KiB chunks streams cleanly via ByteQueue without O(N^2) stall
    const bigPayload = "x".repeat(3 * 1024 * 1024);
    const largeArchive = buildSyntheticArchive([
      { db: "app", collection: "blobs", docs: [{ _id: "big", data: bigPayload }] },
      {
        db: "",
        collection: "",
        docs: [{ ts: new Timestamp({ t: 200, i: 1 }), op: "i", ns: "app.blobs", o: { _id: 1 } }],
      },
    ]);
    const chunks = [];
    for (let i = 0; i < largeArchive.length; i += 4096) {
      chunks.push(largeArchive.subarray(i, i + 4096));
    }

    const tapLarge = new ArchiveOplogTap();
    Readable.from(chunks).pipe(tapLarge);
    for await (const _c of tapLarge) {
      // drain
    }
    expect(tapLarge.bounds.validArchive).toBe(true);
    expect(tapLarge.bounds.archiveTerminated).toBe(true);
    expect(tapLarge.bounds.corrupted).toBe(false);
    expect(tapLarge.bounds.entries).toBe(1);

    // Also exercise readBsonDocumentsFromStream & streamArchiveCollections on chunked docs
    const bigDoc = Buffer.from(BSON.serialize({ _id: "big-bson", data: bigPayload }));
    const bsonChunks = [];
    for (let i = 0; i < bigDoc.length; i += 4096) {
      bsonChunks.push(bigDoc.subarray(i, i + 4096));
    }
    const readCount = await readBsonDocumentsFromStream(Readable.from(bsonChunks), async () => {});
    expect(readCount).toBe(1);

    const archiveCount = await streamArchiveCollections(
      Readable.from(chunks),
      (db, coll) => db === "app" && coll === "blobs",
      async () => {},
    );
    expect(archiveCount).toBe(1);
  });

  it("[F-LOG-019] checks storage.head in describeBackup and validates .cluster manifest.size against digest or child sum", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const config = makeBaseConfig();

    // (a) Manifest exists while archive object was deleted -> describeBackup rejects with IntegrityError
    const orphanKey = "mv/_cluster/2026-03-10/20260310T090000Z.archive.gz";
    await writeManifest(storage, {
      version: 2,
      key: orphanKey,
      database: "*",
      createdAt: "2026-03-10T09:00:00.000Z",
      completedAt: "2026-03-10T09:00:05.000Z",
      durationMs: 1000,
      size: 123,
      sha256: "a".repeat(64),
      compression: "gzip",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: {},
    });
    await expect(describeBackup({ config, storage, logger }, orphanKey)).rejects.toThrow(
      IntegrityError,
    );

    // (b) .cluster manifest.size mismatch (neither digest.bytes nor childBackupSizeSum) throws IntegrityError
    const cfgGz = gzipSync(Buffer.from("cfg-archive"));
    const s1Gz = gzipSync(Buffer.from("shard-archive"));
    const cfgKey = "mv/configsvr/b1.archive.gz";
    const s1Key = "mv/shards/rs1/b1.archive.gz";
    const clusterKey = "mv/_cluster/2026-03-10/b1.cluster";

    for (const [k, buf] of [
      [cfgKey, cfgGz],
      [s1Key, s1Gz],
    ]) {
      await storage.upload(k, buf);
      await writeManifest(storage, {
        version: 2,
        key: k,
        database: "*",
        createdAt: "2026-03-10T10:00:00.000Z",
        completedAt: "2026-03-10T10:00:01.000Z",
        durationMs: 100,
        size: buf.length,
        sha256: createHash("sha256").update(buf).digest("hex"),
        compression: "gzip",
        encryption: null,
        oplog: false,
        excludedCollections: [],
        collections: {},
      });
    }

    const clusterMeta = {
      clusterTime: "100:1",
      configsvr: { backupKey: cfgKey, status: "success" },
      shards: { rs1: { backupKey: s1Key, status: "success" } },
    };
    const clusterBuf = Buffer.from(JSON.stringify(clusterMeta));
    const clusterSha = createHash("sha256").update(clusterBuf).digest("hex");
    await storage.upload(clusterKey, clusterBuf);

    // Valid with childBackupSizeSum
    await writeManifest(storage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: "2026-03-10T10:00:00.000Z",
      completedAt: "2026-03-10T10:00:01.000Z",
      durationMs: 100,
      size: cfgGz.length + s1Gz.length,
      sha256: clusterSha,
      compression: "none",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: {},
      sharding: clusterMeta,
    });
    await expect(verifyBackup({ config, storage, logger }, clusterKey)).resolves.toMatchObject({
      key: clusterKey,
      size: clusterBuf.length,
    });

    // Valid with digest.bytes
    await writeManifest(storage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: "2026-03-10T10:00:00.000Z",
      completedAt: "2026-03-10T10:00:01.000Z",
      durationMs: 100,
      size: clusterBuf.length,
      sha256: clusterSha,
      compression: "none",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: {},
      sharding: clusterMeta,
    });
    await expect(verifyBackup({ config, storage, logger }, clusterKey)).resolves.toMatchObject({
      key: clusterKey,
      size: clusterBuf.length,
    });

    // Invalid size (neither digest.bytes nor childBackupSizeSum) -> rejects IntegrityError
    await writeManifest(storage, {
      version: 2,
      type: "sharded-cluster-backup",
      key: clusterKey,
      database: "*",
      createdAt: "2026-03-10T10:00:00.000Z",
      completedAt: "2026-03-10T10:00:01.000Z",
      durationMs: 100,
      size: 999999,
      sha256: clusterSha,
      compression: "none",
      encryption: null,
      oplog: false,
      excludedCollections: [],
      collections: {},
      sharding: clusterMeta,
    });
    await expect(verifyBackup({ config, storage, logger }, clusterKey)).rejects.toThrow(
      IntegrityError,
    );
  });

  it("[Challenger-Iter1-Validation-And-EdgeCases] rejects invalid namespace options before PITR drops, skips cleanup on lock-loss/lease-expired aborts, and strips #fragment before inspecting URI query params", async () => {
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-challenger-iter1-"));
    try {
      // (a) restoreBackup & restorePointInTime reject invalid nsInclude/nsExclude/nsFrom without any dropDatabase/collection.drop calls
      const storageA = makeMemoryStorage();
      const logger = makeLogger();
      const configA = makeBaseConfig({ workDir });
      const scopeA = scopeOf(configA);

      const baseGz = gzipSync(
        buildSyntheticArchive([{ db: "shop", collection: "orders", docs: [{ _id: 1 }] }]),
      );
      const baseKey = archiveKey(scopeA, new Date("2026-03-10T10:00:00Z"), { encrypted: false });
      await storageA.upload(baseKey, Readable.from([baseGz]));
      await writeManifest(storageA, {
        version: 2,
        engine: "logical",
        key: baseKey,
        database: "*",
        createdAt: "2026-03-10T10:00:00.000Z",
        completedAt: "2026-03-10T10:01:00.000Z",
        durationMs: 1000,
        size: baseGz.length,
        sha256: createHash("sha256").update(baseGz).digest("hex"),
        compression: "gzip",
        encryption: null,
        oplog: true,
        oplogWindow: { startTs: "100:1", endTs: "110:1" },
        excludedCollections: [],
        collections: { "shop.orders": 1 },
      });

      const sliceDoc = Buffer.from(
        BSON.serialize({
          ts: toBsonTimestamp({ t: 120, i: 1 }),
          op: "i",
          ns: "shop.orders",
          o: { _id: 2 },
        }),
      );
      await uploadOplogSlice({
        config: configA,
        storage: storageA,
        logger,
        rawDocs: [sliceDoc],
        startTs: { t: 100, i: 1 },
        endTs: { t: 130, i: 1 },
        previousEndTs: { t: 100, i: 1 },
      });

      const droppedTargets = [];
      const makeTrackedClient = () => ({
        async connect() {},
        async close() {},
        db(name) {
          if (name === "admin") {
            return {
              async command(cmd) {
                if (cmd?.listDatabases) {
                  return {
                    databases: [
                      { name: "admin" },
                      { name: "local" },
                      { name: "config" },
                      { name: "shop" },
                      { name: "unrelated_prod_db" },
                    ],
                  };
                }
                return { ok: 1 };
              },
            };
          }
          return {
            async dropDatabase() {
              droppedTargets.push(name);
            },
            listCollections() {
              return {
                async toArray() {
                  return [{ name: "orders" }];
                },
              };
            },
            collection(coll) {
              return {
                async drop() {
                  droppedTargets.push(`${name}.${coll}`);
                },
              };
            },
          };
        },
      });
      mongoClientOverride.factory = makeTrackedClient;

      const makeRestoreTools = () => ({
        async mongorestore(opts) {
          // Mirror real mongorestore validation when reached
          const includeList =
            opts.nsInclude !== undefined
              ? opts.nsInclude
                  .split(",")
                  .map((p) => p.trim())
                  .filter(Boolean)
              : [];
          if (opts.nsInclude !== undefined && includeList.length === 0) {
            throw new Error("nsInclude must contain at least one non-empty namespace pattern");
          }
          const excludeList =
            opts.nsExclude !== undefined
              ? opts.nsExclude
                  .split(",")
                  .map((p) => p.trim())
                  .filter(Boolean)
              : [];
          if (opts.nsExclude !== undefined && excludeList.length === 0) {
            throw new Error("nsExclude must contain at least one non-empty namespace pattern");
          }
          const stdin = new PassThrough();
          stdin.resume();
          return { stdin, done: Promise.resolve({ restored: 0, failed: 0 }) };
        },
        async replayOplogFile() {},
      });

      const restoreCtx = {
        config: configA,
        storage: storageA,
        logger,
        createMongoClient: makeTrackedClient,
        tools: makeRestoreTools(),
      };

      await expect(
        restoreBackup(restoreCtx, {
          uri: "mongodb://localhost:27017",
          pitr: "125:1",
          drop: true,
          preVerify: true,
          nsInclude: " , ",
        }),
      ).rejects.toThrow(/nsInclude/);
      expect(droppedTargets).toEqual([]);

      await expect(
        restoreBackup(restoreCtx, {
          uri: "mongodb://localhost:27017",
          pitr: "125:1",
          drop: true,
          preVerify: true,
          nsExclude: " , ",
        }),
      ).rejects.toThrow(/nsExclude/);
      expect(droppedTargets).toEqual([]);

      await expect(
        restoreBackup(restoreCtx, {
          uri: "mongodb://localhost:27017",
          pitr: "125:1",
          drop: true,
          preVerify: true,
          nsFrom: "a.*",
        }),
      ).rejects.toThrow(/nsFrom and nsTo/);
      expect(droppedTargets).toEqual([]);

      await expect(
        restorePointInTime(restoreCtx, {
          uri: "mongodb://localhost:27017",
          pitr: "125:1",
          drop: true,
          preVerify: true,
          nsInclude: " , ",
        }),
      ).rejects.toThrow(/nsInclude/);
      expect(droppedTargets).toEqual([]);

      // (b) runBackup lock-loss abort during storage.head() skips storage.remove(key)
      for (const lockMsg of [
        "Lost distributed backup lock",
        'Distributed lock lease expired on "mv.lock" after 90000ms without successful renewal',
      ]) {
        const storageB = makeMemoryStorage();
        const configB = makeBaseConfig({ mongo: { ...makeBaseConfig().mongo, oplog: false } });
        const ac = new AbortController();
        const origHead = storageB.head.bind(storageB);
        storageB.head = vi.fn(async (k) => {
          ac.abort(new Error(lockMsg));
          return origHead(k);
        });
        const removeSpy = vi.spyOn(storageB, "remove");

        await expect(
          runBackup({
            config: configB,
            storage: storageB,
            logger,
            signal: ac.signal,
            tools: {
              mongodump: vi.fn(async () => ({
                stdout: Readable.from([Buffer.from("archive-payload")]),
                done: Promise.resolve({ collections: { "app.users": 1 } }),
              })),
            },
          }),
        ).rejects.toThrow(lockMsg);

        expect(removeSpy).not.toHaveBeenCalled();
      }

      // (c) URI helpers strip #fragment before inspecting ? query parameters
      expect(uriHasReadPreference("mongodb://localhost:27017#?readPreference=primary")).toBe(false);
      expect(
        withUriReadPreference(
          "mongodb://localhost:27017#?readPreference=primary",
          "secondaryPreferred",
        ),
      ).toBe(
        "mongodb://localhost:27017/?readPreference=secondaryPreferred#?readPreference=primary",
      );

      let capturedArgs = [];
      spawnOverride.fn = (_cmd, args) => {
        capturedArgs = args;
        return createFakeChild({
          exitCode: 0,
          stderrLines: ["1 document(s) restored successfully. 0 document(s) failed."],
        });
      };
      const mr = await mongorestore({
        uri: "mongodb://localhost:27017#?w=1",
        workDir,
      });
      mr.stdin.end();
      await mr.done;
      expect(capturedArgs).toContain("--writeConcern=majority");
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });

  it("[F-LOG-002-RealCluster] getOplogBounds passes readPreference to replSetGetStatus and clamps majorityTs to read target's oplog.rs newestTs", async () => {
    // Case A: Lagged secondary (newestDoc=190:1) while replSetGetStatus reports majorityTs=210:1
    let capturedCmdOpts = null;
    const makeClient = (oldest, newest, majority) => ({
      connect: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
      db: vi.fn((name) => {
        if (name === "local") {
          return {
            collection: () => ({
              find: () => ({
                sort: (s) => ({
                  limit: () => ({
                    next: async () => ({
                      ts: s.$natural === 1 ? toBsonTimestamp(oldest) : toBsonTimestamp(newest),
                    }),
                  }),
                }),
              }),
            }),
          };
        }
        if (name === "admin") {
          return {
            command: vi.fn(async (_cmd, opts) => {
              capturedCmdOpts = opts;
              return {
                optimes: {
                  lastCommittedOpTime: { ts: toBsonTimestamp(majority) },
                  appliedOpTime: { ts: toBsonTimestamp(majority) },
                },
              };
            }),
          };
        }
        return { collection: () => ({}) };
      }),
    });

    const secBounds = await getOplogBounds("mongodb://localhost:27017/?readPreference=secondary", {
      phase: "pre",
      createMongoClient: () => makeClient({ t: 100, i: 1 }, { t: 190, i: 1 }, { t: 210, i: 1 }),
    });
    expect(capturedCmdOpts).toEqual({ readPreference: "secondary" });
    expect(secBounds.newestTs).toEqual({ t: 190, i: 1 });

    // Case B: Primary with uncommitted w:1 write (newestDoc=210:1) while majorityTs=190:1
    const priBounds = await getOplogBounds("mongodb://localhost:27017/?readPreference=primary", {
      phase: "pre",
      createMongoClient: () => makeClient({ t: 100, i: 1 }, { t: 210, i: 1 }, { t: 190, i: 1 }),
    });
    expect(capturedCmdOpts).toEqual({ readPreference: "primary" });
    expect(priBounds.newestTs).toEqual({ t: 190, i: 1 });
  });

  it("[F-LOG-002-StepDown] runBackup with readPreference=primary aborts and cleans up partial upload if primary steps down mid-backup", async () => {
    const storage = makeMemoryStorage();
    const logger = makeLogger();
    const workDir = await fs.mkdtemp(path.join(os.tmpdir(), "mv-stepdown-"));
    try {
      const config = makeBaseConfig({
        workDir,
        mongo: {
          uri: "mongodb://127.0.0.1:27017/?directConnection=true",
          database: "*",
          oplog: true,
          excludeCollections: [],
          parallelCollections: 1,
          readPreference: "primary",
        },
      });

      let helloCalls = 0;
      mongoClientOverride.factory = () => ({
        connect: vi.fn(async () => {}),
        close: vi.fn(async () => {}),
        db: vi.fn((name) => {
          if (name === "admin") {
            return {
              command: vi.fn(async (cmd) => {
                if (cmd.hello) {
                  helloCalls++;
                  return {
                    isWritablePrimary: helloCalls === 1,
                    me: "127.0.0.1:27017",
                  };
                }
                if (cmd.replSetGetStatus) {
                  return {
                    optimes: {
                      lastCommittedOpTime: { ts: toBsonTimestamp({ t: 100, i: 1 }) },
                      appliedOpTime: { ts: toBsonTimestamp({ t: 100, i: 1 }) },
                    },
                  };
                }
                if (cmd.listDatabases) return { databases: [] };
                return {};
              }),
            };
          }
          if (name === "local") {
            return {
              collection: () => ({
                find: () => ({
                  sort: () => ({
                    limit: () => ({
                      next: async () => ({ ts: toBsonTimestamp({ t: 100, i: 1 }) }),
                    }),
                  }),
                }),
              }),
            };
          }
          return { collection: () => ({}) };
        }),
      });

      const validArchive = Buffer.concat([
        uint32LE(ARCHIVE_MAGIC_NUMBER),
        Buffer.from(BSON.serialize({ concurrent_collections: 1, version: "0.1" })),
        int32LE(-1),
        int32LE(-1),
      ]);

      spawnOverride.fn = () =>
        createFakeChild({
          exitCode: 0,
          stdoutChunks: [validArchive],
          stderrLines: ["done dumping app.users (1 document)"],
        });

      await expect(runBackup({ config, storage, logger })).rejects.toThrow(/stepped down/i);
      const keys = [];
      for await (const obj of storage.list("")) keys.push(obj.key);
      expect(keys).toEqual([]);
    } finally {
      await fs.rm(workDir, { recursive: true, force: true });
    }
  });
});
