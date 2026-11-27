import { Readable } from "node:stream";
import { BSON, Long, Timestamp } from "mongodb";
import { describe, expect, it } from "vitest";
import {
  ARCHIVE_MAGIC_NUMBER,
  ArchiveNamespaceFilter,
  ArchiveOplogTap,
  readBsonDocumentsFromStream,
  streamArchiveCollections,
} from "../src/archive-stream.js";

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

function buildSyntheticArchive(chunks) {
  const parts = [];
  // 1. Magic number
  parts.push(uint32LE(ARCHIVE_MAGIC_NUMBER));
  // 2. Prelude header
  parts.push(Buffer.from(BSON.serialize({ concurrent_collections: 1, version: "0.1" })));
  // 3. Collection metadata entries (optional) + terminator (-1)
  parts.push(
    Buffer.from(
      BSON.serialize({ db: "app", collection: "users", metadata: "{}", type: "collection" }),
    ),
  );
  parts.push(int32LE(-1));

  // 4. Data / oplog chunks
  for (const chunk of chunks) {
    parts.push(Buffer.from(BSON.serialize({ db: chunk.db, collection: chunk.collection })));
    for (const doc of chunk.docs) {
      parts.push(Buffer.from(BSON.serialize(doc)));
    }
    parts.push(int32LE(-1));
  }
  // End of archive terminator (-1)
  parts.push(int32LE(-1));
  return Buffer.concat(parts);
}

describe("archive-stream (zero-allocation mongodump --archive parser)", () => {
  it("extracts exact firstTs, lastTs, and count of embedded oplog while passing all bytes untouched", async () => {
    const rawArchive = buildSyntheticArchive([
      {
        db: "app",
        collection: "users",
        docs: [
          { _id: 1, name: "alice" },
          { _id: 2, name: "bob" },
        ],
      },
      {
        db: "",
        collection: "",
        docs: [
          { ts: new Timestamp({ t: 1700000001, i: 1 }), op: "i", ns: "app.users", o: { _id: 3 } },
          { ts: new Timestamp({ t: 1700000005, i: 2 }), op: "i", ns: "app.users", o: { _id: 4 } },
          { ts: new Timestamp({ t: 1700000009, i: 7 }), op: "u", ns: "app.users", o: { $set: {} } },
        ],
      },
    ]);

    // Feed in tiny 7-byte chunks to stress state-machine boundary handling
    const tinyChunks = [];
    for (let i = 0; i < rawArchive.length; i += 7) {
      tinyChunks.push(rawArchive.subarray(i, i + 7));
    }

    const tap = new ArchiveOplogTap();
    const out = [];
    const source = Readable.from(tinyChunks);
    source.pipe(tap);
    for await (const c of tap) {
      out.push(c);
    }

    expect(Buffer.concat(out).equals(rawArchive)).toBe(true);
    expect(tap.bounds.validArchive).toBe(true);
    expect(tap.bounds.entries).toBe(3);
    expect(tap.bounds.firstTs).toEqual({ t: 1700000001, i: 1 });
    expect(tap.bounds.lastTs).toEqual({ t: 1700000009, i: 7 });
  });

  it("falls back cleanly to pure passthrough on non-archive bytes", async () => {
    const data = Buffer.from("NOT_A_MONGODUMP_ARCHIVE_STREAM_12345");
    const tap = new ArchiveOplogTap();
    const out = [];
    Readable.from([data.subarray(0, 3), data.subarray(3)]).pipe(tap);
    for await (const c of tap) out.push(c);

    expect(Buffer.concat(out).equals(data)).toBe(true);
    expect(tap.bounds.validArchive).toBe(false);
    expect(tap.bounds.entries).toBe(0);
    expect(tap.bounds.lastTs).toBeNull();
  });

  it("streams filtered collections via streamArchiveCollections without buffering other collections", async () => {
    const rawArchive = buildSyntheticArchive([
      {
        db: "config",
        collection: "databases",
        docs: [{ _id: "shop", primary: "shard01" }],
      },
      {
        db: "config",
        collection: "lockpings",
        docs: [{ _id: "ignored-lock" }],
      },
      {
        db: "config",
        collection: "chunks",
        docs: [
          { _id: "c1", shard: "shard01" },
          { _id: "c2", shard: "shard02" },
        ],
      },
    ]);

    const seen = [];
    await streamArchiveCollections(
      Readable.from([rawArchive]),
      (db, coll) => db === "config" && (coll === "databases" || coll === "chunks"),
      async ({ db, collection, rawDoc }) => {
        seen.push({ ns: `${db}.${collection}`, doc: BSON.deserialize(rawDoc) });
      },
    );

    expect(seen.map((s) => s.ns)).toEqual(["config.databases", "config.chunks", "config.chunks"]);
    expect(seen[0].doc._id).toBe("shop");
    expect(seen[2].doc._id).toBe("c2");
  });

  it("detects truncated BSON documents in readBsonDocumentsFromStream", async () => {
    const valid = Buffer.from(BSON.serialize({ _id: 1, v: "hello" }));
    const truncated = valid.subarray(0, valid.length - 3);
    await expect(
      readBsonDocumentsFromStream(Readable.from([truncated]), async () => {}),
    ).rejects.toThrow(/truncated/i);
  });
});

function buildMongodumpV100Archive(
  collections,
  {
    includeOplog = true,
    oplogDocs = [],
    omitPreludeMeta = false,
    omitOplogEof = false,
    trailingArchiveTerminator = false,
  } = {},
) {
  const parts = [];
  // 1. Magic number
  parts.push(uint32LE(ARCHIVE_MAGIC_NUMBER));
  // 2. Prelude header matching mongodump 100.13.0 against mongod 8.0.38
  parts.push(
    Buffer.from(
      BSON.serialize({
        concurrent_collections: 4,
        version: "0.1",
        server_version: "8.0.38",
        tool_version: "100.13.0",
      }),
    ),
  );
  // 3. Prelude CollectionMetadata entries + terminator (-1)
  if (!omitPreludeMeta) {
    for (const col of collections) {
      parts.push(
        Buffer.from(
          BSON.serialize({
            db: col.db,
            collection: col.collection,
            metadata: col.metadata ?? "{}",
            type: col.type ?? "collection",
          }),
        ),
      );
    }
    if (includeOplog) {
      parts.push(
        Buffer.from(
          BSON.serialize({
            db: "",
            collection: "oplog",
            metadata: "",
            type: "",
          }),
        ),
      );
    }
  }
  parts.push(int32LE(-1));

  // 4. Data chunks + per-namespace EOF:true chunks with negative int64 CRC
  for (const col of collections) {
    const chunkDb = col.chunkDb ?? col.db;
    const chunkCollection = col.chunkCollection ?? col.collection;
    const docs = col.docs ?? [];
    if (docs.length > 0) {
      parts.push(
        Buffer.from(
          BSON.serialize({
            db: chunkDb,
            collection: chunkCollection,
            EOF: false,
            CRC: new Long(0, 0, false),
          }),
        ),
      );
      for (const doc of docs) {
        parts.push(Buffer.from(BSON.serialize(doc)));
      }
      parts.push(int32LE(-1));
    }
    if (!col.omitEof) {
      parts.push(
        Buffer.from(
          BSON.serialize({
            db: chunkDb,
            collection: chunkCollection,
            EOF: true,
            CRC: new Long(-1, -1, false),
          }),
        ),
      );
      for (const badDoc of col.eofUnexpectedDocs ?? []) {
        parts.push(Buffer.from(BSON.serialize(badDoc)));
      }
      parts.push(int32LE(-1));
    }
  }

  // 5. Embedded oplog chunk(s) (db: "", collection: "oplog") + EOF:true
  if (includeOplog) {
    if (oplogDocs.length > 0) {
      parts.push(
        Buffer.from(
          BSON.serialize({
            db: "",
            collection: "oplog",
            EOF: false,
            CRC: new Long(0, 0, false),
          }),
        ),
      );
      for (const doc of oplogDocs) {
        parts.push(Buffer.from(BSON.serialize(doc)));
      }
      parts.push(int32LE(-1));
    }
    if (!omitOplogEof) {
      parts.push(
        Buffer.from(
          BSON.serialize({
            db: "",
            collection: "oplog",
            EOF: true,
            CRC: new Long(-1, -1, false),
          }),
        ),
      );
      parts.push(int32LE(-1));
    }
  }

  // Real mongodump 100.13.0 NEVER emits an archive-level trailing -1
  if (trailingArchiveTerminator) {
    parts.push(int32LE(-1));
  }

  return Buffer.concat(parts);
}

function splitIntoChunks(buf, chunkSize) {
  const chunks = [];
  for (let i = 0; i < buf.length; i += chunkSize) {
    chunks.push(buf.subarray(i, i + chunkSize));
  }
  return chunks;
}

async function runArchiveOplogTap(buf, chunkSize = 7) {
  const tap = new ArchiveOplogTap();
  const out = [];
  Readable.from(splitIntoChunks(buf, chunkSize)).pipe(tap);
  for await (const c of tap) {
    out.push(c);
  }
  return { output: Buffer.concat(out), bounds: tap.bounds };
}

async function runArchiveNamespaceFilter(buf, excludePatterns, chunkSize = 13) {
  const filter = new ArchiveNamespaceFilter(excludePatterns);
  const out = [];
  const source = Readable.from(splitIntoChunks(buf, chunkSize));
  const streamError = new Promise((_, reject) => {
    source.on("error", reject);
  });
  source.pipe(filter);
  const collect = (async () => {
    for await (const c of filter) {
      out.push(c);
    }
    return Buffer.concat(out);
  })();
  return Promise.race([collect, streamError]);
}

describe("archive-stream (mongodump 100.13.0 real --archive framing regression)", () => {
  it("Test A: ArchiveOplogTap parses real mongodump 100.13.0 --oplog --archive framing without trailing -1", async () => {
    const rawArchive = buildMongodumpV100Archive(
      [
        {
          db: "app",
          collection: "users",
          docs: [
            { _id: 1, name: "alice" },
            { _id: 2, name: "bob" },
          ],
        },
        {
          db: "app",
          collection: "orders",
          docs: [{ _id: 10, total: 42 }],
        },
      ],
      {
        includeOplog: true,
        oplogDocs: [
          { ts: new Timestamp({ t: 1700000010, i: 1 }), op: "i", ns: "app.users", o: { _id: 3 } },
          { ts: new Timestamp({ t: 1700000012, i: 2 }), op: "i", ns: "app.users", o: { _id: 4 } },
          { ts: new Timestamp({ t: 1700000015, i: 4 }), op: "u", ns: "app.users", o: { $set: {} } },
        ],
      },
    );

    const { output, bounds } = await runArchiveOplogTap(rawArchive, 7);

    expect(output.equals(rawArchive)).toBe(true);
    expect(bounds.validArchive).toBe(true);
    expect(bounds.archiveTerminated).toBe(true);
    expect(bounds.corrupted).toBe(false);
    expect(bounds.entries).toBe(3);
    expect(bounds.firstTs).toEqual({ t: 1700000010, i: 1 });
    expect(bounds.lastTs).toEqual({ t: 1700000015, i: 4 });
  });

  it("Test B1: ArchiveOplogTap accepts quiet --oplog archive (oplog has only EOF:true, 0 docs, no trailing -1)", async () => {
    const rawArchive = buildMongodumpV100Archive(
      [{ db: "app", collection: "users", docs: [{ _id: 1 }] }],
      { includeOplog: true, oplogDocs: [] },
    );

    const { output, bounds } = await runArchiveOplogTap(rawArchive, 7);

    expect(output.equals(rawArchive)).toBe(true);
    expect(bounds.validArchive).toBe(true);
    expect(bounds.archiveTerminated).toBe(true);
    expect(bounds.corrupted).toBe(false);
    expect(bounds.entries).toBe(0);
    expect(bounds.firstTs).toBeNull();
    expect(bounds.lastTs).toBeNull();
  });

  it("Test B2: ArchiveOplogTap accepts empty-DB archive (magic + preludeHeader + -1, 0 collections, no trailing -1)", async () => {
    const rawArchive = buildMongodumpV100Archive([], { includeOplog: false });

    const { output, bounds } = await runArchiveOplogTap(rawArchive, 5);

    expect(output.equals(rawArchive)).toBe(true);
    expect(bounds.validArchive).toBe(true);
    expect(bounds.archiveTerminated).toBe(true);
    expect(bounds.corrupted).toBe(false);
    expect(bounds.entries).toBe(0);
    expect(bounds.firstTs).toBeNull();
    expect(bounds.lastTs).toBeNull();
  });

  it("Test C: ArchiveOplogTap handles pre-8.3 timeseries prelude (type: timeseries) paired with system.buckets.<coll> chunks", async () => {
    const rawArchive = buildMongodumpV100Archive(
      [
        {
          db: "app",
          collection: "metrics",
          type: "timeseries",
          chunkCollection: "system.buckets.metrics",
          docs: [{ _id: 1, control: { version: 1 } }],
        },
      ],
      {
        includeOplog: true,
        oplogDocs: [
          {
            ts: new Timestamp({ t: 1700000100, i: 1 }),
            op: "i",
            ns: "app.system.buckets.metrics",
            o: { _id: 2 },
          },
        ],
      },
    );

    const { output, bounds } = await runArchiveOplogTap(rawArchive, 9);

    expect(output.equals(rawArchive)).toBe(true);
    expect(bounds.validArchive).toBe(true);
    expect(bounds.archiveTerminated).toBe(true);
    expect(bounds.corrupted).toBe(false);
    expect(bounds.entries).toBe(1);
    expect(bounds.firstTs).toEqual({ t: 1700000100, i: 1 });
    expect(bounds.lastTs).toEqual({ t: 1700000100, i: 1 });
  });

  it("Test D: ArchiveNamespaceFilter preserves collection:oplog & EOF:true CRC while filtering system namespaces on real framing", async () => {
    const rawArchive = buildMongodumpV100Archive(
      [
        { db: "admin", collection: "system.keys", docs: [{ _id: "k1" }] },
        { db: "config", collection: "system.sessions", docs: [{ _id: "s1" }] },
        { db: "app", collection: "users", docs: [{ _id: 1, name: "alice" }] },
      ],
      {
        includeOplog: true,
        oplogDocs: [
          { ts: new Timestamp({ t: 1700000200, i: 1 }), op: "i", ns: "app.users", o: { _id: 2 } },
        ],
      },
    );

    // Include "*.oplog" to verify embedded oplog (db: "", collection: "oplog") is exempt from exclusion
    const filtered = await runArchiveNamespaceFilter(
      rawArchive,
      ["admin.system.keys", "config.system.sessions", "*.oplog"],
      11,
    );

    const expectedOplogEofHdr = Buffer.from(
      BSON.serialize({
        db: "",
        collection: "oplog",
        EOF: true,
        CRC: new Long(-1, -1, false),
      }),
    );
    expect(filtered.includes(expectedOplogEofHdr)).toBe(true);

    const seen = [];
    await streamArchiveCollections(
      Readable.from([filtered]),
      () => true,
      async ({ db, collection, rawDoc }) => {
        seen.push({ ns: `${db}.${collection}`, doc: BSON.deserialize(rawDoc) });
      },
    );
    expect(seen.map((s) => s.ns)).toEqual(["app.users", ".oplog"]);
    expect(seen[0].doc).toEqual({ _id: 1, name: "alice" });
    expect(seen[1].doc.ns).toBe("app.users");
  });

  it("Test E: streamArchiveCollections streams config.databases and db:'',collection:'oplog' on real framing without trailing -1", async () => {
    const rawArchive = buildMongodumpV100Archive(
      [
        { db: "config", collection: "databases", docs: [{ _id: "shop", primary: "shard01" }] },
        { db: "config", collection: "lockpings", docs: [{ _id: "lock1" }] },
      ],
      {
        includeOplog: true,
        oplogDocs: [
          { ts: new Timestamp({ t: 1700000300, i: 3 }), op: "i", ns: "shop.items", o: { _id: 9 } },
        ],
      },
    );

    const seen = [];
    const count = await streamArchiveCollections(
      Readable.from(splitIntoChunks(rawArchive, 13)),
      (db, coll) => (db === "config" && coll === "databases") || (db === "" && coll === "oplog"),
      async ({ db, collection, rawDoc }) => {
        seen.push({ db, collection, doc: BSON.deserialize(rawDoc) });
      },
    );

    expect(count).toBe(2);
    expect(seen.map((s) => `${s.db}.${s.collection}`)).toEqual(["config.databases", ".oplog"]);
    expect(seen[0].doc._id).toBe("shop");
    expect(seen[1].doc.o).toEqual({ _id: 9 });
  });

  it("Test F1: all 3 parsers fail closed when truncated after app.users EOF:true before oplog EOF:true arrives", async () => {
    const truncated = buildMongodumpV100Archive(
      [{ db: "app", collection: "users", docs: [{ _id: 1 }] }],
      { includeOplog: true, oplogDocs: [], omitOplogEof: true },
    );

    const { bounds } = await runArchiveOplogTap(truncated, 7);
    expect(bounds.corrupted).toBe(true);
    expect(bounds.validArchive).toBe(false);
    expect(bounds.archiveTerminated).toBe(false);

    await expect(runArchiveNamespaceFilter(truncated, ["admin.*"], 7)).rejects.toThrow(
      /Truncated/i,
    );
    await expect(
      streamArchiveCollections(
        Readable.from([truncated]),
        () => true,
        async () => {},
      ),
    ).rejects.toThrow(/Truncated/i);
  });

  it("Test F2: all 3 parsers fail closed when truncated after EOF:false chunk before EOF:true arrives (even with empty prelude)", async () => {
    const truncated = buildMongodumpV100Archive(
      [{ db: "app", collection: "users", docs: [{ _id: 1 }], omitEof: true }],
      { includeOplog: false, omitPreludeMeta: true },
    );

    const { bounds } = await runArchiveOplogTap(truncated, 7);
    expect(bounds.corrupted).toBe(true);
    expect(bounds.validArchive).toBe(false);
    expect(bounds.archiveTerminated).toBe(false);

    await expect(runArchiveNamespaceFilter(truncated, [], 7)).rejects.toThrow(/Truncated/i);
    await expect(
      streamArchiveCollections(
        Readable.from([truncated]),
        () => true,
        async () => {},
      ),
    ).rejects.toThrow(/Truncated/i);
  });

  it("Test F3: all 3 parsers fail closed when a body BSON document appears inside an EOF:true block", async () => {
    const corrupted = buildMongodumpV100Archive(
      [
        {
          db: "app",
          collection: "users",
          docs: [{ _id: 1 }],
          eofUnexpectedDocs: [{ _id: 999, unexpected: true }],
        },
      ],
      { includeOplog: false, trailingArchiveTerminator: true },
    );

    const { bounds } = await runArchiveOplogTap(corrupted, 7);
    expect(bounds.corrupted).toBe(true);
    expect(bounds.validArchive).toBe(false);
    expect(bounds.archiveTerminated).toBe(false);

    await expect(runArchiveNamespaceFilter(corrupted, [], 7)).rejects.toThrow(/EOF|Corrupted/i);
    await expect(
      streamArchiveCollections(
        Readable.from([corrupted]),
        () => true,
        async () => {},
      ),
    ).rejects.toThrow(/EOF|Corrupted/i);
  });

  it("Test F4: all 3 parsers fail closed on 1-3 dangling bytes at CHUNK_HEADER", async () => {
    const base = buildMongodumpV100Archive(
      [{ db: "app", collection: "users", docs: [{ _id: 1 }] }],
      { includeOplog: false },
    );
    const dangling = Buffer.concat([base, Buffer.from([0x10, 0x00])]);

    const { bounds } = await runArchiveOplogTap(dangling, 7);
    expect(bounds.corrupted).toBe(true);
    expect(bounds.validArchive).toBe(false);
    expect(bounds.archiveTerminated).toBe(false);

    await expect(runArchiveNamespaceFilter(dangling, [], 7)).rejects.toThrow(/Truncated/i);
    await expect(
      streamArchiveCollections(
        Readable.from([dangling]),
        () => true,
        async () => {},
      ),
    ).rejects.toThrow(/Truncated/i);
  });

  it("Test F5: all 3 parsers fail closed on trailing bytes after optional archive-level -1", async () => {
    const base = buildMongodumpV100Archive(
      [{ db: "app", collection: "users", docs: [{ _id: 1 }] }],
      { includeOplog: false, trailingArchiveTerminator: true },
    );
    const trailing = Buffer.concat([base, Buffer.from([0x01, 0x02, 0x03, 0x04])]);

    const { bounds } = await runArchiveOplogTap(trailing, 7);
    expect(bounds.corrupted).toBe(true);
    expect(bounds.validArchive).toBe(false);
    expect(bounds.archiveTerminated).toBe(false);

    await expect(runArchiveNamespaceFilter(trailing, [], 7)).rejects.toThrow(/trailing bytes/i);
    await expect(
      streamArchiveCollections(
        Readable.from([trailing]),
        () => true,
        async () => {},
      ),
    ).rejects.toThrow(/trailing bytes/i);
  });

  it("Test G: ArchiveNamespaceFilter strips conflicting idIndex/_id_ from timeseries prelude metadata while preserving regular collections", async () => {
    const tsMeta = {
      options: {
        timeseries: { timeField: "ts", metaField: "meta" },
        idIndex: { v: 2, key: { _id: 1 }, name: "_id_" },
      },
      indexes: [
        { v: 2, key: { _id: 1 }, name: "_id_" },
        { v: 2, key: { meta: 1, ts: 1 }, name: "meta_1_ts_1" },
      ],
      uuid: "11111111222233334444555566667777",
    };
    const regularMeta = {
      options: {},
      indexes: [{ v: 2, key: { _id: 1 }, name: "_id_" }],
      uuid: "aaaaaaaabbbbccccddddeeeeeeeeeeee",
    };

    const parts = [
      uint32LE(ARCHIVE_MAGIC_NUMBER),
      Buffer.from(BSON.serialize({ server_version: "9.0.0", tool_version: "100.13.0" })),
      Buffer.from(
        BSON.serialize({
          db: "tsdb",
          collection: "metrics",
          type: "timeseries",
          metadata: JSON.stringify(tsMeta),
        }),
      ),
      Buffer.from(
        BSON.serialize({
          db: "tsdb",
          collection: "marker",
          type: "collection",
          metadata: JSON.stringify(regularMeta),
        }),
      ),
      int32LE(-1),
      Buffer.from(BSON.serialize({ db: "tsdb", collection: "metrics" })),
      Buffer.from(BSON.serialize({ _id: 1, ts: new Date("2026-01-01T00:00:00Z") })),
      int32LE(-1),
      Buffer.from(BSON.serialize({ db: "tsdb", collection: "marker" })),
      Buffer.from(BSON.serialize({ _id: "m1" })),
      int32LE(-1),
      int32LE(-1),
    ];
    const rawArchive = Buffer.concat(parts);
    const filtered = await runArchiveNamespaceFilter(rawArchive, [], 13);

    // Parse prelude docs from filtered output
    let offset = 4;
    const headerLen = filtered.readInt32LE(offset);
    offset += headerLen;
    const metas = [];
    while (filtered.readInt32LE(offset) !== -1) {
      const len = filtered.readInt32LE(offset);
      metas.push(
        BSON.deserialize(filtered.subarray(offset, offset + len), { promoteValues: true }),
      );
      offset += len;
    }

    expect(metas).toHaveLength(2);
    const parsedTs = JSON.parse(metas[0].metadata);
    expect(parsedTs.options.timeseries).toEqual({ timeField: "ts", metaField: "meta" });
    expect(parsedTs.options.idIndex).toBeUndefined();
    expect(parsedTs.indexes).toEqual([{ v: 2, key: { meta: 1, ts: 1 }, name: "meta_1_ts_1" }]);
    expect(parsedTs.uuid).toBe("11111111222233334444555566667777");

    const parsedRegular = JSON.parse(metas[1].metadata);
    expect(parsedRegular.indexes).toEqual([{ v: 2, key: { _id: 1 }, name: "_id_" }]);
  });
});
