import { Readable } from "node:stream";
import { BSON, Timestamp } from "mongodb";
import { describe, expect, it } from "vitest";
import {
  ARCHIVE_MAGIC_NUMBER,
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
