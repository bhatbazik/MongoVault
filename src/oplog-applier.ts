import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { Readable } from "node:stream";
import { BSON, Binary, type MongoClient } from "mongodb";
import { readBsonDocumentsFromStream } from "./archive-stream.js";
import { isClusterScope } from "./catalog.js";
import type { Logger } from "./types/logger.js";
import type { OplogApplierStats, OplogSkipBreakdown, OplogTimestamp } from "./types/oplog.js";

export type { OplogApplierStats, OplogSkipBreakdown };

const TXN_SPILL_THRESHOLD_BYTES = 8 * 1024 * 1024; // 8 MiB per-txn RAM cap before spilling to disk

export class OplogReplayError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OplogReplayError";
  }
}

/**
 * Config collections allowed during oplog replay (`configCollToKeep` + `settings`).
 * All other `config.*` collections (locks, lockpings, mongos, cache.*, transactions, rangeDeletions,
 * system.sessions, system.preimages, actionlog, changelog, migrations) are excluded.
 */
const ALLOWED_CONFIG_COLLECTIONS = new Set([
  "chunks",
  "collections",
  "databases",
  "tags",
  "settings",
]);

const DISALLOWED_CONFIG_SETTINGS = new Set(["balancer", "automerge"]);

const SYSTEM_EXCLUDED_EXACT = new Set([
  "admin.system.version",
  "config.system.sessions",
  "config.rangeDeletions",
  "config.transactions",
  "config.shards",
  "config.version",
]);

const DONT_PRESERVE_UUID_EXACT = new Set([
  "admin.system.users",
  "admin.system.roles",
  "admin.system.keys",
  "config.databases",
  "config.collections",
  "config.chunks",
]);

const IGNORED_SAFE_OPLOG_COMMANDS = new Set([
  "setIndexCommitQuorum",
  "migrateCloneStart",
  "migrateCloneComplete",
  "noop",
]);

const KNOWN_OPLOG_COMMANDS = new Set([
  "renameCollection",
  "dropDatabase",
  "applyOps",
  "dbCheck",
  "create",
  "convertToCapped",
  "emptycapped",
  "drop",
  "createIndexes",
  "deleteIndex",
  "deleteIndexes",
  "dropIndex",
  "dropIndexes",
  "collMod",
  "startIndexBuild",
  "abortIndexBuild",
  "commitIndexBuild",
  "refineCollectionShardKey",
  "reshardCollection",
  "createSearchIndex",
  "updateSearchIndex",
  "dropSearchIndex",
]);

const TARGET_COLL_COMMANDS = new Set([
  "create",
  "drop",
  "createIndexes",
  "deleteIndex",
  "deleteIndexes",
  "dropIndex",
  "dropIndexes",
  "collMod",
  "commitIndexBuild",
]);

export interface OplogDoc {
  ts?: unknown;
  t?: unknown;
  h?: unknown;
  v?: unknown;
  op?: string;
  ns?: string;
  ui?: unknown;
  lsid?: Record<string, unknown>;
  txnNumber?: unknown;
  stmtId?: unknown;
  prevOpTime?: unknown;
  o?: Record<string, unknown>;
  o2?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface CloneNamespaceRule {
  fromDb: string;
  toDb: string;
  fromColl?: string;
  toColl?: string;
}

export interface OplogFilterOptions {
  database?: string;
  nsInclude?: string;
  nsExclude?: string;
  clone?: CloneNamespaceRule;
  /** Initial UUID (hex) of `config.system.sessions` from backup CSRS to exclude from config.collections & config.chunks. */
  sessionsUuidHex?: string;
}

export interface CommittedDistTxn {
  id: string;
  commitTs: OplogTimestamp | null;
}

export function createEmptyOplogStats(): OplogApplierStats {
  return {
    appliedEntries: 0,
    skippedEntries: 0,
    skipBreakdown: {
      beforeWindow: 0,
      noop: 0,
      filtered: 0,
      indexLifecycle: 0,
      benignReplayRace: 0,
      unrecognized: 0,
    },
    indexesBuilt: 0,
    lastAppliedTs: null,
    uncommittedTxns: 0,
    partialTxns: 0,
    unknownCommands: [],
  };
}

export function mergeOplogStats(statsList: OplogApplierStats[]): OplogApplierStats {
  const merged = createEmptyOplogStats();
  for (const s of statsList) {
    merged.appliedEntries += s.appliedEntries;
    merged.skippedEntries += s.skippedEntries;
    merged.skipBreakdown.beforeWindow += s.skipBreakdown.beforeWindow;
    merged.skipBreakdown.noop += s.skipBreakdown.noop;
    merged.skipBreakdown.filtered += s.skipBreakdown.filtered;
    merged.skipBreakdown.indexLifecycle += s.skipBreakdown.indexLifecycle;
    merged.skipBreakdown.benignReplayRace += s.skipBreakdown.benignReplayRace;
    merged.skipBreakdown.unrecognized += s.skipBreakdown.unrecognized;
    merged.indexesBuilt += s.indexesBuilt;
    merged.uncommittedTxns += s.uncommittedTxns;
    merged.partialTxns += s.partialTxns;
    merged.unknownCommands.push(...s.unknownCommands);
    if (
      s.lastAppliedTs &&
      (!merged.lastAppliedTs || cmpTs(s.lastAppliedTs, merged.lastAppliedTs) > 0)
    ) {
      merged.lastAppliedTs = s.lastAppliedTs;
    }
  }
  return merged;
}

/**
 * Abstraction over MongoDB operations required by `OplogApplier` (Dependency Inversion Principle).
 * Enables thorough unit testing without mocking MongoClient internals.
 */
export interface OplogTargetClient {
  applyOps(entries: OplogDoc[]): Promise<void>;
  createIndexes(
    dbName: string,
    collection: string,
    indexes: Record<string, unknown>[],
    options?: { commitQuorum?: string | number },
  ): Promise<void>;
  ensureSystemViews(dbName: string): Promise<void>;
  documentExists(ns: string, id: unknown): Promise<boolean>;
  collectionExists(ns: string): Promise<boolean>;
}

/**
 * Production `OplogTargetClient` backed by a live `MongoClient`.
 */
export class MongoOplogTargetClient implements OplogTargetClient {
  constructor(private readonly client: MongoClient) {}

  async applyOps(entries: OplogDoc[]): Promise<void> {
    const res = await this.client.db("admin").command({
      applyOps: entries,
      bypassDocumentValidation: true,
    });
    if (!res || res.ok !== 1) {
      throw new Error(`applyOps failed: ${res?.errmsg ?? JSON.stringify(res)}`);
    }
  }

  async createIndexes(
    dbName: string,
    collection: string,
    indexes: Record<string, unknown>[],
    options?: { commitQuorum?: string | number },
  ): Promise<void> {
    const cmd: Record<string, unknown> = {
      createIndexes: collection,
      indexes,
      ignoreUnknownIndexOptions: true,
    };
    if (options?.commitQuorum !== undefined) {
      cmd.commitQuorum = options.commitQuorum;
    }
    await this.client.db(dbName).command(cmd);
  }

  async ensureSystemViews(dbName: string): Promise<void> {
    try {
      await this.client.db(dbName).createCollection("system.views");
    } catch (err: unknown) {
      const code = (err as { code?: number })?.code;
      // MongoDB 5.0/6.0 returns NamespaceExists (48); 7.0/8.0 succeeds idempotently
      if (code !== 48) throw err;
    }
  }

  async documentExists(ns: string, id: unknown): Promise<boolean> {
    const dot = ns.indexOf(".");
    if (dot <= 0) return false;
    const dbName = ns.slice(0, dot);
    const collName = ns.slice(dot + 1);
    const found = await this.client
      .db(dbName)
      .collection(collName)
      .findOne({ _id: id as never }, { projection: { _id: 1 } });
    return found !== null;
  }

  async collectionExists(ns: string): Promise<boolean> {
    const dot = ns.indexOf(".");
    if (dot <= 0) return false;
    const dbName = ns.slice(0, dot);
    const collName = ns.slice(dot + 1);
    const items = await this.client
      .db(dbName)
      .listCollections({ name: collName }, { nameOnly: true })
      .toArray();
    return items.length > 0;
  }
}

export function extractBsonTimestamp(value: unknown): OplogTimestamp | null {
  if (!value || typeof value !== "object") return null;
  const ts = value as {
    getHighBits?: () => number;
    getLowBits?: () => number;
    t?: number;
    i?: number;
  };
  if (typeof ts.getHighBits === "function" && typeof ts.getLowBits === "function") {
    return { t: ts.getHighBits() >>> 0, i: ts.getLowBits() >>> 0 };
  }
  if (typeof ts.t === "number" && typeof ts.i === "number") {
    return { t: ts.t >>> 0, i: ts.i >>> 0 };
  }
  return null;
}

function cmpTs(a: OplogTimestamp, b: OplogTimestamp): number {
  if (a.t !== b.t) return a.t < b.t ? -1 : 1;
  if (a.i !== b.i) return a.i < b.i ? -1 : 1;
  return 0;
}

function extractBinaryHex(val: unknown): string | null {
  if (!val) return null;
  if (val instanceof Binary) {
    return Buffer.from(val.buffer).toString("hex");
  }
  if (Buffer.isBuffer(val) || val instanceof Uint8Array) {
    return Buffer.from(val).toString("hex");
  }
  if (typeof val === "object" && "buffer" in val) {
    const buf = (val as { buffer?: Uint8Array }).buffer;
    if (buf) return Buffer.from(buf).toString("hex");
  }
  return null;
}

export function splitNamespacePatterns(pattern?: string): string[] {
  if (!pattern) return [];
  return pattern
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean);
}

export function matchesNamespacePattern(ns: string, patternList: string): boolean {
  const parts = splitNamespacePatterns(patternList);
  if (parts.length === 0) return false;
  return parts.some((p) => {
    const escaped = p.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*");
    return new RegExp(`^${escaped}$`).test(ns);
  });
}

function getFirstCommandKey(o?: Record<string, unknown>): string | null {
  if (!o || typeof o !== "object") return null;
  const keys = Object.keys(o);
  return keys.length > 0 ? keys[0]! : null;
}

/**
 * Streaming oplog namespace, config, sessions-UUID, and DDL sanitization filter.
 */
export class OplogFilter {
  private readonly database?: string;
  private readonly nsInclude?: string;
  private readonly nsExclude?: string;
  private readonly clone?: CloneNamespaceRule;
  private sessionsUuidHex: string;

  constructor(options: OplogFilterOptions = {}) {
    this.database = options.database;
    this.nsInclude = options.nsInclude;
    this.nsExclude = options.nsExclude;
    this.clone = options.clone;
    this.sessionsUuidHex = options.sessionsUuidHex ?? "";
  }

  setSessionsUuidHex(hex: string): void {
    this.sessionsUuidHex = hex;
  }

  getSessionsUuidHex(): string {
    return this.sessionsUuidHex;
  }

  isSystemExcludedNs(ns: string): boolean {
    if (!ns) return true;
    return (
      SYSTEM_EXCLUDED_EXACT.has(ns) || ns.startsWith("local.") || ns.startsWith("admin.mongovault")
    );
  }

  isConfigOpAllowed(doc: OplogDoc): boolean {
    const ns = String(doc.ns || "");
    if (!ns.startsWith("config.")) return true;

    const coll = ns.slice("config.".length);
    if (coll === "settings") {
      const id = doc.o2?._id ?? doc.o?._id;
      if (typeof id === "string" && DISALLOWED_CONFIG_SETTINGS.has(id)) {
        return false;
      }
      return true;
    }

    if (ALLOWED_CONFIG_COLLECTIONS.has(coll)) {
      return true;
    }

    if (coll === "$cmd" && doc.o) {
      const cmdKey = getFirstCommandKey(doc.o);
      if (cmdKey === "applyOps") return true;
      if (cmdKey && TARGET_COLL_COMMANDS.has(cmdKey)) {
        const target = doc.o[cmdKey];
        return typeof target === "string" && ALLOWED_CONFIG_COLLECTIONS.has(target);
      }
    }

    return false;
  }

  /**
   * Filters `config.collections` and `config.chunks` routing records belonging to
   * `config.system.sessions`.
   */
  isRoutingDocExcluded(doc: OplogDoc): boolean {
    const ns = String(doc.ns || "");
    if (ns === "config.collections") {
      const id = doc.o?._id ?? doc.o2?._id;
      if (id === "config.system.sessions") {
        const uuidHex = extractBinaryHex(doc.o?.uuid);
        if (uuidHex && !this.sessionsUuidHex && doc.op === "i") {
          this.sessionsUuidHex = uuidHex;
        }
        return true;
      }
      return false;
    }

    if (ns === "config.chunks" && this.sessionsUuidHex) {
      const uuidHex = extractBinaryHex(doc.o?.uuid ?? doc.o2?.uuid);
      if (uuidHex && uuidHex === this.sessionsUuidHex) {
        return true;
      }
    }

    return false;
  }

  isNsSelected(ns: string): boolean {
    if (this.isSystemExcludedNs(ns)) return false;

    if (this.clone) {
      if (this.clone.fromColl) {
        if (ns !== `${this.clone.fromDb}.${this.clone.fromColl}`) {
          return false;
        }
      } else if (ns !== `${this.clone.fromDb}.$cmd` && !ns.startsWith(`${this.clone.fromDb}.`)) {
        return false;
      }
    }

    if (this.database && !isClusterScope(this.database)) {
      if (ns !== `${this.database}.$cmd` && !ns.startsWith(`${this.database}.`)) {
        return false;
      }
    }
    if (this.nsInclude && !matchesNamespacePattern(ns, this.nsInclude)) {
      return false;
    }
    if (this.nsExclude && matchesNamespacePattern(ns, this.nsExclude)) {
      return false;
    }
    return true;
  }

  isIncluded(doc: OplogDoc): boolean {
    if (doc.op === "n") return false;
    if (
      doc.op === "c" &&
      doc.o &&
      (doc.o.startIndexBuild !== undefined || doc.o.abortIndexBuild !== undefined)
    ) {
      return false;
    }

    if (!this.isConfigOpAllowed(doc)) return false;
    if (this.isRoutingDocExcluded(doc)) return false;

    const ns = String(doc.ns || "");
    const o = doc.o;

    // applyOps envelope: inner ops will be filtered individually
    if (doc.op === "c" && o && Array.isArray(o.applyOps)) {
      return true;
    }

    if (ns === "admin.$cmd" && o && typeof o.renameCollection === "string") {
      const fromNs = o.renameCollection;
      const toNs = typeof o.to === "string" ? o.to : "";
      return this.isNsSelected(fromNs) && (!toNs || this.isNsSelected(toNs));
    }

    if (ns.endsWith(".$cmd") && o) {
      const dbName = ns.slice(0, -".$cmd".length);
      const cmdKey = getFirstCommandKey(o);

      if (cmdKey === "dropDatabase") {
        // Only allow dropDatabase if the entire database is selected (not individual collection filter)
        if (this.nsExclude) return false;
        if (this.clone) {
          return !this.clone.fromColl && dbName === this.clone.fromDb;
        }
        if (this.nsInclude) {
          return matchesNamespacePattern(`${dbName}.*`, this.nsInclude);
        }
        return !this.database || isClusterScope(this.database) || this.database === dbName;
      }

      if (cmdKey && TARGET_COLL_COMMANDS.has(cmdKey)) {
        const targetColl = o[cmdKey];
        if (typeof targetColl === "string" && targetColl.length > 0) {
          return this.isNsSelected(`${dbName}.${targetColl}`);
        }
      }
    }

    return this.isNsSelected(ns);
  }

  shouldStripUuid(doc: OplogDoc): boolean {
    const ns = String(doc.ns || "");
    if (
      DONT_PRESERVE_UUID_EXACT.has(ns) ||
      ns.includes(".system.buckets.") ||
      ns.endsWith(".system.views")
    ) {
      return true;
    }

    // Also inspect DDL create target (`db.$cmd` with `{ create: "system.views" | "system.buckets.*" }`)
    if (doc.op === "c" && ns.endsWith(".$cmd") && doc.o && typeof doc.o.create === "string") {
      const dbName = ns.slice(0, -".$cmd".length);
      const targetNs = `${dbName}.${doc.o.create}`;
      if (
        DONT_PRESERVE_UUID_EXACT.has(targetNs) ||
        targetNs.includes(".system.buckets.") ||
        targetNs.endsWith(".system.views")
      ) {
        return true;
      }
    }

    return false;
  }

  /**
   * Sanitizes UUIDs, MongoDB 8.3 `create` `o2` metadata, deprecated `collMod` options,
   * and applies DB/collection cloning rewrites in place. Returns true if modified.
   */
  sanitizeAndClone(doc: OplogDoc): boolean {
    let modified = false;

    if (doc.ui !== undefined && this.shouldStripUuid(doc)) {
      delete doc.ui;
      modified = true;
    }

    // MongoDB 8.3 create oplog entries may include physical storage idents in o2
    if (doc.op === "c" && doc.o && "create" in doc.o && doc.o2 !== undefined) {
      delete doc.o2;
      modified = true;
    }

    // Strip deprecated collMod options rejected by MongoDB >= 4.2
    if (doc.op === "c" && doc.o && "collMod" in doc.o) {
      if ("noPadding" in doc.o) {
        delete doc.o.noPadding;
        modified = true;
      }
      if ("usePowerOf2Sizes" in doc.o) {
        delete doc.o.usePowerOf2Sizes;
        modified = true;
      }
    }

    // Database/collection cloning rewrite
    if (this.clone) {
      const { fromDb, toDb, fromColl, toColl } = this.clone;
      const ns = String(doc.ns || "");
      const dot = ns.indexOf(".");
      if (dot > 0) {
        const dbName = ns.slice(0, dot);
        const collName = ns.slice(dot + 1);
        if (dbName === fromDb) {
          if (collName === "$cmd" && doc.o) {
            const cmdKey = getFirstCommandKey(doc.o);
            if (cmdKey && TARGET_COLL_COMMANDS.has(cmdKey)) {
              const target = doc.o[cmdKey];
              if (!fromColl || target === fromColl) {
                doc.ns = `${toDb}.$cmd`;
                if (fromColl && toColl && target === fromColl) {
                  doc.o[cmdKey] = toColl;
                }
                if (doc.ui !== undefined) delete doc.ui;
                if (cmdKey === "create" && doc.o2 !== undefined) delete doc.o2;
                modified = true;
              }
            }
          } else if (!fromColl || collName === fromColl) {
            doc.ns = `${toDb}.${fromColl && toColl ? toColl : collName}`;
            if (doc.ui !== undefined) delete doc.ui;
            modified = true;
          }
        }
      }
    }

    return modified;
  }
}

/**
 * Tracks index creations, drops, renames, and collMod changes observed during oplog replay
 * so they can be cleanly executed via `createIndexes` commands (MongoDB 4.4–8.0 safe).
 */
export class IndexCatalog {
  private readonly catalog = new Map<string, Map<string, Record<string, unknown>>>();

  private nsKey(db: string, coll: string): string {
    return `${db}\0${coll}`;
  }

  addIndex(db: string, coll: string, spec: Record<string, unknown>): void {
    const name = typeof spec.name === "string" ? spec.name : null;
    if (!name || !spec.key) return;
    // Skip _id index (created automatically with collection)
    if (name === "_id_") return;

    const key = this.nsKey(db, coll);
    let colMap = this.catalog.get(key);
    if (!colMap) {
      colMap = new Map();
      this.catalog.set(key, colMap);
    }
    const cleanSpec = { ...spec };
    delete cleanSpec.v;
    delete cleanSpec.ns;
    colMap.set(name, cleanSpec);
  }

  dropCollection(db: string, coll: string): void {
    this.catalog.delete(this.nsKey(db, coll));
  }

  dropDatabase(db: string): void {
    const prefix = `${db}\0`;
    for (const k of this.catalog.keys()) {
      if (k.startsWith(prefix)) this.catalog.delete(k);
    }
  }

  renameCollection(fromDb: string, fromColl: string, toDb: string, toColl: string): void {
    const fromKey = this.nsKey(fromDb, fromColl);
    const toKey = this.nsKey(toDb, toColl);
    if (fromKey === toKey) return;
    const existing = this.catalog.get(fromKey);
    this.catalog.delete(fromKey);
    if (existing && existing.size > 0) {
      this.catalog.set(toKey, existing);
    } else {
      this.catalog.delete(toKey);
    }
  }

  deleteIndexes(db: string, coll: string, indexSpec: unknown): boolean {
    const key = this.nsKey(db, coll);
    const colMap = this.catalog.get(key);
    if (!colMap) return false;

    if (indexSpec === "*") {
      colMap.clear();
      return true;
    }
    if (typeof indexSpec === "string") {
      return colMap.delete(indexSpec);
    }
    if (indexSpec && typeof indexSpec === "object") {
      const targetJson = JSON.stringify(indexSpec);
      let removed = false;
      for (const [name, spec] of colMap.entries()) {
        if (JSON.stringify(spec.key) === targetJson) {
          colMap.delete(name);
          removed = true;
        }
      }
      return removed;
    }
    return false;
  }

  collMod(db: string, coll: string, indexMod: Record<string, unknown>): boolean {
    const colMap = this.catalog.get(this.nsKey(db, coll));
    if (!colMap) return false;

    let target: Record<string, unknown> | undefined;
    if (typeof indexMod.name === "string") {
      target = colMap.get(indexMod.name);
    } else if (indexMod.keyPattern && typeof indexMod.keyPattern === "object") {
      const targetJson = JSON.stringify(indexMod.keyPattern);
      for (const spec of colMap.values()) {
        if (JSON.stringify(spec.key) === targetJson) {
          target = spec;
          break;
        }
      }
    }
    if (!target) return false;

    if (indexMod.expireAfterSeconds !== undefined) {
      target.expireAfterSeconds = indexMod.expireAfterSeconds;
    }
    if (indexMod.hidden !== undefined) {
      target.hidden = indexMod.hidden;
    }
    return true;
  }

  entries(): Array<{ db: string; collection: string; indexes: Record<string, unknown>[] }> {
    const out: Array<{ db: string; collection: string; indexes: Record<string, unknown>[] }> = [];
    for (const [key, colMap] of this.catalog.entries()) {
      if (colMap.size === 0) continue;
      const [db, collection] = key.split("\0");
      if (db && collection) {
        out.push({ db, collection, indexes: [...colMap.values()] });
      }
    }
    return out;
  }
}

interface BufferedTxn {
  id: string;
  allOps: boolean;
  totalBytes: number;
  inMemoryOps: Buffer[];
  spillFile: string | null;
}

/**
 * Buffers multi-document and distributed transactions across oplog slices with automatic
 * disk spilling for single transactions exceeding `spillThresholdBytes` (protecting small-RAM hosts).
 */
export class TxnBuffer {
  private readonly txns = new Map<string, BufferedTxn>();
  private readonly committedDistTxns = new Map<string, OplogTimestamp | null>();
  private lastCommittedId: string | null = null;
  private readonly workDir: string;
  private readonly spillThresholdBytes: number;

  constructor(workDir: string, spillThresholdBytes = TXN_SPILL_THRESHOLD_BYTES) {
    this.workDir = workDir;
    this.spillThresholdBytes = spillThresholdBytes;
  }

  async addApplyOps(txnId: string, innerOps: OplogDoc[], isPartial: boolean): Promise<void> {
    let entry = this.txns.get(txnId);
    if (!entry) {
      entry = {
        id: txnId,
        allOps: !isPartial,
        totalBytes: 0,
        inMemoryOps: [],
        spillFile: null,
      };
      this.txns.set(txnId, entry);
    } else {
      entry.allOps = !isPartial;
    }

    const serialized = innerOps.map((op) => Buffer.from(BSON.serialize(op)));
    const addedBytes = serialized.reduce((sum, b) => sum + b.length, 0);
    entry.totalBytes += addedBytes;

    if (!entry.spillFile && entry.totalBytes > this.spillThresholdBytes) {
      await fs.mkdir(this.workDir, { recursive: true });
      entry.spillFile = path.join(this.workDir, `.mongovault-txn-${randomUUID()}.bson`);
      const existing = Buffer.concat(entry.inMemoryOps);
      entry.inMemoryOps = [];
      await fs.writeFile(entry.spillFile, existing, { mode: 0o600 });
    }

    if (entry.spillFile) {
      await fs.appendFile(entry.spillFile, Buffer.concat(serialized));
    } else {
      entry.inMemoryOps.push(...serialized);
    }
  }

  has(txnId: string): boolean {
    return this.txns.has(txnId);
  }

  isAllOps(txnId: string): boolean {
    return this.txns.get(txnId)?.allOps ?? false;
  }

  async consumeOps(txnId: string, onOp: (op: OplogDoc) => Promise<void>): Promise<void> {
    const entry = this.txns.get(txnId);
    if (!entry) return;

    try {
      if (entry.spillFile) {
        const { createReadStream } = await import("node:fs");
        const stream = createReadStream(entry.spillFile);
        await readBsonDocumentsFromStream(stream, async (rawBuf) => {
          const op = BSON.deserialize(rawBuf, { promoteValues: false }) as OplogDoc;
          await onOp(op);
        });
      } else {
        for (const rawBuf of entry.inMemoryOps) {
          const op = BSON.deserialize(rawBuf, { promoteValues: false }) as OplogDoc;
          await onOp(op);
        }
      }
    } finally {
      await this.discard(txnId);
    }
  }

  async discard(txnId: string): Promise<void> {
    const entry = this.txns.get(txnId);
    if (!entry) return;
    this.txns.delete(txnId);
    if (entry.spillFile) {
      await fs.rm(entry.spillFile, { force: true }).catch(() => {});
    }
  }

  recordDistCommit(txnId: string, commitTs: OplogTimestamp | null): void {
    this.committedDistTxns.set(txnId, commitTs);
    this.lastCommittedId = txnId;
  }

  lastCommitId(): string | null {
    return this.lastCommittedId;
  }

  getLeftovers(): { uncommittedIds: string[]; committedDistTxns: CommittedDistTxn[] } {
    return {
      uncommittedIds: [...this.txns.keys()],
      committedDistTxns: [...this.committedDistTxns.entries()].map(([id, commitTs]) => ({
        id,
        commitTs,
      })),
    };
  }

  async dispose(): Promise<void> {
    const ids = [...this.txns.keys()];
    for (const id of ids) {
      await this.discard(id);
    }
  }
}

export interface OplogApplierOptions {
  target: OplogTargetClient;
  workDir: string;
  logger?: Logger;
  /** Skip entries strictly before `startTs`. */
  startTs?: OplogTimestamp;
  /** Skip entries `<= startExclusiveTs` (use when base restore already replayed up to `baseEndTs`). */
  startExclusiveTs?: OplogTimestamp;
  /** Stop cleanly on entries `> endTs` (inclusive upper bound). */
  endTs?: OplogTimestamp;
  filter?: OplogFilterOptions;
  backupType?: "logical" | "physical";
  /** If true, ignores DuplicateKey on `config.chunks` during unsafe/physical replay. */
  unsafe?: boolean;
  /**
   * Fail closed on unrecognized oplog commands or malformed entries (default: true).
   * Disable only for explicit diagnostic overrides.
   */
  strict?: boolean;
}

/**
 * Stateful, streaming oplog applier.
 * Handles multi-slice transactions, cross-shard distributed transaction reconciliation,
 * modern index DDL (`commitIndexBuild`/`createIndexes`/`collMod`), UUID rules,
 * `config.system.sessions` isolation, `system.views` creation, and
 * time-series bucket TTL delta edge cases.
 */
export class OplogApplier {
  private readonly target: OplogTargetClient;
  private readonly filter: OplogFilter;
  private readonly indexCatalog = new IndexCatalog();
  private readonly txnBuffer: TxnBuffer;
  private readonly logger?: Logger;
  private readonly startTs?: OplogTimestamp;
  private readonly startExclusiveTs?: OplogTimestamp;
  private readonly endTs?: OplogTimestamp;
  private readonly backupType: "logical" | "physical";
  private readonly unsafe: boolean;
  private readonly strict: boolean;

  private appliedEntries = 0;
  private skippedEntries = 0;
  private readonly skipBreakdown: OplogSkipBreakdown = {
    beforeWindow: 0,
    noop: 0,
    filtered: 0,
    indexLifecycle: 0,
    benignReplayRace: 0,
    unrecognized: 0,
  };
  private indexesBuilt = 0;
  private partialTxns = 0;
  private readonly unknownCommands: string[] = [];
  private lastAppliedTs: OplogTimestamp | null = null;
  private reachedEnd = false;

  constructor(options: OplogApplierOptions) {
    this.target = options.target;
    this.filter = new OplogFilter(options.filter);
    this.txnBuffer = new TxnBuffer(options.workDir);
    this.logger = options.logger;
    this.startTs = options.startTs;
    this.startExclusiveTs = options.startExclusiveTs;
    this.endTs = options.endTs;
    this.backupType = options.backupType ?? "logical";
    this.unsafe = Boolean(options.unsafe);
    this.strict = options.strict !== false;
  }

  private recordSkip(reason: keyof OplogSkipBreakdown): void {
    this.skippedEntries++;
    this.skipBreakdown[reason]++;
  }

  getStats(): OplogApplierStats {
    return {
      appliedEntries: this.appliedEntries,
      skippedEntries: this.skippedEntries,
      skipBreakdown: { ...this.skipBreakdown },
      indexesBuilt: this.indexesBuilt,
      lastAppliedTs: this.lastAppliedTs,
      uncommittedTxns: this.txnBuffer.getLeftovers().uncommittedIds.length,
      partialTxns: this.partialTxns,
      unknownCommands: [...this.unknownCommands],
    };
  }

  assertComplete(): void {
    if (this.partialTxns > 0) {
      throw new OplogReplayError(
        `Oplog replay integrity check failed: ${this.partialTxns} committed transaction(s) had incomplete operations`,
      );
    }
    if (this.strict && this.unknownCommands.length > 0) {
      throw new OplogReplayError(
        `Oplog replay integrity check failed: encountered unrecognized oplog command(s): ${[...new Set(this.unknownCommands)].join(", ")}`,
      );
    }
  }

  getTxnLeftovers(): { uncommittedIds: string[]; committedDistTxns: CommittedDistTxn[] } {
    return this.txnBuffer.getLeftovers();
  }

  /**
   * Streams raw BSON oplog entries from `readable` (typically gunzipped slice stream)
   * and applies them sequentially.
   */
  async applyStream(readable: Readable, signal?: AbortSignal): Promise<void> {
    if (this.reachedEnd) {
      readable.resume();
      return;
    }
    await readBsonDocumentsFromStream(
      readable,
      async (rawDoc) => {
        if (this.reachedEnd) return;
        await this.applyRawDoc(rawDoc);
      },
      signal,
    );
  }

  async applyRawDoc(rawDoc: Buffer): Promise<void> {
    const doc = BSON.deserialize(rawDoc, { promoteValues: false }) as OplogDoc;
    await this.handleOp(doc);
  }

  async handleOp(doc: OplogDoc): Promise<void> {
    const ts = extractBsonTimestamp(doc.ts);
    if (ts) {
      if (this.startTs && cmpTs(ts, this.startTs) < 0) {
        this.recordSkip("beforeWindow");
        return;
      }
      if (this.startExclusiveTs && cmpTs(ts, this.startExclusiveTs) <= 0) {
        this.recordSkip("beforeWindow");
        return;
      }
      if (this.lastAppliedTs && cmpTs(ts, this.lastAppliedTs) <= 0) {
        this.recordSkip("beforeWindow");
        return;
      }
      if (this.endTs && cmpTs(ts, this.endTs) > 0) {
        this.reachedEnd = true;
        return;
      }
    }

    if (doc.op === "n") {
      this.recordSkip("noop");
      return;
    }

    if (this.isTxnEntry(doc)) {
      await this.handleTxnOp(doc);
      if (ts) this.lastAppliedTs = ts;
      return;
    }

    if (!this.filter.isIncluded(doc)) {
      const isIdxLifecycle =
        doc.op === "c" &&
        doc.o &&
        (doc.o.startIndexBuild !== undefined || doc.o.abortIndexBuild !== undefined);
      this.recordSkip(isIdxLifecycle ? "indexLifecycle" : "filtered");
      return;
    }

    await this.handleNonTxnOp(doc);
    if (ts) this.lastAppliedTs = ts;
  }

  /**
   * Reconciles prepared/uncommitted distributed transactions against the union of
   * committed transaction IDs reported by all shards.
   * Fails closed if any transaction known to have committed on another shard has only
   * partial prepared operations locally.
   */
  async handleUncommittedTxn(
    clusterCommits: Map<string, OplogTimestamp | null>,
  ): Promise<{ applied: number; partial: number; leftUncommitted: number }> {
    const { uncommittedIds } = this.txnBuffer.getLeftovers();
    let applied = 0;
    let partial = 0;
    const partialIds: string[] = [];

    for (const id of uncommittedIds) {
      if (!clusterCommits.has(id)) continue;
      if (!this.txnBuffer.isAllOps(id)) {
        partial++;
        this.partialTxns++;
        partialIds.push(id);
        this.logger?.error(
          { txnId: id },
          "CRITICAL: Distributed transaction committed on another shard but only partial prepared ops were present before cutoff",
        );
        await this.txnBuffer.discard(id);
        continue;
      }
      await this.txnBuffer.consumeOps(id, async (innerOp) => {
        if (!this.filter.isIncluded(innerOp)) {
          this.recordSkip("filtered");
          return;
        }
        await this.handleNonTxnOp(innerOp);
      });
      applied++;
    }

    if (partialIds.length > 0) {
      throw new OplogReplayError(
        `Cannot complete consistent recovery: ${partialIds.length} committed distributed transaction(s) had only partial prepared operations before cutoff (${partialIds.join(", ")})`,
      );
    }

    const remaining = this.txnBuffer.getLeftovers().uncommittedIds.length;
    if (remaining > 0) {
      this.logger?.info(
        { uncommittedCount: remaining },
        "Discarding uncommitted transactions with no commit observed on any shard",
      );
    }
    return { applied, partial, leftUncommitted: remaining };
  }

  /**
   * Builds all pending indexes recorded in `IndexCatalog` during oplog replay.
   * For standalone recovery `mongod` (physical PITR), omits `commitQuorum`.
   */
  async buildPendingIndexes(options: { standalone?: boolean } = {}): Promise<number> {
    const groups = this.indexCatalog.entries();
    let built = 0;

    for (const group of groups) {
      this.logger?.info(
        { db: group.db, collection: group.collection, count: group.indexes.length },
        "Building indexes from oplog replay",
      );
      await this.target.createIndexes(
        group.db,
        group.collection,
        group.indexes,
        options.standalone ? undefined : { commitQuorum: "votingMembers" },
      );
      built += group.indexes.length;
    }

    this.indexesBuilt += built;
    return built;
  }

  async dispose(): Promise<void> {
    await this.txnBuffer.dispose();
  }

  private isTxnEntry(doc: OplogDoc): boolean {
    // In MongoDB 4.0–8.0, single-document retryable writes (op: "i" | "u" | "d") also carry
    // top-level `lsid` and `txnNumber`. Only `op === "c"` entries with `applyOps`,
    // `commitTransaction`, or `abortTransaction` are multi-document / distributed transactions.
    if (doc.op !== "c" || !doc.lsid || doc.txnNumber === undefined || doc.txnNumber === null) {
      return false;
    }
    const o = doc.o;
    if (!o || typeof o !== "object") return false;
    return (
      Array.isArray(o.applyOps) ||
      o.commitTransaction !== undefined ||
      o.abortTransaction !== undefined
    );
  }

  private getTxnId(doc: OplogDoc): string {
    const lsidBytes = BSON.serialize(doc.lsid as Record<string, unknown>);
    const txnNum =
      typeof doc.txnNumber === "object" && doc.txnNumber !== null && "toString" in doc.txnNumber
        ? String(doc.txnNumber)
        : String(doc.txnNumber);
    return `${Buffer.from(lsidBytes).toString("base64url")}-${txnNum}`;
  }

  private async handleTxnOp(doc: OplogDoc): Promise<void> {
    const txnId = this.getTxnId(doc);
    const o = doc.o ?? {};
    const hasApplyOps = Array.isArray(o.applyOps);
    const isPartial = Boolean(o.partialTxn);
    const isAbort = Boolean(o.abortTransaction);
    const isPrepare = Boolean(o.prepare);
    const isExplicitCommit = Boolean(o.commitTransaction);
    // Non-prepared, non-partial single-entry applyOps commits immediately
    const isSingleEntryCommit = hasApplyOps && !isPrepare && !isPartial && !isAbort;
    const isCommit = isExplicitCommit || isSingleEntryCommit;

    if (hasApplyOps) {
      const innerOps = (o.applyOps as OplogDoc[]).map((inner) => ({
        ...inner,
        ts: inner.ts ?? doc.ts,
        t: inner.t ?? doc.t,
      }));
      await this.txnBuffer.addApplyOps(txnId, innerOps, isPartial);
    }

    if (isAbort) {
      await this.txnBuffer.discard(txnId);
      return;
    }

    if (!isCommit) {
      return;
    }

    // Deduplicate commit entry across overlapping slice boundaries
    if (this.txnBuffer.lastCommitId() === txnId && !this.txnBuffer.has(txnId)) {
      return;
    }

    // Explicit `commitTransaction` without inline data marks a distributed transaction
    if (isExplicitCommit && !hasApplyOps) {
      const commitTs = extractBsonTimestamp(o.commitTimestamp);
      this.txnBuffer.recordDistCommit(txnId, commitTs);
    }

    if (!this.txnBuffer.has(txnId)) {
      return;
    }

    if (!this.txnBuffer.isAllOps(txnId)) {
      this.partialTxns++;
      await this.txnBuffer.discard(txnId);
      throw new OplogReplayError(
        `Cannot commit transaction "${txnId}": explicit commitTransaction received while buffered operations are still incomplete (missing final applyOps chunk)`,
      );
    }

    await this.txnBuffer.consumeOps(txnId, async (innerOp) => {
      if (!this.filter.isIncluded(innerOp)) {
        this.recordSkip("filtered");
        return;
      }
      await this.handleNonTxnOp(innerOp);
    });
  }

  private async handleNonTxnOp(rawOp: OplogDoc): Promise<void> {
    const op: OplogDoc = {
      ...rawOp,
      ...(rawOp.o ? { o: { ...rawOp.o } } : {}),
      ...(rawOp.o2 ? { o2: { ...rawOp.o2 } } : {}),
    };

    this.filter.sanitizeAndClone(op);

    const ns = String(op.ns || "");
    const dot = ns.indexOf(".");
    const dbName = dot > 0 ? ns.slice(0, dot) : ns;
    const collName = dot > 0 ? ns.slice(dot + 1) : "";

    let renameFrom: { db: string; coll: string } | null = null;
    let renameTo: { db: string; coll: string } | null = null;

    if (op.op !== "i" && op.op !== "u" && op.op !== "d" && op.op !== "c") {
      if (this.strict) {
        throw new OplogReplayError(
          `Unsupported oplog operation type "${String(op.op)}" on namespace "${ns}"`,
        );
      }
      this.logger?.warn({ op: op.op, ns }, "Skipping unsupported oplog operation type");
      this.recordSkip("unrecognized");
      return;
    }

    if (op.op === "c") {
      const o = op.o;
      const cmdKey = getFirstCommandKey(o);
      if (!o || !cmdKey) {
        if (this.strict) {
          throw new OplogReplayError(`Malformed empty command oplog entry on namespace "${ns}"`);
        }
        this.recordSkip("unrecognized");
        return;
      }

      if (cmdKey === "startIndexBuild" || cmdKey === "abortIndexBuild") {
        this.recordSkip("indexLifecycle");
        return;
      }

      if (IGNORED_SAFE_OPLOG_COMMANDS.has(cmdKey)) {
        this.recordSkip("noop");
        return;
      }

      if (!KNOWN_OPLOG_COMMANDS.has(cmdKey)) {
        this.unknownCommands.push(cmdKey);
        if (this.strict) {
          throw new OplogReplayError(
            `Unrecognized oplog command "${cmdKey}" on namespace "${ns}"; refusing to silently skip potentially data-mutating operation`,
          );
        }
        this.logger?.warn({ cmdKey, ns }, "Skipping unrecognized oplog command (non-strict mode)");
        this.recordSkip("unrecognized");
        return;
      }

      switch (cmdKey) {
        case "applyOps": {
          const nested = Array.isArray(o.applyOps) ? (o.applyOps as OplogDoc[]) : [];
          for (const item of nested) {
            if (!this.filter.isIncluded(item)) {
              this.recordSkip("filtered");
              continue;
            }
            await this.handleNonTxnOp(item);
          }
          return;
        }

        case "commitIndexBuild": {
          const targetColl = String(o.commitIndexBuild || "");
          const indexes = Array.isArray(o.indexes) ? (o.indexes as Record<string, unknown>[]) : [];
          for (const idx of indexes) {
            this.indexCatalog.addIndex(dbName, targetColl, idx);
          }
          this.appliedEntries++;
          return;
        }

        case "createIndexes": {
          const targetColl = String(o.createIndexes || "");
          const spec: Record<string, unknown> = { ...o };
          delete spec.createIndexes;
          this.indexCatalog.addIndex(dbName, targetColl, spec);
          this.appliedEntries++;
          return;
        }

        case "dropDatabase": {
          this.indexCatalog.dropDatabase(dbName);
          break;
        }

        case "drop": {
          const targetColl = String(o.drop || "");
          this.indexCatalog.dropCollection(dbName, targetColl);
          break;
        }

        case "renameCollection": {
          const from = String(o.renameCollection || "");
          const to = String(o.to || "");
          const fDot = from.indexOf(".");
          const tDot = to.indexOf(".");
          if (fDot > 0 && tDot > 0) {
            renameFrom = { db: from.slice(0, fDot), coll: from.slice(fDot + 1) };
            renameTo = { db: to.slice(0, tDot), coll: to.slice(tDot + 1) };
          }
          break;
        }

        case "deleteIndex":
        case "deleteIndexes":
        case "dropIndex":
        case "dropIndexes": {
          const targetColl = String(o[cmdKey] || "");
          const removedFromCatalog = this.indexCatalog.deleteIndexes(dbName, targetColl, o.index);
          if (removedFromCatalog && this.backupType === "logical") {
            this.appliedEntries++;
            return;
          }
          break;
        }

        case "collMod": {
          if (o.index && typeof o.index === "object") {
            const targetColl = String(o.collMod || "");
            const updatedInCatalog = this.indexCatalog.collMod(
              dbName,
              targetColl,
              o.index as Record<string, unknown>,
            );
            if (updatedInCatalog) {
              delete o.index;
              if (Object.keys(o).length <= 1) {
                this.appliedEntries++;
                return;
              }
            }
          }
          break;
        }

        case "create": {
          const targetColl = String(o.create || "");
          this.indexCatalog.dropCollection(dbName, targetColl);
          // Ensure idempotent create when replaying onto existing collection
          await this.target
            .applyOps([{ op: "c", ns: `${dbName}.$cmd`, o: { drop: targetColl } }])
            .catch(() => {});
          break;
        }
      }
    } else if (op.op === "i" && collName === "system.views") {
      // Ensure system.views exists before inserting views / timeseries metadata
      await this.target.ensureSystemViews(dbName);
    }

    // Strip non-applyOps metadata fields (lsid/txnNumber/stmtId/prevOpTime/wall) before applyOps
    const cleanOp: OplogDoc = {
      op: op.op,
      ns: op.ns,
      o: op.o,
      ...(op.o2 !== undefined ? { o2: op.o2 } : {}),
      ...(op.ui !== undefined ? { ui: op.ui } : {}),
    };

    try {
      await this.target.applyOps([cleanOp]);
      this.appliedEntries++;
    } catch (err: unknown) {
      // MongoDB 8.x time-series bucket delta update when TTL deleted the bucket before update
      if (await this.isMissingTimeseriesBucketUpdate(cleanOp)) {
        this.logger?.debug(
          { ns: cleanOp.ns, query: cleanOp.o2 },
          "Skipping update to missing time-series bucket (TTL race)",
        );
        this.recordSkip("benignReplayRace");
        return;
      }

      // Allow duplicate key on config.chunks in unsafe/physical replay
      const errCode = (err as { code?: number })?.code;
      if (this.unsafe && cleanOp.ns === "config.chunks" && errCode === 11000) {
        this.recordSkip("benignReplayRace");
        return;
      }

      // Ignore drop/dropIndexes on already-absent namespaces during idempotent replay
      if (
        cleanOp.op === "c" &&
        cleanOp.o &&
        ("drop" in cleanOp.o || "dropIndexes" in cleanOp.o || "deleteIndexes" in cleanOp.o) &&
        (errCode === 26 || errCode === 27)
      ) {
        this.recordSkip("benignReplayRace");
        return;
      }

      throw err;
    }

    if (renameFrom && renameTo) {
      this.indexCatalog.renameCollection(
        renameFrom.db,
        renameFrom.coll,
        renameTo.db,
        renameTo.coll,
      );
    }
  }

  private async isMissingTimeseriesBucketUpdate(op: OplogDoc): Promise<boolean> {
    if (op.op !== "u") return false;
    const ns = String(op.ns || "");
    const dot = ns.indexOf(".");
    if (dot <= 0) return false;
    const collName = ns.slice(dot + 1);
    if (!collName.startsWith("system.buckets.")) return false;
    const o = op.o;
    if (!o || o.diff === undefined) return false;
    const vVal =
      typeof o.$v === "object" && o.$v !== null && "valueOf" in o.$v
        ? Number(o.$v.valueOf())
        : o.$v;
    if (vVal !== 2) return false;
    const id = op.o2?._id;
    if (id === undefined) return false;

    const docExists = await this.target.documentExists(ns, id);
    if (docExists) return false;
    return this.target.collectionExists(ns);
  }
}
