# MongoVault

[![CI](https://github.com/bhatbazik/MongoVault/actions/workflows/ci.yml/badge.svg)](https://github.com/bhatbazik/MongoVault/actions/workflows/ci.yml)
[![Node](https://img.shields.io/badge/node-%3E%3D20.0.0-blue.svg)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-3178C6.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Streaming MongoDB backup daemon and CLI with S3/Cloudflare R2 multipart uploads, framed AES-256-GCM envelope encryption, continuous Point-in-Time Recovery (PITR) oplog slicing, automated restore drills, and Prometheus metrics.

---

## Overview

MongoVault is designed for running reliable MongoDB backups on hosts where local scratch disk is limited or ephemeral (such as Kubernetes pods, Docker containers, and small VPS instances). Instead of staging dumps to `/tmp` before uploading, MongoVault streams data directly from MongoDB through compression, chunked authenticated encryption, and SHA-256 hashing into S3-compatible object storage or mounted filesystems.

### Key Capabilities

- **Zero-Temporary-Disk Streaming Pipeline:** Pipes `mongodump --archive` or WiredTiger `$backupCursor` output through `zlib`, `aes-256-gcm-chunked`, and `DigestStream` directly into S3 multipart uploads. Restores stream directly from storage through chunk verification into `mongorestore` or the POSIX UStar extractor.
- **Four Backup Engines:**
  - `logical` — Streaming `mongodump` / `mongorestore` archive pipeline with optional oplog capture and namespace filtering.
  - `physical` — Hot WiredTiger file copy via `$backupCursor` and `$backupCursorExtend` (with `fsyncLock` fallback on standalone instances).
  - `snapshot` — Quiesces writes with `fsyncLock`, triggers cloud block-volume snapshots (AWS EBS, GCP Persistent Disk, Azure Managed Disk, or custom shell hooks), and immediately releases the lock.
  - `sharded` — Coordinates sharded cluster backups via `mongos`: stops and drains the balancer, records cluster time, and backs up `configsvr` plus each shard replica set under isolated storage prefixes.
- **Continuous PITR Oplog Archiver:** Tails `local.oplog.rs` up to the replica set majority commit point (`lastCommittedOpTime`), checks `config.transactions` for in-flight distributed transactions, and archives compressed/encrypted `.oplog.bson.gz` slices. Point-in-time restores (`--pitr`) automatically resolve the latest base backup prior to the target timestamp and replay contiguous slices up to the target second/ordinal.
- **Framed AEAD Envelope Encryption (`aes-256-gcm-chunked`):** Each backup generates a unique 256-bit Data Encryption Key (DEK) wrapped by the master key. Stream payloads are encrypted in 64 KiB frames with per-chunk nonces, 128-bit GCM authentication tags, and a terminal `0xFFFFFFFF` sentinel frame to detect mid-stream truncation before corrupt bytes reach the database.
- **Authenticated Manifests & Key Rotation:** Every backup and oplog slice writes a `.manifest.json` signed with HMAC-SHA256. Historical backups encrypted under older master keys remain restorable via `ENCRYPTION_PREVIOUS_KEYS`.
- **S3 Object Lock (WORM) & GFS Retention:** Supports `GOVERNANCE` and `COMPLIANCE` S3 Object Lock headers and Legal Hold. Timezone-aware Grandfather-Father-Son (`keepLast`, `keepDaily`, `keepWeekly`, `keepMonthly`) pruning uses batched S3 `DeleteObjects` calls and blocks accidental deletion of the base backup anchoring active PITR slices.
- **Automated Restore Drills:** Periodically restores the latest backup into an isolated `_drill_<db>_<timestamp>` database, verifies collection availability and document counts against the manifest, and cleans up the staging database.
- **Hybrid Concurrency Locking:** Combines local atomic PID lockfiles (`O_CREAT | O_EXCL`) with distributed S3 lease objects (`.locks/backup.lock.json`) and background heartbeat renewal.

---

## Architecture

```
                          ┌─────────────────────────────────────────────────────────────┐
                          │                         MongoVault                          │
                          │                                                             │
┌──────────────┐          │  ┌───────────┐      ┌───────────┐      ┌─────────────────┐  │      ┌─────────────────┐
│  mongodump   │ stdout   │  │   Gzip    │      │  Chunked  │      │  S3 Multipart   │  │ PUT  │   AWS S3 / R2   │
│      or      ├──────────┼─►│   Stream  ├─────►│  AES-GCM  ├─────►│   + SHA-256     ├──┼─────►│  / MinIO / FS   │
│$backupCursor │          │  │           │      │ (64 KiB)  │      │  (32 MiB parts) │  │      │                 │
└──────────────┘          │  └───────────┘      └───────────┘      └─────────────────┘  │      └────────┬────────┘
                          │                                                             │               │
                          │  ┌───────────────────────┐     ┌─────────────────────────┐  │               │
                          │  │  Prometheus & Probes  │     │  Hybrid Local + S3 Lock │  │               │
                          │  │ (/metrics, /healthz)  │     │  (Heartbeat Lease)      │  │               │
                          │  └───────────────────────┘     └─────────────────────────┘  │               │
                          └─────────────────────────────────────────────────────────────┘               │
                                                                                                        │
                                       SCHEDULED RESTORE DRILL & PITR REPLAY                            │
                                ┌───────────────────────────────────────────────────────────────────────┘
                                ▼
                     ┌─────────────────────┐      verified       ┌───────────────────────┐
                     │  Ephemeral Restore  ├────────────────────►│  Notifications        │
                     │  (Manifest Assert)  │                     │ (Slack/Webhook/SMTP)  │
                     └─────────────────────┘                     └───────────────────────┘
```

### Cryptographic Wire Format (`aes-256-gcm-chunked`)

Encrypted archives (`.enc`) use a framed binary layout so decryption can authenticate data in 64 KiB increments without buffering the entire archive on disk:

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Header: Magic "MV01" (4B) │ Chunk Index (4B BE) │ Ciphertext Len (4B BE) │
├──────────────────────────────────────────────────────────────────────────┤
│ Ciphertext Payload (up to 65,536 bytes)                                  │
├──────────────────────────────────────────────────────────────────────────┤
│ GCM Authentication Tag (16 bytes)                                        │
└──────────────────────────────────────────────────────────────────────────┘
  ... repeated for chunks 0 .. N-1 ...
┌──────────────────────────────────────────────────────────────────────────┐
│ Terminal Sentinel Frame: "MV01" │ 0xFFFFFFFF │ 0x00000000 │ Tag (16B)    │
└──────────────────────────────────────────────────────────────────────────┘
```

- **Per-Frame Nonce Derivation:** A 12-byte base IV is generated per archive; each frame derives its nonce by XORing the big-endian chunk index into bytes `8..11` of the base IV, and the sentinel frame flips bit 7 of byte 0.
- **Additional Authenticated Data (AAD):** Each frame binds `MV01 || chunkIndex || plainLen` as GCM AAD, preventing frame reordering, splicing, or truncation.

---

## Quickstart

### Prerequisites

- **Node.js** `>= 20.0.0`
- **MongoDB Database Tools** (`mongodump` and `mongorestore` in `PATH` for logical backups and oplog replay)

### 1. Run with Docker Compose

Starts MongoDB 7.0, MinIO S3 storage, and the MongoVault daemon:

```bash
docker compose up -d
```

Inspect health and Prometheus metrics:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/metrics
```

### 2. Build and Run Locally

```bash
git clone https://github.com/bhatbazik/MongoVault.git
cd MongoVault
npm install
npm run build

cp .env.example .env
# Configure MONGODB_URI and storage settings in .env
```

Generate a 32-byte master encryption key:

```bash
npm run key:generate
```

Run preflight checks and start the daemon:

```bash
node dist/index.js check
npm start
```

---

## CLI Reference

After running `npm run build` (or installing the package globally), invoke commands via `node dist/index.js <command>` or the `mongovault` binary:

```bash
# Inspect cluster topology, storage health, PITR timeline, and active locks
node dist/index.js status

# Run preflight verification (mongodump/mongorestore, MongoDB, storage R/W probe, crypto roundtrip)
node dist/index.js check

# Run logical backup (defaults to secondaryPreferred read preference)
node dist/index.js backup

# Run logical backup with point-in-time oplog capture
node dist/index.js backup --oplog

# Run physical WiredTiger hot backup ($backupCursor + $backupCursorExtend)
node dist/index.js backup --engine physical --db-path /var/lib/mongodb

# Restore physical backup into target WiredTiger dbPath
node dist/index.js restore --engine physical --key latest --db-path /var/lib/mongodb

# Run cloud volume snapshot under fsyncLock
node dist/index.js backup --engine snapshot --volume-id vol-0123456789abcdef0

# Run coordinated multi-shard cluster backup via mongos
node dist/index.js backup --sharding

# List available backups in storage
node dist/index.js list

# Inspect backup manifest metadata and per-collection document counts without downloading archive
node dist/index.js describe --key latest

# Verify archive integrity (streams archive and validates SHA-256, byte size, and GCM tags)
node dist/index.js verify --key latest

# Restore latest backup to source database (dropping target collections first)
node dist/index.js restore --drop

# Selective restore by namespace pattern
node dist/index.js restore --ns-include "production.users,production.orders*" --drop
node dist/index.js restore --ns-exclude "production.audit_log" --drop

# Restore to a different database or cluster (automatically remaps namespaces when needed)
node dist/index.js restore --key <key> --uri "mongodb://staging:27017" --database staging_db --drop

# Point-in-time restore using base backup + continuous PITR oplog slices
node dist/index.js restore --pitr "2026-03-10T14:30:00Z" --drop
node dist/index.js restore --pitr "1774092000:1" --drop

# Continuous PITR oplog operations
node dist/index.js oplog:slice
node dist/index.js oplog:tail
node dist/index.js oplog:list
node dist/index.js oplog:prune --older-than "2026-03-01T00:00:00Z"

# Delete a specific backup (blocks if it anchors active PITR slices unless --force is set)
node dist/index.js delete --key <key>
node dist/index.js delete --key <key> --force

# Evaluate or apply GFS retention pruning
node dist/index.js prune --dry-run
node dist/index.js prune

# Execute an immediate automated restore verification drill
node dist/index.js drill

# Export sanitized diagnostic bundle (system resources, tool versions, topology, recent manifests)
node dist/index.js diagnostic --output ./diagnostic-bundle.json
```

---

## Configuration Reference

All settings are validated at startup via Zod ([`src/config.ts`](src/config.ts)):

| Variable                           |     Type      |       Default        | Description                                                                 |
| :--------------------------------- | :-----------: | :------------------: | :-------------------------------------------------------------------------- |
| **MongoDB**                        |               |                      |                                                                             |
| `MONGODB_URI`                      |    String     |      _Required_      | MongoDB connection string                                                   |
| `MONGODB_DATABASE`                 |    String     |        `"*"`         | Database name (`*` or `all` for cluster-wide backup)                        |
| `MONGODB_EXCLUDE_COLLECTIONS`      |    String     |         `""`         | Comma-separated collections to exclude                                      |
| `MONGODB_OPLOG`                    |    Boolean    |       `false`        | Capture oplog during logical dump (requires `MONGODB_DATABASE=*`)           |
| `MONGODB_PARALLEL_COLLECTIONS`     |    Number     |         `2`          | Collections dumped/restored in parallel (`1`–`32`)                          |
| `MONGODB_READ_PREFERENCE`          |    String     | `secondaryPreferred` | `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred`, `nearest` |
| **Backup Engines & Sharding**      |               |                      |                                                                             |
| `BACKUP_ENGINE`                    |    String     |      `logical`       | Default engine: `logical`, `physical`, or `snapshot`                        |
| `PHYSICAL_DB_PATH`                 |     Path      |       Optional       | WiredTiger `dbPath` on local filesystem (for `physical` engine)             |
| `SNAPSHOT_PROVIDER`                |    String     |      `aws-ebs`       | `aws-ebs`, `gcp-pd`, `azure-disk`, or `custom`                              |
| `SNAPSHOT_VOLUME_ID`               |    String     |       Optional       | Cloud volume identifier to snapshot                                         |
| `SNAPSHOT_REGION`                  |    String     |       Optional       | Cloud provider region for volume snapshots                                  |
| `SNAPSHOT_COMMAND_HOOK`            |    String     |       Optional       | External executable invoked for custom snapshot create/restore              |
| `SHARDING_ENABLED`                 |    Boolean    |       `false`        | Coordinate multi-shard backup across `mongos`, `configsvr`, and shards      |
| `SHARDING_CONFIGSVR_URI`           |    String     |       Optional       | Optional dedicated Config Server connection URI                             |
| **Point-in-Time Recovery (PITR)**  |               |                      |                                                                             |
| `PITR_ENABLED`                     |    Boolean    |       `false`        | Run continuous background oplog slicer in daemon mode                       |
| `PITR_FLUSH_INTERVAL_SECONDS`      |    Number     |         `60`         | Interval between oplog slice flushes (`5`–`3600` seconds)                   |
| `PITR_MAX_SLICE_MB`                |    Number     |         `16`         | Maximum uncompressed BSON size per oplog slice (`1`–`512` MiB)              |
| **Storage (`s3` or `filesystem`)** |               |                      |                                                                             |
| `STORAGE_TYPE`                     |    String     |         `s3`         | `s3` or `filesystem`                                                        |
| `STORAGE_PATH`                     |     Path      |       Optional       | Directory path when `STORAGE_TYPE=filesystem`                               |
| `S3_BUCKET`                        |    String     |   _Required (S3)_    | Target S3 / R2 / MinIO bucket name                                          |
| `S3_ACCESS_KEY_ID`                 |    String     |   _Required (S3)_    | S3 access key ID                                                            |
| `S3_SECRET_ACCESS_KEY`             |    String     |   _Required (S3)_    | S3 secret access key                                                        |
| `S3_PREFIX`                        |    String     |     `"mongodb"`      | Object key prefix inside bucket                                             |
| `S3_REGION`                        |    String     |       `"auto"`       | S3 region                                                                   |
| `S3_ENDPOINT`                      |      URL      |       Optional       | Custom S3 endpoint (Cloudflare R2, MinIO, Wasabi, DigitalOcean)             |
| `R2_ACCOUNT_ID`                    |    String     |       Optional       | Cloudflare account ID (auto-configures `S3_ENDPOINT` and region)            |
| `S3_FORCE_PATH_STYLE`              |    Boolean    |       `false`        | Force path-style addressing (required for MinIO)                            |
| `S3_PART_SIZE_MB`                  |    Number     |         `32`         | Multipart upload chunk size (`5`–`512` MiB)                                 |
| `S3_QUEUE_SIZE`                    |    Number     |         `2`          | Concurrent multipart upload streams (`1`–`16`)                              |
| `S3_STORAGE_CLASS`                 |    String     |     `"STANDARD"`     | S3 storage class (`STANDARD`, `STANDARD_IA`, `GLACIER_IR`, etc.)            |
| **S3 Object Lock (WORM)**          |               |                      |                                                                             |
| `S3_OBJECT_LOCK_ENABLED`           |    Boolean    |       `false`        | Apply S3 Object Lock headers to uploaded archives                           |
| `S3_OBJECT_LOCK_MODE`              |    String     |     `GOVERNANCE`     | `GOVERNANCE` or `COMPLIANCE`                                                |
| `S3_OBJECT_LOCK_RETENTION_DAYS`    |    Number     |         `30`         | Days to retain locked objects (`1`–`36500`)                                 |
| `S3_OBJECT_LOCK_LEGAL_HOLD`        |    Boolean    |       `false`        | Apply S3 Object Lock Legal Hold (`ON`)                                      |
| **Encryption**                     |               |                      |                                                                             |
| `ENCRYPTION_KEY`                   |  Base64/Hex   |       Optional       | 32-byte master key for AES-256-GCM envelope encryption                      |
| `ENCRYPTION_PREVIOUS_KEYS`         |    String     |       Optional       | Comma-separated historical 32-byte keys for decrypting older backups        |
| **Schedule & Retention**           |               |                      |                                                                             |
| `BACKUP_CRON`                      |    String     |    `"0 2 * * *"`     | 5-field cron schedule for automated backups                                 |
| `BACKUP_TIMEZONE`                  |    String     |       `"UTC"`        | IANA timezone for cron schedules and retention buckets                      |
| `BACKUP_TIMEOUT_MINUTES`           |    Number     |        `180`         | Maximum backup execution duration before abort                              |
| `BACKUP_ON_START`                  |    Boolean    |       `false`        | Trigger an immediate backup when daemon starts                              |
| `WORK_DIR`                         |     Path      |      OS tmpdir       | Local scratch directory for lock files and temporary PITR replay            |
| `RETENTION_KEEP_LAST`              |    Number     |       Optional       | Always keep the $N$ newest backups                                          |
| `RETENTION_KEEP_DAILY`             |    Number     |       Optional       | Keep 1 backup per day for $N$ days                                          |
| `RETENTION_KEEP_WEEKLY`            |    Number     |       Optional       | Keep 1 backup per ISO week for $N$ weeks                                    |
| `RETENTION_KEEP_MONTHLY`           |    Number     |       Optional       | Keep 1 backup per month for $N$ months                                      |
| **Restore Drills, Alerts & HTTP**  |               |                      |                                                                             |
| `DRILL_MONGODB_URI`                |    String     |       Optional       | Staging MongoDB URI used for automated restore drills                       |
| `DRILL_CRON`                       |    String     |    `"0 4 * * 0"`     | Cron schedule for restore drills                                            |
| `HEARTBEAT_URL`                    |      URL      |       Optional       | Dead-man's switch ping URL (Healthchecks.io, Cronitor, BetterStack)         |
| `NOTIFY_ON_SUCCESS`                |    Boolean    |        `true`        | Dispatch notifications on successful runs as well as failures               |
| `SLACK_WEBHOOK_URL`                |      URL      |       Optional       | Slack incoming webhook URL                                                  |
| `WEBHOOK_URL`                      |      URL      |       Optional       | Generic JSON webhook URL (compatible with Discord/PagerDuty bridges)        |
| `SMTP_HOST` / `SMTP_PORT`          | String/Number |        `587`         | SMTP server settings (`SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`, `SMTP_TO`) |
| `HTTP_PORT`                        |    Number     |        `8080`        | Port for `/healthz`, `/livez`, `/readyz`, `/metrics` (`0` disables)         |
| `LOG_LEVEL`                        |    String     |       `"info"`       | `fatal`, `error`, `warn`, `info`, `debug`, `trace`, `silent`                |

---

## Storage Layout & Manifest Format

```
s3://<bucket>/<prefix>/
  ├── _cluster/                              # or <database>/ when scoped to a single DB
  │   ├── 2026-03-10/
  │   │   ├── 20260310T020000Z.archive.gz.enc
  │   │   └── 20260310T020000Z.archive.gz.enc.manifest.json
  │   └── _pitr/
  │       ├── state.json
  │       └── 2026-03-10/
  │           ├── 1773108000_00000001__1773108060_00000014.oplog.bson.gz.enc
  │           └── 1773108000_00000001__1773108060_00000014.oplog.bson.gz.enc.manifest.json
  ├── configsvr/                             # created during --sharding cluster backups
  └── shards/
      ├── shard01/
      └── shard02/
```

---

## Operational Considerations

- **Physical Engine (`--engine physical`) Locality:** MongoDB's `$backupCursor` returns file descriptors paths residing on the `mongod` node's local `dbPath`. When using the `physical` engine, MongoVault must run on the same host or share a mounted volume with the target `mongod` data directory, and Physical restores must be performed while the target `mongod` process is stopped.
- **Sharded Cluster Coordination (`--sharding`):** MongoVault operates as a stateless coordinator without requiring agents on individual database nodes. During a sharded backup, it stops the cluster balancer through `mongos`, backs up `configsvr` metadata, and sequentially streams each shard replica set. For multi-terabyte sharded clusters with high cross-shard write rates, keep the balancer window and shard dump duration in mind.
- **Memory & Multipart Sizing:** With `S3_PART_SIZE_MB=32` and `S3_QUEUE_SIZE=2`, in-flight upload buffers stay under ~70 MiB while supporting single archives up to **320 GiB** within S3's 10,000-part limit. Increase `S3_PART_SIZE_MB` to `64` or `128` for larger single-stream archives.

---

## Development & Testing

```bash
# Run unit and pipeline integration tests (Vitest)
npm test

# Run TypeScript typecheck, ESLint, Prettier check, and build
npm run check
```

---

## License

Released under the [MIT License](LICENSE).
