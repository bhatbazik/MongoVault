# mongodb-backup-service

[![CI](https://github.com/bhatbazik/mongodb-backup-service/actions/workflows/ci.yml/badge.svg)](https://github.com/bhatbazik/mongodb-backup-service/actions/workflows/ci.yml)
[![Node](https://img.shields.io/badge/node-%3E%3D20.0.0-blue.svg)](https://nodejs.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED?logo=docker&logoColor=white)](https://hub.docker.com)

A streaming MongoDB backup daemon and CLI tool with S3/Cloudflare R2 multipart uploads, client-side AES-256-GCM envelope encryption, automated restore drills, and Prometheus observability.

---

## Architecture

```
                          ┌─────────────────────────────────────────────────────────────┐
                          │                 mongodb-backup-service                      │
                          │                                                             │
┌──────────────┐          │  ┌───────────┐      ┌───────────┐      ┌─────────────────┐  │      ┌─────────────────┐
│              │ stdout   │  │  Stream   │      │  Digest   │      │  S3 Multipart   │  │ PUT  │  Cloudflare R2  │
│  mongodump   ├──────────┼─►│ Cipheriv  ├─────►│  Stream   ├─────►│     Upload      ├──┼─────►│     / AWS S3    │
│              │ (gzip)   │  │(AES-256)  │      │ (SHA-256) │      │  (32MB parts)   │  │      │     / MinIO     │
└──────────────┘          │  └───────────┘      └───────────┘      └─────────────────┘  │      └────────┬────────┘
                          │                                                             │               │
                          │  ┌───────────────────────┐     ┌─────────────────────────┐  │               │
                          │  │  Prometheus Metrics   │     │  Atomic Lock & Token    │  │               │
                          │  │  (/metrics, /healthz) │     │  (Stale PID Recovery)   │  │               │
                          │  └───────────────────────┘     └─────────────────────────┘  │               │
                          └─────────────────────────────────────────────────────────────┘               │
                                                                                                        │
                                       WEEKLY AUTOMATED RESTORE DRILL                                   │
                                ┌───────────────────────────────────────────────────────────────────────┘
                                ▼
                     ┌─────────────────────┐      verified?      ┌───────────────────────┐
                     │   Ephemeral Mongo   ├────────────────────►│  Alerts & Heartbeats  │
                     │  (count assertion)  │      count match    │ (Healthchecks, Slack) │
                     └─────────────────────┘                     └───────────────────────┘
```

### Highlights

- **Zero-disk streaming pipeline (Backup & Restore):** Streams `mongodump` archive output directly into S3 multipart uploads and streams downloads directly into `mongorestore` stdin through in-flight hashing and encryption transforms. A 100 GB database requires 0 bytes of temporary disk space during both backup and restore.
- **Chunked AEAD Envelope Encryption (`aes-256-gcm-chunked`):** Archives are encrypted in 64 KB frames, each with an independent nonce and 128-bit authentication tag. Tampering, corruption, or stream truncation is detected immediately at the corrupted frame, aborting before corrupt data reaches the restore target. Backward-compatible with legacy single-block AES-GCM.
- **Cluster-Wide & Multi-Database Backups:** Back up entire clusters (`MONGODB_DATABASE=*`) or individual databases. Collections are automatically namespaced (`database.collection`) to avoid collisions.
- **Distributed S3 & Atomic Local Locking:** Hybrid lease locking coordinates multiple daemon instances across Kubernetes pods or Docker containers via S3 object leases with automatic heartbeat renewal, while atomic POSIX rename prevents local TOCTOU race conditions.
- **High-Throughput Batch Pruning:** Prunes retention targets using S3 `DeleteObjectsCommand` batch requests (up to 1,000 objects per API call) instead of sequential deletes.
- **Automated Restore Drills:** Periodically restores the latest backup to an isolated staging database, verifies collection accessibility, and accounts for point-in-time oplog write shifts to eliminate false alarms.
- **Native AWS S3, Cloudflare R2, & MinIO:** Works out-of-the-box with AWS S3 regional endpoints, Cloudflare R2, or MinIO.
- **Grandfather-Father-Son (GFS) Retention:** Automated pruning supports `keep-last`, `keep-daily`, `keep-weekly`, and `keep-monthly` policies natively without relying solely on cloud bucket lifecycle rules.
- **Process Security & Isolation:** Passes connection credentials to child processes through ephemeral `mode 0600` config files with synchronous process-exit cleanup to avoid leaking connection strings in `ps aux`.
- **Prometheus & Dead-Man's Switch:** Exposes standard `/metrics` and `/healthz` HTTP endpoints and pings dead-man's switch services (e.g. Healthchecks.io) to catch silent daemon crashes.

---

## Quickstart

### 1. Run with Docker Compose

Spin up a self-contained environment with MongoDB 7.0, MinIO S3 storage, and the backup service:

```bash
docker compose up -d
```

Check the health and Prometheus metrics endpoints:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/metrics
```

### 2. Run Locally

```bash
git clone https://github.com/bhatbazik/mongodb-backup-service.git
cd mongodb-backup-service
npm install

cp .env.example .env
# Edit .env with your MongoDB and S3/R2 credentials
```

Generate an AES-256 encryption key:

```bash
npm run key:generate
```

Start the daemon:

```bash
npm start
```

---

## CLI Reference

The service ships with an executable CLI (`mongodb-backup`):

```bash
# Run manual backup
node src/index.js backup

# Run manual backup with point-in-time oplog capture
node src/index.js backup --oplog

# Run manual backup with JSON output
node src/index.js backup --json

# List backups in bucket
node src/index.js list

# Verify integrity and checksum of latest backup
node src/index.js verify

# Verify specific backup key
node src/index.js verify --key mongodb/production/2026-03-10/20260310T020000Z.archive.gz.enc

# Restore latest backup to source database (drops collections first)
node src/index.js restore --drop

# Restore with point-in-time oplog replay to an exact second
node src/index.js restore --oplog-replay --oplog-limit 1774092000:1 --drop

# Restore to a different target database or cluster
node src/index.js restore --key <key> --uri "mongodb://staging:27017" --database staging_db --drop

# Simulate retention pruning without deleting files
node src/index.js prune --dry-run

# Apply retention pruning
node src/index.js prune

# Run manual restore drill
node src/index.js drill
```

---

## Configuration Reference

Set these variables in `.env` or pass them as system environment variables:

| Variable | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `MONGODB_URI` | String | *Required* | Connection string for source MongoDB instance |
| `MONGODB_DATABASE` | String | *Required* | Target database name |
| `MONGODB_EXCLUDE_COLLECTIONS`| String | `""` | Comma-delimited list of collections to exclude |
| `MONGODB_OPLOG` | Boolean | `false` | Capture point-in-time oplog during backup (replica sets) |
| `MONGODB_PARALLEL_COLLECTIONS`| Number | `2` | Number of collections dumped concurrently |
| `S3_BUCKET` | String | *Required* | Target bucket name |
| `S3_ACCESS_KEY_ID` | String | *Required* | S3 API access key ID |
| `S3_SECRET_ACCESS_KEY` | String | *Required* | S3 API secret access key |
| `S3_PART_SIZE_MB` | Number | `32` | S3 multipart chunk size (32MB handles up to 320GB) |
| `S3_QUEUE_SIZE` | Number | `2` | Concurrent multipart uploads (controls memory footprint) |
| `R2_ACCOUNT_ID` | String | Optional | Cloudflare Account ID (auto-derives endpoint & region) |
| `S3_ENDPOINT` | URL | Optional | Custom S3 endpoint URL (MinIO, Wasabi, AWS) |
| `S3_REGION` | String | `"auto"` | S3 region identifier |
| `S3_FORCE_PATH_STYLE` | Boolean | `false` | Enable path-style S3 URLs (required for MinIO) |
| `S3_PREFIX` | String | `"mongodb"` | Root prefix key in bucket |
| `BACKUP_CRON` | String | `"0 2 * * *"` | 5-field cron expression for backup schedule |
| `BACKUP_TIMEZONE` | String | `"UTC"` | Timezone for cron schedules and directory stamps |
| `BACKUP_TIMEOUT_MINUTES` | Number | `180` | Process execution timeout |
| `BACKUP_ON_START` | Boolean | `false` | Run an immediate backup upon daemon boot |
| `WORK_DIR` | Path | OS tmp dir | Scratch folder for lock files and temp restore streams |
| `ENCRYPTION_KEY` | Base64/Hex | Optional | 32-byte master key for AES-256-GCM envelope encryption |
| `RETENTION_KEEP_LAST` | Number | Optional | Keep N most recent backups |
| `RETENTION_KEEP_DAILY` | Number | Optional | Keep 1 backup per day for N days |
| `RETENTION_KEEP_WEEKLY` | Number | Optional | Keep 1 backup per week for N weeks |
| `RETENTION_KEEP_MONTHLY` | Number | Optional | Keep 1 backup per month for N months |
| `DRILL_MONGODB_URI` | String | Optional | Target MongoDB instance for restore drills |
| `DRILL_CRON` | String | Optional | Cron schedule for automated restore drills |
| `HEARTBEAT_URL` | URL | Optional | Healthchecks.io / Cronitor heartbeat ping URL |
| `NOTIFY_ON_SUCCESS` | Boolean | `true` | Send notifications on successful runs |
| `SLACK_WEBHOOK_URL` | URL | Optional | Incoming Slack webhook URL |
| `WEBHOOK_URL` | URL | Optional | Generic webhook URL for status events (POST JSON) |
| `SMTP_HOST` | String | Optional | SMTP server hostname |
| `SMTP_PORT` | Number | `587` | SMTP port |
| `SMTP_SECURE` | Boolean | `false` | Use TLS directly (port 465) |
| `SMTP_USER` | String | Optional | SMTP username |
| `SMTP_PASSWORD` | String | Optional | SMTP password |
| `SMTP_FROM` | String | Optional | Email sender address |
| `SMTP_TO` | String | Optional | Comma-delimited recipient email addresses |
| `HTTP_PORT` | Number | `8080` | Port for `/healthz` and `/metrics` server (0 disables) |
| `LOG_LEVEL` | String | `"info"` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |

---

## Production Deployment

### Docker

Build and run the non-root container:

```bash
docker build -t mongodb-backup-service .

docker run -d \
  --name mongo-backup \
  --restart unless-stopped \
  --env-file .env \
  -p 8080:8080 \
  mongodb-backup-service
```

### PM2

```bash
npm install -g pm2
pm2 start src/index.js --name mongo-backup -- daemon
pm2 save
pm2 startup
```

### Kubernetes CronJob

An example CronJob manifest for ephemeral backup execution:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: mongodb-backup
spec:
  schedule: "0 2 * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 5
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: backup
              image: mongodb-backup-service:latest
              args: ["backup"]
              envFrom:
                - secretRef:
                    name: mongodb-backup-credentials
```

---

## Storage & Manifest Layout

Backups in S3 or Cloudflare R2 are structured hierarchically:

```
s3://<bucket>/<prefix>/<database>/
  ├── 2026-03-10/
  │   ├── 20260310T020000Z.archive.gz.enc
  │   └── 20260310T020000Z.archive.gz.enc.manifest.json
  └── 2026-03-11/
      ├── 20260311T020000Z.archive.gz.enc
      └── 20260311T020000Z.archive.gz.enc.manifest.json
```

### Manifest Schema (`.manifest.json`)

Each archive is accompanied by an authenticated JSON manifest written upon successful upload:

```json
{
  "version": 2,
  "key": "mongodb/production/2026-03-10/20260310T020000Z.archive.gz.enc",
  "database": "production",
  "createdAt": "2026-03-10T02:00:00.000Z",
  "completedAt": "2026-03-10T02:02:14.000Z",
  "durationMs": 134000,
  "size": 154210940,
  "sha256": "4b971a82e98fa87c...",
  "compression": "gzip",
  "encryption": {
    "algorithm": "aes-256-gcm-chunked",
    "keyId": "9f81a7b3c2e10d4f",
    "wrappedKey": "tGv/F...==",
    "iv": "3aV+...==",
    "chunkSize": 65536,
    "manifestHmac": "8d4c1f0a..."
  },
  "excludedCollections": ["sessions"],
  "oplog": false,
  "collections": {
    "users": 154200,
    "orders": 894102,
    "products": 4120
  }
}
```

---

## 100 GB+ Scaling on Constrained Hosts

Backing up multi-gigabyte or 100 GB+ databases on small VPS hosts (e.g. 1 vCPU, 1 GB RAM, 10 GB disk) requires deliberate resource boundaries:

1. **Zero Temporary Disk Usage (Backup & Restore):**
   - Traditional backup scripts buffer dumps or restore archives to `/tmp`. A 100 GB database on a 20 GB disk will crash with `ENOSPC`.
   - `mongodb-backup-service` pipes `mongodump` stdout directly through in-memory cryptographic and hashing transforms straight into S3/R2 multipart uploads without touching local disk. During restore, it streams directly from S3 through chunk-authenticated decryptors into `mongorestore` stdin with 0 bytes saved to disk.

2. **Strict Stream Backpressure (`highWaterMark`):**
   - If `mongodump` produces data faster than your network uplink can send it, Node.js streams buffer data in heap memory.
   - We enforce a `1 MB` stream `highWaterMark`. When the buffer fills, backpressure pauses the `mongodump` child process at the Linux kernel pipe level, keeping RSS memory flat throughout a 2-hour 100 GB dump.

3. **S3/R2 10,000 Part Limit & Dynamic Part Sizing:**
   - S3-compatible APIs enforce a hard limit of **10,000 parts** per multipart upload. A default 5 MB or 10 MB chunk size fails at 50 GB or 100 GB with `TooManyParts`.
   - Default `S3_PART_SIZE_MB=32` allows up to **320 GB** per archive. Setting `S3_PART_SIZE_MB=64` enables backups up to **640 GB**.
   - Concurrency is throttled via `S3_QUEUE_SIZE=2` to keep active in-flight buffer memory under ~65 MB.

4. **Cluster I/O Throttling (`--numParallelCollections`):**
   - By default, `mongodump` reads 4 collections simultaneously, creating severe I/O thrashing on active production databases.
   - Setting `MONGODB_PARALLEL_COLLECTIONS=2` (or `1`) guarantees smooth, non-disruptive streaming.

---

## Point-In-Time Recovery (PITR)

A 100 GB dump takes 1 to 2 hours. Without point-in-time consistency, collections dumped early in the run are out-of-sync with collections dumped later.

### 1. Consistent Snapshot Backups (`--oplog`)
Enable `MONGODB_OPLOG=true` or pass `--oplog` on the CLI:
```bash
node src/index.js backup --oplog
```
`mongodump` tails and records all oplog transactions that occur during the dump. Restoring automatically applies these operations, producing a transactionally consistent snapshot of the exact second the backup finished.

### 2. Point-In-Time Replay (`--oplog-limit`)
To roll back the database to a specific second (e.g. right before an accidental table drop):
```bash
node src/index.js restore --oplog-replay --oplog-limit 1774092000:1 --drop
```
`mongorestore` applies all oplog operations up to timestamp `1774092000` (Unix epoch seconds) and increment `1`.

---

## Cloudflare R2 & Checksum Architecture

### Why Client-Side Envelope Encryption Eliminates Server-Side Checksum Limitations
Older S3 backup scripts relied on AWS S3's `ChecksumAlgorithm: SHA256` where S3 computes hashes server-side on upload. Cloudflare R2 did not historically support this proprietary header for multipart uploads.

In this architecture, client-side **AES-256-GCM** makes server-side hash computation redundant:
- **Cryptographic AEAD Guarantee:** AES-256-GCM produces a 128-bit authentication tag (`authTag`). If a single bit in the R2 object is modified, truncated, or corrupted in transit or at rest, the decryption cipher fails immediately with an authentication error.
- **In-flight SHA-256:** Computed in-memory during streaming and recorded in the `.manifest.json`.
- **R2 Multipart Cleanup:** Cloudflare R2 automatically purges incomplete multipart uploads after 7 days, avoiding lingering storage costs from aborted network runs.
- **Hybrid Retention:** In-app GFS retention prunes backups according to schedule (daily, weekly, monthly) while an R2 bucket lifecycle rule serves as an outer defense-in-depth safety net.

---

## Observability & Metrics

The built-in HTTP server exposes standard metrics compatible with Prometheus, Grafana, and Datadog:

| Metric | Type | Description |
| :--- | :---: | :--- |
| `mongodb_backup_runs_total` | Counter | Total backups run, labeled by `status` (success/failure) and `database` |
| `mongodb_backup_duration_seconds` | Histogram | Execution time distribution in seconds |
| `mongodb_backup_size_bytes` | Gauge | Byte size of the most recent backup archive |
| `mongodb_backup_last_success_timestamp_seconds` | Gauge | Epoch timestamp of last successful backup |
| `mongodb_backup_drill_runs_total` | Counter | Total restore drills run, labeled by `status` |
| `mongodb_backup_drill_last_success_timestamp_seconds`| Gauge | Epoch timestamp of last successful drill |

---

## Testing

Run unit and integration test suites:

```bash
# Run all tests once
npm test

# Run tests in watch mode
npm run test:watch
```

Test coverage includes:
- AES-256-GCM envelope encryption & deciphering with auth tag validation
- Atomic token lock lifecycle, contention, and stale PID recovery
- GFS retention planning algorithms across day/week/month boundaries
- Zod configuration validation & edge cases
- In-flight SHA-256 digest computation & byte streaming
- Full mock pipeline backup, restore, verify, and corruption rejection

---

## License

[MIT](LICENSE)
