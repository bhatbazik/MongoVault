# MongoVault

[![CI](https://github.com/bhatbazik/MongoVault/actions/workflows/ci.yml/badge.svg)](https://github.com/bhatbazik/MongoVault/actions/workflows/ci.yml)
[![Node](https://img.shields.io/badge/node-%3E%3D20.0.0-blue.svg)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-6.0-3178C6.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**MongoVault** is a MongoDB backup engine, continuous Point-in-Time Recovery (PITR) daemon, and CLI tool. Built for production clusters and resource-constrained environments (Kubernetes pods, Docker containers, low-memory VPS instances), MongoVault features a **zero-temporary-disk streaming architecture**. It processes database streams directly through gzip compression, chunked authenticated encryption (`aes-256-gcm-chunked`), and SHA-256 checksum hashing into AWS S3 multipart storage or local filesystems without buffering data on local disk.

---

## Table of Contents

- [Part 1 — Introduction and Mental Model](#part-1--introduction-and-mental-model)
  - [The Problem MongoVault Solves](#the-problem-mongovault-solves)
  - [Zero-Temporary-Disk Streaming Pipeline](#zero-temporary-disk-streaming-pipeline)
  - [Backup Mental Model](#backup-mental-model)
  - [Restore Mental Model](#restore-mental-model)
  - [Core Capabilities](#core-capabilities)
  - [Major Architectural Decisions](#major-architectural-decisions)
  - [High-Level Architecture Diagram](#high-level-architecture-diagram)
- [Part 2 — System Architecture](#part-2--system-architecture)
  - [Subsystem Breakdown and Responsibilities](#subsystem-breakdown-and-responsibilities)
  - [Detailed Component Interaction Diagram](#detailed-component-interaction-diagram)
- [Part 3 — End-to-End Execution Flows](#part-3--end-to-end-execution-flows)
  - [Backup Execution Flow](#backup-execution-flow)
  - [Backup Flow Sequence Diagram](#backup-flow-sequence-diagram)
  - [Restore Execution Flow](#restore-execution-flow)
  - [Restore Flow Sequence Diagram](#restore-flow-sequence-diagram)
  - [Point-in-Time Recovery (PITR) Replay Flow](#point-in-time-recovery-pitr-replay-flow)
- [Part 4 — Backup Engines Explained](#part-4--backup-engines-explained)
  - [Engine Resolution Logic](#engine-resolution-logic)
  - [1. Logical Backup Engine (`logical`)](#1-logical-backup-engine-logical)
  - [2. Physical Hot Backup Engine (`physical`)](#2-physical-hot-backup-engine-physical)
  - [3. Cloud Volume Snapshot Engine (`snapshot`)](#3-cloud-volume-snapshot-engine-snapshot)
  - [4. Sharded Cluster Engine (`sharded`)](#4-sharded-cluster-engine-sharded)
  - [Engine Comparison Matrix](#engine-comparison-matrix)
- [Part 5 — Subsystems Deep Dive](#part-5--subsystems-deep-dive)
  - [5.1 Streaming Architecture, Backpressure and Cancellation](#51-streaming-architecture-backpressure-and-cancellation)
  - [5.2 Compression Pipeline](#52-compression-pipeline)
  - [5.3 Cryptographic Subsystem and Wire Format](#53-cryptographic-subsystem-and-wire-format)
  - [5.4 Authentication, Integrity and Manifest Verification](#54-authentication-integrity-and-manifest-verification)
  - [5.5 Object Storage and Filesystem Backends](#55-object-storage-and-filesystem-backends)
  - [5.6 Hybrid Concurrency Locking Subsystem](#56-hybrid-concurrency-locking-subsystem)
  - [5.7 Retention Policy Engine (Grandfather-Father-Son)](#57-retention-policy-engine-grandfather-father-son)
  - [5.8 Automated Restore Drills and Verification](#58-automated-restore-drills-and-verification)
  - [5.9 Observability, Health Probes and Prometheus Metrics](#59-observability-health-probes-and-prometheus-metrics)
  - [5.10 Notification Dispatcher](#510-notification-dispatcher)
- [Part 6 — Point-in-Time Recovery (PITR)](#part-6--point-in-time-recovery-pitr)
  - [Why Ordinary Backups Are Insufficient](#why-ordinary-backups-are-insufficient)
  - [MongoDB Oplog Mechanics](#mongodb-oplog-mechanics)
  - [Continuous Slicing Architecture](#continuous-slicing-architecture)
  - [Timeline Continuity and Gap Detection](#timeline-continuity-and-gap-detection)
  - [PITR Plan Resolution and Two-Stage Replay](#pitr-plan-resolution-and-two-stage-replay)
  - [PITR Timeline and Execution Diagrams](#pitr-timeline-and-execution-diagrams)
- [Part 7 — Security and Data Integrity](#part-7--security-and-data-integrity)
  - [Threat Model and Trust Boundaries](#threat-model-and-trust-boundaries)
  - [Envelope Encryption and Key Derivation](#envelope-encryption-and-key-derivation)
  - [Frame Nonce Management](#frame-nonce-management)
  - [Cryptographic AAD Binding](#cryptographic-aad-binding)
  - [Manifest Tampering Protection](#manifest-tampering-protection)
  - [S3 Object Lock (WORM Immutability)](#s3-object-lock-worm-immutability)
  - [Secret Handling and Process Table Protection](#secret-handling-and-process-table-protection)
  - [Security Limitations and Assumptions](#security-limitations-and-assumptions)
- [Part 8 — Failure Scenarios and Recovery Runbook](#part-8--failure-scenarios-and-recovery-runbook)
  - [Failure Matrix](#failure-matrix)
  - [Recovery Runbooks](#recovery-runbooks)
- [Part 9 — Installation and Practical Usage](#part-9--installation-and-practical-usage)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Quickstart with Docker Compose](#quickstart-with-docker-compose)
  - [CLI Command Reference](#cli-command-reference)
  - [Practical Operational Recipes](#practical-operational-recipes)
- [Part 10 — Configuration Reference](#part-10--configuration-reference)
- [Part 11 — Operational Considerations](#part-11--operational-considerations)
  - [Memory Footprint and S3 Multipart Sizing](#memory-footprint-and-s3-multipart-sizing)
  - [Physical Backup Host Locality](#physical-backup-host-locality)
  - [Sharded Cluster Operational Limits](#sharded-cluster-operational-limits)
  - [Recovery Objectives (RPO and RTO)](#recovery-objectives-rpo-and-rto)
- [Part 12 — Project Structure and Development](#part-12--project-structure-and-development)
  - [Source Code Organization](#source-code-organization)
  - [Development Workflow and Scripts](#development-workflow-and-scripts)
  - [Testing and Verification](#testing-and-verification)
  - [Contributing](#contributing)

---

## Part 1 — Introduction and Mental Model

### The Problem MongoVault Solves

Traditional database backup tools stage full database dumps onto local disk before compressing, encrypting, and uploading them to remote object storage. For large production databases (tens to hundreds of gigabytes), this approach suffers from severe operational flaws:

1. **Local Disk Exhaustion:** In containerized environments (Kubernetes pods, Docker containers), local scratch disk space (`/tmp`) is constrained or mounted on ephemeral storage. Staging a 200 GB dump requires at least 200 GB of free local disk space, frequently leading to node out-of-disk crashes (`ENOSPC`) and evicted pods.
2. **Double I/O Overhead:** Writing raw data to disk and then reading it back for encryption and upload doubles local disk I/O, competing with the database engine for disk throughput and memory page cache.
3. **Security Exposure:** Storing unencrypted dump archives on local scratch disks—even temporarily—creates an attack vector where sensitive data remains in swap space or unscrubbed filesystem blocks.
4. **Data Loss Window (High RPO):** Nightly dumps leave an unacceptable 24-hour data loss window. If a cluster fails at 23:59, all modifications made since 02:00 are lost.
5. **Untested Restores:** Backups are rarely tested until disaster strikes. Without automated restore verification drills, silent corruption, lost encryption keys, or schema incompatibility remain undiscovered until recovery fails.

### Zero-Temporary-Disk Streaming Pipeline

MongoVault eliminates intermediate disk staging entirely. It treats backup and restore operations as continuous, backpressured stream pipelines.

- **During Backup:** Data emitted by MongoDB (`mongodump --archive` or WiredTiger `$backupCursor`) is piped directly through memory into gzip compression, chunked authenticated encryption (`aes-256-gcm-chunked`), an inline SHA-256 digest calculator, and straight into an AWS S3 multipart upload (or mounted filesystem). Memory buffers are governed by Node.js stream backpressure (`highWaterMark: 1 MiB`), keeping the resident memory footprint predictable (typically under 70 MiB under default configuration).
- **During Restore:** Encrypted data is pulled as a byte stream from S3 or disk, verified against per-chunk AEAD authentication tags in 64 KiB frames, decrypted, decompressed, and fed straight into `mongorestore` or a POSIX UStar tar extractor.

> [!NOTE]
> **Diskless Architecture Scope & PITR Qualification:**
> The primary backup streaming pipeline, base restore pipeline, physical hot copy, and verification engine are strictly **zero-temporary-disk**. However, incremental Point-in-Time Recovery (PITR) slice replay temporarily stages individual uncompressed oplog slices to `WORK_DIR` as ephemeral `.pitr-slice-<uuid>.oplog.bson` files. This is required because `mongorestore` mandates a seekable filesystem path when using `--oplogFile` with `--oplogLimit`. Each slice file is deleted immediately upon replay completion.

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                             BACKUP STREAM PIPELINE (ZERO DISK)                              │
│                                                                                             │
│  ┌──────────────┐ stdout  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐  │
│  │  mongodump   ├────────►│  Gzip (zlib) ├───────►│  Chunked GCM ├───────►│ DigestStream │  │
│  │ (archive/gz) │ (pipe)  │ (inline gz)  │ (pipe) │  Encryptor   │ (pipe) │  (SHA-256)   │  │
│  └──────────────┘         └──────────────┘        └──────┬───────┘        └──────┬───────┘  │
│                                                          │                       │          │
│                                                  64 KiB AEAD Frames         Bytes & Hash    │
│                                                          │                       │          │
│                                                          ▼                       ▼          │
│                                            ┌─────────────────────────────────────────────┐  │
│                                            │         PassThrough Stream Buffer           │  │
│                                            └─────────────────────┬───────────────────────┘  │
└──────────────────────────────────────────────────────────────────┼──────────────────────────┘
                                                                   │
                                                   S3 Multipart Upload (32 MiB parts)
                                                                   │
                                                                   ▼
                                                     ┌───────────────────────────┐
                                                     │   AWS S3 / Local Storage  │
                                                     │  (.archive.gz.enc / WORM) │
                                                     └───────────────────────────┘
```

### Backup Mental Model

When a backup is requested (via cron, daemon startup, or CLI):

1. **Locking:** MongoVault acquires a local atomic PID lockfile and a distributed lease object in storage (`.locks/backup.lock.json`) with automated background heartbeat renewal to ensure single-writer exclusivity.
2. **Topology Discovery & Strategy Selection:** The engine inspects the target MongoDB instance, verifying cluster topology (standalone, replica set, or sharded cluster), feature compatibility, and whether logical, physical, snapshot, or sharded backup is specified.
3. **Boundary Recording:** If oplog capture or Point-in-Time Recovery is active, MongoVault inspects `local.oplog.rs` and the replica set majority commit point (`lastCommittedOpTime`) to record the exact transaction boundaries.
4. **Streaming Transformation:** The selected engine initiates data extraction, passing chunks through gzip compression, AEAD chunk encryption using a freshly generated 256-bit Data Encryption Key (DEK), and SHA-256 hashing.
5. **Atomic Storage & Manifest Finalization:** The stream is uploaded in parts. When complete, MongoVault verifies the uploaded size against storage headers (`head`), generates a cryptographic manifest (`.manifest.json`) containing collection document counts and SHA-256 checksums, signs the manifest with HMAC-SHA256, and writes it to storage.
6. **Post-Backup Lifecycle:** Configured retention policies (Grandfather-Father-Son) prune expired archives and orphan uploads, metric counters are incremented, and success notifications are dispatched.

### Restore Mental Model

When a restore is requested:

1. **Manifest Resolution & Authentication:** MongoVault fetches the target backup manifest, verifies its HMAC-SHA256 signature against the configured master key ring, and unwraps the Data Encryption Key (DEK).
2. **Pre-Verification Safety Gate:** If `--drop` is specified (dropping target collections), MongoVault streams and verifies the archive's SHA-256 checksum, chunk tags, and gzip integrity _before_ touching existing database collections.
3. **Namespace Remapping & Filtering:** Restore parameters (`--ns-include`, `--ns-exclude`, `--database`) are dynamically translated into `mongorestore` arguments or database renames.
4. **Zero-Disk Ingestion:** Data is streamed from storage, decrypted and authenticated frame-by-frame, decompressed, and ingested directly into MongoDB over the wire.
5. **Point-in-Time Oplog Splicing (if `--pitr` is requested):** After restoring the base dump, MongoVault identifies, downloads, decrypts, and sequentially replays contiguous incremental oplog slices up to the exact target second and ordinal limit.

### Core Capabilities

- **Zero-Disk Streaming:** Direct streaming from MongoDB through compression and encryption into storage.
- **Four Backup Engines:**
  - `logical`: Streaming `mongodump` archive with collection parallelism and optional oplog capture.
  - `physical`: Hot WiredTiger database file backup via `$backupCursor` and `$backupCursorExtend` with `fsyncLock` fallback.
  - `snapshot`: Cloud volume snapshots (AWS EBS or custom hooks) coordinated under `fsyncLock`.
  - `sharded`: Multi-shard cluster backup coordinating `mongos`, balancer drain, `configsvr`, and shard replica sets.
- **Second-Level Point-in-Time Recovery (PITR):** Continuous background oplog archiver tails `local.oplog.rs`, checks for uncommitted distributed transactions in `config.transactions`, and writes contiguous `.oplog.bson.gz` slices.
- **Framed AEAD Envelope Encryption (`aes-256-gcm-chunked`):** 256-bit DEK generated per archive, wrapped by master keys. Streams are encrypted in 64 KiB frames with per-chunk nonces, 128-bit GCM authentication tags, frame sequence counters, and explicit final frame markers to prevent truncation attacks.
- **Authenticated Manifests & Key Rotation:** Manifests signed with HMAC-SHA256. Historical archives encrypted under older keys remain restorable using `ENCRYPTION_KEY_RING`.
- **Ransomware Immutability (S3 Object Lock WORM):** Supports AWS S3 Object Lock in `GOVERNANCE` or `COMPLIANCE` modes and Legal Hold.
- **Grandfather-Father-Son (GFS) Retention:** Timezone-aware retention buckets (`keepLast`, `keepDaily`, `keepWeekly`, `keepMonthly`) with safety gates preventing deletion of base backups anchoring active PITR chains.
- **Automated Restore Drills:** Scheduled validation that restores backups into isolated ephemeral databases (`_drill_<db>_<timestamp>`), verifies document counts, and reports oplog variances.
- **Hybrid Concurrency Locking:** Local atomic PID lockfiles (`flag: "wx"`, stale takeover via atomic rename) combined with distributed S3 conditional PUT leases (`IfNoneMatch: "*"` / `IfMatch: etag`).
- **Observability:** Prometheus metrics (`/metrics`), Kubernetes probes (`/healthz`, `/ready`), and alerting across Slack, generic Webhooks, SMTP email, and dead-man's switch pings.

### Major Architectural Decisions

| Decision                                        | Rationale                                                                                                                                                                       | Trade-off / Implication                                                                                               |
| :---------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------------------------------- |
| **Streaming over File Staging**                 | Avoids local disk footprint; enables execution on diskless or small container nodes.                                                                                            | Requires network upload speed to keep pace with database dump speed; uses Node.js backpressure buffers (~70 MiB RAM). |
| **Framed AEAD (`aes-256-gcm-chunked`)**         | Standard AES-GCM requires the authentication tag at the end, forcing tools to buffer the whole file before decrypting. Chunked framing authenticates data in 64 KiB increments. | Adds 33 bytes of framing overhead per 64 KiB chunk; requires MongoVault (or compatible decryptor) to unpack.          |
| **Envelope Encryption (DEK + Master Key)**      | Enables master key rotation without re-encrypting large historical backup archives.                                                                                             | Requires secure storage of master keys; wrapped keys are stored in manifests.                                         |
| **S3 Conditional PUT for Distributed Locking**  | Guarantees atomic distributed mutual exclusion without requiring external coordination clusters like Redis, Consul, or ZooKeeper.                                               | Requires S3-compatible storage supporting ETag conditional headers (`If-Match`, `If-None-Match`).                     |
| **Process Table Protection via Config Files**   | Passing connection URIs with passwords via CLI flags exposes credentials in `/proc/<pid>/cmdline` and `ps`. MongoVault writes temporary `0o600` YAML configs.                   | Creates ephemeral config files in `WORK_DIR` which are deleted on process exit or signal.                             |
| **Pre-Verification Before Destructive Restore** | If `--drop` is specified, corrupt archives would wipe production tables before failing. MongoVault tests the stream first.                                                      | Doubles restore read time when `--drop` is active, but prevents unrecoverable data loss.                              |

---

### High-Level Architecture Diagram

```mermaid
flowchart TD
    subgraph Client["Execution Triggers"]
        CLI["CLI Commands\n(backup, restore, pitr, drill)"]
        Cron["Internal Scheduler\n(node-cron daemon)"]
        HTTP["HTTP Endpoints\n(/healthz, /ready, /metrics)"]
    end

    subgraph Core["MongoVault Daemon Core"]
        Config["Configuration & Validation\n(Zod schema)"]
        LockMgr["Hybrid Lock Manager\n(Local PID + S3 Lease)"]
        Scheduler["Job Scheduler\n(Backup, Drill, PITR Slicer)"]
        Engines["Engine Registry\n(Logical, Physical, Snapshot, Sharded)"]
    end

    subgraph DataPlane["Streaming Data Plane"]
        Dump["mongodump / $backupCursor\n(Process stdout)"]
        Gzip["Gzip Compression\n(zlib transform)"]
        Crypto["AES-256-GCM Chunked\n(64 KiB AEAD Frames)"]
        Digest["DigestStream\n(SHA-256 inline)"]
        PassThrough["PassThrough Buffer\n(1 MiB highWaterMark)"]
    end

    subgraph Target["External Storage & Database"]
        MongoDB[(MongoDB Cluster\nPrimary / Secondary)]
        Storage[(Storage Target\nAWS S3 / Filesystem)]
        Prometheus["Prometheus / Monitoring"]
        Alerts["Notification Channels\n(Slack, Webhook, SMTP, Ping)"]
    end

    CLI --> Config
    Cron --> Scheduler
    HTTP --> Scheduler
    Config --> LockMgr
    LockMgr --> Engines
    Scheduler --> Engines

    Engines --> Dump
    MongoDB -.->|Stream BSON| Dump
    Dump --> Gzip --> Crypto --> Digest --> PassThrough
    PassThrough -->|Multipart Upload| Storage

    Crypto -.->|"HMAC Manifest"| Storage
    Scheduler -.->|Metrics| Prometheus
    Scheduler -.->|"Failure/Success Alerts"| Alerts
```

---

## Part 2 — System Architecture

### Subsystem Breakdown and Responsibilities

MongoVault is structured as modular, decoupled subsystems where data processing components adhere strictly to the Node.js Stream API.

```
src/
├── index.ts              # Process entrypoint and top-level error trapping
├── cli.ts                # CLI parser, command registry lookup, help formatter
├── config.ts             # Configuration parsing, type coercion, Zod validation
├── engines.ts            # BackupEngineStrategy interface and EngineRegistry
├── backup.ts             # Logical backup coordinator and pipeline builder
├── restore.ts            # Restore pipeline, selective filters, pre-verification
├── physical.ts           # Physical hot backup via $backupCursor / fsyncLock
├── snapshot.ts           # Cloud volume snapshot orchestrator (EBS / custom)
├── sharding.ts           # Sharded cluster backup coordinator & balancer control
├── oplog.ts              # Continuous PITR oplog tailing, slicing, and replay
├── crypto.ts             # AES-256-GCM chunked AEAD, key wrapping, HMAC signing
├── stream.ts             # Variadic pipeline executor and backpressure runner
├── digest.ts             # In-flight SHA-256 hashing and byte count transform
├── catalog.ts            # Storage catalog, pathing, and manifest read/write
├── storage.ts            # S3 client (Upload lib) and Filesystem client
├── lock.ts               # Local PID file lock and S3 distributed lease lock
├── retention.ts          # Grandfather-Father-Son retention planner and pruner
├── drill.ts              # Automated restore drill runner and document counter
├── metrics.ts            # Prometheus registry and HTTP server (/healthz, /ready)
├── notify.ts             # Notification dispatcher (Email, Slack, Webhook, Ping)
├── mongo.ts              # Native MongoDB connection and cluster inspection
├── tools.ts              # Safe subprocess executor for mongodump / mongorestore
├── tar.ts                # POSIX UStar tar streaming generator and extractor
├── abort.ts              # LinkedAbortController and settleAll promise aggregator
├── logger.ts             # Structured JSON logger (Pino)
├── format.ts             # Human-readable formatting utilities
└── scheduler.ts          # Daemon cron coordinator and lifecycle manager
```

#### 1. CLI and Application Dispatcher (`src/cli.ts`, `src/commands/*`)

- **Responsibility:** Parses command-line arguments using Node.js `util.parseArgs`, verifies options against the command signature, initialises the logger, storage backend, notifier, and metrics registry, and delegates execution to the registered `CommandHandler`.
- **Inputs:** `process.argv` and runtime `AppConfig`.
- **Outputs:** Process exit codes (`0` for success, `1` for error) and structured console logs or JSON outputs.

#### 2. Configuration Subsystem (`src/config.ts`)

- **Responsibility:** Validates all environment variables at startup against a strict Zod schema. Handles type coercion (strings to booleans, numbers, arrays, and cron expressions), verifies IANA timezones, ensures conditional requirements (e.g., S3 credentials when `STORAGE_TYPE=s3`, or Config Server URIs when sharding is enabled), and parses encryption keys.
- **Inputs:** `process.env`.
- **Outputs:** Immutable, strongly typed `AppConfig` object. Throws `ConfigError` with path-specific diagnostics if validation fails.

#### 3. Backup and Restore Engines (`src/engines.ts`, `src/backup.ts`, `src/physical.ts`, `src/snapshot.ts`, `src/sharding.ts`)

- **Responsibility:** Implements the `BackupEngineStrategy` interface across four concrete strategies: `LogicalBackupEngine`, `PhysicalBackupEngine`, `SnapshotBackupEngine`, and `ShardedBackupEngine`. Manages database connection state, locks, and subprocess execution.
- **Inputs:** `BackupEngineOptions` or `RestoreEngineOptions`.
- **Outputs:** `BackupManifest` record upon backup completion; restoration summary object upon restore completion.

#### 4. Cryptographic Subsystem (`src/crypto.ts`)

- **Responsibility:** Implements authenticated envelope encryption (`aes-256-gcm-chunked`). Generates unique ephemeral 256-bit Data Encryption Keys (DEK), wraps DEKs using the master key, divides streams into 64 KiB AEAD frames with derived IVs and AAD, validates authentication tags, detects stream truncation, and signs manifests using HMAC-SHA256.
- **Inputs:** Master encryption key (`Buffer`), plaintext byte streams, ciphertext frames.
- **Outputs:** Encrypted/decrypted streams, `EncryptionEnvelope` structures, HMAC signatures.

#### 5. Storage Layer (`src/storage.ts`)

- **Responsibility:** Provides an abstract `StorageClient` contract with two production implementations:
  - `S3StorageClient`: Wraps `@aws-sdk/client-s3` and `@aws-sdk/lib-storage`. Manages managed multipart streaming uploads (`Upload`), download streams, ETag-conditional PUTs for distributed locks, batch deletions (`DeleteObjects`), and S3 Object Lock retention headers.
  - `FilesystemStorageClient`: Stores backups on local or network-attached (NFS/EFS) storage with atomic writes (temporary `.tmp-<uuid>` files renamed upon completion), path traversal protection, and directory walking.
- **Inputs:** Storage keys, byte streams, metadata.
- **Outputs:** Upload confirmations, download streams, object listings, lock tokens.

#### 6. Streaming Pipeline & Digest Engine (`src/stream.ts`, `src/digest.ts`, `src/abort.ts`)

- **Responsibility:** Chains stream stages using Node.js `stream/promises` pipeline. `DigestStream` calculates the exact byte length and SHA-256 checksum on the fly. `settleAll` and `linkedController` guarantee that if any stage in the pipeline fails, all upstream and downstream processes (including subprocesses and S3 upload streams) are aborted immediately.
- **Inputs:** Array of `StreamStage` elements and `AbortSignal`.
- **Outputs:** Completed stream promise or aggregated failure reason.

#### 7. Catalog & Manifest Subsystem (`src/catalog.ts`)

- **Responsibility:** Governs object storage naming conventions, directory prefix structures, backup listing, and manifest persistence. Formats canonical JSON payloads for cryptographic signing.
- **Inputs:** `BackupScope`, `BackupManifest`.
- **Outputs:** Storage keys, parsed timestamps, serialized and signed manifest files.

#### 8. Hybrid Locking Subsystem (`src/lock.ts`)

- **Responsibility:** Prevents split-brain and concurrent overlapping backup executions. Combines local PID-based file locks (`flag: "wx"`, atomic stale takeover via rename, and PID liveness probing via `process.kill(pid, 0)`) with distributed S3 lease locks using conditional PUT operations (`IfNoneMatch: "*"` / `IfMatch: etag`) and background heartbeat timers.
- **Inputs:** Local lock path, storage lock key, TTL configurations.
- **Outputs:** Asynchronous lock release callback `ReleaseLockFn` or `null` if lock is currently held.

#### 9. Retention & GFS Pruner (`src/retention.ts`)

- **Responsibility:** Implements Grandfather-Father-Son (GFS) pruning (`keepLast`, `keepDaily`, `keepWeekly`, `keepMonthly`) calculated within the configured timezone. Deletes expired backups and orphan uploads (> 1 hour old). Prevents accidental deletion of base backups that anchor active PITR oplog slices.
- **Inputs:** Storage client, retention policy, active backup catalog.
- **Outputs:** `PruneResult` detailing kept and removed storage keys.

#### 10. Point-in-Time Recovery (PITR) Engine (`src/oplog.ts`)

- **Responsibility:** Continuous background daemon that monitors `local.oplog.rs` up to the replica set majority commit point (`lastCommittedOpTime`), inspects `config.transactions` for active transactions, batches BSON documents up to `PITR_MAX_SLICE_MB`, compresses and encrypts incremental slices, tracks timeline continuity, and resolves multi-slice replay plans.
- **Inputs:** Replica set oplog entries, target timestamp (`Date` or `t:i`).
- **Outputs:** Archived `.oplog.bson.gz` slices, `PitrPlan` execution recipes.

#### 11. Automated Restore Drills (`src/drill.ts`)

- **Responsibility:** Periodically verifies backup restorability. Enforces safety isolation (`DRILL_MONGODB_URI` cannot equal `MONGODB_URI`), restores the latest backup into a temporary staging database (`_drill_<db>_<timestamp>`), asserts that all collections exist and document counts match the manifest (accounting for oplog variance), and drops the staging database.
- **Inputs:** Staging MongoDB URI, latest backup manifest.
- **Outputs:** `DrillResult` verification report; throws `DrillError` on document count mismatches.

#### 12. Monitoring, Observability & Notifications (`src/metrics.ts`, `src/notify.ts`)

- **Responsibility:** Exposes Prometheus metrics and Kubernetes probes over an embedded HTTP server (`/healthz`, `/ready`, `/metrics`). Dispatches formatted alerts via SMTP email (Nodemailer), Slack webhooks, generic JSON webhooks, and dead-man's switch pings (`HEARTBEAT_URL`).
- **Inputs:** System events, backup manifests, errors.
- **Outputs:** HTTP responses, Prometheus scrape data, alert payloads.

---

### Detailed Component Interaction Diagram

```mermaid
flowchart LR
    subgraph Execution["1. Execution & Coordination"]
        CLI["CLI Command"]
        Scheduler["Scheduler Daemon"]
        LockMgr["Hybrid Lock Manager\n(Local PID + S3 Conditional)"]
    end

    subgraph ConfigSubsystem["2. Configuration"]
        Config["Zod AppConfig\n(Strict Validation)"]
    end

    subgraph EngineSubsystem["3. Backup Engine"]
        EngineReg["Engine Registry"]
        LogicalEng["Logical Engine\n(mongodump)"]
        PhysicalEng["Physical Engine\n($backupCursor)"]
        SnapshotEng["Snapshot Engine\n(AWS EBS / Hook)"]
        ShardedEng["Sharded Engine\n(mongos coordinator)"]
    end

    subgraph StreamSubsystem["4. Streaming Pipeline"]
        Gzip["zlib Gzip"]
        Crypto["aes-256-gcm-chunked\n(64 KiB Frames)"]
        Digest["DigestStream\n(SHA-256)"]
        PassThrough["PassThrough Buffer"]
    end

    subgraph StorageSubsystem["5. Storage Layer"]
        StorageMgr["Storage Client\n(S3 / Filesystem)"]
        ArchiveDest["Backup Archive\n(.enc / .gz)"]
        ManifestDest["Manifest Object\n(.manifest.json)"]
        LockObject[".locks/backup.lock.json"]
    end

    CLI --> Config
    Scheduler --> Config
    Config --> LockMgr
    LockMgr -.->|"Conditional PUT"| LockObject
    LockMgr --> EngineReg

    EngineReg --> LogicalEng
    EngineReg --> PhysicalEng
    EngineReg --> SnapshotEng
    EngineReg --> ShardedEng

    LogicalEng --> StreamSubsystem
    PhysicalEng --> StreamSubsystem

    StreamSubsystem --> Gzip --> Crypto --> Digest --> PassThrough
    PassThrough -->|"Multipart Upload"| StorageMgr
    StorageMgr --> ArchiveDest
    StorageMgr --> ManifestDest
```

---

## Part 3 — End-to-End Execution Flows

### Backup Execution Flow

The following chronological sequence occurs when executing a backup (`mongodb-backup backup` or scheduled cron trigger):

1. **Invocation & Argument Parsing:** The CLI parses command-line flags (`--engine`, `--oplog`, `--database`, `--read-preference`, `--parallel`).
2. **Configuration Validation:** `loadConfig()` parses environment variables through Zod. If any required variables are missing or mutually exclusive (e.g. `MONGODB_OPLOG=true` with single-database scope), execution halts immediately with exit code `1`.
3. **Lock Acquisition:** `acquireHybridLock()` executes:
   - Creates a local lockfile (`${WORK_DIR}/.backup.lock`) using `flag: "wx"`. If locked, it inspects the holding PID and file modification time; stale locks are taken over via atomic rename.
   - If storage supports locking, acquires a distributed lease (`<prefix>/.locks/backup.lock.json`) via an S3 conditional PUT (`IfNoneMatch: "*"`). Spawns a background unref'd heartbeat timer to renew the lease every 30 seconds.
4. **Engine Resolution:** `EngineRegistry.resolveForBackup()` selects the strategy (`logical`, `physical`, `snapshot`, or `sharded`).
5. **Pre-Dump Metadata & Oplog Bounds:** For logical backups with oplog capture, MongoVault connects to MongoDB and queries `local.oplog.rs` for initial timestamp bounds (`preOplog`).
6. **Subprocess Spawning with Credential Protection:** MongoVault writes the MongoDB connection URI into an ephemeral YAML configuration file (`${WORK_DIR}/.<uuid>.yaml`, mode `0o600`), and spawns `mongodump` passing `--config=<file>`. This ensures database credentials are never visible in system process tables (`ps aux`).
7. **Pipeline Assembly & Streaming Execution:**
   - MongoVault links an `AbortController` across all components.
   - If encryption is enabled, it generates a 256-bit DEK, wraps it with the master key, and instantiates `ChunkedAeadEncryptStream`.
   - The stream pipeline connects: `mongodump.stdout -> [Encryptor] -> DigestStream -> PassThrough`.
   - The `PassThrough` stream is piped into `storage.upload()`, which uploads data in 32 MiB parts (`S3_PART_SIZE_MB`).
8. **Subprocess Completion & Collection Counting:** `mongodump` stderr is monitored line-by-line using regular expressions to capture collection document counts. When the dump process finishes, the oplog end boundary is queried immediately.
9. **Upload Finalization & Verification:** The S3 multipart upload completes. MongoVault sends a `head` request to storage and verifies that the byte size reported by S3 matches `digest.bytes`. If sizes mismatch, an error is thrown.
10. **Manifest Generation & HMAC Signing:** A Version 2 `BackupManifest` is constructed, populated with collection counts, SHA-256 checksum, duration, and encryption envelope. The manifest is signed with HMAC-SHA256 and written to storage (`<archive>.manifest.json`).
11. **Post-Backup Retention & Cleanup:**
    - The ephemeral URI YAML file is unlinked.
    - If `RETENTION_KEEP_*` policies are set, `prune()` evaluates retention buckets and executes batched deletions.
    - Prometheus metrics are recorded (`mongodb_backup_runs_total`, `mongodb_backup_size_bytes`).
    - Notifications are dispatched to configured channels (Slack, Email, Webhook, Heartbeat).
    - Local and distributed locks are released.

---

### Backup Flow Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator / Cron
    participant CLI as CLI / Scheduler
    participant Lock as Hybrid Lock Manager
    participant Mongo as MongoDB Cluster
    participant Pipe as Streaming Pipeline (Gzip+Crypto+Digest)
    participant S3 as Object Storage (S3)
    participant Notif as Notifier / Metrics

    Op->>CLI: Execute backup command
    CLI->>Lock: acquireHybridLock()
    Lock->>Lock: Create local PID lockfile (wx)
    Lock->>S3: PutObject(.locks/backup.lock.json, IfNoneMatch: "*")
    S3-->>Lock: 200 OK (Lease Acquired)
    Lock-->>CLI: Lock Acquired (Heartbeat timer started)

    CLI->>Mongo: Connect & query initial oplog bounds (preOplog)
    Mongo-->>CLI: Oplog bounds (startTs)

    CLI->>CLI: Write temporary URI config (0600)
    CLI->>Mongo: Spawn mongodump --config=<file> --archive --gzip
    CLI->>Pipe: Initialize Pipeline [mongodump.stdout -> Encrypt -> Digest -> PassThrough]

    par Streaming Upload
        Mongo->>Pipe: Stream raw compressed BSON chunks
        Pipe->>Pipe: Encrypt in 64 KiB AEAD frames & update SHA-256
        Pipe->>S3: S3 Upload (32 MiB multipart chunks)
    and Collection Counting
        Mongo-->>CLI: Stream stderr progress (parse document counts)
    end

    Mongo-->>CLI: mongodump exit 0
    Pipe-->>CLI: Pipeline finished (bytes & sha256 computed)
    S3-->>CLI: Multipart upload complete

    CLI->>S3: head(archiveKey)
    S3-->>CLI: ContentLength
    CLI->>CLI: Assert ContentLength == digest.bytes

    CLI->>CLI: Generate Manifest & compute HMAC-SHA256 signature
    CLI->>S3: PutJson(archiveKey.manifest.json)
    S3-->>CLI: 200 OK

    opt Retention Policy Enabled
        CLI->>S3: Evaluate GFS retention & prune expired backups
    end

    CLI->>Notif: Record metrics & dispatch alerts (Slack/Email/Ping)
    CLI->>Lock: Release hybrid lock (local file + S3 delete)
    CLI-->>Op: Backup Completed Successfully
```

---

### Restore Execution Flow

When executing a restore (`mongodb-backup restore`):

1. **Reference Resolution:** MongoVault resolves the target backup key (defaults to `"latest"` by listing storage and selecting the newest completed backup with a valid manifest).
2. **Manifest Verification & HMAC Authentication:** The `.manifest.json` file is fetched. If encrypted, the HMAC-SHA256 signature is verified against the master key ring using constant-time comparison (`timingSafeEqual`). If tampered with, execution aborts with `IntegrityError`.
3. **Key Unwrapping:** The Data Encryption Key (DEK) is unwrapped from `envelope.wrappedKey` using the matching master key.
4. **Namespace Analysis:** Target namespace arguments (`--ns-include`, `--ns-exclude`, `--database`) are analyzed. If renaming databases, namespace transformation flags (`--nsFrom`, `--nsTo`) are generated.
5. **Pre-Verification Safety Gate (if `--drop` is set):** If collections will be dropped, MongoVault pre-verifies the archive before touching the database. It streams the archive from S3, checks chunk AEAD tags, decompresses data through gzip into `/dev/null`, and verifies the SHA-256 hash. If verification fails, the database is left completely untouched.
6. **Subprocess Spawning:** MongoVault writes the target URI to a temporary `0o600` YAML file and spawns `mongorestore` with `--archive --gzip` listening on `stdin`.
7. **Direct Decryption Streaming:** Encrypted data is downloaded from storage and piped through `DigestStream -> ChunkedAeadDecryptStream -> mongorestore.stdin`. Data is authenticated in 64 KiB frames before reaching the decompression and restore process.
8. **Final Integrity Assertion:** Upon stream completion, MongoVault confirms the calculated SHA-256 checksum and total byte count match the manifest. It parses `mongorestore` stderr to ensure `failed: 0`.

---

### Restore Flow Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant Op as Operator
    participant CLI as Restore Controller
    participant S3 as Object Storage (S3)
    participant Pipe as Decrypt & Verify Pipeline
    participant Mongo as MongoDB Target

    Op->>CLI: Execute restore --drop --key latest
    CLI->>S3: Fetch latest .manifest.json
    S3-->>CLI: BackupManifest
    CLI->>CLI: Verify Manifest HMAC-SHA256 (timingSafeEqual)
    CLI->>CLI: Unwrap DEK using Master Key Ring

    alt Pre-verification Active (--drop is true)
        Note over CLI,S3: Pre-verify archive before dropping database tables
        CLI->>S3: Download stream
        S3-->>Pipe: Stream ciphertext
        Pipe->>Pipe: Authenticate 64 KiB frames & decompress to discard
        Pipe-->>CLI: Pre-verification PASSED (SHA-256 matches)
    end

    CLI->>CLI: Write temporary URI config (0600)
    CLI->>Mongo: Spawn mongorestore --config=<file> --drop --archive --gzip
    CLI->>S3: storage.download(key)
    S3-->>Pipe: Stream ciphertext chunks

    par Streaming Restore
        Pipe->>Pipe: DigestStream (compute SHA-256)
        Pipe->>Pipe: ChunkedAeadDecryptStream (authenticate & decrypt 64 KiB frames)
        Pipe->>Mongo: Pipe plaintext gzip stream into mongorestore stdin
    and Process Monitoring
        Mongo-->>CLI: Monitor stderr ("X documents restored, Y failed")
    end

    Mongo-->>CLI: mongorestore exit 0
    CLI->>CLI: Assert actual SHA-256 == manifest.sha256
    CLI->>CLI: Assert failed documents == 0
    CLI-->>Op: Restore Completed Successfully
```

---

### Point-in-Time Recovery (PITR) Replay Flow

When executing a point-in-time restore (`mongodb-backup restore --pitr "2026-03-10T14:30:00Z"`):

1. **Target Parsing:** The target timestamp is parsed into second and ordinal components (`{ t: 1773153000, i: 4294967295 }`).
2. **Plan Resolution (`resolvePitrPlan`):**
   - MongoVault scans storage for all completed base backups, selecting the newest base backup created **at or before** the target timestamp.
   - It lists all archived `.oplog.bson.gz` slices covering the window between the base backup commit point and the target timestamp.
   - It verifies continuity across slice manifests: each slice's `previousEndTs` must match or overlap the prior slice's `endTs`. If an oplog rollover gap is detected, execution halts with an error.
3. **Stage 1 (Base Dump Restore):** The base backup archive is restored using the standard restore pipeline, establishing the database state at the base snapshot time.
4. **Stage 2 (Sequential Slice Replay):**
   - For each verified slice, MongoVault downloads the encrypted slice and extracts it to an isolated temporary BSON file in `WORK_DIR` (`.pitr-slice-<uuid>.oplog.bson`).
   - It invokes `mongorestore --oplogReplay --oplogLimit=<target> --oplogFile=<sliceFile>`.
   - `mongorestore` applies oplog operations idempotently up to the target timestamp.
   - The temporary slice file is securely removed immediately after replay.
5. **Completion:** MongoVault logs the total number of replayed slices and operations.

---

## Part 4 — Backup Engines Explained

### Engine Resolution Logic

MongoVault supports multiple backup engines selected via the `--engine` CLI option or the `BACKUP_ENGINE` configuration variable. Resolution follows strict precedence:

```mermaid
flowchart TD
    Start["Request Operation\n(Backup or Restore)"] --> IsBackup{"Operation Type?"}

    IsBackup -- Backup --> CheckEngineParam{"--engine flag\nprovided?"}
    CheckEngineParam -- Yes --> UseFlagEngine["Use Engine from --engine\n(logical | physical | snapshot)"]
    CheckEngineParam -- No --> CheckSharding{"--sharding flag or\nSHARDING_ENABLED=true?"}
    CheckSharding -- Yes --> UseSharded["Use 'sharded' Engine"]
    CheckSharding -- No --> UseConfigEngine["Use BACKUP_ENGINE\n(default: 'logical')"]

    IsBackup -- Restore --> CheckRestoreFlags{"Restore Flags?"}
    CheckRestoreFlags -- "--db-path or *.physical.tar.gz" --> UsePhysicalRestore["Use 'physical' Engine"]
    CheckRestoreFlags -- "--engine snapshot or *.snapshot" --> UseSnapshotRestore["Use 'snapshot' Engine"]
    CheckRestoreFlags -- "--sharding or *.cluster" --> UseShardedRestore["Use 'sharded' Engine"]
    CheckRestoreFlags -- Default --> UseLogicalRestore["Use 'logical' Engine"]
```

---

### 1. Logical Backup Engine (`logical`)

#### What it is

The `logical` engine is MongoVault's default backup mechanism. It executes `mongodump` with `--archive` and `--gzip`, streaming BSON document dumps directly from MongoDB into the encryption and upload pipeline.

#### How it works internally

1. Spawns `mongodump` with `--archive --gzip` and writes database connection options to a temporary YAML config.
2. In cluster-wide backups (`MONGODB_DATABASE=*`), `--oplog` captures all oplog entries written during the dump, ensuring point-in-time consistency.
3. Collection dumping concurrency is tuned via `--numParallelCollections`.
4. Parses `mongodump` stderr output in real time to capture per-collection document counts for the manifest.
5. Uploads the stream in multipart parts directly to storage.

#### Interaction with MongoDB

- Communicates over standard MongoDB client connections (TCP/TLS) via Wire Protocol queries.
- Respects read preferences (`MONGODB_READ_PREFERENCE=secondaryPreferred` by default), minimizing CPU and disk load on the primary replica set node.

#### Advantages

- Completely hardware and platform agnostic; works with MongoDB Atlas, on-premise clusters, Docker, and Kubernetes.
- Allows granular namespace filtering (`--ns-include`, `--ns-exclude`) and selective restoration into different databases or clusters.
- Zero local disk requirement.

#### Limitations

- Slower on massive multi-terabyte datasets compared to block-level or file-level snapshots due to BSON document serialization and index reconstruction during restore.

#### When to use

- Default choice for small to medium databases (up to ~500 GB).
- Environments where access to the underlying storage volume or filesystem is unavailable (e.g. managed DBaaS like MongoDB Atlas).

---

### 2. Physical Hot Backup Engine (`physical`)

#### What it is

The `physical` engine performs hot physical copies of raw WiredTiger storage engine data files directly from disk while MongoDB remains online and operational.

#### How it works internally

1. Connects to `admin` database and invokes the WiredTiger backup cursor:
   ```javascript
   db.adminCommand({ aggregate: 1, pipeline: [{ $backupCursor: {} }], cursor: {} });
   ```
2. The cursor locks the checkpoint and returns an ordered list of active database files (e.g., `WiredTiger.wt`, `collection-*.wt`, `index-*.wt`).
3. If checkpoint conflicts occur (MongoDB error code `50915`), MongoVault applies exponential backoff retries.
4. If supported by MongoDB Enterprise, extended journal files are requested via `$backupCursorExtend` to record write changes that occur during file copying.
5. In standalone instances or engines without `$backupCursor` support, it falls back to `db.fsyncLock()`.
6. Files are packed on the fly into standard 512-byte POSIX UStar tar headers (`src/tar.ts`), compressed with gzip, encrypted, and uploaded to storage without creating an intermediate tarball on disk.
7. An asynchronous keep-alive timer pings the cursor every 45 seconds to prevent MongoDB from timing out the backup cursor during large uploads.

#### Execution Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> ProbeCapabilities: Connect to admin db
    ProbeCapabilities --> OpenBackupCursor: $backupCursor supported
    ProbeCapabilities --> AcquireFsyncLock: $backupCursor unsupported (Fallback)

    OpenBackupCursor --> HandleConflict: Error 50915 / Checkpoint in progress
    HandleConflict --> OpenBackupCursor: Retry backoff (up to 10 attempts)
    HandleConflict --> AcquireFsyncLock: Retries exhausted

    OpenBackupCursor --> QueryFileList: Cursor established
    QueryFileList --> ExtendJournal: Request $backupCursorExtend
    ExtendJournal --> StreamTarPack: Add storage.bson & journal logs
    AcquireFsyncLock --> ScanDbPath: Read files under lock
    ScanDbPath --> StreamTarPack: Pack files into stream

    StreamTarPack --> UploadS3: Pipe POSIX Tar -> Gzip -> Encrypt -> S3
    UploadS3 --> CloseCursor: Complete upload
    CloseCursor --> ReleaseLock: Unlock fsyncLock (if acquired)
    ReleaseLock --> WriteManifest: Write physical manifest
    WriteManifest --> [*]
```

#### Advantages

- Extremely fast restore times for large datasets (multi-terabytes); restores bypass BSON deserialization and index rebuilding by copying raw WiredTiger data blocks directly into the target `dbPath`.

#### Limitations

- Requires **host locality**: MongoVault must run on the same physical host or have shared volume mount access to the MongoDB `dbPath`.
- Target MongoDB service must be completely stopped before restoring physical files into its `dbPath`.

---

### 3. Cloud Volume Snapshot Engine (`snapshot`)

#### What it is

The `snapshot` engine coordinates point-in-time cloud storage block-volume snapshots (AWS EBS or custom cloud infrastructure) under write quiescence.

#### How it works internally

1. Connects to MongoDB `admin` database and executes `db.fsyncLock()`, flushing all dirty in-memory pages to disk and freezing writes.
2. Queries the cluster's replica set status to record the exact majority-committed cluster timestamp (`lastCommittedOpTime`).
3. Invokes the snapshot driver:
   - `aws-ebs`: Issues volume snapshot requests against the AWS EC2 API for the configured `SNAPSHOT_VOLUME_ID`.
   - `custom`: Executes an external executable hook (`SNAPSHOT_COMMAND_HOOK create <volumeId>`).
4. Immediately executes `db.fsyncUnlock()`, unfreezing writes. The write lock is held only for the seconds required to initiate the volume snapshot, not for the duration of the data transfer.
5. Writes a `.snapshot` manifest to storage recording the cloud `snapshotId`, `volumeId`, and cluster timestamp.

#### Advantages

- Near-instantaneous backup execution regardless of database size; offloads data movement entirely to cloud storage hypervisors.
- Write lock is held for mere seconds.

#### Limitations

- Requires AWS API permissions or custom volume snapshot tooling.
- Restore must be performed at the cloud infrastructure level (attaching new volumes) rather than through database tools.

---

### 4. Sharded Cluster Engine (`sharded`)

#### What it is

The `sharded` engine acts as a stateless coordinator across a MongoDB Sharded Cluster (`mongos`, Config Servers, and Shard Replica Sets).

#### How it works internally

1. Connects to `mongos` and executes `hello` / `isMaster` to confirm cluster topology (`msg: "isdbgrid"`).
2. Queries `config.shards` to discover all shard replica set connection addresses.
3. Stops the cluster balancer via `db.adminCommand({ balancerStop: 1 })` and polls `balancerStatus` until all in-flight chunk migrations are fully drained.
4. Captures the cluster-wide logical time (`$clusterTime`) via an admin ping command.
5. Backs up Config Server metadata (`config` database) under `<prefix>/configsvr` to record routing tables, chunk distribution maps, and collection schemas.
6. Iterates through each shard replica set sequentially, deriving direct shard URIs with preserved credentials via `buildShardUri()`, and streams a full backup of each shard under `<prefix>/shards/<shardId>` with `oplog: true`.
7. Restarts the cluster balancer via `db.adminCommand({ balancerStart: 1 })`.
8. Writes a unified `.cluster` manifest binding the Config Server and shard backup manifests to the recorded cluster timestamp.

#### Coordinated Multi-Shard Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Coord as MongoVault Coordinator
    participant Mongos as mongos Router
    participant ConfigSvr as Config Server (configsvr)
    participant Shard1 as Shard Replica Set 1
    participant Shard2 as Shard Replica Set 2
    participant Storage as Object Storage (S3)

    Coord->>Mongos: Connect & discover topology (config.shards)
    Mongos-->>Coord: Topology (ConfigSvr + Shard1 + Shard2)

    Coord->>Mongos: db.adminCommand({ balancerStop: 1 })
    loop Drain Chunk Migrations
        Coord->>Mongos: balancerStatus()
        Mongos-->>Coord: inBalancerRound: false, mode: "off"
    end

    Coord->>Mongos: Capture cluster timestamp ($clusterTime)
    Mongos-->>Coord: clusterTime (t:i)

    Note over Coord,Storage: 1. Backup Config Server Metadata
    Coord->>ConfigSvr: mongodump (database: config)
    ConfigSvr-->>Storage: Stream to <prefix>/configsvr/

    Note over Coord,Storage: 2. Sequentially Backup Shard Data & Oplog
    Coord->>Shard1: mongodump with oplog (buildShardUri)
    Shard1-->>Storage: Stream to <prefix>/shards/shard01/

    Coord->>Shard2: mongodump with oplog (buildShardUri)
    Shard2-->>Storage: Stream to <prefix>/shards/shard02/

    Coord->>Mongos: db.adminCommand({ balancerStart: 1 })
    Mongos-->>Coord: Balancer restarted

    Coord->>Storage: Write unified .cluster manifest
    Storage-->>Coord: Manifest committed
```

> [!WARNING]
> **Cross-Shard Consistency & Balancer Safety Enforcement:**
> During sharded backups, MongoVault invokes `balancerStop` and verifies that active migrations finish (`mode: "off"` and `inBalancerRound: false`).
>
> - **Fail-Closed Migration Drain:** If chunk migrations are active when the timeout expires or if balancer status fails to confirm `mode: "off"`, MongoVault **fails closed** and throws `ActiveChunkMigrationError`. It does not proceed with sequential shard dumping under unverified balancer states.
> - **Timestamp Anchoring on Restore:** During restore (`restoreShardedCluster`), all shard restores replay oplog operations up to the unified `$clusterTime` captured in the cluster manifest (`oplogReplay: true`, `oplogLimit: clusterMeta.clusterTime`).
> - **Cross-Shard Application Writes:** While stopping the balancer halts chunk moves, ongoing application writes can still occur between sequential shard dumps. For zero-skew guarantees on clusters with high-volume cross-shard distributed transactions, schedule backups during quiet windows or consider volume snapshots.

---

### Engine Comparison Matrix

| Feature / Dimension      | `logical`                      | `physical`                     | `snapshot`                      | `sharded`                               |
| :----------------------- | :----------------------------- | :----------------------------- | :------------------------------ | :-------------------------------------- |
| **Backup Mechanism**     | `mongodump` BSON stream        | WiredTiger `$backupCursor` Tar | Cloud Volume Snapshot           | `mongos` multi-node coordinator         |
| **Data Format**          | `.archive.gz[.enc]`            | `.physical.tar.gz[.enc]`       | Cloud Provider Volume           | Multi-prefix archives + `.cluster`      |
| **Hardware Locality**    | Network accessible             | Same host / volume mount       | Cloud provider API access       | Network accessible to `mongos` & shards |
| **Database Consistency** | Point-in-time via `--oplog`    | WiredTiger checkpoint          | Storage-flush under `fsyncLock` | Balancer-quiesced + shard oplogs        |
| **Impact on Primary**    | Minimal (`secondaryPreferred`) | None (reads raw files)         | Writes locked for seconds       | Minimal (secondary preferred per shard) |
| **Restore Speed**        | Moderate (rebuilds indexes)    | Maximum (raw file extract)     | Instantaneous (volume attach)   | Moderate (restores config + shards)     |
| **Selective Restore**    | Yes (namespace filtering)      | No (full database directory)   | No (full block volume)          | Yes (can restore individual shards)     |
| **Target MongoDB State** | Online and operational         | **Must be stopped**            | Attached as new volume          | Target cluster online; balancer stopped |

---

## Part 5 — Subsystems Deep Dive

### 5.1 Streaming Architecture, Backpressure and Cancellation

Node.js streams process data in discrete chunks. Without backpressure regulation, a fast producer (like a local database dump) will overwhelm a slower consumer (like a cloud upload stream), buffering gigabytes in memory until the process crashes with `JavaScript heap out of memory`.

MongoVault guarantees memory safety by using standard Node.js stream backpressure:

- A `PassThrough` stream with `highWaterMark: 1048576` (1 MiB) connects the transformation stages to storage.
- If S3 multipart upload workers are saturated, `write()` returns `false`, causing `DigestStream`, `ChunkedAeadEncryptStream`, `zlib.createGzip()`, and `mongodump` stdout pipes to pause execution until downstream buffers drain.
- **Fail-Safe Abortion (`src/abort.ts`):** Operations wrap an internal `LinkedAbortController`. Using `settleAll()`, if any stage rejects (e.g., S3 returns HTTP 500, or `mongodump` exits with a syntax error), the controller immediately aborts all peer streams, destroys running subprocesses via `SIGTERM` (followed by `SIGKILL` after a 10s grace period), and unlinks incomplete uploads from storage.

```mermaid
flowchart LR
    A["mongodump stdout<br/>(Fast Producer)"] -->|pipe| B["Gzip Transform<br/>(zlib)"]
    B -->|pipe| C["ChunkedAeadEncryptStream<br/>(64 KiB framing)"]
    C -->|pipe| D["DigestStream<br/>(SHA-256)"]
    D -->|"pipe<br/>highWaterMark: 1 MiB"| E["PassThrough Buffer"]
    E -->|"write() == false<br/>Backpressure Pause"| F["S3 Multipart Upload<br/>(Slow Consumer)"]

    F -.->|"drain event<br/>Resume Flow"| E
    E -.->|unpause| D -.->|unpause| C -.->|unpause| B -.->|unpause| A
```

---

### 5.2 Compression Pipeline

Compression is handled by Node.js native `zlib`:

- **Logical Archives:** Compressed using `createGzip()` with standard DEFLATE compression. When `--archive` is passed directly from `mongodump`, `mongodump`'s internal compression can be utilized or wrapped.
- **Decompression:** Verified during restores using `createGunzip()`. Incomplete gzip footers or corrupted crc32 trailers immediately reject the stream with an `IntegrityError`.

---

### 5.3 Cryptographic Subsystem and Wire Format

Standard single-block AES-256-GCM requires the authentication tag to be validated before trusting decrypted plaintext. For multi-gigabyte files, this would require reading the entire file to disk before decryption could start.

MongoVault solves this with a **Framed Binary Wire Format** (`aes-256-gcm-chunked` in `src/crypto.ts`), encrypting plaintext in isolated 64 KiB frames:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        REGULAR FRAME (FRAME INDEX 0 .. N-2)                 │
├──────────────────────┬──────────────────┬───────────────────────────────────┤
│ Ciphertext Len (4B)  │ isFinal Flag (1B)│ Derived Nonce (12B)               │
│ Big-Endian UInt32    │ 0x00             │ Base IV (8B) || Frame Index (4B)  │
├──────────────────────┴──────────────────┴───────────────────────────────────┤
│ Ciphertext Payload (Ciphertext Len bytes, up to 65,536 bytes)               │
├─────────────────────────────────────────────────────────────────────────────┤
│ GCM Authentication Tag (16 bytes)                                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          TERMINAL FRAME (FRAME INDEX N-1)                   │
├──────────────────────┬──────────────────┬───────────────────────────────────┤
│ Ciphertext Len (4B)  │ isFinal Flag (1B)│ Derived Nonce (12B)               │
│ Big-Endian UInt32    │ 0x01             │ Base IV (8B) || Frame Index (4B)  │
├──────────────────────┴──────────────────┴───────────────────────────────────┤
│ Final Ciphertext Payload (1 .. 65,536 bytes)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ GCM Authentication Tag (16 bytes)                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Frame Nonce Derivation

A 12-byte cryptographically secure random base IV is generated per backup:

```typescript
export function deriveNonce(baseNonce: Buffer, frameIndex: number): Buffer {
  const nonce = Buffer.from(baseNonce);
  nonce.writeUInt32BE(frameIndex >>> 0, 8); // Offset 8..11 stores frame sequence
  return nonce;
}
```

#### Additional Authenticated Data (AAD)

To prevent frame reordering, deletion, splicing, or truncation attacks, each frame cryptographically binds a 5-byte AAD header into the GCM tag:

```typescript
export function computeAad(frameIndex: number, isFinal: boolean): Buffer {
  const aad = Buffer.alloc(5);
  aad.writeUInt32BE(frameIndex >>> 0, 0); // Bytes 0..3: Frame sequence number
  aad.writeUInt8(isFinal ? 1 : 0, 4); // Byte 4: Final frame boolean flag
  return aad;
}
```

#### Truncation Attack Prevention

If a network connection drops or an attacker truncates the file, decryption detects this immediately. If the stream ends without encountering a frame where `isFinal === true`, or if trailing bytes remain, `ChunkedAeadDecryptStream` emits an error:
`Truncated AEAD stream: end of stream reached before final frame`.

#### Envelope Key Wrapping and HKDF Key Separation

1. A unique 32-byte Data Encryption Key (DEK) is generated per archive using `crypto.randomBytes(32)`.
2. MongoVault derives distinct sub-keys from `masterKey` via HKDF (RFC 5869 / NIST SP 800-108) with cryptographic domain separation:
   - $K_{\text{wrap}} = \text{HKDF-SHA256}(\text{masterKey}, \text{"mongovault-v2-dek-wrap"}, 32)$: Dedicated to ephemeral DEK wrapping.
   - $K_{\text{manifest}} = \text{HKDF-SHA256}(\text{masterKey}, \text{"mongovault-v2-manifest-hmac"}, 32)$: Dedicated to manifest HMAC authentication.
3. The DEK is wrapped using $K_{\text{wrap}}$ via AES-256-GCM:
   `wrap(masterKey, dataKey) = Base64( IV[12B] || AuthTag[16B] || Ciphertext[32B] )`.
4. The wrapped DEK and the Key ID (`sha256(masterKey).slice(0, 16)`) are stored in the manifest.
5. Historical backups can be decrypted using older keys configured in `ENCRYPTION_KEY_RING`.

---

### 5.4 Authentication, Integrity and Manifest Verification

Every backup generates an authenticated manifest file (`<archive>.manifest.json`).

```json
{
  "version": 2,
  "type": "logical",
  "engine": "logical",
  "key": "mongodb/_cluster/2026-03-10/20260310T020000Z.archive.gz.enc",
  "database": "*",
  "createdAt": "2026-03-10T02:00:00.000Z",
  "completedAt": "2026-03-10T02:04:12.345Z",
  "durationMs": 252345,
  "size": 1845612349,
  "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "compression": "gzip",
  "encryption": {
    "algorithm": "aes-256-gcm-chunked",
    "keyId": "a1b2c3d4e5f60718",
    "wrappedKey": "b64...",
    "iv": "b64...",
    "chunkSize": 65536,
    "manifestHmac": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
  },
  "oplog": true,
  "oplogWindow": {
    "startTs": "1773108000:1",
    "endTs": "1773108252:14"
  },
  "collections": {
    "production.users": 142050,
    "production.orders": 984120
  }
}
```

- **Manifest HMAC Verification:** Manifests are serialized to canonical JSON (excluding signatures) and signed with HMAC-SHA256 using the configured key ring (`manifest.signature`). Manifest authentication operates **independently of archive payload encryption**—even unencrypted plain gzip backups receive cryptographic HMAC signatures if an encryption key or key ring is configured. When reading the manifest before restore, MongoVault recalculates the HMAC and verifies it using `crypto.timingSafeEqual()`. If an attacker tampers with document counts, collection names, or SHA-256 archive hashes in the manifest, the restore fails immediately.
- **Upload Size & Checksum Integrity:** Upon upload completion, MongoVault calls `storage.head()` to confirm the uploaded size matches the exact bytes emitted by the stream. During restores, `DigestStream` recalculates the SHA-256 hash over the downloaded ciphertext, asserting equality with `manifest.sha256`.

---

### 5.5 Object Storage and Filesystem Backends

MongoVault abstracts storage operations behind the `StorageClient` interface (`src/types/storage.ts`):

```typescript
export interface StorageClient {
  readonly bucket: string;
  upload(key: string, body: unknown, options?: UploadOptions): Promise<void>;
  download(key: string, options?: { signal?: AbortSignal }): Promise<Readable>;
  head(key: string): Promise<StorageHeadResult | null>;
  putJson(key: string, value: unknown): Promise<void>;
  getJson<T = unknown>(key: string): Promise<GetJsonResult<T> | null>;
  list(prefix: string): AsyncGenerator<StorageObject, void, unknown>;
  remove(key: string): Promise<void>;
  removeMany(keys: string[]): Promise<void>;
  acquireLock?(key: string, options: { holder?: string; ttlMs: number }): Promise<string | null>;
  refreshLock?(key: string, token: string, ttlMs: number): Promise<boolean>;
  releaseLock?(key: string, token: string): Promise<void>;
}
```

- **S3 Multipart Uploads (`@aws-sdk/lib-storage`):**
  - Part size is controlled via `S3_PART_SIZE_MB` (default: 32 MiB, min: 5 MiB).
  - Parallel upload concurrency is set via `S3_QUEUE_SIZE` (default: 2 streams).
  - In-flight memory is bounded: `32 MiB * 2 workers = ~64 MiB`.
  - `leavePartsOnError: false` ensures aborted uploads do not leave orphan multipart chunks accumulating storage costs in S3.
- **Filesystem Storage (`FilesystemStorageClient`):**
  - Used when `STORAGE_TYPE=filesystem`. Writes files to `STORAGE_PATH`.
  - All writes are staged to atomic temporary files (`${file}.tmp-<uuid>`) with file permissions `0o600` and committed via atomic filesystem renames (`fs.rename`), preventing partially written files from being read.

#### Storage Layout

```
s3://<bucket>/<prefix>/
  ├── _cluster/                                     # Scope for MONGODB_DATABASE=* or "all"
  │   ├── 2026-03-10/
  │   │   ├── 20260310T020000Z.archive.gz.enc       # Logical backup archive
  │   │   ├── 20260310T020000Z.archive.gz.enc.manifest.json
  │   │   ├── 20260310T040000Z.physical.tar.gz.enc  # Physical hot backup
  │   │   └── 20260310T040000Z.physical.tar.gz.enc.manifest.json
  │   ├── .locks/
  │   │   └── backup.lock.json                      # Distributed S3 lease object
  │   └── _pitr/
  │       ├── state.json                            # High-water mark state file
  │       └── 2026-03-10/
  │           ├── 1773108000_00000001__1773108060_00000014.oplog.bson.gz.enc
  │           └── 1773108000_00000001__1773108060_00000014.oplog.bson.gz.enc.manifest.json
  ├── configsvr/                                    # Created during --sharding backup
  │   └── 2026-03-10/
  └── shards/
      ├── shard01/
      └── shard02/
```

---

### 5.6 Hybrid Concurrency Locking Subsystem

MongoVault prevents race conditions using a two-tier hybrid lock (`src/lock.ts`):

```mermaid
stateDiagram-v2
    [*] --> LocalLock: Request backup execution
    LocalLock --> CheckLocal: Create local lockfile with wx flag

    CheckLocal --> StaleCheck: File already exists (EEXIST)
    CheckLocal --> DistributedLock: Local lock acquired

    StaleCheck --> IsStale: Inspect holding PID and mtime
    IsStale --> StealLock: Stale lock detected
    IsStale --> RejectExecution: Active process running

    StealLock --> AtomicRename: Atomic rename tombstone
    AtomicRename --> LocalLock: Removed tombstone; retry acquisition
    AtomicRename --> RejectExecution: Rename failed (Lost race)

    DistributedLock --> CheckS3: S3 PutObject with IfNoneMatch conditional
    CheckS3 --> HeartbeatStart: 200 OK (Distributed lease acquired)
    CheckS3 --> ReleaseLocalAndExit: 412 PreconditionFailed (Lease held)

    HeartbeatStart --> RunningBackup: Heartbeat refresh every 30s
    RunningBackup --> ReleaseAll: Backup finishes or aborts
    ReleaseLocalAndExit --> [*]
    RejectExecution --> [*]
    ReleaseAll --> [*]
```

1. **Local Tier:** An atomic lockfile (`.backup.lock`) is written to `WORK_DIR` using `O_CREAT | O_EXCL` (`flag: "wx"`).
   - **Active Heartbeat:** An unref'd heartbeat timer periodically touches the lock file (`fs.utimes`), refreshing its modification timestamp every `heartbeatIntervalMs`. This ensures long-running backups (e.g. 10+ minutes) cannot lose their local lock while the process is alive.
   - **Liveness-First Staleness Check:** When another process checks for lock staleness, it reads the owner payload and checks process liveness (`process.kill(pid, 0)`). If the owning PID is alive and actively heartbeating, the lock is never considered stale. If the owning PID is dead, the lock is immediately declared stale. If the PID is alive but the heartbeat has stopped for longer than `staleAfterMs` (e.g. process hung or PID recycled), it is reclaimed.
   - **Atomic Takeover:** Stale locks are claimed via an **atomic rename tombstone** (`fs.rename(file, file.stale.<uuid>)`) to eliminate Time-Of-Check-To-Time-Of-Use (TOCTOU) races.
2. **Distributed Tier:** If storage locking is available, it acquires an S3 lease at `<prefix>/.locks/backup.lock.json`. It performs an atomic conditional write:
   - On new lock: `IfNoneMatch: "*"`.
   - On refresh: Checks expiration timestamp and writes with `IfMatch: etag`.
   - A background timer renews the lease every 30 seconds (`heartbeatIntervalMs`).
3. **Distributed Lease-Loss Handling & Fencing:**
   - If a lease renewal call returns `false` (lease was stolen or acquired by another node) or if renewals fail past `ttlMs`, the lock manager transitions into a **lost-lease** state.
   - It fires an `onLost` callback that immediately aborts the active `AbortController`.
   - The backup pipeline terminates: subprocesses (`mongodump`) receive `SIGTERM`/`SIGKILL`, active stream pipelines are terminated, in-flight multipart uploads are aborted, and manifest finalization is strictly prevented.
4. If distributed acquisition fails initially (HTTP 412 Precondition Failed), the local lock is immediately released and execution halts.

---

### 5.7 Retention Policy Engine (Grandfather-Father-Son)

Retention pruning is evaluated via `planRetention()` (`src/retention.ts`):

- `keepLast`: Retains the $N$ most recent complete backups unconditionally.
- `keepDaily`: Groups backups into calendar day buckets (`YYYY-MM-DD`) within the configured `BACKUP_TIMEZONE` and keeps the newest backup per day for $N$ days.
- `keepWeekly`: Groups backups into ISO week buckets (`YYYY-Www`) within the timezone and keeps 1 per week for $N$ weeks.
- `keepMonthly`: Groups backups into month buckets (`YYYY-MM`) and keeps 1 per month for $N$ months.

#### Safety Gates

1. **PITR Base Anchor Protection:** `deleteBackup()` checks if a backup is the latest base backup anchoring active PITR oplog slices. If so, deletion is blocked unless `--force` is explicitly provided.
2. **Orphan Cleanup:** Incomplete backups (e.g. from crashed processes missing a `.manifest.json`) are automatically purged if older than 1 hour (`ORPHAN_AGE_MS`).
3. **S3 Object Lock Handling:** If AWS S3 returns an Object Lock or Legal Hold error during deletion, MongoVault catches `isObjectLockError()`, logs a clear warning, and continues without crashing.

---

### 5.8 Automated Restore Drills and Verification

Restore drills (`src/drill.ts`) automatically prove that backups can be restored successfully:

- **Strict Isolation Check:** MongoVault ensures `DRILL_MONGODB_URI` does not match `MONGODB_URI`. Drills against production are blocked.
- **Staging Database:** Restores into a temporary staging database named `${database}_drill_${Date.now()}`.
- **Document Count Assertions:** Compares collection document counts against `manifest.collections`.
- **Oplog Variance Allowance:** If the backup was taken with `--oplog`, intermediate writes during the dump can cause document counts to differ from the initial dump count. MongoVault tracks these as expected `oplogVariances` rather than hard failures.
- **Cleanup:** Drops all restored staging databases in a `finally` block.

---

### 5.9 Observability, Health Probes and Prometheus Metrics

The internal HTTP server (`src/metrics.ts`, default port `8080`) provides standard observability:

- `GET /healthz` and `GET /ready`: Returns HTTP `200` with JSON status when healthy; returns HTTP `503` if the scheduler status is `"error"` or the process is shutting down.
- `GET /metrics`: Prometheus text metrics endpoint.

#### Prometheus Metrics Catalog

| Metric Name                                           | Type      | Labels                                 | Description                                           |
| :---------------------------------------------------- | :-------- | :------------------------------------- | :---------------------------------------------------- |
| `mongodb_backup_runs_total`                           | Counter   | `status` (success/failure), `database` | Total count of backup executions                      |
| `mongodb_backup_duration_seconds`                     | Histogram | `database`                             | Backup run duration in seconds (buckets: 1s to 3600s) |
| `mongodb_backup_size_bytes`                           | Gauge     | `database`                             | Byte size of the most recently generated archive      |
| `mongodb_backup_last_success_timestamp_seconds`       | Gauge     | `database`                             | Unix timestamp of the most recent successful backup   |
| `mongodb_backup_drill_runs_total`                     | Counter   | `status` (success/failure)             | Total count of automated restore drill runs           |
| `mongodb_backup_drill_last_success_timestamp_seconds` | Gauge     | None                                   | Unix timestamp of the most recent successful drill    |

---

### 5.10 Notification Dispatcher

The notification subsystem (`src/notify.ts`) dispatches alerts through `CompositeNotifier`:

- **Slack:** Formatted markdown blocks sent to `SLACK_WEBHOOK_URL`.
- **Generic Webhook:** JSON payloads posted to `WEBHOOK_URL` containing event type, status, error, and manifest metadata.
- **SMTP Email:** HTML/text summary sent via Nodemailer to `SMTP_TO`.
- **Dead Man's Switch Heartbeat:** Pings `HEARTBEAT_URL` on success, and posts to `<HEARTBEAT_URL>/fail` on error. Operates independently of `NOTIFY_ON_SUCCESS`.

---

## Part 6 — Point-in-Time Recovery (PITR)

### Why Ordinary Backups Are Insufficient

Full database backups capture state at a single moment in time. If a database is backed up daily at 02:00, and an accidental `db.collection.drop()` or database failure occurs at 16:45:

- Restoring the 02:00 backup loses **14 hours and 45 minutes of production data**.
- Point-in-Time Recovery (PITR) captures every write operation occurring between base backups, enabling recovery to any exact second or operation ordinal.

### MongoDB Oplog Mechanics

In MongoDB replica sets, write operations are logged to the capped collection `local.oplog.rs`. Each operation contains an internal BSON Timestamp `ts`:

- **`t` (32-bit unsigned integer):** Unix epoch seconds.
- **`i` (32-bit unsigned integer):** Increment counter (ordinal) for operations within that second.

```
Timestamp(1773153000, 14)  ==>  Second: 1773153000, Ordinal: 14
```

### Continuous Slicing Architecture

When `PITR_ENABLED=true`, MongoVault runs a continuous archiver (`src/oplog.ts`):

1. **High-Water Mark Tracking:** Reads `<prefix>/_pitr/state.json` to find the last archived timestamp (`lastTs`). If absent, it queries the newest completed base backup manifest.
2. **Fail-Closed Majority Commit Filtering:** MongoVault queries `admin.command({ replSetGetStatus: 1 })` to identify `lastCommittedOpTime` (or `readConcernMajorityOpTime`). If the query fails or optimes cannot be determined, slice capture **fails closed** and throws an error rather than continuing with an open upper boundary, ensuring operations that may roll back during failover are never archived.
3. **Transaction-Boundary Protection:** MongoVault inspects `config.transactions` for active distributed transactions in state `prepared` or `inProgress`. If an active transaction began prior to the majority commit point (`startOpTime < majorityTs`), the slice upper boundary is capped at `startOpTime`, and slice advancement pauses until the transaction finishes. This guarantees that multi-document transactions are never split or torn across slices prior to commitment.
4. **BSON Type Preservation:** Queries `local.oplog.rs` with `{ promoteValues: false }`, ensuring BSON `Long`, `Double`, and `Int32` types are preserved identically without lossy conversion to JavaScript numbers.
5. **Batch Slicing & Upload:** Batches entries up to `PITR_MAX_SLICE_MB` (default: 16 MiB). Compresses with gzip, encrypts with AEAD, computes SHA-256, and uploads to:
   `<prefix>/_pitr/<YYYY-MM-DD>/<startTs.t>_<startTs.i>__<endTs.t>_<endTs.i>.oplog.bson.gz[.enc]`
6. **State Update:** Updates `state.json` with the new `lastTs`.

### Timeline Continuity and Gap Detection

If MongoDB write volume exceeds the size of the capped `local.oplog.rs` collection between slicer runs, the oplog will roll over, creating a **timeline gap**.

- During slice capture, MongoVault checks if `oldestOplogTs > lastArchivedTs`. If true, it flags `gapDetected: true` in the slice manifest and state file, and logs a critical warning.
- During PITR restore planning (`resolvePitrPlan`), MongoVault validates slice contiguity:
  `slice[N].previousEndTs <= slice[N-1].endTs`.
- If any gap is detected, restore execution halts immediately, preventing an inconsistent database state.

---

### PITR Timeline and Execution Diagrams

#### Timeline Representation

```
Day 1 (02:00)                          Day 1 (14:30:15)                Day 2 (02:00)
    │                                         │                              │
    ▼                                         ▼                              ▼
┌──────────────┐   Slice 1       Slice 2      │ Target (1773153015:1)   ┌──────────────┐
│ Base Backup  ├───►┌────────┐───►┌────────┐──┼────────────────────────►│ Base Backup  │
│  (02:00:00)  │    │ 02:00  │    │ 02:01  │  │                         │  (02:00:00)  │
└──────────────┘    │ -02:01 │    │ -02:02 │  │                         └──────────────┘
                    └────────┘    └────────┘  │
                         ▲             ▲      │
                         │             │      ▼
                  Contiguous Incremental Oplog Slices (.oplog.bson.gz)
```

#### PITR Execution Flow Diagram

```mermaid
flowchart TD
    A["Operator requests PITR<br/>(--pitr 2026-03-10T14:30:15Z)"] --> B["Parse Target Timestamp<br/>(1773153015:4294967295)"]
    B --> C["resolvePitrPlan()<br/>Find newest base backup at or prior to target"]
    C --> D{"Base backup<br/>found?"}
    D -- No --> E["Error: No base backup prior to target"]
    D -- Yes --> F["Gather candidate slices<br/>(baseEndTs to targetTs)"]

    F --> G["Verify Oplog Chain Contiguity<br/>(Check previousEndTs & gaps)"]
    G --> H{"Gap detected or<br/>missing slices?"}
    H -- Yes --> I["Error: PITR timeline gap detected<br/>Oplog rolled over"]
    H -- No --> J["Stage 1: Restore Base Backup<br/>(Stream & drop target tables)"]

    J --> K["Stage 2: Sequential Slice Replay"]
    K --> L["Download & decrypt slice to<br/>.pitr-slice-uuid.oplog.bson"]
    L --> M["mongorestore --oplogReplay<br/>--oplogLimit=1773153015:4294967295"]
    M --> N["Delete temporary slice file"]
    N --> O{"More slices<br/>in plan?"}
    O -- Yes --> L
    O -- No --> P["PITR Restore Complete<br/>(Database restored to exact second)"]
```

---

## Part 7 — Security and Data Integrity

### Threat Model and Trust Boundaries

MongoVault is designed to operate securely in zero-trust infrastructure:

- **Untrusted Object Storage:** Cloud storage providers or compromised S3 buckets can inspect or tamper with stored objects. MongoVault treats storage as untrusted: all data is encrypted before upload, and manifests are HMAC-signed.
- **Local Multi-Tenant Host:** Process inspection tools (`ps aux`) on shared hosts could expose credentials. MongoVault isolates secrets in temporary files with POSIX `0o600` permissions.
- **Tamper Evidence:** Modifying even a single bit in a backup archive or manifest causes decryption authentication or HMAC verification to fail before data reaches MongoDB.

---

### Envelope Encryption and Key Derivation

```mermaid
flowchart TD
    subgraph MasterKeySpace["Operator Secret Management"]
        MK["Master Encryption Key\n(32 bytes / 256 bits)\nFrom ENCRYPTION_KEY"]
    end

    subgraph Derivation["HKDF Subkey Derivation (RFC 5869)"]
        MK --> HKDF_Wrap["HKDF-SHA256\ninfo: mongovault-v2-dek-wrap"]
        MK --> HKDF_Hmac["HKDF-SHA256\ninfo: mongovault-v2-manifest-hmac"]
        HKDF_Wrap --> WrapKey["Wrap Key (32B)"]
        HKDF_Hmac --> HmacKey["HMAC Signing Key (32B)"]
    end

    subgraph DEKSpace["Per-Backup Ephemeral Crypto"]
        DEK["Data Encryption Key (DEK)\n(32 bytes cryptographically secure random)"]
        BaseIV["Base Nonce (12 bytes random)"]
        WrapIV["Key Wrap IV (12 bytes random)"]
    end

    subgraph Wrapping["Key Wrapping (AES-256-GCM)"]
        WrapKey --> WrapEngine["AES-256-GCM Wrap"]
        DEK --> WrapEngine
        WrapIV --> WrapEngine
        WrapEngine --> WrappedDEK["wrappedKey (Base64)\nIV (12B) + Tag (16B) + Ciphertext (32B)"]
    end

    subgraph PayloadFraming["Streaming Encryption (aes-256-gcm-chunked)"]
        DEK --> EncryptChunk["Chunk Encryptor"]
        BaseIV --> DeriveNonce["deriveNonce(baseIV, frameIndex)"]
        DeriveNonce --> EncryptChunk
        Plaintext["64 KiB Plaintext Stream"] --> EncryptChunk
        EncryptChunk --> Frame["64 KiB Authenticated Frame\n(Header + Ciphertext + Tag)"]
    end
```

---

### Frame Nonce Management

- Each archive uses a unique, random 12-byte `baseNonce`.
- Frame indices start at `0` and increment sequentially up to `MAX_FRAME_INDEX` (`0xFFFFFFFF`, supporting archives up to **281 Terabytes** per single stream).
- Nonce derivation writes the 32-bit big-endian frame index into bytes 8..11 of the base IV:
  `nonce = [ BaseIV (8 bytes) ] || [ FrameIndex (4 bytes BE) ]`.
- Because each backup generates a fresh DEK and base IV, nonces are guaranteed never to repeat across chunks or archives.

---

### Cryptographic AAD Binding

Every frame binds the following 5-byte AAD into the GCM cipher:
`AAD = UInt32BE(frameIndex) || UInt8(isFinal ? 1 : 0)`.

- If an attacker removes a frame, frame index validation fails (`AEAD frame sequence corrupted or reordered`).
- If an attacker swaps frame order, the AAD verification fails and the GCM tag rejects.
- If an attacker truncates the last frame, `isFinal` check fails.

---

### Manifest Tampering Protection

Manifests contain metadata that directs restore behavior (e.g. `oplogWindow`, collection counts, DEK envelopes, SHA-256 archive digests).

- **Independent Authentication:** Manifest signing is decoupled from archive payload encryption. Whether archive files are encrypted (`aes-256-gcm-chunked`) or standard compressed gzip, the manifest is signed with HMAC-SHA256 whenever a key is configured.
- To prevent tampering, MongoVault produces a canonical JSON string of the manifest and signs it with HMAC-SHA256 using the dedicated HKDF-derived manifest key (`deriveHmacKey(masterKey)`).
- The resulting signature is stored in `signature.hmac` (and `encryption.manifestHmac` when encrypted for backwards compatibility).
- Verification enforces key matching and uses `crypto.timingSafeEqual` to prevent side-channel timing attacks. Restores reject any manifest with an invalid or tampered signature.

---

### S3 Object Lock (WORM Immutability)

To protect against ransomware and malicious credential deletion:

- `S3_OBJECT_LOCK_ENABLED=true` instructs MongoVault to attach Object Lock headers to all S3 uploads (`PutObject` and multipart uploads).
- **Modes:**
  - `GOVERNANCE`: Protects objects from deletion by users without special IAM permissions (`s3:BypassGovernanceRetention`).
  - `COMPLIANCE`: Protects objects unconditionally; cannot be deleted by **any** user (including the AWS root account) until the retention days expire.
- `S3_OBJECT_LOCK_LEGAL_HOLD=true`: Places an indefinite legal hold on uploaded archives.

---

### Secret Handling and Process Table Protection

- MongoDB connection URIs frequently contain username and password credentials.
- MongoVault never passes `--uri="mongodb://user:pass@host"` on the command line.
- Instead, it writes a temporary YAML configuration file (`src/tools.ts`):
  ```yaml
  uri: "mongodb://user:password@host:27017/db"
  ```
- File permissions are explicitly set to `0o600` (read/write only by the process owner).
- An exit listener ensures the file is wiped from disk if the process terminates or crashes.

---

### Security Limitations and Assumptions

1. **Master Key Protection:** MongoVault's security model assumes the master encryption key (`ENCRYPTION_KEY`) is stored securely (e.g., Kubernetes Secrets, AWS Secrets Manager, HashiCorp Vault) and is not accessible to unauthorized users.
2. **Plaintext Memory:** Decrypted database chunks reside briefly in process RAM buffers during streaming. An attacker with root/kernel memory inspection privileges on the host could extract plaintext chunks.
3. **WiredTiger File Permissions:** When using the `physical` engine, MongoVault requires read permissions on files within `PHYSICAL_DB_PATH`.

---

## Part 8 — Failure Scenarios and Recovery Runbook

### Failure Matrix

| Failure Scenario                          | Detection Mechanism                                      | Internal Behavior                                                                               | Data Loss Risk                                   | Operator Remediation                                                                              |
| :---------------------------------------- | :------------------------------------------------------- | :---------------------------------------------------------------------------------------------- | :----------------------------------------------- | :------------------------------------------------------------------------------------------------ |
| **MongoDB drops during backup**           | Subprocess error code / stream `error` event             | `settleAll` aborts upload; calls `storage.remove()` to clean up partial archive.                | None (backup fails cleanly).                     | Check MongoDB logs, verify connectivity, re-run backup.                                           |
| **Network drops during S3 upload**        | `@aws-sdk/lib-storage` error / timeout                   | S3 multipart upload aborts; `leavePartsOnError: false` cleans up uploaded parts.                | None.                                            | Check network/VPC routing, verify S3 endpoint, retry.                                             |
| **Process receives SIGKILL / Host crash** | Uncaught termination                                     | Local lock and S3 lease remain until TTL expires.                                               | None (no partial manifest is written).           | Wait for lease TTL to expire (90s) or manually remove `.locks/backup.lock.json`.                  |
| **Stale lock left behind**                | Stale check in `acquireLock()`                           | Checks PID liveness; if PID is dead and age > TTL, atomically steals lock via rename.           | None.                                            | Automated; no operator action required.                                                           |
| **Concurrent backup started**             | S3 Conditional PUT returns 412 / Local lock EEXIST       | Second process logs warning and exits cleanly without running.                                  | None.                                            | Ensure scheduled cron intervals do not exceed backup duration.                                    |
| **Oplog rollover (PITR gap)**             | `captureOplogSlice` compares `oldestTs > lastArchivedTs` | Sets `gapDetected: true` in manifest; `resolvePitrPlan` blocks restore across gap.              | Changes during gap cannot be recovered via PITR. | Take a new full base backup immediately; increase oplog collection size or reduce flush interval. |
| **Corrupted archive payload**             | GCM Auth Tag failure or SHA-256 mismatch                 | Decryption throws `IntegrityError`; `mongorestore` terminates before corrupt data is committed. | None.                                            | Restore from an earlier verified backup; investigate storage integrity.                           |
| **Tampered manifest file**                | `verifyManifest` HMAC mismatch                           | Throws `IntegrityError: Manifest HMAC verification failed`.                                     | None.                                            | Investigate unauthorized storage access; use `ENCRYPTION_KEY_RING` if key was rotated.            |
| **Wrong Master Encryption Key**           | Key ID mismatch in manifest envelope                     | Throws `Backup was encrypted with key X, but keyring has [Y]`.                                  | None.                                            | Add the original 32-byte key to `ENCRYPTION_KEY_RING`.                                            |
| **Restore fails halfway**                 | `mongorestore` stderr `failed > 0`                       | Throws error; collections restored up to that point remain.                                     | Target database is in partial state.             | Re-run restore with `--drop` to clean up and re-apply from beginning.                             |
| **S3 Object Lock blocks prune**           | AWS S3 returns AccessDenied / WORM error                 | `retention.ts` catches error, logs warning, and skips deletion.                                 | None.                                            | None (expected behavior); objects will be pruned once WORM retention expires.                     |

---

### Recovery Runbooks

#### Runbook 1: Clearing an Orphaned Lock

If a backup daemon process was killed with `SIGKILL` and immediate execution is required before the 90-second TTL expires:

```bash
# 1. Remove local lockfile if present
rm -f /tmp/mongodb-backup/.backup.lock

# 2. If using S3 storage, remove the distributed lock object via AWS CLI
aws s3 rm s3://<S3_BUCKET>/<S3_PREFIX>/_cluster/.locks/backup.lock.json
```

#### Runbook 2: Recovering from a PITR Timeline Gap

If `oplog:slice` reports that an oplog gap was detected:

1. Immediately execute a new full logical backup to establish a fresh base anchor:
   ```bash
   node dist/index.js backup --oplog
   ```
2. Future PITR restores will automatically bind to this new base backup.

#### Runbook 3: Safe Production Restores (Staging & Atomic Cutover)

Running an in-place destructive restore (`restore --drop`) directly against an active production database carries the risk of leaving the database in a partially restored, torn state if the network drops or the process terminates halfway through.

For disaster-safe production restores:

1. **Restore into an Isolated Staging Database:**
   ```bash
   node dist/index.js restore \
     --key latest \
     --uri "mongodb://cluster:27017" \
     --database production_staged \
     --drop
   ```
2. **Execute Post-Restore Data Validation:**
   Query `production_staged` to verify document counts, indexes, and application integrity against the newly restored data.
3. **Perform Controlled Cutover:**
   Execute atomic collection swaps using MongoDB's `renameCollection` command with `dropTarget: true`:
   ```javascript
   db.adminCommand({
     renameCollection: "production_staged.orders",
     to: "production.orders",
     dropTarget: true,
   });
   ```
4. **Clean up:**
   Once verified, drop the temporary `production_staged` database.

---

## Part 9 — Installation and Practical Usage

### Prerequisites

- **Node.js:** `>= 20.0.0`
- **MongoDB Database Tools:** `mongodump` and `mongorestore` must be installed and present in your system `PATH` for logical backups and oplog replay.

To install database tools on Debian/Ubuntu:

```bash
sudo apt-get install -y mongodb-database-tools
```

---

### Installation

#### From Source

```bash
git clone https://github.com/bhatbazik/MongoVault.git
cd MongoVault
npm install
npm run build
```

The compiled binary will be located at `dist/index.js`. You can invoke it directly or link it globally:

```bash
npm link
# 'mongovault' and 'mongodb-backup' commands are now globally available
```

---

### Quickstart with Docker Compose

The repository includes a ready-to-run `docker-compose.yml` spinning up MongoDB 7.0, LocalStack (emulating AWS S3), and the MongoVault daemon:

```bash
docker compose up -d
```

Verify service health and Prometheus metrics:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/metrics
```

---

### CLI Command Reference

All commands support `--json` for machine-readable automation and `-h` / `--help` for syntax help.

#### 1. System Health & Diagnostics

##### `status`

Inspects real-time MongoDB cluster topology, storage connectivity, PITR timeline, and active concurrency locks.

```bash
node dist/index.js status
```

##### `check`

Runs exhaustive preflight verification: checks database tools in PATH, tests MongoDB connectivity and oplog accessibility, probes storage read/write/delete permissions, tests AES-256-GCM crypto roundtrips, and verifies scratch directory permissions.

```bash
node dist/index.js check
```

##### `diagnostic`

Generates a sanitized environment and diagnostic report (system resources, process memory, database build info, storage inventory, and recent manifests). Credentials in URIs are automatically masked.

```bash
node dist/index.js diagnostic --output ./diagnostic-report.json
```

---

#### 2. Backup Commands

##### `backup`

Executes an immediate backup to configured storage.

```bash
# Standard logical backup of configured database
node dist/index.js backup

# Full cluster logical backup with point-in-time oplog capture
node dist/index.js backup --oplog

# Backup a specific database with custom read preference
node dist/index.js backup --database analytics --read-preference secondary

# Physical WiredTiger hot backup
node dist/index.js backup --engine physical --db-path /var/lib/mongodb

# Cloud volume snapshot under fsyncLock
node dist/index.js backup --engine snapshot --volume-id vol-0123456789abcdef0

# Synchronized multi-shard cluster backup via mongos
node dist/index.js backup --sharding
```

---

#### 3. Catalog & Inspection Commands

##### `list`

Lists all available completed and unverified backups in storage.

```bash
node dist/index.js list
```

##### `describe`

Fetches and inspects backup manifest metadata and per-collection document counts without downloading the archive.

```bash
node dist/index.js describe --key latest
node dist/index.js describe --key "mongodb/_cluster/2026-03-10/20260310T020000Z.archive.gz.enc"
```

##### `verify`

Streams the remote archive through decompression and SHA-256 validation to verify byte-for-byte integrity and chunk encryption tags without writing data to disk.

```bash
node dist/index.js verify --key latest
```

---

#### 4. Restore Commands

##### `restore`

Restores a backup archive or executes point-in-time recovery into MongoDB.

```bash
# Restore latest backup (dropping existing collections first)
node dist/index.js restore --drop

# Selective restore by namespace patterns
node dist/index.js restore --ns-include "production.users,production.orders*" --drop
node dist/index.js restore --ns-exclude "production.audit_log" --drop

# Restore to a different target MongoDB instance or database name
node dist/index.js restore --key latest --uri "mongodb://staging:27017" --database staging_db --drop

# Point-in-Time Restore using ISO timestamp
node dist/index.js restore --pitr "2026-03-10T14:30:00Z" --drop

# Point-in-Time Restore using exact oplog timestamp (second:ordinal)
node dist/index.js restore --pitr "1773153000:1" --drop

# Restore physical WiredTiger backup into target dbPath (mongod must be stopped)
node dist/index.js restore --engine physical --key latest --db-path /var/lib/mongodb
```

---

#### 5. Point-in-Time Recovery (PITR) Commands

```bash
# Capture and upload a single incremental oplog slice immediately
node dist/index.js oplog:slice

# Run continuous foreground oplog archiver daemon
node dist/index.js oplog:tail

# List archived PITR oplog slices in storage
node dist/index.js oplog:list

# Prune archived PITR oplog slices older than a target timestamp
node dist/index.js oplog:prune --older-than "2026-03-01T00:00:00Z"
```

---

#### 6. Retention & Pruning Commands

##### `prune`

Evaluates configured Grandfather-Father-Son retention rules and removes expired backups and orphan uploads.

```bash
# Dry run: view what would be pruned without modifying storage
node dist/index.js prune --dry-run

# Apply retention policy
node dist/index.js prune
```

##### `delete`

Safely deletes a specific backup. Blocks deletion if the backup anchors active PITR oplog slices.

```bash
# Safe delete
node dist/index.js delete --key <key>

# Force delete (bypasses PITR anchor protection)
node dist/index.js delete --key <key> --force
```

---

#### 7. Verification Drills & Cryptographic Utilities

##### `drill`

Executes an immediate automated restore drill against `DRILL_MONGODB_URI`, verifies document counts against the manifest, and drops the staging database.

```bash
node dist/index.js drill
```

##### `key:generate`

Generates a new cryptographically secure 32-byte (256-bit) base64-encoded encryption key.

```bash
npm run key:generate
# Output: e.g. 4B3qK9lZ1vX7mN2pQ5rT8wY0uI3oA6sD9fG2hJ5kL8M=
```

---

### Practical Operational Recipes

#### Recipe 1: Setting up Continuous PITR Archiving

In `.env`:

```env
MONGODB_URI=mongodb://user:pass@rs01.db:27017,rs02.db:27017/?replicaSet=rs0
MONGODB_DATABASE=*
PITR_ENABLED=true
PITR_FLUSH_INTERVAL_SECONDS=60
PITR_MAX_SLICE_MB=16
```

Start the daemon:

```bash
npm start
```

MongoVault will take daily base backups according to `BACKUP_CRON` and continuously archive oplog slices every 60 seconds.

#### Recipe 2: Restoring Staging from Production with Sanitization

To restore production data into a staging environment without audit logs:

```bash
node dist/index.js restore \
  --key latest \
  --uri "mongodb://staging.db:27017" \
  --database staging \
  --ns-exclude "production.audit_logs" \
  --drop
```

---

## Part 10 — Configuration Reference

Configuration settings are validated at startup via Zod (`src/config.ts`).

| Variable                               |  Type   |       Default        | Required? | Description & Implications                                                                    |
| :------------------------------------- | :-----: | :------------------: | :-------: | :-------------------------------------------------------------------------------------------- |
| **MongoDB Settings**                   |         |                      |           |                                                                                               |
| `MONGODB_URI`                          | String  |        _None_        |  **Yes**  | Standard MongoDB connection URI. Supports standalone, replica sets, and `mongos`.             |
| `MONGODB_DATABASE`                     | String  |        `"*"`         |    No     | Target database. Set to `*` or `all` for full cluster backup (required for oplog/PITR).       |
| `MONGODB_EXCLUDE_COLLECTIONS`          | String  |         `""`         |    No     | Comma-separated collections to exclude. Cannot be used with full cluster backup (`*`).        |
| `MONGODB_OPLOG`                        | Boolean |       `false`        |    No     | Capture point-in-time oplog during logical dump. Requires `MONGODB_DATABASE=*`.               |
| `MONGODB_PARALLEL_COLLECTIONS`         | Number  |         `2`          |    No     | Concurrently dumped collections (`1`–`32`). Lower to `1`–`2` for memory-constrained hosts.    |
| `MONGODB_READ_PREFERENCE`              | String  | `secondaryPreferred` |    No     | Read preference: `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred`, `nearest`. |
| **Backup Engines & Sharding**          |         |                      |           |                                                                                               |
| `BACKUP_ENGINE`                        | String  |      `logical`       |    No     | Default engine: `logical`, `physical`, or `snapshot`.                                         |
| `PHYSICAL_DB_PATH`                     |  Path   |        _None_        |   Cond.   | Local WiredTiger filesystem path. Required when `BACKUP_ENGINE=physical`.                     |
| `SNAPSHOT_PROVIDER`                    | String  |      `aws-ebs`       |    No     | Cloud snapshot provider: `aws-ebs` or `custom`.                                               |
| `SNAPSHOT_VOLUME_ID`                   | String  |        _None_        |   Cond.   | Cloud block-volume ID. Required when `BACKUP_ENGINE=snapshot`.                                |
| `SNAPSHOT_REGION`                      | String  |        _None_        |    No     | AWS region for EBS volume snapshot calls.                                                     |
| `SNAPSHOT_COMMAND_HOOK`                | String  |        _None_        |    No     | External shell executable invoked for custom snapshot creation and restoration.               |
| `SHARDING_ENABLED`                     | Boolean |       `false`        |    No     | Coordinates distributed backup across `mongos`, `configsvr`, and shard replica sets.          |
| `SHARDING_CONFIGSVR_URI`               | String  |        _None_        |    No     | Optional dedicated Config Server connection URI.                                              |
| **Point-in-Time Recovery (PITR)**      |         |                      |           |                                                                                               |
| `PITR_ENABLED`                         | Boolean |       `false`        |    No     | Runs continuous oplog slicer daemon. Requires replica set and `MONGODB_DATABASE=*`.           |
| `PITR_FLUSH_INTERVAL_SECONDS`          | Number  |         `60`         |    No     | Interval between oplog slice flushes (`5`–`3600` seconds). Governs recovery point objective.  |
| `PITR_MAX_SLICE_MB`                    | Number  |         `16`         |    No     | Maximum uncompressed BSON size per oplog slice (`1`–`256` MiB).                               |
| **Storage Destination**                |         |                      |           |                                                                                               |
| `STORAGE_TYPE`                         | String  |         `s3`         |    No     | Storage backend: `s3` or `filesystem`.                                                        |
| `STORAGE_PATH`                         |  Path   |        _None_        |   Cond.   | Directory path when `STORAGE_TYPE=filesystem`. Required if filesystem storage is used.        |
| `S3_BUCKET`                            | String  |  `mongodb-backups`   |   Cond.   | S3 bucket name. Required when `STORAGE_TYPE=s3`.                                              |
| `S3_ACCESS_KEY_ID`                     | String  |         `""`         |   Cond.   | AWS Access Key ID. Required when `STORAGE_TYPE=s3`.                                           |
| `S3_SECRET_ACCESS_KEY`                 | String  |         `""`         |   Cond.   | AWS Secret Access Key. Required when `STORAGE_TYPE=s3`.                                       |
| `S3_REGION`                            | String  |     `ap-south-1`     |    No     | AWS region. Falls back to `process.env.AWS_REGION` or `ap-south-1`.                           |
| `S3_ENDPOINT`                          |   URL   |        _None_        |    No     | Custom S3 endpoint URL (e.g. LocalStack, MinIO, AWS VPC Endpoint).                            |
| `S3_FORCE_PATH_STYLE`                  | Boolean |       `false`        |    No     | Forces path-style addressing (`http://endpoint/bucket`). Required for LocalStack/MinIO.       |
| `S3_PREFIX`                            | String  |     `"mongodb"`      |    No     | Key prefix inside the bucket.                                                                 |
| `S3_PART_SIZE_MB`                      | Number  |         `32`         |    No     | S3 multipart upload part size (`5`–`512` MiB). Controls buffer memory and max archive size.   |
| `S3_QUEUE_SIZE`                        | Number  |         `2`          |    No     | Concurrent multipart upload streams (`1`–`16`).                                               |
| **S3 Object Lock (WORM Immutability)** |         |                      |           |                                                                                               |
| `S3_OBJECT_LOCK_ENABLED`               | Boolean |       `false`        |    No     | Applies S3 Object Lock retention headers to uploaded archives.                                |
| `S3_OBJECT_LOCK_MODE`                  | String  |     `GOVERNANCE`     |    No     | Lock mode: `GOVERNANCE` (bypassable with IAM permissions) or `COMPLIANCE` (unconditional).    |
| `S3_OBJECT_LOCK_RETENTION_DAYS`        | Number  |         `30`         |    No     | Number of days to enforce WORM retention (`1`–`36500`).                                       |
| `S3_OBJECT_LOCK_LEGAL_HOLD`            | Boolean |       `false`        |    No     | Applies S3 Legal Hold status (`ON`) to uploaded objects.                                      |
| **Client-Side Encryption**             |         |                      |           |                                                                                               |
| `ENCRYPTION_KEY`                       | String  |        _None_        |    No     | 32-byte master key (Base64 or Hex). Enables AES-256-GCM chunked envelope encryption.          |
| `ENCRYPTION_KEY_RING`                  | String  |        _None_        |    No     | Comma-separated list of historical 32-byte keys for decrypting older rotated backups.         |
| **Schedule & Execution**               |         |                      |           |                                                                                               |
| `BACKUP_CRON`                          | String  |    `"0 2 * * *"`     |    No     | Standard 5-field cron expression for automated backups (default: 02:00 daily).                |
| `BACKUP_TIMEZONE`                      | String  |       `"UTC"`        |    No     | IANA timezone identifier for cron schedules and retention date bucketing.                     |
| `BACKUP_TIMEOUT_MINUTES`               | Number  |        `180`         |    No     | Maximum execution duration before timing out and aborting backup (minutes).                   |
| `BACKUP_ON_START`                      | Boolean |       `false`        |    No     | Triggers an immediate backup run upon daemon startup.                                         |
| `WORK_DIR`                             |  Path   |    `os.tmpdir()`     |    No     | Local directory for PID lockfiles and temporary PITR slice replay staging.                    |
| **Retention Policy (GFS)**             |         |                      |           |                                                                                               |
| `RETENTION_KEEP_LAST`                  | Number  |        _None_        |    No     | Keep the $N$ most recent complete backups unconditionally.                                    |
| `RETENTION_KEEP_DAILY`                 | Number  |        _None_        |    No     | Keep 1 backup per day for $N$ days.                                                           |
| `RETENTION_KEEP_WEEKLY`                | Number  |        _None_        |    No     | Keep 1 backup per ISO week for $N$ weeks.                                                     |
| `RETENTION_KEEP_MONTHLY`               | Number  |        _None_        |    No     | Keep 1 backup per calendar month for $N$ months.                                              |
| **Automated Restore Drills**           |         |                      |           |                                                                                               |
| `DRILL_MONGODB_URI`                    | String  |        _None_        |    No     | Isolated staging MongoDB connection URI. Must NOT match `MONGODB_URI`.                        |
| `DRILL_CRON`                           | String  |        _None_        |   Cond.   | Cron expression for restore drills. Requires `DRILL_MONGODB_URI`.                             |
| **Notifications & Observability**      |         |                      |           |                                                                                               |
| `HEARTBEAT_URL`                        |   URL   |        _None_        |    No     | Dead-man's switch ping URL (e.g. Healthchecks.io). Appends `/fail` on failure.                |
| `DRILL_HEARTBEAT_URL`                  |   URL   |        _None_        |    No     | Dedicated dead-man's switch ping URL for restore drill executions.                            |
| `NOTIFY_ON_SUCCESS`                    | Boolean |        `true`        |    No     | Dispatch notification messages on successful backup runs as well as failures.                 |
| `SLACK_WEBHOOK_URL`                    |   URL   |        _None_        |    No     | Slack incoming webhook URL for alerts.                                                        |
| `WEBHOOK_URL`                          |   URL   |        _None_        |    No     | Generic HTTP POST endpoint receiving JSON alert payloads.                                     |
| `SMTP_HOST`                            | String  |        _None_        |    No     | SMTP mail server hostname. Requires `SMTP_FROM` and `SMTP_TO`.                                |
| `SMTP_PORT`                            | Number  |        `587`         |    No     | SMTP server port.                                                                             |
| `SMTP_SECURE`                          | Boolean |       `false`        |    No     | Enable TLS/SSL connection for SMTP.                                                           |
| `SMTP_USER` / `SMTP_PASSWORD`          | String  |        _None_        |    No     | SMTP authentication credentials.                                                              |
| `SMTP_FROM`                            | String  |        _None_        |   Cond.   | Email sender address. Required if `SMTP_HOST` is set.                                         |
| `SMTP_TO`                              | String  |        _None_        |   Cond.   | Comma-separated recipient email addresses. Required if `SMTP_HOST` is set.                    |
| `HTTP_PORT`                            | Number  |        `8080`        |    No     | Port for `/healthz`, `/ready`, and `/metrics`. Set to `0` to disable HTTP server.             |
| `LOG_LEVEL`                            | String  |       `"info"`       |    No     | Logger level: `fatal`, `error`, `warn`, `info`, `debug`, `trace`.                             |

---

## Part 11 — Operational Considerations

### Memory Footprint and S3 Multipart Sizing

MongoVault does not buffer dumps to disk. The streaming memory footprint is determined by:
$$\text{Memory Footprint} \approx (\text{Part Size} \times \text{Queue Size}) + \text{Stream Buffers}$$

With defaults (`S3_PART_SIZE_MB=32`, `S3_QUEUE_SIZE=2`):
$$\text{Memory} \approx (32\text{ MiB} \times 2) + 1\text{ MiB} \approx 65\text{--}75\text{ MiB}$$

#### S3 Part Sizing vs Maximum Archive Size

AWS S3 enforces a hard limit of **10,000 parts** per multipart upload:

| `S3_PART_SIZE_MB`  | Active Upload Memory Buffer | Maximum Single Archive Size Limit | Recommended Database Size |
| :----------------: | :-------------------------: | :-------------------------------: | :-----------------------: |
|    `5` (S3 Min)    |           ~15 MiB           |               50 GB               |          < 40 GB          |
| **`32` (Default)** |       **~65–75 MiB**        |            **320 GB**             |     **Up to 250 GB**      |
|        `64`        |        ~130–150 MiB         |              640 GB               |       Up to 500 GB        |
|       `128`        |        ~260–300 MiB         |              1.28 TB              |        Up to 1 TB         |
|       `512`        |        ~1.0–1.2 GiB         |     5.0 TB (S3 Object Limit)      |      Multi-Terabyte       |

---

### Physical Backup Host Locality

When using `--engine physical`:

1. MongoVault must run on the same node as `mongod` or share access to the WiredTiger data directory (`PHYSICAL_DB_PATH`).
2. MongoDB user permissions: MongoVault requires read permissions on `.wt` files.
3. **Restoration Safety:** The target `mongod` instance must be shut down before running physical restore. MongoVault extracts raw files directly into `dbPath` and automatically deletes stale `mongod.lock` and `WiredTiger.lock` files.

---

### Sharded Cluster Operational Limits

When using `--sharding`:

1. **Balancer Drain Window:** MongoVault stops the cluster balancer and waits up to 30 seconds for in-flight chunk migrations to complete. If a migration round cannot conclude within 30 seconds, it aborts the backup with an `ActiveChunkMigrationError` to guarantee strict cross-shard consistency and eliminate duplicate or missing documents.
2. **Network Direct Access:** MongoVault derives direct shard replica set URIs using the seed host addresses stored in `config.shards`. The host running MongoVault must have network route access to every individual shard host, not merely the `mongos` router.

---

### Recovery Objectives (RPO and RTO)

- **Recovery Point Objective (RPO):**
  - Logical / Physical Backups: `RPO = Backup Frequency` (e.g., 24 hours with daily cron).
  - Continuous PITR: `RPO ≤ PITR_FLUSH_INTERVAL_SECONDS` (default: 60 seconds; reducible to 5 seconds).
- **Recovery Time Objective (RTO):**
  - Logical Engine: Moderate (governed by network download speed and index rebuild time).
  - Physical Engine: Minimal (governed purely by network download speed and disk write throughput).

---

## Part 12 — Project Structure and Development

### Source Code Organization

```
src/
├── commands/             # CLI command implementations (Command pattern)
│   ├── backup.ts         # Backup command handler
│   ├── restore.ts        # Restore command handler
│   ├── status.ts         # Status inspection handler
│   ├── check.ts          # Preflight checks handler
│   ├── describe.ts       # Manifest metadata viewer
│   ├── verify.ts         # Archive stream verification handler
│   ├── list.ts           # Backup catalog lister
│   ├── delete.ts         # Safe deletion handler
│   ├── prune.ts          # Retention pruner handler
│   ├── drill.ts          # Restore drill handler
│   ├── oplog.ts          # PITR slice/tail/prune handlers
│   ├── diagnostic.ts     # System diagnostic bundle exporter
│   └── daemon.ts         # Long-running scheduler daemon handler
├── types/                # TypeScript interface definitions
│   ├── config.ts         # Zod schemas and configuration contracts
│   ├── catalog.ts        # Manifest and catalog definitions
│   ├── storage.ts        # StorageClient interface and options
│   ├── crypto.ts         # Encryption envelope and stream types
│   ├── oplog.ts          # PITR oplog structures and timestamp types
│   └── ...
├── backup.ts             # Logical backup pipeline
├── restore.ts            # Restore pipeline and verification
├── physical.ts           # WiredTiger $backupCursor implementation
├── snapshot.ts           # Cloud volume snapshot driver implementation
├── sharding.ts           # Coordinated multi-shard cluster engine
├── oplog.ts              # PITR archiver, tailer, and replay logic
├── crypto.ts             # AEAD 64 KiB chunked cipher and key wrapping
├── storage.ts            # S3 client and filesystem client
├── lock.ts               # Local PID lockfile and distributed S3 lease
├── retention.ts          # Grandfather-Father-Son retention planner
├── drill.ts              # Automated restore drill runner
├── metrics.ts            # Prometheus metrics and HTTP server
├── notify.ts             # Slack, Webhook, and SMTP notification channels
├── tools.ts              # Subprocess spawner (mongodump/mongorestore)
└── tar.ts                # POSIX UStar tar streaming generator & extractor
```

---

### Development Workflow and Scripts

```bash
# Install dependencies
npm install

# Run TypeScript compilation
npm run build

# Run type checking without code generation
npm run typecheck

# Execute Vitest test suite
npm test

# Run tests in watch mode
npm run test:watch

# Format code with Prettier
npm run format

# Run ESLint linter
npm run lint

# Comprehensive pre-commit check (typecheck + lint + format + build)
npm run check
```

---

### Testing and Verification

MongoVault maintains an extensive automated test suite covering all subsystems:

- `tests/crypto.test.js`: Validates 64 KiB AEAD chunk framing, key wrapping roundtrips, tampered tag rejection, and truncation detection.
- `tests/pipeline.test.js`: Verifies end-to-end zero-disk streaming pipelines and backpressure propagation.
- `tests/pitr.test.js`: Verifies oplog timestamp parsing, slice chunking, and multi-slice continuity validation.
- `tests/lock.test.js`: Tests local PID creation, stale lock detection, atomic rename takeovers, and S3 conditional lease acquisition.
- `tests/retention.test.js`: Tests timezone-aware Grandfather-Father-Son bucketing, orphan pruning, and PITR anchor deletion protection.
- `tests/physical.test.js`: Tests WiredTiger cursor retries on checkpoint conflicts (code `50915`), POSIX UStar tar streaming, and `fsyncLock` fallbacks.
- `tests/sharding.test.js`: Verifies topology inspection, balancer stopping/draining, and shard URI building.

To run the full test suite:

```bash
npm test
```

---

### Contributing

1. Fork the repository and create a feature branch (`git checkout -b feature/my-feature`).
2. Implement your changes, preserving existing documentation comments and error handling semantics.
3. Ensure all tests and type checks pass:
   ```bash
   npm run check
   npm test
   ```
4. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/) standards.
5. Submit a Pull Request with a clear technical description of the implementation.

---

## License

Released under the [MIT License](LICENSE).
