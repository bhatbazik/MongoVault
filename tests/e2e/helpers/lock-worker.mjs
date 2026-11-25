import { parseArgs } from "node:util";
import { acquireHybridLock } from "../../../dist/lock.js";
import { createStorage } from "../../../dist/storage.js";

function emit(payload) {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
}

function parseConfig() {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      config: { type: "string" },
      mode: { type: "string" },
      "lock-file": { type: "string" },
      "storage-type": { type: "string" },
      "storage-path": { type: "string" },
      "s3-endpoint": { type: "string" },
      "s3-bucket": { type: "string" },
      "s3-region": { type: "string" },
      "s3-access-key": { type: "string" },
      "s3-secret-key": { type: "string" },
      "s3-force-path-style": { type: "boolean" },
      "lock-key": { type: "string" },
      "stale-ms": { type: "string" },
      "ttl-ms": { type: "string" },
      "heartbeat-ms": { type: "string" },
      "hold-ms": { type: "string" },
      holder: { type: "string" },
      "release-immediately": { type: "string" },
    },
  });

  let cfg = {};
  if (values.config) {
    cfg = JSON.parse(values.config);
  } else if (positionals[0]) {
    cfg = JSON.parse(positionals[0]);
  } else if (process.env.LOCK_WORKER_CONFIG) {
    cfg = JSON.parse(process.env.LOCK_WORKER_CONFIG);
  }

  const s3 = cfg.s3 ?? {};
  const storageType =
    values["storage-type"] ??
    cfg.storageType ??
    (s3.endpoint || values["s3-endpoint"] ? "s3" : undefined);

  return {
    mode: values.mode ?? cfg.mode ?? "hold",
    lockFile: values["lock-file"] ?? cfg.lockFile,
    lockKey: values["lock-key"] ?? cfg.lockKey,
    storageType,
    storagePath: values["storage-path"] ?? cfg.storagePath,
    s3:
      storageType === "s3" || s3.endpoint || values["s3-endpoint"]
        ? {
            endpoint: values["s3-endpoint"] ?? s3.endpoint,
            bucket: values["s3-bucket"] ?? s3.bucket ?? "test-bucket",
            region: values["s3-region"] ?? s3.region ?? "us-east-1",
            accessKeyId: values["s3-access-key"] ?? s3.accessKeyId ?? "test-key",
            secretAccessKey: values["s3-secret-key"] ?? s3.secretAccessKey ?? "test-secret",
            forcePathStyle: values["s3-force-path-style"] ?? s3.forcePathStyle ?? true,
          }
        : null,
    staleAfterMs: Number(values["stale-ms"] ?? cfg.staleAfterMs ?? 30_000),
    distributedTtlMs: Number(values["ttl-ms"] ?? cfg.distributedTtlMs ?? 30_000),
    heartbeatIntervalMs: Number(values["heartbeat-ms"] ?? cfg.heartbeatIntervalMs ?? 500),
    holdMs: values["hold-ms"] !== undefined ? Number(values["hold-ms"]) : (cfg.holdMs ?? 0),
    holder: values.holder ?? cfg.holder ?? `worker:${process.pid}`,
    releaseImmediately:
      values["release-immediately"] !== undefined
        ? values["release-immediately"] !== "false"
        : (cfg.releaseImmediately ?? true),
  };
}

async function main() {
  const cfg = parseConfig();
  if (!cfg.lockFile) {
    throw new Error("lockFile is required");
  }

  let storage;
  if (cfg.storageType === "filesystem" && cfg.storagePath) {
    storage = createStorage({ type: "filesystem", path: cfg.storagePath });
  } else if (cfg.s3) {
    storage = createStorage({
      type: "s3",
      endpoint: cfg.s3.endpoint,
      bucket: cfg.s3.bucket,
      region: cfg.s3.region,
      accessKeyId: cfg.s3.accessKeyId,
      secretAccessKey: cfg.s3.secretAccessKey,
      forcePathStyle: cfg.s3.forcePathStyle,
    });
  }

  let shuttingDown = false;
  let releaseFn = null;
  let keepAliveTimer = null;
  let holdTimer = null;

  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    if (keepAliveTimer) clearInterval(keepAliveTimer);
    if (holdTimer) clearTimeout(holdTimer);
    if (releaseFn) {
      await releaseFn();
      emit({ event: "released", pid: process.pid });
    }
    process.exit(0);
  };

  process.on("SIGTERM", () => void shutdown());
  process.on("SIGINT", () => void shutdown());

  releaseFn = await acquireHybridLock({
    file: cfg.lockFile,
    storage,
    lockKey: cfg.lockKey,
    staleAfterMs: cfg.staleAfterMs,
    distributedTtlMs: cfg.distributedTtlMs,
    heartbeatIntervalMs: cfg.heartbeatIntervalMs,
    holder: cfg.holder,
    onLost: (err) => {
      if (keepAliveTimer) clearInterval(keepAliveTimer);
      if (holdTimer) clearTimeout(holdTimer);
      emit({ event: "lost", pid: process.pid, message: err.message });
      process.exit(3);
    },
  });

  if (!releaseFn) {
    emit({ event: "busy", acquired: false, pid: process.pid });
    process.exit(2);
  }

  emit({ event: "acquired", acquired: true, pid: process.pid });

  if (cfg.mode === "acquire-once") {
    if (cfg.holdMs > 0) {
      await new Promise((r) => setTimeout(r, cfg.holdMs));
    }
    if (cfg.releaseImmediately !== false) {
      await releaseFn();
      emit({ event: "released", pid: process.pid });
    }
    process.exit(0);
  }

  keepAliveTimer = setInterval(() => {}, 1000);
  if (cfg.holdMs > 0) {
    holdTimer = setTimeout(() => {
      void shutdown();
    }, cfg.holdMs);
  }
}

main().catch((err) => {
  process.stderr.write(
    `${JSON.stringify({ event: "error", pid: process.pid, message: err instanceof Error ? err.message : String(err) })}\n`,
  );
  process.exit(1);
});
