import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import { MongoClient } from "mongodb";
import { formatOplogTimestamp, fromBsonTimestamp } from "../../src/oplog.js";

export async function findFreePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close((err) => (err ? reject(err) : resolve(port)));
    });
    srv.on("error", reject);
  });
}

async function waitForPing(uri, proc, logPath, timeoutMs = 45_000) {
  const deadline = Date.now() + timeoutMs;
  let lastErr = null;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null || proc.signalCode !== null) {
      let tail = "";
      try {
        tail = (await fs.readFile(logPath, "utf8")).trim().split("\n").slice(-20).join("\n");
      } catch {}
      throw new Error(`Process exited early (code=${proc.exitCode}):\n${tail}`);
    }
    const client = new MongoClient(uri, {
      directConnection: true,
      serverSelectionTimeoutMS: 1500,
    });
    try {
      await client.connect();
      await client.db("admin").command({ ping: 1 });
      await client.close();
      return;
    } catch (err) {
      lastErr = err;
      await client.close().catch(() => {});
      await new Promise((r) => setTimeout(r, 250));
    }
  }
  throw new Error(
    `Timed out waiting for MongoDB at ${uri}: ${lastErr instanceof Error ? lastErr.message : String(lastErr)}`,
  );
}

async function waitForPrimary(uri, timeoutMs = 45_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const client = new MongoClient(uri, {
      directConnection: true,
      serverSelectionTimeoutMS: 2000,
    });
    try {
      await client.connect();
      const hello = await client.db("admin").command({ hello: 1 });
      await client.close();
      if (hello.isWritablePrimary) return;
    } catch {
      await client.close().catch(() => {});
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`Replica set at ${uri} did not elect primary within ${timeoutMs}ms`);
}

async function stopProcess(proc, uri) {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  const exitPromise = new Promise((resolve) => {
    proc.once("close", resolve);
    proc.once("error", resolve);
  });
  if (uri) {
    const client = new MongoClient(uri, {
      directConnection: true,
      serverSelectionTimeoutMS: 2000,
    });
    try {
      await client.connect();
      await client
        .db("admin")
        .command({ shutdown: 1, force: true })
        .catch(() => {});
    } catch {
      // Ignore
    } finally {
      await client.close().catch(() => {});
    }
  }
  if (proc.exitCode === null && proc.signalCode === null) {
    proc.kill("SIGTERM");
  }
  await Promise.race([exitPromise, new Promise((r) => setTimeout(r, 10_000))]);
  if (proc.exitCode === null && proc.signalCode === null) {
    proc.kill("SIGKILL");
    await exitPromise;
  }
}

export async function startReplicaSet({ rootDir, name, role, dbPath: customDbPath }) {
  const port = await findFreePort();
  const dbPath = customDbPath ?? path.join(rootDir, `db-${name}`);
  const logPath = path.join(rootDir, `${name}.log`);
  await fs.mkdir(dbPath, { recursive: true });

  const args = [
    `--replSet=${name}`,
    `--port=${port}`,
    `--dbpath=${dbPath}`,
    "--bind_ip=127.0.0.1",
    "--oplogSize=64",
    `--logpath=${logPath}`,
  ];
  if (role === "configsvr") args.push("--configsvr");
  if (role === "shardsvr") args.push("--shardsvr");

  const proc = spawn("mongod", args, { stdio: "ignore" });
  const directUri = `mongodb://127.0.0.1:${port}/?directConnection=true`;
  const rsUri = `mongodb://127.0.0.1:${port}/?replicaSet=${name}`;

  await waitForPing(directUri, proc, logPath);

  const client = new MongoClient(directUri, { directConnection: true });
  try {
    await client.connect();
    await client.db("admin").command({
      replSetInitiate: {
        _id: name,
        ...(role === "configsvr" ? { configsvr: true } : {}),
        members: [{ _id: 0, host: `127.0.0.1:${port}` }],
      },
    });
  } finally {
    await client.close().catch(() => {});
  }

  await waitForPrimary(directUri);

  return {
    name,
    port,
    dbPath,
    logPath,
    host: `${name}/127.0.0.1:${port}`,
    directUri,
    uri: rsUri,
    stop: () => stopProcess(proc, directUri),
  };
}

export async function startStandalone({ rootDir, dbPath, name = "standalone" }) {
  const port = await findFreePort();
  const logPath = path.join(rootDir, `${name}-${port}.log`);
  const args = [
    `--port=${port}`,
    `--dbpath=${dbPath}`,
    "--bind_ip=127.0.0.1",
    `--logpath=${logPath}`,
  ];
  const proc = spawn("mongod", args, { stdio: "ignore" });
  const uri = `mongodb://127.0.0.1:${port}/?directConnection=true`;
  await waitForPing(uri, proc, logPath);
  return {
    port,
    uri,
    dbPath,
    stop: () => stopProcess(proc, uri),
  };
}

export async function startShardedCluster(rootDir) {
  const [cfgRs, shard01, shard02] = await Promise.all([
    startReplicaSet({ rootDir, name: "cfgRs", role: "configsvr" }),
    startReplicaSet({ rootDir, name: "shard01", role: "shardsvr" }),
    startReplicaSet({ rootDir, name: "shard02", role: "shardsvr" }),
  ]);

  const mongosPort = await findFreePort();
  const mongosLog = path.join(rootDir, "mongos.log");
  const mongosProc = spawn(
    "mongos",
    [
      `--configdb=${cfgRs.host}`,
      `--port=${mongosPort}`,
      "--bind_ip=127.0.0.1",
      `--logpath=${mongosLog}`,
    ],
    { stdio: "ignore" },
  );

  const mongosUri = `mongodb://127.0.0.1:${mongosPort}/`;
  await waitForPing(mongosUri, mongosProc, mongosLog);

  const client = new MongoClient(mongosUri);
  try {
    await client.connect();
    await client.db("admin").command({ addShard: shard01.host });
    await client.db("admin").command({ addShard: shard02.host });
  } finally {
    await client.close().catch(() => {});
  }

  return {
    mongosUri,
    mongosPort,
    cfgRs,
    shard01,
    shard02,
    async stop() {
      await stopProcess(mongosProc, mongosUri);
      await Promise.all([shard01.stop(), shard02.stop(), cfgRs.stop()]);
    },
  };
}

export async function getLatestOplogTs(uri) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const doc = await client
      .db("local")
      .collection("oplog.rs")
      .find({}, { projection: { ts: 1 } })
      .sort({ $natural: -1 })
      .limit(1)
      .next();
    return fromBsonTimestamp(doc?.ts);
  } finally {
    await client.close().catch(() => {});
  }
}

export async function getClusterTime(mongosUri) {
  const client = new MongoClient(mongosUri);
  try {
    await client.connect();
    const res = await client.db("admin").command({ ping: 1 });
    return fromBsonTimestamp(res?.$clusterTime?.clusterTime);
  } finally {
    await client.close().catch(() => {});
  }
}

export async function waitForMajorityCatchup(uri, targetTs, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const client = new MongoClient(uri);
    try {
      await client.connect();
      const status = await client.db("admin").command({ replSetGetStatus: 1 });
      const committed = fromBsonTimestamp(status?.optimes?.lastCommittedOpTime?.ts);
      await client.close();
      if (
        committed &&
        (committed.t > targetTs.t || (committed.t === targetTs.t && committed.i >= targetTs.i))
      ) {
        return;
      }
    } catch {
      await client.close().catch(() => {});
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}

export { formatOplogTimestamp };
