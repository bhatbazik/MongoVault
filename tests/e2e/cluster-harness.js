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

export async function waitForPing(uri, proc, logPath, timeoutMs = 45_000) {
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

export async function waitForPrimary(uriOrUris, timeoutMs = 45_000) {
  const uris = Array.isArray(uriOrUris) ? uriOrUris : [uriOrUris];
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    for (const uri of uris) {
      const client = new MongoClient(uri, {
        directConnection: true,
        serverSelectionTimeoutMS: 2000,
      });
      try {
        await client.connect();
        const hello = await client.db("admin").command({ hello: 1 });
        await client.close();
        if (hello.isWritablePrimary) return uri;
      } catch {
        await client.close().catch(() => {});
      }
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`Replica set at ${uris.join(", ")} did not elect primary within ${timeoutMs}ms`);
}

export async function stopProcess(proc, uri) {
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
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

async function waitForSecondariesReady(primaryUri, expectedSecondaries, timeoutMs = 45_000) {
  if (expectedSecondaries <= 0) return;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const client = new MongoClient(primaryUri, {
      directConnection: true,
      serverSelectionTimeoutMS: 2000,
    });
    try {
      await client.connect();
      const status = await client.db("admin").command({ replSetGetStatus: 1 });
      await client.close();
      const members = Array.isArray(status?.members) ? status.members : [];
      const primaryCount = members.filter((m) => m.stateStr === "PRIMARY").length;
      const secondaryCount = members.filter((m) => m.stateStr === "SECONDARY").length;
      if (primaryCount === 1 && secondaryCount >= expectedSecondaries) {
        return;
      }
    } catch {
      await client.close().catch(() => {});
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(
    `Replica set at ${primaryUri} did not reach ${expectedSecondaries} SECONDARY members within ${timeoutMs}ms`,
  );
}

function normalizeFailPointOptions(optsOrData) {
  if (optsOrData === "off" || optsOrData === null) {
    return { mode: "off", data: undefined };
  }
  if (typeof optsOrData === "object" && "mode" in optsOrData) {
    return { mode: optsOrData.mode, data: optsOrData.data };
  }
  return { mode: "alwaysOn", data: optsOrData };
}

export async function startReplicaSet({
  rootDir,
  name,
  role,
  dbPath: customDbPath,
  members = 1,
  oplogSizeMb = 64,
  priorities,
  enableTestCommands = false,
  startupTimeoutMs = 45_000,
}) {
  const ports = await Promise.all(Array.from({ length: members }, () => findFreePort()));

  const nodes = await Promise.all(
    ports.map(async (port, idx) => {
      const nodeDbPath =
        members === 1 && customDbPath
          ? customDbPath
          : idx === 0 && customDbPath
            ? customDbPath
            : members === 1
              ? path.join(rootDir, `db-${name}`)
              : path.join(rootDir, `db-${name}-n${idx}`);
      const nodeLogPath =
        members === 1
          ? path.join(rootDir, `${name}.log`)
          : path.join(rootDir, `${name}-n${idx}.log`);
      await fs.mkdir(nodeDbPath, { recursive: true });

      const args = [
        `--replSet=${name}`,
        `--port=${port}`,
        `--dbpath=${nodeDbPath}`,
        "--bind_ip=127.0.0.1",
        `--oplogSize=${oplogSizeMb}`,
        `--logpath=${nodeLogPath}`,
      ];
      if (role === "configsvr") args.push("--configsvr");
      if (role === "shardsvr") args.push("--shardsvr");
      if (enableTestCommands) args.push("--setParameter=enableTestCommands=1");

      let currentProc = spawn("mongod", args, { stdio: "ignore" });
      const directUri = `mongodb://127.0.0.1:${port}/?directConnection=true`;

      const isRunning = () =>
        currentProc !== null && currentProc.exitCode === null && currentProc.signalCode === null;

      return {
        id: idx,
        port,
        dbPath: nodeDbPath,
        logPath: nodeLogPath,
        directUri,
        args,
        get proc() {
          return currentProc;
        },
        get pid() {
          return currentProc?.pid ?? null;
        },
        isRunning,
        async stop() {
          await stopProcess(currentProc, directUri);
        },
        async kill(sig = "SIGKILL") {
          if (!isRunning()) return;
          if (sig === "SIGSTOP" || sig === "SIGCONT") {
            currentProc.kill(sig);
            return;
          }
          const exitPromise = new Promise((resolve) => {
            currentProc.once("close", resolve);
            currentProc.once("error", resolve);
          });
          currentProc.kill(sig);
          await exitPromise;
        },
        async restart() {
          if (isRunning()) {
            await stopProcess(currentProc, directUri);
          }
          currentProc = spawn("mongod", args, { stdio: "ignore" });
          await waitForPing(directUri, currentProc, nodeLogPath, startupTimeoutMs);
        },
      };
    }),
  );

  await Promise.all(
    nodes.map((node) => waitForPing(node.directUri, node.proc, node.logPath, startupTimeoutMs)),
  );

  const initClient = new MongoClient(nodes[0].directUri, { directConnection: true });
  try {
    await initClient.connect();
    await initClient.db("admin").command({
      replSetInitiate: {
        _id: name,
        ...(role === "configsvr" ? { configsvr: true } : {}),
        members: nodes.map((n, i) => ({
          _id: i,
          host: `127.0.0.1:${n.port}`,
          ...(priorities?.[i] !== undefined ? { priority: priorities[i] } : {}),
        })),
      },
    });
  } finally {
    await initClient.close().catch(() => {});
  }

  const primaryUri = await waitForPrimary(
    priorities?.[0] === 0 ? nodes.map((n) => n.directUri) : nodes[0].directUri,
    startupTimeoutMs,
  );
  if (members > 1) {
    await waitForSecondariesReady(primaryUri, members - 1, startupTimeoutMs);
  }

  const hostsCsv = nodes.map((n) => `127.0.0.1:${n.port}`).join(",");
  const host = `${name}/${hostsCsv}`;
  const rsUri = `mongodb://${hostsCsv}/?replicaSet=${name}`;

  let stallState = null;

  async function getPrimaryNode() {
    for (const node of nodes) {
      if (!node.isRunning()) continue;
      const client = new MongoClient(node.directUri, {
        directConnection: true,
        serverSelectionTimeoutMS: 2000,
      });
      try {
        await client.connect();
        const hello = await client.db("admin").command({ hello: 1 });
        if (hello.isWritablePrimary) {
          return node;
        }
      } catch {
        // Node unreachable or stepping down
      } finally {
        await client.close().catch(() => {});
      }
    }
    throw new Error(`No writable PRIMARY found in replica set "${name}"`);
  }

  async function getSecondaryNodes() {
    const secondaries = [];
    for (const node of nodes) {
      if (!node.isRunning()) continue;
      const client = new MongoClient(node.directUri, {
        directConnection: true,
        serverSelectionTimeoutMS: 2000,
      });
      try {
        await client.connect();
        const hello = await client.db("admin").command({ hello: 1 });
        if (hello.secondary) {
          secondaries.push(node);
        }
      } catch {
        // Ignore unreachable node
      } finally {
        await client.close().catch(() => {});
      }
    }
    return secondaries;
  }

  async function setFailPoint(nodeOrUri, failPointName, optsOrData = { mode: "alwaysOn" }) {
    const targetUri = typeof nodeOrUri === "string" ? nodeOrUri : nodeOrUri.directUri;
    const { mode, data } = normalizeFailPointOptions(optsOrData);
    const client = new MongoClient(targetUri, {
      directConnection: true,
      serverSelectionTimeoutMS: 5000,
    });
    try {
      await client.connect();
      await client.db("admin").command({
        configureFailPoint: failPointName,
        mode,
        ...(data !== undefined ? { data } : {}),
      });
    } finally {
      await client.close().catch(() => {});
    }
  }

  async function clearFailPoint(nodeOrUri, failPointName) {
    return setFailPoint(nodeOrUri, failPointName, { mode: "off" });
  }

  async function stepDownPrimary(
    stepDownSecs = 30,
    secondaryCatchUpPeriodSecs = 5,
    timeoutMs = 45_000,
  ) {
    if (nodes.length < 2) {
      throw new Error(`stepDownPrimary requires at least 2 members (got ${nodes.length})`);
    }
    const oldPrimary = await getPrimaryNode();
    const client = new MongoClient(oldPrimary.directUri, {
      directConnection: true,
      serverSelectionTimeoutMS: 5000,
    });
    try {
      await client.connect();
      await client
        .db("admin")
        .command({
          replSetStepDown: stepDownSecs,
          secondaryCatchUpPeriodSecs,
        })
        .catch(() => {});
    } finally {
      await client.close().catch(() => {});
    }

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      for (const node of nodes) {
        if (node.id === oldPrimary.id || !node.isRunning()) continue;
        const c = new MongoClient(node.directUri, {
          directConnection: true,
          serverSelectionTimeoutMS: 1500,
        });
        try {
          await c.connect();
          const hello = await c.db("admin").command({ hello: 1 });
          if (hello.isWritablePrimary) {
            return node;
          }
        } catch {
          // Election in progress
        } finally {
          await c.close().catch(() => {});
        }
      }
      await new Promise((r) => setTimeout(r, 200));
    }
    throw new Error(
      `Replica set "${name}" did not elect a new primary after stepDown within ${timeoutMs}ms`,
    );
  }

  async function induceMajorityStall({ mode = "freezeCommitPoint" } = {}) {
    if (nodes.length < 3) {
      throw new Error(`induceMajorityStall requires at least 3 members (got ${nodes.length})`);
    }
    const secondaries = await getSecondaryNodes();
    if (secondaries.length < 2) {
      throw new Error(`Expected at least 2 secondaries, found ${secondaries.length}`);
    }
    if (mode === "freezeCommitPoint") {
      if (!enableTestCommands) {
        throw new Error(
          'induceMajorityStall({ mode: "freezeCommitPoint" }) requires enableTestCommands: true',
        );
      }
      await setFailPoint(secondaries[0], "rsSyncApplyStop", { mode: "alwaysOn" });
      await secondaries[1].stop();
      stallState = {
        mode,
        pausedNode: secondaries[0],
        stoppedNodes: [secondaries[1]],
      };
      return stallState;
    }
    if (mode === "loseQuorum") {
      const toStop = secondaries.slice(0, 2);
      await Promise.all(toStop.map((n) => n.stop()));
      stallState = {
        mode,
        pausedNode: null,
        stoppedNodes: toStop,
      };
      return stallState;
    }
    throw new Error(`Unknown induceMajorityStall mode: ${mode}`);
  }

  async function restoreMajority() {
    if (!stallState) return;
    const { pausedNode, stoppedNodes } = stallState;
    stallState = null;

    if (pausedNode && pausedNode.isRunning()) {
      await clearFailPoint(pausedNode, "rsSyncApplyStop").catch(() => {});
    }
    if (stoppedNodes.length > 0) {
      await Promise.all(stoppedNodes.map((n) => n.restart()));
    }

    const activePrimaryUri = await waitForPrimary(
      nodes.map((n) => n.directUri),
      45_000,
    );
    await waitForSecondariesReady(activePrimaryUri, nodes.length - 1, 45_000);
    const latest = await getLatestOplogTs(rsUri);
    if (latest) {
      await waitForMajorityCatchup(rsUri, latest, 30_000);
    }
  }

  return {
    name,
    port: nodes[0].port,
    dbPath: nodes[0].dbPath,
    logPath: nodes[0].logPath,
    host,
    directUri: nodes[0].directUri,
    uri: rsUri,
    nodes,
    oplogSizeMb,
    enableTestCommands,
    get proc() {
      return nodes[0].proc;
    },
    get pid() {
      return nodes[0].pid;
    },
    getPrimaryNode,
    getSecondaryNodes,
    setFailPoint,
    clearFailPoint,
    stepDownPrimary,
    induceMajorityStall,
    restoreMajority,
    stop: async () => {
      await Promise.all(nodes.map((n) => n.stop()));
    },
  };
}

export async function startMultiNodeReplicaSet(options) {
  return startReplicaSet({
    members: 3,
    priorities: [2, 1, 1],
    ...options,
  });
}

export async function startStandalone({
  rootDir,
  dbPath,
  name = "standalone",
  enableTestCommands = false,
  startupTimeoutMs = 45_000,
}) {
  const port = await findFreePort();
  const logPath = path.join(rootDir, `${name}-${port}.log`);
  await fs.mkdir(dbPath, { recursive: true });
  const args = [
    `--port=${port}`,
    `--dbpath=${dbPath}`,
    "--bind_ip=127.0.0.1",
    `--logpath=${logPath}`,
  ];
  if (enableTestCommands) args.push("--setParameter=enableTestCommands=1");

  const proc = spawn("mongod", args, { stdio: "ignore" });
  const uri = `mongodb://127.0.0.1:${port}/?directConnection=true`;
  await waitForPing(uri, proc, logPath, startupTimeoutMs);
  return {
    port,
    uri,
    dbPath,
    logPath,
    proc,
    get pid() {
      return proc.pid ?? null;
    },
    stop: () => stopProcess(proc, uri),
  };
}

export async function startShardedCluster(rootDirOrOptions) {
  const opts =
    typeof rootDirOrOptions === "string" ? { rootDir: rootDirOrOptions } : rootDirOrOptions;
  const {
    rootDir,
    enableTestCommands = false,
    cfgMembers = 1,
    shardMembers = 1,
    oplogSizeMb = 64,
    startupTimeoutMs = 45_000,
  } = opts;

  const [cfgRs, shard01, shard02] = await Promise.all([
    startReplicaSet({
      rootDir,
      name: "cfgRs",
      role: "configsvr",
      members: cfgMembers,
      oplogSizeMb,
      enableTestCommands,
      startupTimeoutMs,
    }),
    startReplicaSet({
      rootDir,
      name: "shard01",
      role: "shardsvr",
      members: shardMembers,
      oplogSizeMb,
      enableTestCommands,
      startupTimeoutMs,
    }),
    startReplicaSet({
      rootDir,
      name: "shard02",
      role: "shardsvr",
      members: shardMembers,
      oplogSizeMb,
      enableTestCommands,
      startupTimeoutMs,
    }),
  ]);

  const mongosPort = await findFreePort();
  const mongosLog = path.join(rootDir, "mongos.log");
  const mongosArgs = [
    `--configdb=${cfgRs.host}`,
    `--port=${mongosPort}`,
    "--bind_ip=127.0.0.1",
    `--logpath=${mongosLog}`,
  ];
  if (enableTestCommands) {
    mongosArgs.push("--setParameter=enableTestCommands=1");
  }

  let currentMongosProc = spawn("mongos", mongosArgs, { stdio: "ignore" });
  const mongosUri = `mongodb://127.0.0.1:${mongosPort}/`;
  await waitForPing(mongosUri, currentMongosProc, mongosLog, startupTimeoutMs);

  const client = new MongoClient(mongosUri);
  try {
    await client.connect();
    await client.db("admin").command({ addShard: shard01.host });
    await client.db("admin").command({ addShard: shard02.host });
  } finally {
    await client.close().catch(() => {});
  }

  const stopMongos = async () => {
    await stopProcess(currentMongosProc, mongosUri);
  };

  const restartMongos = async () => {
    if (
      currentMongosProc &&
      currentMongosProc.exitCode === null &&
      currentMongosProc.signalCode === null
    ) {
      await stopMongos();
    }
    currentMongosProc = spawn("mongos", mongosArgs, { stdio: "ignore" });
    await waitForPing(mongosUri, currentMongosProc, mongosLog, startupTimeoutMs);
  };

  return {
    mongosUri,
    mongosPort,
    mongosLog,
    get mongosProc() {
      return currentMongosProc;
    },
    get mongosPid() {
      return currentMongosProc?.pid ?? null;
    },
    cfgRs,
    shard01,
    shard02,
    stopMongos,
    restartMongos,
    async stop() {
      await stopMongos();
      await Promise.all([shard01.stop(), shard02.stop(), cfgRs.stop()]);
    },
  };
}

export async function createTcpChaosProxy({ targetHost = "127.0.0.1", targetPort }) {
  let isBlackholed = false;
  let delayMs = 0;
  const pairs = new Set();
  const stats = {
    connections: 0,
    bytesIn: 0,
    bytesOut: 0,
  };

  const destroySock = (sock) => {
    if (!sock || sock.destroyed) return;
    if (typeof sock.resetAndDestroy === "function") {
      sock.resetAndDestroy();
    } else {
      sock.destroy();
    }
  };

  const server = net.createServer((clientSock) => {
    stats.connections++;
    if (isBlackholed) {
      const pair = { clientSock, upstreamSock: null };
      pairs.add(pair);
      clientSock.on("data", (chunk) => {
        stats.bytesIn += chunk.length;
      });
      clientSock.once("close", () => pairs.delete(pair));
      clientSock.once("error", () => pairs.delete(pair));
      return;
    }

    const upstreamSock = net.createConnection({ host: targetHost, port: targetPort });
    const pair = { clientSock, upstreamSock };
    pairs.add(pair);

    let c2uQueue = Promise.resolve();
    let u2cQueue = Promise.resolve();

    const cleanupPair = () => {
      pairs.delete(pair);
      if (!clientSock.destroyed) clientSock.destroy();
      if (!upstreamSock.destroyed) upstreamSock.destroy();
    };

    const forwardChunk = (queue, dest, chunk, counterKey) => {
      stats[counterKey] += chunk.length;
      if (isBlackholed) return queue;
      return queue.then(async () => {
        if (delayMs > 0) {
          await new Promise((r) => setTimeout(r, delayMs));
        }
        if (isBlackholed || dest.destroyed || !dest.writable) return;
        dest.write(chunk);
      });
    };

    clientSock.on("data", (chunk) => {
      c2uQueue = forwardChunk(c2uQueue, upstreamSock, chunk, "bytesIn");
    });

    upstreamSock.on("data", (chunk) => {
      u2cQueue = forwardChunk(u2cQueue, clientSock, chunk, "bytesOut");
    });

    clientSock.once("end", () => {
      c2uQueue = c2uQueue.then(() => {
        if (!isBlackholed && !upstreamSock.destroyed) upstreamSock.end();
      });
    });

    upstreamSock.once("end", () => {
      u2cQueue = u2cQueue.then(() => {
        if (!isBlackholed && !clientSock.destroyed) clientSock.end();
      });
    });

    clientSock.once("error", cleanupPair);
    upstreamSock.once("error", cleanupPair);
    clientSock.once("close", cleanupPair);
    upstreamSock.once("close", cleanupPair);
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const addr = server.address();
  const listenPort = typeof addr === "object" && addr ? addr.port : 0;

  return {
    listenPort,
    blackhole(enabled) {
      isBlackholed = Boolean(enabled);
    },
    resetAll() {
      for (const pair of [...pairs]) {
        destroySock(pair.clientSock);
        destroySock(pair.upstreamSock);
        pairs.delete(pair);
      }
    },
    setDelayMs(ms) {
      delayMs = Math.max(0, Number(ms) || 0);
    },
    getStats() {
      return {
        ...stats,
        activeConnections: pairs.size,
      };
    },
    async close() {
      this.resetAll();
      await new Promise((resolve) => server.close(() => resolve()));
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
  if (!targetTs) return;
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
