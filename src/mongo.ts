import { MongoClient } from "mongodb";
import { formatOplogTimestamp, getOplogBounds } from "./oplog.js";
import type { ClusterStatus } from "./types/status.js";

/**
 * Masks credentials in MongoDB connection URI for safe logging and diagnostic display.
 */
export function maskMongoUri(uri: string): string {
  try {
    return uri.replace(/\/\/[^:]+:[^@]+@/, "//***:***@");
  } catch {
    return "masked";
  }
}

export interface InspectMongoClusterOptions {
  readPreference?: string;
  createMongoClient?: (uri: string) => MongoClient;
}

/**
 * Inspects a MongoDB instance or cluster for topology, version, role, and oplog availability.
 */
export async function inspectMongoCluster(
  client: MongoClient,
  uri: string,
  options: InspectMongoClusterOptions = {},
): Promise<ClusterStatus> {
  const {
    readPreference = "secondaryPreferred",
    createMongoClient = (u: string) => new MongoClient(u),
  } = options;

  try {
    const admin = client.db("admin");
    const [hello, buildInfo] = await Promise.all([
      admin.command({ hello: 1 }).catch(() => admin.command({ isMaster: 1 })),
      admin.command({ buildInfo: 1 }).catch(() => ({ version: "unknown" })),
    ]);

    let role: ClusterStatus["role"] = "standalone";
    if (hello.msg === "isdbgrid") {
      role = "mongos";
    } else if (hello.setName) {
      if (hello.isWritablePrimary) role = "primary";
      else if (hello.secondary) role = "secondary";
      else if (hello.arbiterOnly) role = "arbiter";
      else role = "unknown";
    }

    let oplogInfo: ClusterStatus["oplog"] = undefined;
    if (hello.setName) {
      try {
        const bounds = await getOplogBounds(uri, { createMongoClient });
        if (bounds.oldestTs && bounds.newestTs) {
          const hours = Number(((bounds.newestTs.t - bounds.oldestTs.t) / 3600).toFixed(1));
          oplogInfo = {
            available: true,
            oldestTs: formatOplogTimestamp(bounds.oldestTs),
            newestTs: formatOplogTimestamp(bounds.newestTs),
            retentionHours: Math.max(0, hours),
          };
        }
      } catch {
        oplogInfo = { available: false };
      }
    }

    return {
      connected: true,
      version: String(buildInfo.version || "unknown"),
      setName: hello.setName ? String(hello.setName) : undefined,
      role,
      primaryHost: hello.primary ? String(hello.primary) : undefined,
      me: hello.me ? String(hello.me) : undefined,
      hosts: Array.isArray(hello.hosts) ? hello.hosts.map(String) : undefined,
      readPreference,
      oplog: oplogInfo,
    };
  } catch (error) {
    return {
      connected: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
