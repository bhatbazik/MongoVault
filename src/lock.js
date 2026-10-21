import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

const UNREADABLE_GRACE_MS = 5_000;

export async function acquireLock(file, { staleAfterMs }) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const token = randomUUID();

  try {
    await fs.writeFile(
      file,
      JSON.stringify({ pid: process.pid, token, acquiredAt: new Date().toISOString() }),
      { flag: "wx" },
    );
    return () => release(file, token);
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
  }

  if (!(await isStale(file, staleAfterMs))) {
    return null;
  }
  await fs.rm(file, { force: true });
  return acquireLock(file, { staleAfterMs });
}

export async function readLock(file) {
  try {
    return JSON.parse(await fs.readFile(file, "utf8"));
  } catch {
    return null;
  }
}

async function release(file, token) {
  const owner = await readLock(file);
  if (owner?.token === token) {
    await fs.rm(file, { force: true });
  }
}

async function isStale(file, staleAfterMs) {
  let stat;
  try {
    stat = await fs.stat(file);
  } catch (error) {
    if (error.code === "ENOENT") return true;
    throw error;
  }

  const age = Date.now() - stat.mtimeMs;
  if (age > staleAfterMs) return true;

  const owner = await readLock(file);
  if (!owner) return age > UNREADABLE_GRACE_MS;
  return !isAlive(owner.pid);
}

function isAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === "EPERM";
  }
}
