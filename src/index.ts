#!/usr/bin/env node
import "dotenv/config";
import { runCli } from "./cli.js";
import { loadConfig } from "./config.js";

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const command = argv[0] || "daemon";
  const effectiveArgv = argv.length === 0 ? ["daemon"] : argv;

  if (command === "key:generate" || command === "--help" || command === "-h" || command === "help") {
    await runCli(null, effectiveArgv);
    return;
  }

  const config = loadConfig(process.env);
  await runCli(config, effectiveArgv);
}

main().catch((error: any) => {
  if (error.name === "ConfigError") {
    console.error(error.message);
  } else {
    console.error("Fatal error:", error.stack || error.message);
  }
  process.exit(1);
});
