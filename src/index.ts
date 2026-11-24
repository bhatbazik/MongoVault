#!/usr/bin/env node
import "dotenv/config";
import { runCli } from "./cli.js";
import { loadConfig } from "./config.js";
import { renderErrorBox } from "./tui.js";

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const command = argv[0] || "daemon";
  const effectiveArgv = argv.length === 0 ? ["daemon"] : argv;

  if (
    command === "key:generate" ||
    command === "--help" ||
    command === "-h" ||
    command === "help"
  ) {
    await runCli(null, effectiveArgv);
    return;
  }

  const config = loadConfig(process.env);
  await runCli(config, effectiveArgv);
}

main().catch((error: unknown) => {
  const err = error as { name?: string; message?: string; stack?: string };
  if (err.name === "ConfigError") {
    console.error(renderErrorBox("Configuration Error", error));
  } else {
    console.error(
      renderErrorBox("Fatal Error", error, {
        showStack: process.env.LOG_LEVEL === "debug" || process.env.LOG_LEVEL === "trace",
      }),
    );
  }
  process.exit(1);
});
