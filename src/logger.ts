import pino from "pino";
import type { Logger, LoggerOptions } from "./types/logger.js";

export function createLogger({
  level = "info",
  pretty = false,
  destination = "stdout",
}: LoggerOptions = {}): Logger {
  const fd = destination === "stderr" ? 2 : 1;
  const options: pino.LoggerOptions = {
    level,
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: {
      paths: [
        "password",
        "secretAccessKey",
        "encryptionKey",
        "*.password",
        "*.secretAccessKey",
        "*.encryptionKey",
      ],
      censor: "[REDACTED]",
    },
    formatters: {
      level(label: string) {
        return { level: label };
      },
    },
  };

  if (pretty) {
    return pino({
      ...options,
      transport: {
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "SYS:standard",
          ignore: "pid,hostname",
          destination: fd,
        },
      },
    });
  }

  return pino(options, pino.destination({ fd, sync: false }));
}
