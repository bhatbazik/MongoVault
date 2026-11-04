import pino from "pino";
import type { Logger, LoggerOptions } from "./types/logger.js";

export function createLogger({ level = "info", pretty = false }: LoggerOptions = {}): Logger {
  const options: pino.LoggerOptions = {
    level,
    timestamp: pino.stdTimeFunctions.isoTime,
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
        },
      },
    });
  }

  return pino(options);
}
