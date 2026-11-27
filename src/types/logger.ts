import type pino from "pino";

export type LogLevel = "fatal" | "error" | "warn" | "info" | "debug" | "trace";

export interface LoggerOptions {
  level?: LogLevel | string;
  pretty?: boolean;
  destination?: "stdout" | "stderr";
}

export type Logger = pino.Logger;
