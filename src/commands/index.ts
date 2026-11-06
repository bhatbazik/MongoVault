import { BackupCommand } from "./backup.js";
import { CheckCommand } from "./check.js";
import { DaemonCommand } from "./daemon.js";
import { DeleteCommand } from "./delete.js";
import { DescribeCommand } from "./describe.js";
import { DiagnosticCommand } from "./diagnostic.js";
import { DrillCommand } from "./drill.js";
import { ListCommand } from "./list.js";
import {
  OplogListCommand,
  OplogPruneCommand,
  OplogSliceCommand,
  OplogTailCommand,
} from "./oplog.js";
import { PruneCommand } from "./prune.js";
import { RestoreCommand } from "./restore.js";
import { StatusCommand } from "./status.js";
import type { CommandHandler } from "./types.js";
import { VerifyCommand } from "./verify.js";

export * from "./types.js";

/**
 * Registry mapping CLI command names to their respective handlers (Command Pattern).
 */
export class CommandRegistry {
  private static readonly commands = new Map<string, CommandHandler>([
    ["status", new StatusCommand()],
    ["check", new CheckCommand()],
    ["describe", new DescribeCommand()],
    ["delete", new DeleteCommand()],
    ["diagnostic", new DiagnosticCommand()],
    ["daemon", new DaemonCommand()],
    ["backup", new BackupCommand()],
    ["restore", new RestoreCommand()],
    ["verify", new VerifyCommand()],
    ["list", new ListCommand()],
    ["prune", new PruneCommand()],
    ["drill", new DrillCommand()],
    ["oplog:slice", new OplogSliceCommand()],
    ["oplog:tail", new OplogTailCommand()],
    ["oplog:list", new OplogListCommand()],
    ["oplog:prune", new OplogPruneCommand()],
  ]);

  static register(name: string, handler: CommandHandler): void {
    this.commands.set(name, handler);
  }

  static get(name: string): CommandHandler | undefined {
    return this.commands.get(name);
  }

  static has(name: string): boolean {
    return this.commands.has(name);
  }
}
