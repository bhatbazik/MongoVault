import { startHttpServer } from "../metrics.js";
import { createScheduler } from "../scheduler.js";
import type { CliCommandContext, CommandHandler } from "./types.js";

export class DaemonCommand implements CommandHandler {
  async execute(ctx: CliCommandContext): Promise<void> {
    const { config, storage, logger, notifier, metrics } = ctx;

    const scheduler = createScheduler({ config, storage, logger, notifier, metrics });
    const httpServer = startHttpServer({
      port: config.http.port,
      metrics,
      getState: () => scheduler.getState(),
      logger,
    });

    scheduler.start();

    const shutdown = async (signal: string) => {
      logger.info({ signal }, "Received shutdown signal, stopping daemon...");
      await scheduler.stop();
      if (httpServer) await httpServer.close();
      process.exit(0);
    };

    process.on("SIGINT", () => void shutdown("SIGINT"));
    process.on("SIGTERM", () => void shutdown("SIGTERM"));
  }
}
