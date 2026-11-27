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

    let shutdownPromise: Promise<void> | null = null;
    const shutdown = (signal: string) => {
      if (shutdownPromise) return shutdownPromise;
      shutdownPromise = (async () => {
        logger.info({ signal }, "Received shutdown signal, stopping daemon...");
        try {
          await scheduler.stop();
        } catch (err) {
          logger.error({ err }, "Error while stopping scheduler during daemon shutdown");
        }
        try {
          if (httpServer) await httpServer.close();
        } catch (err) {
          logger.error({ err }, "Error while closing HTTP server during daemon shutdown");
        }
        process.exit(0);
      })();
      return shutdownPromise;
    };

    process.on("SIGINT", () => void shutdown("SIGINT"));
    process.on("SIGTERM", () => void shutdown("SIGTERM"));
  }
}
