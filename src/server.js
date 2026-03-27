const http = require("http");
const { createApp } = require("./app");
const { env } = require("./config/env");
const { logger } = require("./config/logger");
const { connectMongo } = require("./config/mongo");
const { redis } = require("./config/redis");
const { registerRepeatableJobs } = require("./queues");
const { createSocketServer } = require("./socket");
const { rebuildRedisStateFromMongo } = require("./services/recovery.service");

async function bootstrap() {
  await connectMongo();
  await redis.ping();
  await rebuildRedisStateFromMongo();
  await registerRepeatableJobs();

  const app = createApp();
  const server = http.createServer(app);
  await createSocketServer(server);

  server.listen(env.port, () => {
    logger.info("API server listening", { port: env.port });
  });
}

bootstrap().catch((error) => {
  logger.error("Failed to bootstrap server", { message: error.message });
  process.exit(1);
});
