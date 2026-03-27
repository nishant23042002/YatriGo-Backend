const { redis } = require("../config/redis");
const { logger } = require("../config/logger");

async function start() {
  await redis.ping();

  require("./dispatch.worker");
  await require("./persistence.worker");
  await require("./reconciliation.worker");

  logger.info("All workers started");
}

start().catch((error) => {
  logger.error("Failed to start workers", { message: error.message });
  process.exit(1);
});
