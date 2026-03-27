const IORedis = require("ioredis");
const { env } = require("./env");
const { logger } = require("./logger");

function createRedisConnection(name) {
  const client = new IORedis(env.redisUrl, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    lazyConnect: false,
    connectionName: `yatrigo:${name}`
  });

  client.on("error", (error) => {
    logger.error("Redis connection error", {
      category: "REDIS",
      name,
      message: error.message
    });
  });

  return client;
}

const redis = createRedisConnection("app");
const socketPublisherRedis = createRedisConnection("socket-publisher");
const socketSubscriberRedis = createRedisConnection("socket-subscriber");

module.exports = {
  createRedisConnection,
  redis,
  socketPublisherRedis,
  socketSubscriberRedis
};
