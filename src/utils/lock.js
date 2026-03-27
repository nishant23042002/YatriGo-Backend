const { v4: uuidv4 } = require("uuid");
const { env } = require("../config/env");
const { logger } = require("../config/logger");

const RELEASE_LOCK_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`;

async function acquireLock(redis, key, ttlMs = env.redis.lockTtlMs) {
  const token = uuidv4();
  const response = await redis.set(key, token, "PX", ttlMs, "NX");
  if (response === "OK") {
    logger.debug("Lock acquired", { category: "REDIS", key, ttlMs });
  } else {
    logger.debug("Lock busy", { category: "REDIS", key, ttlMs });
  }
  return response === "OK" ? token : null;
}

async function releaseLock(redis, key, token) {
  if (!token) {
    return 0;
  }

  const released = await redis.eval(RELEASE_LOCK_SCRIPT, 1, key, token);
  logger.debug("Lock released", { category: "REDIS", key, released: released === 1 });
  return released;
}

async function withLock(redis, key, ttlMs, fn) {
  const token = await acquireLock(redis, key, ttlMs);
  if (!token) {
    return null;
  }

  try {
    return await fn(token);
  } finally {
    await releaseLock(redis, key, token);
  }
}

module.exports = {
  acquireLock,
  releaseLock,
  withLock
};
