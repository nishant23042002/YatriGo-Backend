const { env } = require("../config/env");

async function markIdempotent(redis, scope, key, ttlSeconds = env.redis.idempotencyTtlSeconds) {
  const namespacedKey = `idempotency:${scope}:${key}`;
  const response = await redis.set(namespacedKey, "1", "EX", ttlSeconds, "NX");
  if(response !== "OK") {
    console.warn(`Idempotency key already exists: ${namespacedKey}`);
  }
  return response === "OK";
}

module.exports = { markIdempotent };
