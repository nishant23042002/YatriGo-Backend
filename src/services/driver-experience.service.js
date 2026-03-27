const { redis } = require("../config/redis");
const { env } = require("../config/env");
const { logger } = require("../config/logger");

function rejectCooldownKey(driverId) {
  return `ops:driver:reject-cooldown:${driverId}`;
}

async function applyRejectCooldown(driverId, reason = "REJECTED") {
  if (!driverId || !env.driver.rejectCooldownSeconds || env.driver.rejectCooldownSeconds <= 0) {
    return;
  }

  await redis.set(
    rejectCooldownKey(driverId),
    JSON.stringify({
      driverId,
      reason,
      at: new Date().toISOString()
    }),
    "EX",
    env.driver.rejectCooldownSeconds
  );

  logger.info("Driver reject cooldown applied", {
    driverId,
    reason,
    cooldownSeconds: env.driver.rejectCooldownSeconds
  });
}

async function isDriverOnRejectCooldown(driverId) {
  if (!driverId || !env.driver.rejectCooldownSeconds || env.driver.rejectCooldownSeconds <= 0) {
    return false;
  }

  const ttlMs = await redis.pttl(rejectCooldownKey(driverId));
  return ttlMs > 0;
}

async function getRejectCooldownRemainingMs(driverId) {
  const ttlMs = await redis.pttl(rejectCooldownKey(driverId));
  return ttlMs > 0 ? ttlMs : 0;
}

module.exports = {
  applyRejectCooldown,
  getRejectCooldownRemainingMs,
  isDriverOnRejectCooldown
};
