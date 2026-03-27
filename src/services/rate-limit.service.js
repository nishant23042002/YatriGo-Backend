const { redis } = require("../config/redis");
const { env } = require("../config/env");
const { logger } = require("../config/logger");
const { AppError } = require("../utils/errors");

function rateLimitKey(scope, actorId) {
  return `rate-limit:${scope}:${actorId}`;
}

async function enforceLimit({ scope, actorId, maxRequests, windowSeconds, errorCode, errorMessage }) {
  if (!actorId || !maxRequests || maxRequests <= 0 || !windowSeconds || windowSeconds <= 0) {
    return;
  }

  const key = rateLimitKey(scope, actorId);
  const count = await redis.incr(key);
  if (count === 1) {
    await redis.expire(key, windowSeconds);
  }

  if (count > maxRequests) {
    logger.warn("Rate limit exceeded", {
      category: "REDIS",
      scope,
      actorId,
      count,
      maxRequests,
      windowSeconds
    });
    throw new AppError(errorMessage, 429, errorCode);
  }
}

async function enforceRideRequestRateLimit(customerId) {
  return enforceLimit({
    scope: "customer:request-ride",
    actorId: customerId,
    maxRequests: env.rateLimit.requestRide.maxRequests,
    windowSeconds: env.rateLimit.requestRide.windowSeconds,
    errorCode: "REQUEST_RIDE_RATE_LIMITED",
    errorMessage: "Too many ride requests, please wait a moment"
  });
}

async function enforceRideAcceptRateLimit(driverId) {
  return enforceLimit({
    scope: "driver:accept-ride",
    actorId: driverId,
    maxRequests: env.rateLimit.acceptRide.maxRequests,
    windowSeconds: env.rateLimit.acceptRide.windowSeconds,
    errorCode: "ACCEPT_RIDE_RATE_LIMITED",
    errorMessage: "Too many accept attempts, please wait a moment"
  });
}

module.exports = {
  enforceRideAcceptRateLimit,
  enforceRideRequestRateLimit
};
