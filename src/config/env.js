const dotenv = require("dotenv");

dotenv.config();

function readNumber(name, fallback) {
  const value = process.env[name];
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function readString(name, fallback) {
  return process.env[name] || fallback;
}

const env = {
  appMode: readString("APP_MODE", "production").toLowerCase() === "test" ? "test" : "production",
  logLevel: readString("LOG_LEVEL", process.env.APP_MODE === "test" ? "DEBUG" : "INFO").toUpperCase(),
  port: readNumber("PORT", 4000),
  mongoUri: process.env.MONGO_URI || "mongodb://127.0.0.1:27017/yatrigo",
  redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6379",
  socketCorsOrigin: process.env.SOCKET_CORS_ORIGIN || "*",
  dispatch: {
    initialSearchRadiusKm: readNumber("INITIAL_SEARCH_RADIUS_KM", 2),
    maxSearchRadiusKm: readNumber("MAX_SEARCH_RADIUS_KM", 8),
    batchSize: readNumber("DISPATCH_BATCH_SIZE", 3),
    responseTtlMs: readNumber("DISPATCH_RESPONSE_TTL_MS", 12000),
    reservationTtlMs: readNumber("DISPATCH_RESERVATION_TTL_MS", 14000),
    retryDelayMs: readNumber("DISPATCH_RETRY_DELAY_MS", 1500),
    maxRounds: readNumber("DISPATCH_MAX_ROUNDS", 4),
    candidateMultiplier: readNumber("DISPATCH_CANDIDATE_MULTIPLIER", 6)
  },
  driver: {
    heartbeatTtlSeconds: readNumber("DRIVER_HEARTBEAT_TTL_SECONDS", 15),
    reconcileIntervalMs: readNumber("DRIVER_RECONCILE_INTERVAL_MS", 15000),
    rejectCooldownSeconds: readNumber("DRIVER_REJECT_COOLDOWN_SECONDS", 10)
  },
  recovery: {
    dispatchRecoveryIntervalMs: readNumber("DISPATCH_RECOVERY_INTERVAL_MS", 10000)
  },
  eta: {
    averageSpeedKmph: readNumber("ETA_AVERAGE_SPEED_KMPH", 28)
  },
  pricing: {
    currency: readString("PRICING_CURRENCY", "INR"),
    defaultRideType: readString("DEFAULT_RIDE_TYPE", "STANDARD").toUpperCase(),
    baseFare: readNumber("PRICING_BASE_FARE", 40),
    perKm: readNumber("PRICING_PER_KM", 12),
    perMinute: readNumber("PRICING_PER_MINUTE", 2),
    minimumFare: readNumber("PRICING_MINIMUM_FARE", 60),
    waitingPerMinute: readNumber("PRICING_WAITING_PER_MINUTE", 1.5),
    commissionPercent: readNumber("PRICING_COMMISSION_PERCENT", 20)
  },
  redis: {
    activeRideTtlSeconds: readNumber("REDIS_ACTIVE_RIDE_TTL_SECONDS", 3600),
    realtimeStateTtlSeconds: readNumber("REDIS_REALTIME_STATE_TTL_SECONDS", 21600),
    idempotencyTtlSeconds: readNumber("IDEMPOTENCY_TTL_SECONDS", 86400),
    lockTtlMs: readNumber("LOCK_TTL_MS", 5000)
  },
  auth: {
    enabled: readString("AUTH_ENABLED", "false").toLowerCase() === "true",
    jwtSecret: readString("AUTH_JWT_SECRET", "change-me"),
    issuer: readString("AUTH_JWT_ISSUER", ""),
    audience: readString("AUTH_JWT_AUDIENCE", "")
  },
  rateLimit: {
    requestRide: {
      maxRequests: readNumber("REQUEST_RIDE_RATE_LIMIT_MAX", 5),
      windowSeconds: readNumber("REQUEST_RIDE_RATE_LIMIT_WINDOW_SECONDS", 60)
    },
    acceptRide: {
      maxRequests: readNumber("ACCEPT_RIDE_RATE_LIMIT_MAX", 20),
      windowSeconds: readNumber("ACCEPT_RIDE_RATE_LIMIT_WINDOW_SECONDS", 60)
    }
  },
  socket: {
    eventAckTtlSeconds: readNumber("SOCKET_EVENT_ACK_TTL_SECONDS", 600)
  },
  testing: {
    tolerateDuplicateSocketEvents: readString("APP_MODE", "production").toLowerCase() === "test",
    verboseSocketLogs: readString("APP_MODE", "production").toLowerCase() === "test"
  }
};

module.exports = { env };
