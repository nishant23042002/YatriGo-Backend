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

module.exports = {
  appMode: (process.env.APP_MODE || "production").toLowerCase() === "test" ? "test" : "production",
  apiBaseUrl: process.env.TEST_API_BASE_URL || `http://127.0.0.1:${process.env.PORT || 4000}`,
  socketUrl: process.env.TEST_SOCKET_URL || `http://127.0.0.1:${process.env.PORT || 4000}`,
  redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6379",
  ackTimeoutMs: readNumber("TEST_ACK_TIMEOUT_MS", (process.env.APP_MODE || "production").toLowerCase() === "test" ? 15000 : 10000),
  eventTimeoutMs: readNumber("TEST_EVENT_TIMEOUT_MS", (process.env.APP_MODE || "production").toLowerCase() === "test" ? 30000 : 20000),
  pollIntervalMs: readNumber("TEST_POLL_INTERVAL_MS", 250),
  recoveryWaitMs: readNumber("TEST_RECOVERY_WAIT_MS", 15000),
  stressRideCount: readNumber("TEST_STRESS_RIDE_COUNT", 50),
  stressDriverCount: readNumber("TEST_STRESS_DRIVER_COUNT", 25),
  raceDriverCount: readNumber("TEST_RACE_DRIVER_COUNT", 10),
  connectRetries: readNumber("TEST_CONNECT_RETRIES", (process.env.APP_MODE || "production").toLowerCase() === "test" ? 3 : 1),
  ackRetries: readNumber("TEST_ACK_RETRIES", (process.env.APP_MODE || "production").toLowerCase() === "test" ? 3 : 1)
};
