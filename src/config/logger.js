const util = require("util");
const { env } = require("./env");

const levelOrder = {
  DEBUG: 10,
  INFO: 20,
  WARN: 30,
  ERROR: 40,
};

function normalizeMeta(meta) {
  if (!meta) {
    return {};
  }

  if (meta instanceof Error) {
    return {
      errorName: meta.name,
      errorMessage: meta.message,
      errorStack: meta.stack,
    };
  }

  return meta;
}

const colors = {
  reset: "\x1b[0m",
  cyan: "\x1b[36m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  magenta: "\x1b[35m",
  blue: "\x1b[34m",
  gray: "\x1b[90m",
};

function inferCategory(message, meta = {}) {
  if (meta.category) {
    return meta.category;
  }

  if (/socket|event ack/i.test(message)) {
    return "SOCKET";
  }

  if (/dispatch/i.test(message)) {
    return "DISPATCH";
  }

  if (/ride/i.test(message)) {
    return "RIDE";
  }

  if (/driver/i.test(message)) {
    return "DRIVER";
  }

  if (/redis|lock/i.test(message)) {
    return "REDIS";
  }

  return "SYSTEM";
}

function inferIcon(level, category, meta = {}) {
  if (meta.direction === "in") {
    return "📥";
  }

  if (meta.direction === "out") {
    return "📤";
  }

  if (level === "ERROR" || level === "WARN") {
    return "⚠️";
  }

  if (meta.success) {
    return "✅";
  }

  switch (category) {
    case "SOCKET":
      return "🚀";
    case "DISPATCH":
      return "🧠";
    case "REDIS":
      return "🗄️";
    case "RIDE":
      return "🚕";
    case "DRIVER":
      return "🚗";
    case "TEST":
      return "🧪";
    default:
      return "ℹ️";
  }
}

function levelColor(level) {
  switch (level) {
    case "DEBUG":
      return colors.gray;
    case "INFO":
      return colors.cyan;
    case "WARN":
      return colors.yellow;
    case "ERROR":
      return colors.red;
    default:
      return colors.reset;
  }
}

function formatMeta(meta = {}) {
  const cleaned = { ...meta };

  delete cleaned.category;
  delete cleaned.direction;
  delete cleaned.success;

  // 🔥 ONLY keep important fields
  const allowedKeys = [
    "rideId",
    "driverId",
    "customerId",
    "status",
    "batchId",
    "reason",
    "eventName",
  ];

  const filtered = {};
  for (const key of allowedKeys) {
    if (cleaned[key] !== undefined) {
      filtered[key] = cleaned[key];
    }
  }

  if (!Object.keys(filtered).length) {
    return "";
  }

  return JSON.stringify(filtered);
}

function format(level, message, meta) {
  const normalizedMeta = normalizeMeta(meta);
  const timestamp = new Date().toISOString();
  const category = inferCategory(message, normalizedMeta);
  const icon = inferIcon(level.toUpperCase(), category, normalizedMeta);
  const color = levelColor(level.toUpperCase());
  const prefix = `${icon} [${category}]`;
  const details = formatMeta(normalizedMeta);

  return `${colors.gray}${timestamp}${colors.reset} ${color}${prefix}${colors.reset} ${message}${details ? ` ${details}` : ""}`;
}

function shouldLog(level) {
  const configuredLevel = levelOrder[env.logLevel] || levelOrder.INFO;
  const requestedLevel = levelOrder[level.toUpperCase()] || levelOrder.INFO;
  return requestedLevel >= configuredLevel;
}

const logger = {
  info(message, meta) {
    if (!shouldLog("INFO")) return;
    console.log(format("INFO", message, meta));
  },

  warn(message, meta) {
    if (!shouldLog("WARN")) return;
    console.warn(format("WARN", message, meta));
  },

  error(message, meta) {
    if (!shouldLog("ERROR")) return;
    console.error(format("ERROR", message, meta));
  },

  debug(message, meta) {
    if (!shouldLog("DEBUG")) return;
    console.debug(format("DEBUG", message, meta));
  },

  child(baseMeta = {}) {
    return {
      info(message, meta) {
        logger.info(message, { ...baseMeta, ...meta });
      },
      warn(message, meta) {
        logger.warn(message, { ...baseMeta, ...meta });
      },
      error(message, meta) {
        logger.error(message, { ...baseMeta, ...meta });
      },
      debug(message, meta) {
        logger.debug(message, { ...baseMeta, ...meta });
      },
    };
  },
};
module.exports = { logger };
