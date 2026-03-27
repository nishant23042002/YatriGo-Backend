const { env } = require("../config/env");
const { logger } = require("../config/logger");

function sanitizePayload(value, depth = 0) {
  if (value == null) {
    return value;
  }

  if (depth > 2) {
    return "[truncated]";
  }

  if (Array.isArray(value)) {
    return value.slice(0, 5).map((item) => sanitizePayload(item, depth + 1));
  }

  if (typeof value === "object") {
    return Object.entries(value).reduce((accumulator, [key, nestedValue]) => {
      if (key.toLowerCase().includes("token") || key.toLowerCase().includes("password")) {
        accumulator[key] = "[redacted]";
        return accumulator;
      }

      accumulator[key] = sanitizePayload(nestedValue, depth + 1);
      return accumulator;
    }, {});
  }

  if (typeof value === "string" && value.length > 200) {
    return `${value.slice(0, 200)}...[truncated]`;
  }

  return value;
}

function socketMeta(socket, extra = {}) {
  return {
    socketId: socket.id,
    role: socket.data.role,
    actorId: socket.data.actorId,
    ...extra
  };
}

function installSocketDebugging(socket) {
  socket.use((packet, next) => {
    const [eventName, payload] = packet;
    logger.debug("Event received", socketMeta(socket, {
      category: "SOCKET",
      direction: "in",
      eventName,
      payload: sanitizePayload(payload)
    }));
    next();
  });

  if (typeof socket.onAnyOutgoing === "function") {
    socket.onAnyOutgoing((eventName, payload) => {
      if (!env.testing.verboseSocketLogs && eventName === "snapshot") {
        return;
      }

      logger.debug("Event sent", socketMeta(socket, {
        category: "SOCKET",
        direction: "out",
        eventName,
        payload: sanitizePayload(payload)
      }));
    });
  }
}

module.exports = {
  installSocketDebugging,
  sanitizePayload,
  socketMeta
};
