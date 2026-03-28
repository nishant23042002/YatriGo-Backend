const { env } = require("../config/env");
const { logger } = require("../config/logger");
const { redis } = require("../config/redis");
const { markIdempotent } = require("../utils/idempotency");
const { sanitizePayload, socketMeta } = require("./debug");

function ackSuccess(socket, eventName, ack, payload = {}) {
  const normalizedPayload = {
    data: {},
    ...payload,
  };

  logger.debug(
    "Socket ACK success",
    socketMeta(socket, {
      category: "SOCKET",
      success: true,
      eventName,
      response: sanitizePayload(normalizedPayload),
    }),
  );
  return ack({ success: true, ...normalizedPayload });
}

function ackFailure(socket, eventName, ack, error) {
  logger.warn(
    "Socket ACK failure",
    socketMeta(socket, {
      category: "SOCKET",
      eventName,
      errorMessage: error.message,
    }),
  );
  return ack({ success: false, error: error.message });
}

async function validateInboundEvent(socket, eventName, payload = {}) {
  const idempotencyKey =
    payload.eventId || `${socket.id}:${eventName}:${Date.now()}`;
  const accepted = await markIdempotent(
    redis,
    "socket-inbound",
    idempotencyKey,
  );

  if (accepted) {
    return { accepted: true, idempotencyKey };
  }

  logger.warn(
    "Duplicate socket event detected",
    socketMeta(socket, {
      category: "SOCKET",
      eventName,
      payload: sanitizePayload(payload),
      mode: env.appMode,
    }),
  );

  if (env.testing.tolerateDuplicateSocketEvents) {
    return {
      accepted: false,
      duplicate: true,
      tolerated: true,
      idempotencyKey,
    };
  }

  return {
    accepted: false,
    duplicate: true,
    tolerated: false,
    idempotencyKey,
  };
}

async function handleSocketEvent(socket, eventName, payload, ack, handler) {
  try {
    const validation = await validateInboundEvent(
      socket,
      eventName,
      payload || {},
    );
    const isCriticalEvent = eventName === "accept_ride";

    if (!validation.accepted) {
      if (isCriticalEvent) {
        return ackFailure(
          socket,
          eventName,
          ack,
          new Error("Duplicate accept not allowed"),
        );
      }

      if (!validation.tolerated) {
        return ackFailure(
          socket,
          eventName,
          ack,
          new Error("Duplicate event rejected"),
        );
      }

      return ackSuccess(socket, eventName, ack, {
        duplicate: true,
        tolerated: validation.tolerated,
      });
    }

    const data = await handler();
    return ackSuccess(socket, eventName, ack, { data });
  } catch (error) {
    return ackFailure(socket, eventName, ack, error);
  }
}

module.exports = {
  ackFailure,
  ackSuccess,
  handleSocketEvent,
  validateInboundEvent,
};
