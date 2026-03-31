const { createAdapter } = require("@socket.io/redis-adapter");
const { Server } = require("socket.io");
const { env } = require("../config/env");
const { logger } = require("../config/logger");
const {
  createRedisConnection,
  redis,
  socketSubscriberRedis,
} = require("../config/redis");
const { resolveSocketActor } = require("../middleware/auth.middleware");
const { keys } = require("../redis/keys");
const driverStateService = require("../services/driver-state.service");
const {
  registerCustomerSocket,
  sendCustomerSnapshot,
} = require("./customer.socket");
const { registerDriverSocket, sendDriverSnapshot } = require("./driver.socket");
const {
  installSocketDebugging,
  sanitizePayload,
  socketMeta,
} = require("./debug");
const Driver = require("../models/Driver");

async function createSocketServer(httpServer) {
  const io = new Server(httpServer, {
    cors: {
      origin: env.socketCorsOrigin,
      credentials: true,
    },
  });

  const adapterPub = createRedisConnection("socket-adapter-pub");
  const adapterSub = createRedisConnection("socket-adapter-sub");
  io.adapter(createAdapter(adapterPub, adapterSub));

  io.use((socket, next) => {
    try {
      const resolvedActor = resolveSocketActor(socket.handshake.auth || {});

      logger.info("Socket auth attempt", {
        category: "SOCKET",
        socketId: socket.id,
        role: resolvedActor.role,
        actorId: resolvedActor.actorId,
        auth: sanitizePayload(socket.handshake.auth),
      });

      socket.data.role = resolvedActor.role;
      socket.data.actorId = resolvedActor.actorId;
      socket.data.claims = resolvedActor.claims || null;
      logger.info(
        "Socket auth success",
        socketMeta(socket, {
          category: "SOCKET",
          success: true,
        }),
      );
      return next();
    } catch (error) {
      logger.warn("Socket auth failed", {
        category: "SOCKET",
        socketId: socket.id,
        auth: sanitizePayload(socket.handshake.auth),
        errorMessage: error.message,
      });
      return next(error);
    }
  });

  io.on("connection", async (socket) => {
    try {
      installSocketDebugging(socket);
      logger.info(
        "Socket connected",
        socketMeta(socket, {
          category: "SOCKET",
          success: true,
        }),
      );

      if (socket.data.role === "CUSTOMER") {
        socket.join(`customer:${socket.data.actorId}`);
        registerCustomerSocket(socket);
        await sendCustomerSnapshot(socket);
      } else if (socket.data.role === "DRIVER") {
        socket.join(`driver:${socket.data.actorId}`);
        registerDriverSocket(socket);

        try {
          await driverStateService.restoreDriverSession(
            socket.data.actorId,
            socket.id,
          );
        } catch (error) {
          logger.warn(
            "Driver session restore failed",
            socketMeta(socket, {
              category: "SOCKET",
              errorMessage: error.message,
            }),
          );
        }

        await sendDriverSnapshot(socket);
      }

      socket.on("event_ack", async (payload = {}, ack = () => {}) => {
        try {
          logger.debug(
            "Socket event ACK received",
            socketMeta(socket, {
              category: "SOCKET",
              direction: "in",
              payload: sanitizePayload(payload),
            }),
          );

          if (payload.eventId) {
            await redis.set(
              keys.socketEventAck(payload.eventId),
              JSON.stringify({
                actorId: socket.data.actorId,
                role: socket.data.role,
                at: new Date().toISOString(),
              }),
              "EX",
              env.socket.eventAckTtlSeconds,
            );
          }

          logger.debug(
            "Socket event ACK stored",
            socketMeta(socket, {
              category: "SOCKET",
              eventId: payload.eventId,
              success: true,
            }),
          );
          ack({ success: true });
        } catch (error) {
          logger.warn(
            "Socket event ACK store failed",
            socketMeta(socket, {
              category: "SOCKET",
              eventId: payload.eventId,
              errorMessage: error.message,
            }),
          );
          ack({ success: false, error: error.message });
        }
      });

      socket.on("disconnect", async (reason) => {
        logger.warn("Socket disconnected", {
          category: "SOCKET",
          reason,
        });

        if (socket.data.role === "DRIVER") {
          const driverId = socket.data.actorId;

          // 🔥 DO NOT mark offline immediately
          logger.warn("Driver temporary disconnect (waiting for reconnect)", {
            driverId,
            reason,
          });

          // optional: store last disconnect time
          await redis.set(
            `driver:${driverId}:lastDisconnect`,
            Date.now(),
            "EX",
            15, // 15 sec grace
          );
        }
      });
    } catch (error) {
      logger.error("Socket connection init failed", {
        category: "SOCKET",
        message: error.message,
        role: socket.data.role,
        actorId: socket.data.actorId,
      });
      socket.disconnect(true);
    }
  });

  await socketSubscriberRedis.subscribe(keys.socketPubSubChannel());
  socketSubscriberRedis.on("message", (_channel, message) => {
    try {
      let parsed;
      try {
        parsed = JSON.parse(message);
      } catch (error) {
        logger.error("❌ Invalid PubSub message (SKIPPED)", {
          raw: message,
          error: error.message,
        });
        return; // 🚫 DO NOT CRASH SOCKET
      }

      logger.debug("Socket pubsub outbound fanout", {
        category: "SOCKET",
        direction: "out",
        room: parsed.room,
        eventName: parsed.event,
        eventId: parsed.eventId,
        payload: sanitizePayload(parsed.payload),
      });

      if (!parsed.event) {
        logger.error("❌ Invalid socket event (SKIPPED)", {
          room: parsed.room,
          payload: parsed.payload,
        });
        return;
      }

      io.to(parsed.room).emit(parsed.event, {
        ...parsed.payload,
        eventId: parsed.eventId,
      });
    } catch (error) {
      logger.error("Failed to fan out socket event", {
        category: "SOCKET",
        message: error.message,
      });
    }
  });

  return io;
}

module.exports = { createSocketServer };
