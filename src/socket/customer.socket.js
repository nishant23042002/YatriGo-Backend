const { redis } = require("../config/redis");
const { keys } = require("../redis/keys");
const { buildCustomerRidePayload, emitCustomerSnapshot } = require("../services/notification.service");
const { enforceRideRequestRateLimit } = require("../services/rate-limit.service");
const rideStateService = require("../services/ride-state.service");
const { handleSocketEvent } = require("./helpers");

function registerCustomerSocket(socket) {
  socket.on("request_ride", async (payload, ack = () => {}) => {
    return handleSocketEvent(socket, "request_ride", payload, ack, async () => {
      await enforceRideRequestRateLimit(socket.data.actorId);
      const ride = await rideStateService.requestRide({
        customerId: socket.data.actorId,
        origin: payload.origin,
        destination: payload.destination,
        metadata: payload.metadata || {}
      });
      socket.join(`ride:${ride.rideId}`);
      return buildCustomerRidePayload(ride);
    });
  });

  socket.on("cancel_ride", async (payload, ack = () => {}) => {
    return handleSocketEvent(socket, "cancel_ride", payload, ack, async () => {
      return rideStateService.cancelRide(
        payload.rideId,
        "CUSTOMER",
        socket.data.actorId,
        payload.reason
      );
    });
  });

  socket.on("subscribe_ride", async (payload, ack = () => {}) => {
    return handleSocketEvent(socket, "subscribe_ride", payload, ack, async () => {
      socket.join(`ride:${payload.rideId}`);
      return buildCustomerRidePayload(await rideStateService.getRide(payload.rideId));
    });
  });
}

async function sendCustomerSnapshot(socket) {
  const activeRideId = await redis.get(keys.customerActiveRide(socket.data.actorId));
  if (!activeRideId) {
    return;
  }

  const ride = await rideStateService.getRide(activeRideId);
  if (!ride) {
    return;
  }

  socket.join(`ride:${ride.rideId}`);
  await emitCustomerSnapshot(socket, ride);
}

module.exports = {
  registerCustomerSocket,
  sendCustomerSnapshot
};
