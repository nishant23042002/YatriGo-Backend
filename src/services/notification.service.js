const { logger } = require("../config/logger");
const socketPublisher = require("./socket-publisher.service");
const { getDriverProfileSnapshot } = require("./driver-profile.service");

async function buildCustomerRidePayload(ride, extra = {}) {
  if (!ride) {
    return null;
  }

  const driverProfile = ride.driverId ? await getDriverProfileSnapshot(ride.driverId) : null;

  return {
    ...ride,
    driverProfile,
    ...extra
  };
}

async function notifyRideRequested(ride) {
  return socketPublisher.emitRideRequested(await buildCustomerRidePayload(ride));
}

async function notifyRideStatusUpdate(ride, extra = {}) {
  return socketPublisher.emitRideStatusUpdate(await buildCustomerRidePayload(ride, extra));
}

async function notifyDriverAssigned(ride) {
  return socketPublisher.emitDriverAssigned(await buildCustomerRidePayload(ride));
}

async function notifyDriverLocationUpdate(customerId, payload) {
  return socketPublisher.emitDriverLocationUpdate(customerId, {
    ...payload,
    timestamp: payload.timestamp || payload.at || new Date().toISOString(),
    speed: payload.speed != null ? payload.speed : null,
    route: payload.route || null
  });
}

async function notifyDriverArriving(ride) {
  return notifyRideStatusUpdate(ride, {
    notificationType: "DRIVER_ARRIVING"
  });
}

async function notifyRideStarted(ride) {
  return notifyRideStatusUpdate(ride, {
    notificationType: "RIDE_STARTED"
  });
}

async function notifyRideCompleted(ride) {
  return notifyRideStatusUpdate(ride, {
    notificationType: "RIDE_COMPLETED"
  });
}

async function notifyRideCancelled(ride, extra = {}) {
  return notifyRideStatusUpdate(ride, {
    notificationType: "RIDE_CANCELLED",
    ...extra
  });
}

async function emitCustomerSnapshot(socket, ride) {
  const payload = await buildCustomerRidePayload(ride);
  socket.emit("snapshot", payload);
  return payload;
}

async function notifyPushPlaceholder(type, payload) {
  logger.debug("Push notification placeholder invoked", {
    category: "SOCKET",
    type,
    payload
  });
  return false;
}

module.exports = {
  buildCustomerRidePayload,
  emitCustomerSnapshot,
  notifyDriverArriving,
  notifyDriverAssigned,
  notifyDriverLocationUpdate,
  notifyPushPlaceholder,
  notifyRideCancelled,
  notifyRideCompleted,
  notifyRideRequested,
  notifyRideStarted,
  notifyRideStatusUpdate
};
