const { v4: uuidv4 } = require("uuid");
const { socketPublisherRedis } = require("../config/redis");
const { keys } = require("../redis/keys");
const { markIdempotent } = require("../utils/idempotency");
const { InternalSocketEvents, SocketEvents } = require("../utils/constants");

async function publish(message, dedupeKey) {
  const eventId = message.eventId || uuidv4();
  const allowed = await markIdempotent(
    socketPublisherRedis,
    "socket-publish",
    dedupeKey || eventId
  );

  if (!allowed) {
    return false;
  }

  await socketPublisherRedis.publish(
    keys.socketPubSubChannel(),
    JSON.stringify({ ...message, eventId, publishedAt: new Date().toISOString() })
  );

  return true;
}

async function emitToRoom(room, event, payload, dedupeKey) {
  return publish({ room, event, payload }, dedupeKey);
}

async function emitRideRequested(ride) {
  return emitToRoom(
    `customer:${ride.customerId}`,
    SocketEvents.CUSTOMER.RIDE_REQUESTED,
    ride,
    `ride-requested:${ride.rideId}:${ride.version}`
  );
}

async function emitRideStatusUpdate(ride, extra = {}) {
  return emitToRoom(
    `customer:${ride.customerId}`,
    SocketEvents.CUSTOMER.RIDE_STATUS_UPDATE,
    { ...ride, ...extra },
    `ride-status:${ride.rideId}:${ride.version}`
  );
}

async function emitDriverAssigned(ride) {
  return emitToRoom(
    `customer:${ride.customerId}`,
    SocketEvents.CUSTOMER.DRIVER_ASSIGNED,
    ride,
    `driver-assigned:${ride.rideId}:${ride.version}`
  );
}

async function emitDriverLocationUpdate(customerId, payload) {
  return emitToRoom(
    `customer:${customerId}`,
    SocketEvents.CUSTOMER.DRIVER_LOCATION_UPDATE,
    payload,
    `driver-location:${payload.rideId}:${payload.driverId}:${payload.at}`
  );
}

async function emitNewRideRequest(driverId, payload) {
  return emitToRoom(
    `driver:${driverId}`,
    SocketEvents.DRIVER.NEW_RIDE_REQUEST,
    payload,
    `new-ride-request:${payload.rideId}:${payload.batchId}:${driverId}`
  );
}

async function emitRideAssignedToDriver(driverId, ride) {
  return emitToRoom(
    `driver:${driverId}`,
    SocketEvents.DRIVER.RIDE_ASSIGNED,
    ride,
    `ride-assigned:${ride.rideId}:${ride.version}:${driverId}`
  );
}

async function emitRideCancelledToDriver(driverId, payload) {
  return emitToRoom(
    `driver:${driverId}`,
    SocketEvents.DRIVER.RIDE_CANCELLED,
    payload,
    `ride-cancelled:${payload.rideId}:${payload.version}:${driverId}`
  );
}

async function emitSnapshot(room, payload, suffix) {
  return emitToRoom(
    room,
    InternalSocketEvents.SNAPSHOT,
    payload,
    `snapshot:${room}:${suffix}`
  );
}

async function emitDriverConnectionLost(customerId, payload) {
  return emitToRoom(
    `customer:${customerId}`,
    InternalSocketEvents.DRIVER_CONNECTION_LOST,
    payload,
    `driver-connection-lost:${payload.rideId}:${payload.driverId}:${payload.at}`
  );
}

module.exports = {
  emitDriverAssigned,
  emitDriverConnectionLost,
  emitDriverLocationUpdate,
  emitNewRideRequest,
  emitRideAssignedToDriver,
  emitRideCancelledToDriver,
  emitRideRequested,
  emitRideStatusUpdate,
  emitSnapshot
};
