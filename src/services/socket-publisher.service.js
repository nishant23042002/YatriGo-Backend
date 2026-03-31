const { v4: uuidv4 } = require("uuid");
const { socketPublisherRedis } = require("../config/redis");
const { keys } = require("../redis/keys");
const { markIdempotent } = require("../utils/idempotency");
const { InternalSocketEvents, SocketEvents } = require("../utils/constants");

function safeStringify(obj) {
  return JSON.stringify(obj, (key, value) => {
    if (value instanceof Date) return value.toISOString();
    if (value === undefined) return null;
    if (typeof value === "function") return null;
    return value;
  });
}

async function publish(message, dedupeKey) {
  const eventId = message.eventId || uuidv4();
  const allowed = await markIdempotent(
    socketPublisherRedis,
    "socket-publish",
    dedupeKey || eventId,
  );

  if (!allowed) {
    return false;
  }

  await socketPublisherRedis.publish(
    keys.socketPubSubChannel(),
    safeStringify({
      ...message,
      eventId,
      publishedAt: new Date().toISOString(),
    }),
  );

  return true;
}

async function emitToRoom(room, event, payload, dedupeKey) {
  if (!event) {
    console.error("❌ Attempted to emit NULL event", { room, payload });
    return false;
  }

  return publish({ room, event, payload }, dedupeKey);
}

async function emitRideRequested(ride) {
  return emitToRoom(
    `customer:${ride.customerId}`,
    SocketEvents.CUSTOMER.RIDE_REQUESTED,
    ride,
    `ride-requested:${ride.rideId}:${ride.version}`,
  );
}

async function emitRideStatusUpdate(ride, extra = {}) {
  const payload = { ...ride, ...extra };

  // ✅ SEND TO CUSTOMER
  await emitToRoom(
    `customer:${ride.customerId}`,
    SocketEvents.CUSTOMER.RIDE_STATUS_UPDATE,
    payload,
    `ride-status:${ride.rideId}:${ride.version}`,
  );

  // 🔥 ADD THIS (CRITICAL FIX)
  if (ride.driverId) {
    await emitToRoom(
      `driver:${ride.driverId}`,
      SocketEvents.DRIVER.RIDE_STATUS_UPDATE, // use correct enum
      payload,
      `ride-status-driver:${ride.rideId}:${ride.version}`,
    );
  }
}

async function emitDriverStateUpdate(driverId, state) {
  return emitToRoom(
    `driver:${driverId}`,
    "driver_state_update",
    state,
    `driver-state:${driverId}:${state.updatedAt}`,
  );
}

async function emitDriverAssigned(ride) {
  return emitToRoom(
    `customer:${ride.customerId}`,
    SocketEvents.CUSTOMER.DRIVER_ASSIGNED,
    ride,
    `driver-assigned:${ride.rideId}:${ride.version}`,
  );
}

async function emitDriverLocationUpdate(customerId, payload) {
  return emitToRoom(
    `customer:${customerId}`,
    SocketEvents.CUSTOMER.DRIVER_LOCATION_UPDATE,
    payload,
    `driver-location:${payload.rideId}:${payload.driverId}:${payload.at}`,
  );
}

async function emitDriverArriving(ride) {
  if (!ride.driverId) return;

  await emitToRoom(
    `driver:${ride.driverId}`,
    "ride_status_update",
    ride,
    `driver-arriving:${ride.rideId}:${ride.version}`,
  );
}

async function emitRideStartedToDriver(ride) {
  if (!ride.driverId) return;

  await emitToRoom(
    `driver:${ride.driverId}`,
    "ride_status_update",
    ride,
    `ride-started:${ride.rideId}:${ride.version}`,
  );
}

async function emitRideCompletedToDriver(ride) {
  if (!ride.driverId) return;

  await emitToRoom(
    `driver:${ride.driverId}`,
    "ride_status_update",
    ride,
    `ride-completed:${ride.rideId}:${ride.version}`,
  );
}

async function emitNewRideRequest(driverId, payload) {
  return emitToRoom(
    `driver:${driverId}`,
    SocketEvents.DRIVER.NEW_RIDE_REQUEST,
    payload,
    `new-ride-request:${payload.rideId}:${payload.batchId}:${driverId}`,
  );
}

async function emitRideAssignedToDriver(driverId, ride) {
  return emitToRoom(
    `driver:${driverId}`,
    SocketEvents.DRIVER.RIDE_ASSIGNED,
    ride,
    `ride-assigned:${ride.rideId}:${ride.version}:${driverId}`,
  );
}

async function emitRideCancelledToDriver(driverId, payload) {
  return emitToRoom(
    `driver:${driverId}`,
    SocketEvents.DRIVER.RIDE_CANCELLED,
    payload,
    `ride-cancelled:${payload.rideId}:${payload.version}:${driverId}`,
  );
}

async function emitSnapshot(room, payload, suffix) {
  return emitToRoom(
    room,
    InternalSocketEvents.SNAPSHOT,
    payload,
    `snapshot:${room}:${suffix}`,
  );
}

async function emitDriverConnectionLost(customerId, payload) {
  return emitToRoom(
    `customer:${customerId}`,
    InternalSocketEvents.DRIVER_CONNECTION_LOST,
    payload,
    `driver-connection-lost:${payload.rideId}:${payload.driverId}:${payload.at}`,
  );
}

module.exports = {
  emitDriverAssigned,
  emitDriverStateUpdate,
  emitDriverConnectionLost,
  emitDriverLocationUpdate,
  emitDriverArriving,
  emitRideStartedToDriver,
  emitRideCompletedToDriver,
  emitNewRideRequest,
  emitRideAssignedToDriver,
  emitRideCancelledToDriver,
  emitRideRequested,
  emitRideStatusUpdate,
  emitSnapshot,
};
