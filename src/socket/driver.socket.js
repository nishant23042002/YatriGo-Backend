const { v4: uuidv4 } = require("uuid");
const dispatchService = require("../services/dispatch.service");
const driverStateService = require("../services/driver-state.service");
const {
  notifyDriverLocationUpdate,
} = require("../services/notification.service");
const {
  enforceRideAcceptRateLimit,
} = require("../services/rate-limit.service");
const recoveryService = require("../services/recovery.service");
const rideStateService = require("../services/ride-state.service");
const { handleSocketEvent } = require("./helpers");

async function pushLocationIfNeeded(driverState) {
  if (!driverState.activeRideId) {
    return;
  }

  const ride = await rideStateService.getRide(driverState.activeRideId);
  if (!ride || !(await driverStateService.canTrackLocation(ride.status))) {
    return;
  }

  await notifyDriverLocationUpdate(ride.customerId, {
    rideId: ride.rideId,
    driverId: driverState.driverId,
    lat: driverState.lat,
    lng: driverState.lng,
    at: driverState.updatedAt,
  });
}

function registerDriverSocket(socket) {
  socket.on("go_online", async (payload, ack = () => {}) =>
    handleSocketEvent(socket, "go_online", payload, ack, async () =>
      driverStateService.goOnline({
        driverId: socket.data.actorId,
        lat: payload.lat,
        lng: payload.lng,
        socketId: socket.id,
        metadata: payload.metadata || {},
      }),
    ),
  );

  socket.on("go_offline", async (payload, ack = () => {}) =>
    handleSocketEvent(socket, "go_offline", payload, ack, async () => {
      const impact = await driverStateService.goOffline({
        driverId: socket.data.actorId,
        reason: payload.reason || "MANUAL",
      });
      await recoveryService.handleDriverDrop({
        driverId: impact.driverId,
        activeRideId: impact.activeRideId,
        pendingRideIds: impact.pendingRideIds,
        reason: payload.reason || "MANUAL",
      });
      return impact;
    }),
  );

  socket.on("location_heartbeat", async (payload, ack = () => {}) => {
    try {
      const currentState = await driverStateService.getDriverState(
        socket.data.actorId,
      );

      // 🔥 HARD BLOCK
      if (!currentState || currentState.status === "OFFLINE") {
        return ack({ success: true });
      }

      const state = await driverStateService.heartbeat({
        driverId: socket.data.actorId,
        lat: payload.lat,
        lng: payload.lng,
        socketId: socket.id,
      });

      await pushLocationIfNeeded(state);

      ack({ success: true });
    } catch (error) {
      ack({ success: false, error: error.message });
    }
  });

  socket.on("accept_ride", async (payload, ack = () => {}) =>
    handleSocketEvent(socket, "accept_ride", payload, ack, async () => {
      await enforceRideAcceptRateLimit(socket.data.actorId);
      return dispatchService.acceptRide(payload.rideId, socket.data.actorId);
    }),
  );

  socket.on("reject_ride", async (payload, ack = () => {}) =>
    handleSocketEvent(socket, "reject_ride", payload, ack, async () =>
      dispatchService.handleDriverRejection(
        payload.rideId,
        socket.data.actorId,
        payload.reason || "REJECTED",
      ),
    ),
  );

  socket.on("ride_arriving", async (payload, ack = () => {}) =>
    handleSocketEvent(socket, "ride_arriving", payload, ack, async () =>
      rideStateService.markDriverArriving(payload.rideId, socket.data.actorId),
    ),
  );

  socket.on("ride_started", async (payload, ack = () => {}) =>
    handleSocketEvent(socket, "ride_started", payload, ack, async () =>
      rideStateService.startRide(payload.rideId, socket.data.actorId),
    ),
  );

  socket.on("ride_completed", async (payload, ack = () => {}) =>
    handleSocketEvent(socket, "ride_completed", payload, ack, async () =>
      rideStateService.completeRide(payload.rideId, socket.data.actorId),
    ),
  );
}

async function sendDriverSnapshot(socket) {
  const [driverState, activeRideId] = await Promise.all([
    driverStateService.getDriverState(socket.data.actorId),
    driverStateService.getActiveRideId(socket.data.actorId),
  ]);

  socket.emit("snapshot", {
    eventId: uuidv4(),
    driverState,
    ride: activeRideId ? await rideStateService.getRide(activeRideId) : null,
  });
}

module.exports = {
  registerDriverSocket,
  sendDriverSnapshot,
};
