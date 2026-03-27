const dispatchService = require("../services/dispatch.service");
const driverStateService = require("../services/driver-state.service");
const { notifyDriverLocationUpdate } = require("../services/notification.service");
const { enforceRideAcceptRateLimit } = require("../services/rate-limit.service");
const recoveryService = require("../services/recovery.service");
const rideStateService = require("../services/ride-state.service");
const { success } = require("../utils/response");

async function goOnline(req, res) {
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  const state = await driverStateService.goOnline({
    ...req.body,
    driverId
  });
  return success(res, state);
}

async function goOffline(req, res) {
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  const impact = await driverStateService.goOffline({
    ...req.body,
    driverId
  });
  await recoveryService.handleDriverDrop({
    driverId: impact.driverId,
    activeRideId: impact.activeRideId,
    pendingRideIds: impact.pendingRideIds,
    reason: req.body.reason || "MANUAL"
  });
  return success(res, impact);
}

async function heartbeat(req, res) {
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  const state = await driverStateService.heartbeat({
    ...req.body,
    driverId
  });
  if (state.activeRideId) {
    const ride = await rideStateService.getRide(state.activeRideId);
    if (ride && (await driverStateService.canTrackLocation(ride.status))) {
      await notifyDriverLocationUpdate(ride.customerId, {
        rideId: ride.rideId,
        driverId: state.driverId,
        lat: state.lat,
        lng: state.lng,
        at: state.updatedAt
      });
    }
  }
  return success(res, state);
}

async function acceptRide(req, res) {
  const { rideId } = req.params;
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  await enforceRideAcceptRateLimit(driverId);
  const ride = await dispatchService.acceptRide(rideId, driverId);
  return success(res, ride);
}

async function rejectRide(req, res) {
  const { rideId } = req.params;
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  const { reason } = req.body;
  const ride = await dispatchService.handleDriverRejection(rideId, driverId, reason || "REJECTED");
  return success(res, ride);
}

async function markArriving(req, res) {
  const { rideId } = req.params;
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  const ride = await rideStateService.markDriverArriving(rideId, driverId);
  return success(res, ride);
}

async function startRide(req, res) {
  const { rideId } = req.params;
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  const ride = await rideStateService.startRide(rideId, driverId);
  return success(res, ride);
}

async function completeRide(req, res) {
  const { rideId } = req.params;
  const driverId = (req.auth && req.auth.actorId) || req.body.driverId;
  const ride = await rideStateService.completeRide(rideId, driverId);
  return success(res, ride);
}

module.exports = {
  acceptRide,
  completeRide,
  goOffline,
  goOnline,
  heartbeat,
  markArriving,
  rejectRide,
  startRide
};
