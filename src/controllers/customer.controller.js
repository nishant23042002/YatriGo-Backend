const { AppError } = require("../utils/errors");
const { buildCustomerRidePayload } = require("../services/notification.service");
const rideStateService = require("../services/ride-state.service");
const { enforceRideRequestRateLimit } = require("../services/rate-limit.service");
const { success } = require("../utils/response");

async function requestRide(req, res) {
  const customerId = (req.auth && req.auth.actorId) || req.body.customerId;
  await enforceRideRequestRateLimit(customerId);
  const ride = await rideStateService.requestRide({
    ...req.body,
    customerId
  });
  return success(res, await buildCustomerRidePayload(ride), 201);
}

async function cancelRide(req, res) {
  const { rideId } = req.params;
  const customerId = (req.auth && req.auth.actorId) || req.body.customerId;
  const { reason } = req.body;
  const ride = await rideStateService.cancelRide(rideId, "CUSTOMER", customerId, reason);
  return success(res, await buildCustomerRidePayload(ride));
}

async function getRide(req, res) {
  const { rideId } = req.params;
  const [ride, timeline] = await Promise.all([
    rideStateService.getRide(rideId),
    rideStateService.getRideTimeline(rideId)
  ]);

  if (!ride) {
    throw new AppError("Ride not found", 404, "RIDE_NOT_FOUND");
  }

  if (req.auth && req.auth.actorId && ride.customerId !== req.auth.actorId) {
    throw new AppError("Ride does not belong to this customer", 403, "RIDE_ACCESS_FORBIDDEN");
  }

  return success(res, { ...(await buildCustomerRidePayload(ride)), timeline });
}

module.exports = {
  cancelRide,
  getRide,
  requestRide
};
