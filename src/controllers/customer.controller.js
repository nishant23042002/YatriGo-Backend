const { AppError } = require("../utils/errors");
const {
  buildCustomerRidePayload,
} = require("../services/notification.service");
const rideStateService = require("../services/ride-state.service");
const {
  enforceRideRequestRateLimit,
} = require("../services/rate-limit.service");
const { success } = require("../utils/response");

function resolveCustomerId(req) {
  return (
    req.auth?.actorId ||
    req.headers["x-actor-id"] ||
    req.body.customerId ||
    req.query.customerId
  );
}

async function requestRide(req, res) {
  const customerId = resolveCustomerId(req);
  await enforceRideRequestRateLimit(customerId);
  const ride = await rideStateService.requestRide({
    customerId,
    origin: req.body.pickup,
    destination: req.body.drop,
    metadata: {
      rideType: req.body.vehicleType,
      passengerCount: req.body.passengerCount,
    },
  });
  return success(res, await buildCustomerRidePayload(ride), 201);
}

async function getRideSnapshot(req, res) {
  try {
    const { rideId } = req.params;
    const customerId = resolveCustomerId(req);

    if (!customerId) {
      throw new AppError("Missing customerId", 400, "CUSTOMER_ID_REQUIRED");
    }

    console.log("🔥 SNAPSHOT API HIT", { rideId, customerId });

    const ride = await rideStateService.getRide(rideId);
    if (ride.customerId !== customerId) {
      throw new AppError("Unauthorized access", 403);
    }

    console.log("📦 RIDE DATA", ride);

    if (!ride) {
      throw new AppError("Ride not found", 404, "RIDE_NOT_FOUND");
    }

    const payload = await buildCustomerRidePayload(ride);

    console.log("✅ SNAPSHOT BUILT", payload);

    return success(res, {
      snapshot: payload,
      version: ride.version,
    });
  } catch (error) {
    console.error("❌ SNAPSHOT ERROR", error);
    throw error;
  }
}

async function cancelRide(req, res) {
  const { rideId } = req.params;
  const customerId = resolveCustomerId(req);
  if (!customerId) {
    throw new AppError("Missing customerId", 400, "CUSTOMER_ID_REQUIRED");
  }

  const { reason } = req.body;
  const ride = await rideStateService.cancelRide(
    rideId,
    "CUSTOMER",
    customerId,
    reason,
  );
  return success(res, await buildCustomerRidePayload(ride));
}

async function getRideStatus(req, res) {
  try {
    const { rideId } = req.params;
    const customerId = resolveCustomerId(req);
    if (!customerId) {
      throw new AppError("Missing customerId", 400, "CUSTOMER_ID_REQUIRED");
    }

    console.log("📊 STATUS API HIT", { rideId, customerId });

    const ride = await rideStateService.getRide(rideId);

    if (!ride) {
      return success(res, {
        rideId,
        status: "NOT_FOUND",
        version: 0,
      });
    }

    if (ride.customerId !== customerId) {
      throw new AppError(
        "Ride does not belong to this customer",
        403,
        "RIDE_ACCESS_FORBIDDEN",
      );
    }

    return success(res, {
      rideId: ride.rideId || ride._id,
      status: ride.status,
      version: ride.version,
      driverId: ride.driverId || null,
      updatedAt: ride.updatedAt,
    });
  } catch (err) {
    console.error("❌ STATUS API ERROR:", err);

    return res.status(200).json({
      success: true,
      data: {
        rideId: req.params.rideId,
        status: "ERROR",
        version: 0,
      },
    });
  }
}

async function getRide(req, res) {
  const { rideId } = req.params;
  const [ride, timeline] = await Promise.all([
    rideStateService.getRide(rideId),
    rideStateService.getRideTimeline(rideId),
  ]);

  if (!ride) {
    throw new AppError("Ride not found", 404, "RIDE_NOT_FOUND");
  }

  if (req.auth && req.auth.actorId && ride.customerId !== req.auth.actorId) {
    throw new AppError(
      "Ride does not belong to this customer",
      403,
      "RIDE_ACCESS_FORBIDDEN",
    );
  }

  return success(res, { ...(await buildCustomerRidePayload(ride)), timeline });
}

module.exports = {
  cancelRide,
  getRide,
  requestRide,
  getRideSnapshot,
  getRideStatus,
};
