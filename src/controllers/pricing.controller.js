const { estimateFare } = require("../services/pricing.service");
const { success } = require("../utils/response");

async function estimate(req, res) {
  const { distanceKm, durationMin, rideType } = req.body;

  const fare = estimateFare({
    distanceKm,
    durationMin,
    rideType,
  });

  return success(res, fare);
}

module.exports = { estimate };
