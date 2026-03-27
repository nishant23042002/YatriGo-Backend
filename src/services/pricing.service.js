const { env } = require("../config/env");
const { normalizeRideType } = require("./eta.service");

const rideTypeFareMultiplier = Object.freeze({
  STANDARD: 1,
  PREMIUM: 1.5,
  AUTO: 0.85
});

function roundCurrency(value) {
  return Number(Number(value || 0).toFixed(2));
}

function resolveMultiplier(rideType) {
  const normalizedRideType = normalizeRideType(rideType);
  return {
    normalizedRideType,
    multiplier: rideTypeFareMultiplier[normalizedRideType] || 1
  };
}

function estimateFare({ distanceKm, durationMin, rideType, waitingMin = 0 }) {
  const { normalizedRideType, multiplier } = resolveMultiplier(rideType);
  const baseFare = roundCurrency(env.pricing.baseFare * multiplier);
  const distanceCharge = roundCurrency(distanceKm * env.pricing.perKm * multiplier);
  const durationCharge = roundCurrency(durationMin * env.pricing.perMinute * multiplier);
  const waitingCharge = roundCurrency(waitingMin * env.pricing.waitingPerMinute * multiplier);
  const subtotal = roundCurrency(baseFare + distanceCharge + durationCharge + waitingCharge);
  const totalFare = Math.max(subtotal, env.pricing.minimumFare);
  const minimumApplied = totalFare > subtotal;
  const commissionAmount = roundCurrency((totalFare * env.pricing.commissionPercent) / 100);
  const driverEarning = roundCurrency(totalFare - commissionAmount);

  return {
    currency: env.pricing.currency,
    rideType: normalizedRideType,
    distanceKm: roundCurrency(distanceKm),
    durationMin: roundCurrency(durationMin),
    waitingMin: roundCurrency(waitingMin),
    baseFare,
    distanceCharge,
    durationCharge,
    waitingCharge,
    minimumFare: roundCurrency(env.pricing.minimumFare),
    minimumApplied,
    totalFare: roundCurrency(totalFare),
    commissionPercent: roundCurrency(env.pricing.commissionPercent),
    commissionAmount,
    driverEarning
  };
}

function calculateFinalFare({ ride, actualDistanceKm, actualDurationMin, waitingMin = 0 }) {
  const rideType = (ride && ride.rideType) || (ride && ride.metadata && ride.metadata.rideType);

  return estimateFare({
    distanceKm: actualDistanceKm != null ? actualDistanceKm : ride.estimatedDistanceKm || 0,
    durationMin: actualDurationMin != null ? actualDurationMin : ride.estimatedDurationMin || 0,
    waitingMin,
    rideType
  });
}

module.exports = {
  calculateFinalFare,
  estimateFare
};
