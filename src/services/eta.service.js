const { env } = require("../config/env");

const rideTypeSpeedMultiplier = Object.freeze({
  STANDARD: 1,
  PREMIUM: 0.95,
  AUTO: 0.85
});

function normalizeRideType(rideType) {
  return String(rideType || env.pricing.defaultRideType || "STANDARD").toUpperCase();
}

function toRadians(value) {
  return (value * Math.PI) / 180;
}

function haversineDistanceKm(origin, destination) {
  const earthRadiusKm = 6371;
  const latDistance = toRadians(destination.lat - origin.lat);
  const lngDistance = toRadians(destination.lng - origin.lng);
  const originLat = toRadians(origin.lat);
  const destinationLat = toRadians(destination.lat);

  const a =
    Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
    Math.cos(originLat) *
      Math.cos(destinationLat) *
      Math.sin(lngDistance / 2) *
      Math.sin(lngDistance / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return Number((earthRadiusKm * c).toFixed(2));
}

function estimateEtaMinutes(distanceKm, rideType) {
  const normalizedRideType = normalizeRideType(rideType);
  const multiplier = rideTypeSpeedMultiplier[normalizedRideType] || 1;
  const averageSpeedKmph = Math.max(env.eta.averageSpeedKmph * multiplier, 8);
  const durationMinutes = (distanceKm / averageSpeedKmph) * 60;
  return Math.max(1, Math.ceil(durationMinutes));
}

function estimateTrip({ origin, destination, rideType }) {
  const normalizedRideType = normalizeRideType(rideType);
  const distanceKm = haversineDistanceKm(origin, destination);
  const estimatedDurationMin = estimateEtaMinutes(distanceKm, normalizedRideType);

  return {
    rideType: normalizedRideType,
    estimatedDistanceKm: distanceKm,
    estimatedDurationMin,
    estimatedEtaMinutes: estimatedDurationMin
  };
}

module.exports = {
  estimateEtaMinutes,
  estimateTrip,
  haversineDistanceKm,
  normalizeRideType
};
