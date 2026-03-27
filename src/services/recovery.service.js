const Ride = require("../models/Ride");
const Driver = require("../models/Driver");
const { redis } = require("../config/redis");
const { env } = require("../config/env");
const { logger } = require("../config/logger");
const { keys } = require("../redis/keys");
const { enqueueBatchTimeout, enqueueDispatchStart } = require("../queues");
const { DriverStatus, RideStatus, TerminalRideStatuses } = require("../utils/constants");
const { stringify } = require("../utils/serializers");
const driverStateService = require("./driver-state.service");
const dispatchService = require("./dispatch.service");
const rideStateService = require("./ride-state.service");
const { emitDriverConnectionLost } = require("./socket-publisher.service");

async function handleDriverDrop({ driverId, activeRideId, pendingRideIds = [], reason }) {
  for (const rideId of pendingRideIds) {
    await dispatchService.markDriverUnavailableDuringDispatch(rideId, driverId, reason);
  }

  if (!activeRideId) {
    return;
  }

  const ride = await rideStateService.getRide(activeRideId);
  if (!ride) {
    return;
  }

  if ([RideStatus.ACCEPTED, RideStatus.ARRIVING].includes(ride.status) && ride.driverId === driverId) {
    await rideStateService.requeueRideAfterDriverLoss(activeRideId, driverId, reason);
    return;
  }

  if (ride.status === RideStatus.ONGOING && ride.driverId === driverId) {
    await emitDriverConnectionLost(ride.customerId, {
      rideId: ride.rideId,
      driverId,
      at: new Date().toISOString(),
      reason
    });
  }
}

async function scanStaleDriversAndRecover() {
  const staleDrivers = await driverStateService.scanStaleDrivers();

  for (const staleDriver of staleDrivers) {
    await handleDriverDrop({
      driverId: staleDriver.driverId,
      activeRideId: staleDriver.activeRideId,
      pendingRideIds: staleDriver.pendingRideIds,
      reason: "HEARTBEAT_TIMEOUT"
    });
  }

  return staleDrivers.length;
}

async function recoverDispatchingRides() {
  const rideIds = await redis.smembers(keys.activeRides());
  const now = Date.now();
  let recovered = 0;

  for (const rideId of rideIds) {
    const ride = await rideStateService.getRide(rideId);
    if (!ride) {
      await redis.srem(keys.activeRides(), rideId);
      logger.warn("Removed ghost ride id from active ride set", { rideId });
      continue;
    }

    if (ride.status !== RideStatus.DISPATCHING) {
      continue;
    }

    if (!ride.currentBatchId) {
      await enqueueDispatchStart(rideId, ride.version, 0, "RECOVERY_NO_BATCH");
      recovered += 1;
      continue;
    }

    if (ride.currentBatchExpiresAt && new Date(ride.currentBatchExpiresAt).getTime() <= now) {
      await enqueueBatchTimeout(rideId, ride.currentBatchId, 0);
      recovered += 1;
    }
  }

  return recovered;
}

async function rebuildRedisStateFromMongo() {
  const activeRides = await Ride.find({
    status: { $nin: Array.from(TerminalRideStatuses) }
  }).lean();

  for (const ride of activeRides) {
    const rideExists = await redis.exists(keys.rideHash(ride.rideId));
    if (rideExists) {
      continue;
    }

    const now = new Date().toISOString();
    const dispatch = ride.dispatch || {};
    const multi = redis.multi();
    multi.hset(keys.rideHash(ride.rideId), {
      rideId: ride.rideId,
      customerId: ride.customerId,
      driverId: ride.driverId || "",
      status: ride.status,
      origin: stringify(ride.origin),
      destination: stringify(ride.destination),
      rideType: ride.rideType || "STANDARD",
      estimatedDistanceKm: String(ride.estimatedDistanceKm || 0),
      estimatedDurationMin: String(ride.estimatedDurationMin || 0),
      estimatedEtaMinutes: String(ride.estimatedEtaMinutes || 0),
      estimatedFare: ride.estimatedFare ? stringify(ride.estimatedFare) : "",
      actualDistanceKm: String(ride.actualDistanceKm || 0),
      actualDurationMin: String(ride.actualDurationMin || 0),
      finalFare: ride.finalFare ? stringify(ride.finalFare) : "",
      billing: ride.billing ? stringify(ride.billing) : "",
      commissionAmount: String(ride.commissionAmount || 0),
      driverEarning: String(ride.driverEarning || 0),
      dispatchRound: String(dispatch.round || 0),
      searchRadiusKm: String(dispatch.radiusKm || env.dispatch.initialSearchRadiusKm),
      currentBatchId: dispatch.currentBatchId || "",
      currentBatchDrivers: stringify(dispatch.currentBatchDrivers || []),
      currentBatchExpiresAt: dispatch.currentBatchExpiresAt
        ? new Date(dispatch.currentBatchExpiresAt).toISOString()
        : "",
      acceptedAt: "",
      cancellation: ride.cancellation ? stringify(ride.cancellation) : "",
      createdAt: new Date(ride.createdAt).toISOString(),
      updatedAt: new Date(ride.updatedAt || ride.createdAt).toISOString(),
      metadata: stringify(ride.metadata || {}),
      version: "1"
    });
    multi.set(keys.customerActiveRide(ride.customerId), ride.rideId);
    multi.sadd(keys.activeRides(), ride.rideId);

    (dispatch.notifiedDrivers || []).forEach((driverId) => {
      multi.sadd(keys.rideNotifiedDrivers(ride.rideId), driverId);
    });

    if (ride.driverId) {
      multi.set(keys.driverActiveRide(ride.driverId), ride.rideId);
      multi.hset(keys.driverHash(ride.driverId), {
        driverId: ride.driverId,
        status: DriverStatus.BUSY,
        activeRideId: ride.rideId,
        updatedAt: now
      });
      multi.sadd(keys.busyDrivers(), ride.driverId);
    }

    for (const timelineItem of ride.timeline || []) {
      multi.rpush(keys.rideTimeline(ride.rideId), stringify(timelineItem));
    }

    await multi.exec();
  }

  const drivers = await Driver.find({ status: { $ne: DriverStatus.OFFLINE } }).lean();
  for (const driver of drivers) {
    const activeRideId = await redis.get(keys.driverActiveRide(driver.driverId));
    if (activeRideId) {
      continue;
    }

    const lat = driver.lastKnownLocation && driver.lastKnownLocation.lat != null
      ? String(driver.lastKnownLocation.lat)
      : "";
    const lng = driver.lastKnownLocation && driver.lastKnownLocation.lng != null
      ? String(driver.lastKnownLocation.lng)
      : "";
    const multi = redis.multi();
    multi.hset(keys.driverHash(driver.driverId), {
      driverId: driver.driverId,
      status: driver.status || DriverStatus.OFFLINE,
      activeRideId: driver.activeRideId || "",
      lat,
      lng,
      updatedAt: new Date().toISOString(),
      metadata: stringify(driver.metadata || {})
    });

    if (driver.status === DriverStatus.ONLINE && lat && lng && !driver.activeRideId) {
      multi.sadd(keys.onlineDrivers(), driver.driverId);
      multi.sadd(keys.availableDrivers(), driver.driverId);
      multi.zrem(keys.busyDrivers(), driver.driverId);
      multi.geoadd(keys.availableDriversGeo(), lng, lat, driver.driverId);
    } else if (driver.status === DriverStatus.BUSY || driver.activeRideId) {
      multi.sadd(keys.onlineDrivers(), driver.driverId);
      multi.sadd(keys.busyDrivers(), driver.driverId);
      multi.srem(keys.availableDrivers(), driver.driverId);
      multi.zrem(keys.availableDriversGeo(), driver.driverId);
    }

    await multi.exec();
  }

  logger.info("Redis state rehydrated from Mongo", {
    rides: activeRides.length,
    drivers: drivers.length
  });
}

module.exports = {
  handleDriverDrop,
  rebuildRedisStateFromMongo,
  recoverDispatchingRides,
  scanStaleDriversAndRecover
};
