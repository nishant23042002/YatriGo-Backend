const { v4: uuidv4 } = require("uuid");
const { redis } = require("../config/redis");
const { env } = require("../config/env");
const { logger } = require("../config/logger");
const { keys } = require("../redis/keys");
const { AppError } = require("../utils/errors");
const { releaseLock, withLock } = require("../utils/lock");
const {
  DriverStatus,
  RideStatus,
  RideTransitions,
  TerminalRideStatuses,
} = require("../utils/constants");
const { parseJson, stringify, toNumber } = require("../utils/serializers");
const {
  enqueueDispatchStart,
  enqueueDriverSync,
  enqueueRideSync,
} = require("../queues");
const { estimateTrip, normalizeRideType } = require("./eta.service");
const {
  notifyRideCancelled,
  notifyRideCompleted,
  notifyRideRequested,
  notifyRideStarted,
  notifyRideStatusUpdate,
  notifyDriverArriving,
} = require("./notification.service");
const { calculateFinalFare, estimateFare } = require("./pricing.service");
const { emitRideCancelledToDriver } = require("./socket-publisher.service");

async function refreshRideRealtimeKeys(rideId) {
  const multi = redis.multi();
  multi.expire(keys.rideHash(rideId), env.redis.realtimeStateTtlSeconds);
  multi.expire(keys.rideTimeline(rideId), env.redis.realtimeStateTtlSeconds);
  multi.expire(
    keys.rideNotifiedDrivers(rideId),
    env.redis.realtimeStateTtlSeconds,
  );
  multi.expire(keys.rideResponses(rideId), env.redis.realtimeStateTtlSeconds);
  await multi.exec();
  logger.debug("Ride realtime TTL refreshed", { rideId });
}

function toRideSnapshot(hash = {}) {
  if (!hash.status) {
    return null;
  }

  return {
    rideId: hash.rideId,
    customerId: hash.customerId,
    driverId: hash.driverId || null,
    status: hash.status,
    origin: parseJson(hash.origin, {}),
    destination: parseJson(hash.destination, {}),
    dispatchRound: toNumber(hash.dispatchRound, 0),
    searchRadiusKm: toNumber(hash.searchRadiusKm, 0),
    rideType: hash.rideType || null,
    estimatedDistanceKm: toNumber(hash.estimatedDistanceKm, 0),
    estimatedDurationMin: toNumber(hash.estimatedDurationMin, 0),
    estimatedEtaMinutes: toNumber(hash.estimatedEtaMinutes, 0),
    estimatedFare: parseJson(hash.estimatedFare, null),
    actualDistanceKm: toNumber(hash.actualDistanceKm, 0),
    actualDurationMin: toNumber(hash.actualDurationMin, 0),
    finalFare: parseJson(hash.finalFare, null),
    billing: parseJson(hash.billing, null),
    commissionAmount: toNumber(hash.commissionAmount, 0),
    driverEarning: toNumber(hash.driverEarning, 0),
    currentBatchId: hash.currentBatchId || null,
    currentBatchDrivers: parseJson(hash.currentBatchDrivers, []),
    currentBatchExpiresAt: hash.currentBatchExpiresAt || null,
    acceptedAt: hash.acceptedAt || null,
    cancellation: parseJson(hash.cancellation, null),
    createdAt: hash.createdAt || null,
    updatedAt: hash.updatedAt || null,
    metadata: parseJson(hash.metadata, {}),
    version: toNumber(hash.version, 0),
  };
}

async function appendTimelineEvent(
  rideId,
  event,
  actorType = "SYSTEM",
  actorId = "",
  metadata = {},
  at = new Date().toISOString(),
) {
  await redis.rpush(
    keys.rideTimeline(rideId),
    stringify({
      event,
      actorType,
      actorId,
      at,
      metadata,
    }),
  );
  await refreshRideRealtimeKeys(rideId);
}

async function getRideTimeline(rideId) {
  const items = await redis.lrange(keys.rideTimeline(rideId), 0, -1);
  return items.map((entry) => parseJson(entry, {}));
}

async function getRide(rideId) {
  const hash = await redis.hgetall(keys.rideHash(rideId));
  return toRideSnapshot(hash);
}

function assertTransition(currentStatus, nextStatus) {
  const allowed = RideTransitions[currentStatus] || [];
  if (!allowed.includes(nextStatus)) {
    throw new AppError(
      `Invalid transition from ${currentStatus} to ${nextStatus}`,
      409,
      "INVALID_RIDE_TRANSITION",
    );
  }
}

function resolveActualDurationMinutes(ride, nowIso) {
  const acceptedAt = ride.acceptedAt || ride.createdAt;
  if (!acceptedAt) {
    return ride.estimatedDurationMin || 0;
  }

  const durationMs =
    new Date(nowIso).getTime() - new Date(acceptedAt).getTime();
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    return ride.estimatedDurationMin || 0;
  }

  return Number((durationMs / 60000).toFixed(2));
}

async function releaseDriverAvailability(driverId, rideId, now) {
  if (!driverId) {
    return;
  }

  const driverHash = await redis.hgetall(keys.driverHash(driverId));
  const driverStatus = driverHash.status || DriverStatus.OFFLINE;
  const driverActiveRide = await redis.get(keys.driverActiveRide(driverId));
  const multi = redis.multi();

  multi.srem(keys.busyDrivers(), driverId);
  multi.srem(keys.driverPendingDispatches(driverId), rideId);

  if (driverActiveRide === rideId) {
    multi.del(keys.driverActiveRide(driverId));
  }

  if (driverStatus === DriverStatus.OFFLINE) {
    multi.hset(keys.driverHash(driverId), {
      activeRideId: "",
      updatedAt: now,
      status: DriverStatus.OFFLINE,
    });
  } else {
    multi.hset(keys.driverHash(driverId), {
      activeRideId: "",
      updatedAt: now,
      status: DriverStatus.ONLINE,
    });
    multi.sadd(keys.onlineDrivers(), driverId);
    multi.sadd(keys.availableDrivers(), driverId);
    if (driverHash.lat && driverHash.lng) {
      multi.geoadd(
        keys.availableDriversGeo(),
        driverHash.lng,
        driverHash.lat,
        driverId,
      );
    }
  }

  await multi.exec();
}

async function cleanupTerminalRideKeys(ride, now) {
  const notifiedDrivers = await redis.smembers(
    keys.rideNotifiedDrivers(ride.rideId),
  );
  const currentCustomerRide = await redis.get(
    keys.customerActiveRide(ride.customerId),
  );
  const multi = redis.multi();

  multi.srem(keys.activeRides(), ride.rideId);
  if (currentCustomerRide === ride.rideId) {
    multi.del(keys.customerActiveRide(ride.customerId));
  }

  multi.expire(keys.rideHash(ride.rideId), env.redis.activeRideTtlSeconds);
  multi.expire(keys.rideTimeline(ride.rideId), env.redis.activeRideTtlSeconds);
  multi.expire(
    keys.rideNotifiedDrivers(ride.rideId),
    env.redis.activeRideTtlSeconds,
  );
  multi.expire(keys.rideResponses(ride.rideId), env.redis.activeRideTtlSeconds);
  notifiedDrivers.forEach((driverId) => {
    multi.srem(keys.driverPendingDispatches(driverId), ride.rideId);
  });
  await multi.exec();

  if (ride.currentBatchId) {
    const token = `${ride.rideId}:${ride.currentBatchId}`;
    await Promise.all(
      (ride.currentBatchDrivers || []).map((driverId) =>
        releaseLock(redis, keys.driverReservation(driverId), token),
      ),
    );
  }

  if (ride.driverId) {
    await releaseDriverAvailability(ride.driverId, ride.rideId, now);
    await enqueueDriverSync(ride.driverId, `RIDE_${ride.status}`);
  }

  logger.info("Ride terminal cleanup completed", {
    rideId: ride.rideId,
    status: ride.status,
    driverId: ride.driverId || null,
  });
}

async function updateRideHash(rideId, fields) {
  await redis.hset(keys.rideHash(rideId), fields);
  await refreshRideRealtimeKeys(rideId);
  logger.debug("Ride hash updated in Redis", {
    rideId,
    fields: Object.keys(fields),
  });
  return getRide(rideId);
}

async function requestRide({ customerId, origin, destination, metadata = {} }) {
  const result = await withLock(
    redis,
    keys.customerLock(customerId),
    env.redis.lockTtlMs,
    async () => {
      const activeRideId = await redis.get(keys.customerActiveRide(customerId));
      if (activeRideId) {
        const existingRide = await getRide(activeRideId);
        if (existingRide && !TerminalRideStatuses.has(existingRide.status)) {
          throw new AppError(
            "Customer already has an active ride",
            409,
            "ACTIVE_RIDE_EXISTS",
          );
        }
      }

      const now = new Date().toISOString();
      const rideId = uuidv4();
      const rideType = normalizeRideType(metadata.rideType);
      const tripEstimate = estimateTrip({
        origin,
        destination,
        rideType,
      });
      const estimatedFare = estimateFare({
        distanceKm: tripEstimate.estimatedDistanceKm,
        durationMin: tripEstimate.estimatedDurationMin,
        rideType,
      });
      const payload = {
        rideId,
        customerId,
        driverId: "",
        status: RideStatus.REQUESTED,
        origin: stringify(origin),
        destination: stringify(destination),
        dispatchRound: "0",
        searchRadiusKm: String(env.dispatch.initialSearchRadiusKm),
        rideType,
        estimatedDistanceKm: String(tripEstimate.estimatedDistanceKm),
        estimatedDurationMin: String(tripEstimate.estimatedDurationMin),
        estimatedEtaMinutes: String(tripEstimate.estimatedEtaMinutes),
        estimatedFare: stringify(estimatedFare),
        actualDistanceKm: "",
        actualDurationMin: "",
        finalFare: "",
        billing: "",
        commissionAmount: "",
        driverEarning: "",
        currentBatchId: "",
        currentBatchDrivers: "[]",
        currentBatchExpiresAt: "",
        acceptedAt: "",
        cancellation: "",
        createdAt: now,
        updatedAt: now,
        metadata: stringify({
          ...metadata,
          rideType,
        }),
        version: "1",
      };

      const multi = redis.multi();
      multi.hset(keys.rideHash(rideId), payload);
      multi.set(keys.customerActiveRide(customerId), rideId);
      multi.sadd(keys.activeRides(), rideId);
      multi.expire(keys.rideHash(rideId), env.redis.realtimeStateTtlSeconds);
      await multi.exec();
      await appendTimelineEvent(
        rideId,
        "RIDE_REQUESTED",
        "CUSTOMER",
        customerId,
        {
          origin,
          destination,
        },
        now,
      );
      await refreshRideRealtimeKeys(rideId);

      const ride = toRideSnapshot(payload);
      await enqueueRideSync(rideId, ride.version);
      await notifyRideRequested(ride);
      await enqueueDispatchStart(rideId, ride.version, 0, "REQUESTED");
      logger.info("Ride request persisted to Redis and queued for dispatch", {
        rideId,
        customerId,
      });
      return ride;
    },
  );

  if (!result) {
    throw new AppError("Customer lock is busy, retry", 409, "CUSTOMER_LOCKED");
  }

  logger.info("Ride requested", { rideId: result.rideId, customerId });
  return result;
}

async function transitionRide(rideId, nextStatus, options = {}) {
  const result = await withLock(
    redis,
    keys.rideLock(rideId),
    env.redis.lockTtlMs,
    async () => {
      const ride = await getRide(rideId);
      if (!ride) {
        throw new AppError("Ride not found", 404, "RIDE_NOT_FOUND");
      }

      assertTransition(ride.status, nextStatus);
      const now = new Date().toISOString();
      const update = {
        status: nextStatus,
        updatedAt: now,
        version: String(ride.version + 1),
      };

      if (options.driverId !== undefined) {
        update.driverId = options.driverId || "";
      }

      if (nextStatus === RideStatus.CANCELLED) {
        update.cancellation = stringify({
          actorType: options.actorType,
          actorId: options.actorId,
          reason: options.reason,
          at: now,
        });
      }

      if (nextStatus === RideStatus.COMPLETED) {
        const actualDistanceKm =
          toNumber(
            options.actualDistanceKm != null
              ? options.actualDistanceKm
              : ride.actualDistanceKm,
            ride.estimatedDistanceKm,
          ) || ride.estimatedDistanceKm;
        const actualDurationMin =
          toNumber(
            options.actualDurationMin != null
              ? options.actualDurationMin
              : ride.actualDurationMin,
            resolveActualDurationMinutes(ride, now),
          ) || resolveActualDurationMinutes(ride, now);
        const waitingMin = toNumber(options.waitingMin, 0);
        const finalFare = calculateFinalFare({
          ride,
          actualDistanceKm,
          actualDurationMin,
          waitingMin,
        });

        update.actualDistanceKm = String(actualDistanceKm);
        update.actualDurationMin = String(actualDurationMin);
        update.finalFare = stringify(finalFare);
        update.billing = stringify({
          currency: finalFare.currency,
          totalFare: finalFare.totalFare,
          baseFare: finalFare.baseFare,
          distanceCharge: finalFare.distanceCharge,
          durationCharge: finalFare.durationCharge,
          waitingCharge: finalFare.waitingCharge,
        });
        update.commissionAmount = String(finalFare.commissionAmount);
        update.driverEarning = String(finalFare.driverEarning);
      }

      await redis.hset(keys.rideHash(rideId), update);
      await refreshRideRealtimeKeys(rideId);
      await appendTimelineEvent(
        rideId,
        `RIDE_${nextStatus}`,
        options.actorType || "SYSTEM",
        options.actorId || "",
        options.metadata || {},
        now,
      );

      const snapshot = await getRide(rideId);
      const notifiedDrivers =
        nextStatus === RideStatus.CANCELLED
          ? await redis.smembers(keys.rideNotifiedDrivers(rideId))
          : [];

      if (TerminalRideStatuses.has(nextStatus)) {
        await cleanupTerminalRideKeys(snapshot, now);
      }

      await enqueueRideSync(rideId, snapshot.version);
      if (nextStatus === RideStatus.ARRIVING) {
        await notifyDriverArriving(snapshot);
      } else if (nextStatus === RideStatus.ONGOING) {
        await notifyRideStarted(snapshot);
      } else if (nextStatus === RideStatus.COMPLETED) {
        await notifyRideCompleted(snapshot);
      } else if (nextStatus === RideStatus.CANCELLED) {
        await notifyRideCancelled(snapshot);
      } else {
        await notifyRideStatusUpdate(snapshot);
      }
      logger.info("Ride state transitioned", {
        rideId,
        previousStatus: ride.status,
        nextStatus,
        actorType: options.actorType || "SYSTEM",
        actorId: options.actorId || "",
      });

      if (nextStatus === RideStatus.CANCELLED) {
        const impactedDrivers = new Set(
          [...notifiedDrivers, snapshot.driverId].filter(Boolean),
        );
        for (const driverId of impactedDrivers) {
          await emitRideCancelledToDriver(driverId, snapshot);
        }
      }

      return snapshot;
    },
  );

  if (!result) {
    throw new AppError("Ride is busy, retry", 409, "RIDE_LOCKED");
  }

  return result;
}

async function cancelRide(rideId, actorType, actorId, reason) {
  const ride = await getRide(rideId);
  if (!ride) {
    throw new AppError("Ride not found", 404, "RIDE_NOT_FOUND");
  }

  if (TerminalRideStatuses.has(ride.status)) {
    return ride;
  }

  return transitionRide(rideId, RideStatus.CANCELLED, {
    actorType,
    actorId,
    reason,
    metadata: { reason },
  });
}

async function markDriverArriving(rideId, driverId) {
  const ride = await getRide(rideId);
  if (!ride || ride.driverId !== driverId) {
    throw new AppError(
      "Driver is not assigned to this ride",
      409,
      "RIDE_DRIVER_MISMATCH",
    );
  }

  return transitionRide(rideId, RideStatus.ARRIVING, {
    actorType: "DRIVER",
    actorId: driverId,
  });
}

async function startRide(rideId, driverId) {
  const ride = await getRide(rideId);
  if (!ride || ride.driverId !== driverId) {
    throw new AppError(
      "Driver is not assigned to this ride",
      409,
      "RIDE_DRIVER_MISMATCH",
    );
  }

  return transitionRide(rideId, RideStatus.ONGOING, {
    actorType: "DRIVER",
    actorId: driverId,
  });
}

async function completeRide(rideId, driverId) {
  const ride = await getRide(rideId);
  if (!ride || ride.driverId !== driverId) {
    throw new AppError(
      "Driver is not assigned to this ride",
      409,
      "RIDE_DRIVER_MISMATCH",
    );
  }

  return transitionRide(rideId, RideStatus.COMPLETED, {
    actorType: "DRIVER",
    actorId: driverId,
  });
}

async function requeueRideAfterDriverLoss(rideId, driverId, reason) {
  const result = await withLock(
    redis,
    keys.rideLock(rideId),
    env.redis.lockTtlMs,
    async () => {
      const ride = await getRide(rideId);
      if (!ride) {
        return null;
      }

      if (![RideStatus.ACCEPTED, RideStatus.ARRIVING].includes(ride.status)) {
        return ride;
      }

      if (ride.driverId !== driverId) {
        return ride;
      }

      const now = new Date().toISOString();
      await releaseDriverAvailability(driverId, rideId, now);

      const multi = redis.multi();
      multi.del(keys.rideNotifiedDrivers(rideId));
      multi.del(keys.rideResponses(rideId));
      multi.hset(keys.rideHash(rideId), {
        status: RideStatus.DISPATCHING,
        driverId: "",
        acceptedAt: "",
        dispatchRound: "0",
        searchRadiusKm: String(env.dispatch.initialSearchRadiusKm),
        currentBatchId: "",
        currentBatchDrivers: "[]",
        currentBatchExpiresAt: "",
        updatedAt: now,
        version: String(ride.version + 1),
      });
      multi.expire(keys.rideHash(rideId), env.redis.realtimeStateTtlSeconds);
      await multi.exec();

      await appendTimelineEvent(
        rideId,
        "RIDE_REQUEUED",
        "SYSTEM",
        driverId,
        { reason },
        now,
      );

      const updated = await getRide(rideId);
      await enqueueRideSync(rideId, updated.version);
      await notifyRideStatusUpdate(updated, { requeuedReason: reason });
      await enqueueDispatchStart(
        rideId,
        updated.version,
        env.dispatch.retryDelayMs,
        "REQUEUE",
      );
      return updated;
    },
  );

  if (!result) {
    throw new AppError("Ride is busy, retry", 409, "RIDE_LOCKED");
  }

  return result;
}

module.exports = {
  appendTimelineEvent,
  cancelRide,
  cleanupTerminalRideKeys,
  completeRide,
  refreshRideRealtimeKeys,
  getRide,
  getRideTimeline,
  markDriverArriving,
  requestRide,
  requeueRideAfterDriverLoss,
  startRide,
  toRideSnapshot,
  transitionRide,
  updateRideHash,
};
