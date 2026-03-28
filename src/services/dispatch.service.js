const { v4: uuidv4 } = require("uuid");
const { redis } = require("../config/redis");
const { env } = require("../config/env");
const { logger } = require("../config/logger");
const { keys } = require("../redis/keys");
const { ACCEPT_RIDE_SCRIPT } = require("../redis/scripts");
const { applyRejectCooldown } = require("./driver-experience.service");
const {
  enqueueBatchTimeout,
  enqueueDispatchStart,
  enqueueDriverSync,
  enqueueRideSync,
} = require("../queues");
const { AppError } = require("../utils/errors");
const { releaseLock, withLock } = require("../utils/lock");
const { RideStatus } = require("../utils/constants");
const { stringify } = require("../utils/serializers");
const driverStateService = require("./driver-state.service");
const {
  appendTimelineEvent,
  cleanupTerminalRideKeys,
  getRide,
  updateRideHash,
} = require("./ride-state.service");
const {
  notifyDriverAssigned,
  notifyRideStatusUpdate,
  notifyPushPlaceholder,
} = require("./notification.service");
const {
  emitNewRideRequest,
  emitRideAssignedToDriver,
  emitRideCancelledToDriver,
} = require("./socket-publisher.service");
const { normalizeId } = require("../utils/identity");

function isPendingResponse(value, batchId) {
  return value === `PENDING:${batchId}`;
}

function reservationToken(rideId, batchId) {
  return `${rideId}:${batchId}`;
}

async function releaseDriverReservations(rideId, batchId, driverIds = []) {
  if (!batchId || !driverIds.length) {
    return;
  }

  const token = reservationToken(rideId, batchId);
  await Promise.all(
    driverIds.map((driverId) =>
      releaseLock(redis, keys.driverReservation(driverId), token),
    ),
  );
}

async function clearPendingMembership(rideId, driverIds = []) {
  if (!driverIds.length) {
    return;
  }

  const multi = redis.multi();
  driverIds.forEach((driverId) => {
    multi.srem(keys.driverPendingDispatches(driverId), rideId);
  });
  await multi.exec();
}

async function reserveDriversForBatch(rideId, batchId, candidateDriverIds) {
  const reservedDrivers = [];
  const token = reservationToken(rideId, batchId);

  for (const driverId of candidateDriverIds) {
    const reservable = await driverStateService.isDriverReservable(driverId);
    if (!reservable) {
      continue;
    }

    const reserved = await redis.set(
      keys.driverReservation(driverId),
      token,
      "PX",
      env.dispatch.reservationTtlMs,
      "NX",
    );

    if (reserved === "OK") {
      reservedDrivers.push(driverId);
      logger.info("Driver reserved for dispatch batch", {
        rideId,
        driverId,
        batchId,
      });
    }

    if (reservedDrivers.length >= env.dispatch.batchSize) {
      break;
    }
  }

  return reservedDrivers;
}

async function dispatchUnderLock(rideId) {
  let ride = await getRide(rideId);
  if (!ride) return null;

  // 🚫 Stop invalid states
  if (
    ride.status !== RideStatus.REQUESTED &&
    ride.status !== RideStatus.DISPATCHING
  ) {
    return ride;
  }

  const now = new Date();
  const nowIso = now.toISOString();

  // ✅ Normalize numeric values
  const dispatchRound = Number(ride.dispatchRound || 0);
  const searchRadiusKm = Number(
    ride.searchRadiusKm || env.dispatch.initialSearchRadiusKm,
  );
  const maxRounds = Number(env.dispatch.maxRounds);
  const maxRadius = Number(env.dispatch.maxSearchRadiusKm);

  // 🚀 First time dispatch
  if (ride.status === RideStatus.REQUESTED) {
    ride = await updateRideHash(rideId, {
      status: RideStatus.DISPATCHING,
      dispatchRound: "1",
      searchRadiusKm: String(env.dispatch.initialSearchRadiusKm),
      updatedAt: nowIso,
      version: String(ride.version + 1),
    });

    // 🔥 Always re-fetch updated state
    ride = await getRide(rideId);

    await appendTimelineEvent(
      rideId,
      "DISPATCH_STARTED",
      "SYSTEM",
      "",
      {},
      nowIso,
    );

    await enqueueRideSync(rideId, ride.version);
    await notifyRideStatusUpdate(ride);

    logger.info("Dispatch started", {
      rideId,
      version: ride.version,
    });
  }

  // 🚫 Handle expired batch
  if (ride.status === RideStatus.DISPATCHING && ride.currentBatchId) {
    const batchExpiresAt = ride.currentBatchExpiresAt
      ? new Date(ride.currentBatchExpiresAt).getTime()
      : 0;

    if (batchExpiresAt > now.getTime()) {
      logger.debug("Active batch still running", {
        rideId,
        batchId: ride.currentBatchId,
      });
      return ride;
    }

    const responses = await redis.hgetall(keys.rideResponses(rideId));
    const currentBatchDrivers = ride.currentBatchDrivers || [];

    const multi = redis.multi();

    currentBatchDrivers.forEach((driverId) => {
      if (isPendingResponse(responses[driverId], ride.currentBatchId)) {
        multi.hset(keys.rideResponses(rideId), driverId, "TIMEOUT");
      }
    });

    multi.hset(keys.rideHash(rideId), {
      currentBatchId: "",
      currentBatchDrivers: "[]",
      currentBatchExpiresAt: "",
      updatedAt: nowIso,
      version: String(ride.version + 1),
    });

    await multi.exec();

    await releaseDriverReservations(
      rideId,
      ride.currentBatchId,
      currentBatchDrivers,
    );

    await clearPendingMembership(rideId, currentBatchDrivers);

    await appendTimelineEvent(
      rideId,
      "DISPATCH_BATCH_TIMEOUT",
      "SYSTEM",
      "",
      {
        batchId: ride.currentBatchId,
        timedOutDrivers: currentBatchDrivers,
      },
      nowIso,
    );

    ride = await getRide(rideId);
  }

  // 🚀 Find drivers
  const notifiedDrivers = new Set(
    await redis.smembers(keys.rideNotifiedDrivers(rideId)),
  );

  let candidates = [];
  try {
    candidates = await driverStateService.getNearestAvailableDrivers(
      ride.origin,
      searchRadiusKm,
      env.dispatch.batchSize * env.dispatch.candidateMultiplier,
    );
  } catch (error) {
    logger.error("GEOSEARCH_FAILED", {
      rideId,
      error: error.message,
    });
    throw error;
  }

  const batchId = uuidv4();

  const nextBatchDrivers = await reserveDriversForBatch(
    rideId,
    batchId,
    candidates.filter(
      (driverId) => !notifiedDrivers.has(driverId) || dispatchRound > 1,
    ),
  );

  // 🚫 No drivers found
  if (!nextBatchDrivers.length) {
    if (dispatchRound >= maxRounds || searchRadiusKm >= maxRadius) {
      ride = await updateRideHash(rideId, {
        status: RideStatus.CANCELLED,
        cancellation: stringify({
          actorType: "SYSTEM",
          actorId: "",
          reason: "NO_DRIVERS_AVAILABLE",
          at: nowIso,
        }),
        currentBatchId: "",
        currentBatchDrivers: "[]",
        currentBatchExpiresAt: "",
        updatedAt: nowIso,
        version: String(ride.version + 1),
      });

      await appendTimelineEvent(
        rideId,
        "DISPATCH_EXHAUSTED",
        "SYSTEM",
        "",
        { reason: "NO_DRIVERS_AVAILABLE" },
        nowIso,
      );

      await cleanupTerminalRideKeys(ride, nowIso);
      await enqueueRideSync(rideId, ride.version);

      await notifyRideStatusUpdate(ride, {
        dispatchOutcome: "NO_DRIVERS_AVAILABLE",
      });

      logger.warn("Dispatch exhausted", { rideId });

      return ride;
    }

    // 🔁 Expand search
    const nextRound = dispatchRound + 1;
    const nextRadius = Math.min(searchRadiusKm * 2, maxRadius);

    ride = await updateRideHash(rideId, {
      dispatchRound: String(nextRound),
      searchRadiusKm: String(nextRadius),
      currentBatchId: "",
      currentBatchDrivers: "[]",
      currentBatchExpiresAt: "",
      updatedAt: nowIso,
      version: String(ride.version + 1),
    });

    await appendTimelineEvent(
      rideId,
      "DISPATCH_EXPANDED",
      "SYSTEM",
      "",
      { round: nextRound, radiusKm: nextRadius },
      nowIso,
    );

    await enqueueRideSync(rideId, ride.version);

    await enqueueDispatchStart(
      rideId,
      ride.version,
      env.dispatch.retryDelayMs,
      "EXPAND_RADIUS",
    );

    logger.info("Dispatch expanded", {
      rideId,
      nextRound,
      nextRadius,
    });

    return ride;
  }

  // 🚀 Send batch
  const expiresAt = new Date(
    now.getTime() + env.dispatch.responseTtlMs,
  ).toISOString();

  const multi = redis.multi();

  multi.hset(keys.rideHash(rideId), {
    currentBatchId: batchId,
    currentBatchDrivers: stringify(nextBatchDrivers),
    currentBatchExpiresAt: expiresAt,
    updatedAt: nowIso,
    version: String(ride.version + 1),
  });

  nextBatchDrivers.forEach((driverId) => {
    multi.sadd(keys.rideNotifiedDrivers(rideId), driverId);
    multi.hset(keys.rideResponses(rideId), driverId, `PENDING:${batchId}`);
    multi.sadd(keys.driverPendingDispatches(driverId), rideId);
  });

  multi.expire(keys.rideResponses(rideId), env.redis.realtimeStateTtlSeconds);
  multi.expire(
    keys.rideNotifiedDrivers(rideId),
    env.redis.realtimeStateTtlSeconds,
  );

  await multi.exec();

  ride = await getRide(rideId);

  await appendTimelineEvent(
    rideId,
    "DISPATCH_BATCH_SENT",
    "SYSTEM",
    "",
    {
      batchId,
      drivers: nextBatchDrivers,
      expiresAt,
    },
    nowIso,
  );

  await enqueueRideSync(rideId, ride.version);
  await enqueueBatchTimeout(rideId, batchId, env.dispatch.responseTtlMs);

  // 🚀 Emit (clean)
  await Promise.all([
    ...nextBatchDrivers.map((driverId) =>
      emitNewRideRequest(driverId, {
        rideId,
        batchId,
        customerId: ride.customerId,
        origin: ride.origin,
        destination: ride.destination,
        expiresAt,
        ackRequired: true,
      }),
    ),
    ...nextBatchDrivers.map((driverId) =>
      notifyPushPlaceholder("NEW_RIDE_REQUEST", {
        driverId,
        rideId,
        batchId,
      }),
    ),
  ]);

  logger.info("Dispatch batch sent", {
    rideId,
    batchId,
    drivers: nextBatchDrivers.length,
    radiusKm: searchRadiusKm,
  });

  return ride;
}

async function startDispatch(rideId) {
  const result = await withLock(
    redis,
    keys.rideLock(rideId),
    env.redis.lockTtlMs,
    async () => dispatchUnderLock(rideId),
  );

  if (!result) {
    throw new AppError("Ride dispatch is locked, retry", 409, "RIDE_LOCKED");
  }

  return result;
}

async function handleBatchTimeout(rideId, batchId) {
  const result = await withLock(
    redis,
    keys.rideLock(rideId),
    env.redis.lockTtlMs,
    async () => {
      let ride = await getRide(rideId);
      if (
        !ride ||
        ride.status !== RideStatus.DISPATCHING ||
        ride.currentBatchId !== batchId
      ) {
        return ride;
      }

      const now = new Date().toISOString();
      const responses = await redis.hgetall(keys.rideResponses(rideId));
      const currentBatchDrivers = ride.currentBatchDrivers || [];
      const timedOutDrivers = currentBatchDrivers.filter((driverId) =>
        isPendingResponse(responses[driverId], batchId),
      );

      const multi = redis.multi();
      currentBatchDrivers.forEach((driverId) => {
        if (isPendingResponse(responses[driverId], batchId)) {
          multi.hset(keys.rideResponses(rideId), driverId, "TIMEOUT");
        }
      });
      multi.hset(keys.rideHash(rideId), {
        currentBatchId: "",
        currentBatchDrivers: "[]",
        currentBatchExpiresAt: "",
        updatedAt: now,
        version: String(ride.version + 1),
      });
      await multi.exec();
      await releaseDriverReservations(rideId, batchId, currentBatchDrivers);
      await clearPendingMembership(rideId, currentBatchDrivers);
      await appendTimelineEvent(
        rideId,
        "DISPATCH_BATCH_TIMEOUT",
        "SYSTEM",
        "",
        {
          batchId,
          timedOutDrivers,
        },
        now,
      );
      logger.warn("Dispatch batch timed out", {
        rideId,
        batchId,
        timedOutDrivers,
      });
      ride = await getRide(rideId);
      await enqueueRideSync(rideId, ride.version);
      return dispatchUnderLock(rideId);
    },
  );

  if (!result) {
    throw new AppError("Ride dispatch is locked, retry", 409, "RIDE_LOCKED");
  }

  return result;
}

async function handleDriverRejection(rideId, driverId, reason = "REJECTED") {
  const result = await withLock(
    redis,
    keys.rideLock(rideId),
    env.redis.lockTtlMs,
    async () => {
      let ride = await getRide(rideId);
      if (!ride || ride.status !== RideStatus.DISPATCHING) {
        return ride;
      }

      const now = new Date().toISOString();
      const responses = await redis.hgetall(keys.rideResponses(rideId));
      const currentBatchDrivers = ride.currentBatchDrivers || [];
      const currentBatchId = ride.currentBatchId;
      const currentResponse = responses[driverId];

      if (!currentResponse || currentResponse === "ACCEPTED") {
        return ride;
      }

      const batchExhausted = currentBatchDrivers.every((candidateDriverId) => {
        if (candidateDriverId === driverId) {
          return true;
        }

        const response = responses[candidateDriverId];
        return !isPendingResponse(response, currentBatchId);
      });

      const multi = redis.multi();
      multi.hset(keys.rideResponses(rideId), driverId, reason);
      multi.srem(keys.driverPendingDispatches(driverId), rideId);
      multi.hset(keys.rideHash(rideId), {
        currentBatchId: batchExhausted ? "" : currentBatchId,
        currentBatchDrivers: batchExhausted
          ? "[]"
          : stringify(currentBatchDrivers),
        currentBatchExpiresAt: batchExhausted
          ? ""
          : ride.currentBatchExpiresAt || "",
        updatedAt: now,
        version: String(ride.version + 1),
      });
      await multi.exec();
      await applyRejectCooldown(driverId, reason);
      await releaseDriverReservations(rideId, currentBatchId, [driverId]);
      await appendTimelineEvent(
        rideId,
        "DRIVER_REJECTED",
        "DRIVER",
        driverId,
        {
          reason,
        },
        now,
      );
      logger.info("Driver rejected dispatch", {
        rideId,
        driverId,
        reason,
        batchId: currentBatchId,
      });
      ride = await getRide(rideId);
      await enqueueRideSync(rideId, ride.version);

      if (!batchExhausted) {
        return ride;
      }

      await releaseDriverReservations(
        rideId,
        currentBatchId,
        currentBatchDrivers,
      );
      return dispatchUnderLock(rideId);
    },
  );

  if (!result) {
    throw new AppError("Ride dispatch is locked, retry", 409, "RIDE_LOCKED");
  }

  return result;
}

async function acceptRide(rideId, driverId) {
  driverId = normalizeId(driverId);
  const preAcceptRide = await getRide(rideId);
  const batchId = preAcceptRide && preAcceptRide.currentBatchId;
  const reservation = batchId ? reservationToken(rideId, batchId) : "";
  const otherDrivers = (
    (preAcceptRide && preAcceptRide.currentBatchDrivers) ||
    []
  ).filter((id) => id !== driverId);
  const acceptedAt = new Date().toISOString();
  const version = await redis.eval(
    ACCEPT_RIDE_SCRIPT,
    10,
    keys.rideHash(rideId),
    keys.driverHash(driverId),
    keys.availableDrivers(),
    keys.availableDriversGeo(),
    keys.busyDrivers(),
    keys.rideNotifiedDrivers(rideId),
    keys.rideResponses(rideId),
    keys.driverActiveRide(driverId),
    keys.driverPendingDispatches(driverId),
    keys.driverReservation(driverId),
    driverId,
    rideId,
    acceptedAt,
    reservation,
  );

  if (Number(version) <= 0) {
    logger.warn("Ride acceptance rejected by atomic guard", {
      rideId,
      driverId,
      reasonCode: Number(version),
    });
    throw new AppError(
      "Ride can no longer be accepted",
      409,
      "RIDE_ACCEPT_REJECTED",
    );
  }

  const ride = await getRide(rideId);
  await releaseDriverReservations(rideId, batchId, otherDrivers);
  await clearPendingMembership(rideId, otherDrivers);
  await appendTimelineEvent(
    rideId,
    "DRIVER_ACCEPTED",
    "DRIVER",
    driverId,
    {},
    acceptedAt,
  );
  await enqueueRideSync(rideId, ride.version);
  await enqueueDriverSync(driverId, "RIDE_ACCEPTED");
  await notifyDriverAssigned(ride);
  await notifyRideStatusUpdate(ride);
  await emitRideAssignedToDriver(driverId, ride);

  for (const otherDriverId of otherDrivers) {
    if(otherDriverId === driverId) continue;
    
    await emitRideCancelledToDriver(otherDriverId, {
      rideId,
      version: ride.version,
      reason: "ASSIGNED_TO_ANOTHER_DRIVER",
    });
  }

  logger.info("Ride assigned to driver", {
    rideId,
    driverId,
    version,
    batchId,
  });
  return ride;
}

async function markDriverUnavailableDuringDispatch(rideId, driverId, reason) {
  return handleDriverRejection(rideId, driverId, reason);
}

module.exports = {
  acceptRide,
  handleBatchTimeout,
  handleDriverRejection,
  markDriverUnavailableDuringDispatch,
  startDispatch,
};
