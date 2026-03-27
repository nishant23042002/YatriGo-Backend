const { redis } = require("../config/redis");
const { env } = require("../config/env");
const { logger } = require("../config/logger");
const { keys } = require("../redis/keys");
const { AppError } = require("../utils/errors");
const { releaseLock, withLock } = require("../utils/lock");
const { DriverStatus, RideStatus } = require("../utils/constants");
const { parseJson, stringify, toNumber } = require("../utils/serializers");
const { isDriverOnRejectCooldown } = require("./driver-experience.service");
const { enqueueDriverSync } = require("../queues");

function toDriverSnapshot(driverId, hash = {}) {
  if (!hash.status) {
    return null;
  }

  return {
    driverId,
    status: hash.status,
    lat: toNumber(hash.lat, null),
    lng: toNumber(hash.lng, null),
    socketId: hash.socketId || null,
    activeRideId: hash.activeRideId || null,
    lastHeartbeatAt: hash.lastHeartbeatAt || null,
    updatedAt: hash.updatedAt || null,
    metadata: parseJson(hash.metadata, {})
  };
}

async function getDriverState(driverId) {
  const hash = await redis.hgetall(keys.driverHash(driverId));
  return toDriverSnapshot(driverId, hash);
}

async function persistDriverRealtimeState(driverId, state) {
  const multi = redis.multi();

  multi.hset(keys.driverHash(driverId), {
    driverId,
    status: state.status,
    lat: state.lat != null ? String(state.lat) : "",
    lng: state.lng != null ? String(state.lng) : "",
    socketId: state.socketId || "",
    activeRideId: state.activeRideId || "",
    lastHeartbeatAt: state.lastHeartbeatAt,
    updatedAt: state.updatedAt,
    metadata: stringify(state.metadata)
  });

  multi.sadd(keys.onlineDrivers(), driverId);
  multi.set(
    keys.driverHeartbeat(driverId),
    state.lastHeartbeatAt,
    "EX",
    env.driver.heartbeatTtlSeconds
  );

  if (state.status === DriverStatus.ONLINE) {
    multi.sadd(keys.availableDrivers(), driverId);
    multi.srem(keys.busyDrivers(), driverId);
    if (state.lat != null && state.lng != null) {
      multi.geoadd(keys.availableDriversGeo(), state.lng, state.lat, driverId);
    }
  } else {
    multi.srem(keys.availableDrivers(), driverId);
    multi.zrem(keys.availableDriversGeo(), driverId);
    if (state.status === DriverStatus.BUSY) {
      multi.sadd(keys.busyDrivers(), driverId);
    } else {
      multi.srem(keys.busyDrivers(), driverId);
    }
  }

  if (state.activeRideId) {
    multi.set(keys.driverActiveRide(driverId), state.activeRideId);
  } else {
    multi.del(keys.driverActiveRide(driverId));
  }

  await multi.exec();
  logger.debug("Driver realtime state persisted in Redis", {
    driverId,
    status: state.status,
    activeRideId: state.activeRideId || null
  });
}

async function cleanupGhostAvailability(driverId) {
  const multi = redis.multi();
  multi.srem(keys.availableDrivers(), driverId);
  multi.srem(keys.busyDrivers(), driverId);
  multi.srem(keys.onlineDrivers(), driverId);
  multi.zrem(keys.availableDriversGeo(), driverId);
  multi.del(keys.driverHeartbeat(driverId));
  multi.del(keys.driverReservation(driverId));
  await multi.exec();

  logger.warn("Ghost driver cleaned from Redis indexes", { driverId });
}

async function goOnline({ driverId, lat, lng, socketId, metadata = {} }) {
  const result = await withLock(
    redis,
    keys.driverLock(driverId),
    env.redis.lockTtlMs,
    async () => {
      const existing = (await getDriverState(driverId)) || {};
      const activeRideId =
        (await redis.get(keys.driverActiveRide(driverId))) || existing.activeRideId || "";
      const now = new Date().toISOString();
      const snapshot = {
        driverId,
        status: activeRideId ? DriverStatus.BUSY : DriverStatus.ONLINE,
        lat,
        lng,
        socketId,
        activeRideId,
        lastHeartbeatAt: now,
        updatedAt: now,
        metadata: { ...existing.metadata, ...metadata }
      };

      await persistDriverRealtimeState(driverId, snapshot);
      logger.info("Driver went online", { driverId, activeRideId });
      return snapshot;
    }
  );

  if (!result) {
    throw new AppError("Driver state is busy, retry", 409, "DRIVER_LOCKED");
  }

  await enqueueDriverSync(driverId, "ONLINE", result.updatedAt);
  return result;
}

async function heartbeat({ driverId, lat, lng, socketId }) {
  const result = await withLock(
    redis,
    keys.driverLock(driverId),
    env.redis.lockTtlMs,
    async () => {
      const existing = await getDriverState(driverId);
      if (!existing) {
        throw new AppError("Driver is not registered online", 404, "DRIVER_NOT_ONLINE");
      }

      const now = new Date().toISOString();
      const snapshot = {
        ...existing,
        lat,
        lng,
        socketId: socketId || existing.socketId,
        lastHeartbeatAt: now,
        updatedAt: now
      };

      await persistDriverRealtimeState(driverId, snapshot);
      return snapshot;
    }
  );

  if (!result) {
    throw new AppError("Driver heartbeat lock busy, retry", 409, "DRIVER_LOCKED");
  }

  await enqueueDriverSync(driverId, "HEARTBEAT", result.updatedAt);
  logger.debug("Driver heartbeat accepted", {
    driverId,
    activeRideId: result.activeRideId || null,
    lat: result.lat,
    lng: result.lng
  });
  return result;
}

async function goOffline({ driverId, reason = "MANUAL" }) {
  const result = await withLock(
    redis,
    keys.driverLock(driverId),
    env.redis.lockTtlMs,
    async () => {
      const existing = await getDriverState(driverId);
      const pendingRideIds = await redis.smembers(keys.driverPendingDispatches(driverId));
      const activeRideId =
        (await redis.get(keys.driverActiveRide(driverId))) ||
        (existing ? existing.activeRideId : null);
      const now = new Date().toISOString();

      const multi = redis.multi();
      multi.srem(keys.onlineDrivers(), driverId);
      multi.srem(keys.availableDrivers(), driverId);
      multi.srem(keys.busyDrivers(), driverId);
      multi.zrem(keys.availableDriversGeo(), driverId);
      multi.del(keys.driverHeartbeat(driverId));
      multi.del(keys.driverReservation(driverId));
      multi.hset(keys.driverHash(driverId), {
        driverId,
        status: DriverStatus.OFFLINE,
        lat: existing && existing.lat != null ? String(existing.lat) : "",
        lng: existing && existing.lng != null ? String(existing.lng) : "",
        socketId: "",
        activeRideId: activeRideId || "",
        lastHeartbeatAt: now,
        updatedAt: now,
        metadata: stringify({
          ...(existing ? existing.metadata : {}),
          lastOfflineReason: reason
        })
      });
      await multi.exec();

      logger.warn("Driver went offline", { driverId, reason, activeRideId, pendingRideIds });

      return {
        driverId,
        activeRideId,
        pendingRideIds,
        previousStatus: existing ? existing.status : DriverStatus.OFFLINE
      };
    }
  );

  if (!result) {
    throw new AppError("Driver state is busy, retry", 409, "DRIVER_LOCKED");
  }

  await enqueueDriverSync(driverId, "OFFLINE", new Date().toISOString());
  return result;
}

async function restoreDriverSession(driverId, socketId) {
  const result = await withLock(
    redis,
    keys.driverLock(driverId),
    env.redis.lockTtlMs,
    async () => {
      const existing = await getDriverState(driverId);
      if (!existing || existing.status === DriverStatus.OFFLINE) {
        return null;
      }

      const now = new Date().toISOString();
      const snapshot = {
        ...existing,
        socketId,
        lastHeartbeatAt: now,
        updatedAt: now
      };

      await persistDriverRealtimeState(driverId, snapshot);
      logger.info("Driver session restored after reconnect", {
        driverId,
        status: snapshot.status
      });
      return snapshot;
    }
  );

  if (!result) {
    return null;
  }

  await enqueueDriverSync(driverId, "RECONNECT", result.updatedAt);
  return result;
}

async function getNearestAvailableDrivers(origin, radiusKm, count) {
  const response = await redis.call(
    "GEOSEARCH",
    keys.availableDriversGeo(),
    "FROMLONLAT",
    origin.lng,
    origin.lat,
    "BYRADIUS",
    radiusKm,
    "km",
    "ASC",
    "COUNT",
    count
  );

  return Array.isArray(response) ? response : [];
}

async function scanStaleDrivers() {
  const onlineDrivers = await redis.smembers(keys.onlineDrivers());
  const staleDrivers = [];

  for (const driverId of onlineDrivers) {
    const heartbeatExists = await redis.exists(keys.driverHeartbeat(driverId));
    if (heartbeatExists) {
      continue;
    }

    staleDrivers.push(await goOffline({ driverId, reason: "HEARTBEAT_TIMEOUT" }));
  }

  return staleDrivers;
}

async function isDriverReservable(driverId) {
  const [driverState, activeRideId, reservation, heartbeatExists, rejectCooldown] = await Promise.all([
    getDriverState(driverId),
    redis.get(keys.driverActiveRide(driverId)),
    redis.get(keys.driverReservation(driverId)),
    redis.exists(keys.driverHeartbeat(driverId)),
    isDriverOnRejectCooldown(driverId)
  ]);

  if (!driverState) {
    await cleanupGhostAvailability(driverId);
    return false;
  }

  if (!heartbeatExists) {
    await cleanupGhostAvailability(driverId);
    return false;
  }

  if (driverState.status !== DriverStatus.ONLINE || activeRideId || reservation || rejectCooldown) {
    return false;
  }

  return true;
}

async function clearDriverReservation(driverId, token) {
  return releaseLock(redis, keys.driverReservation(driverId), token);
}

async function getPendingDispatches(driverId) {
  return redis.smembers(keys.driverPendingDispatches(driverId));
}

async function getActiveRideId(driverId) {
  return redis.get(keys.driverActiveRide(driverId));
}

async function canTrackLocation(status) {
  return [RideStatus.ACCEPTED, RideStatus.ARRIVING, RideStatus.ONGOING].includes(status);
}

module.exports = {
  canTrackLocation,
  clearDriverReservation,
  cleanupGhostAvailability,
  getActiveRideId,
  getDriverState,
  getNearestAvailableDrivers,
  getPendingDispatches,
  goOffline,
  goOnline,
  heartbeat,
  isDriverReservable,
  restoreDriverSession,
  scanStaleDrivers
};
