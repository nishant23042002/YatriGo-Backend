const { Worker } = require("bullmq");
const Driver = require("../models/Driver");
const Ride = require("../models/Ride");
const { connectMongo } = require("../config/mongo");
const { createRedisConnection, redis } = require("../config/redis");
const { logger } = require("../config/logger");
const { keys } = require("../redis/keys");
const { PersistenceJobs, QueueNames } = require("../utils/constants");
const { getDriverProfileSnapshot } = require("../services/driver-profile.service");
const driverStateService = require("../services/driver-state.service");
const rideStateService = require("../services/ride-state.service");

async function syncRide(rideId) {
  const ride = await rideStateService.getRide(rideId);
  if (!ride) {
    return null;
  }

  const [timeline, notifiedDrivers] = await Promise.all([
    rideStateService.getRideTimeline(rideId),
    redis.smembers(keys.rideNotifiedDrivers(rideId))
  ]);
  const driverSnapshot = ride.driverId ? await getDriverProfileSnapshot(ride.driverId) : null;

  return Ride.findOneAndUpdate(
    { rideId },
    {
      $set: {
        customerId: ride.customerId,
        driverId: ride.driverId,
        status: ride.status,
        origin: ride.origin,
        destination: ride.destination,
        rideType: ride.rideType,
        estimatedDistanceKm: ride.estimatedDistanceKm,
        estimatedDurationMin: ride.estimatedDurationMin,
        estimatedEtaMinutes: ride.estimatedEtaMinutes,
        estimatedFare: ride.estimatedFare,
        actualDistanceKm: ride.actualDistanceKm,
        actualDurationMin: ride.actualDurationMin,
        finalFare: ride.finalFare,
        billing: ride.billing,
        commissionAmount: ride.commissionAmount,
        driverEarning: ride.driverEarning,
        driverSnapshot,
        dispatch: {
          round: ride.dispatchRound,
          radiusKm: ride.searchRadiusKm,
          currentBatchId: ride.currentBatchId,
          currentBatchDrivers: ride.currentBatchDrivers,
          currentBatchExpiresAt: ride.currentBatchExpiresAt,
          notifiedDrivers
        },
        cancellation: ride.cancellation,
        timeline,
        metadata: ride.metadata
      }
    },
    {
      upsert: true,
      new: true,
      setDefaultsOnInsert: true
    }
  );
}

async function syncDriver(driverId) {
  const driverState = await driverStateService.getDriverState(driverId);
  if (!driverState) {
    return null;
  }

  return Driver.findOneAndUpdate(
    { driverId },
    {
      $set: {
        status: driverState.status,
        activeRideId: driverState.activeRideId,
        lastKnownLocation: {
          lat: driverState.lat,
          lng: driverState.lng,
          updatedAt: driverState.updatedAt
        },
        metadata: driverState.metadata
      },
      $setOnInsert: {
        driverId,
        userId: driverId
      }
    },
    {
      upsert: true,
      new: true,
      setDefaultsOnInsert: true
    }
  );
}

async function startPersistenceWorker() {
  await connectMongo();

  const worker = new Worker(
    QueueNames.PERSISTENCE,
    async (job) => {
      logger.info("Persistence job started", {
        name: job.name,
        id: job.id,
        attemptsMade: job.attemptsMade,
        data: job.data
      });

      switch (job.name) {
        case PersistenceJobs.SYNC_RIDE:
          return syncRide(job.data.rideId);
        case PersistenceJobs.SYNC_DRIVER:
          return syncDriver(job.data.driverId);
        default:
          logger.warn("Unknown persistence job", { name: job.name });
          return null;
      }
    },
    {
      connection: createRedisConnection("worker-persistence"),
      concurrency: 20
    }
  );

  worker.on("completed", (job) => {
    logger.info("Persistence job completed", { name: job.name, id: job.id });
  });

  worker.on("failed", (job, error) => {
    logger.error("Persistence job failed", {
      id: job && job.id,
      name: job && job.name,
      attemptsMade: job && job.attemptsMade,
      message: error.message
    });
  });

  worker.on("stalled", (jobId) => {
    logger.warn("Persistence job stalled", { jobId });
  });

  return worker;
}

module.exports = startPersistenceWorker();
