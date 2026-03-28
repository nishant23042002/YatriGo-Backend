const { Worker } = require("bullmq");
const { createRedisConnection } = require("../config/redis");
const { logger } = require("../config/logger");
const { DispatchJobs, QueueNames } = require("../utils/constants");
const dispatchService = require("../services/dispatch.service");

const worker = new Worker(
  QueueNames.DISPATCH,
  async (job) => {
    logger.info("Dispatch job started", {
      name: job.name,
      id: job.id,
      attemptsMade: job.attemptsMade,
      data: job.data,
    });

    switch (job.name) {
      case DispatchJobs.START_DISPATCH:
        try {
          return await dispatchService.startDispatch(job.data.rideId);
        } catch (err) {
          logger.error("Dispatch failed", {
            rideId: job.data.rideId,
            error: err.message,
          });

          return null; // prevent retry loop
        }
      case DispatchJobs.BATCH_TIMEOUT:
        return dispatchService.handleBatchTimeout(
          job.data.rideId,
          job.data.batchId,
        );
      case DispatchJobs.DRIVER_UNAVAILABLE:
        return dispatchService.markDriverUnavailableDuringDispatch(
          job.data.rideId,
          job.data.driverId,
          job.data.reason,
        );
      default:
        logger.warn("Unknown dispatch job", { name: job.name });
        return null;
    }
  },
  {
    connection: createRedisConnection("worker-dispatch"),
    concurrency: 20,
  },
);

worker.on("completed", (job) => {
  logger.info("Dispatch job completed", { name: job.name, id: job.id });
});

worker.on("failed", (job, error) => {
  logger.error("Dispatch job failed", {
    id: job && job.id,
    name: job && job.name,
    attemptsMade: job && job.attemptsMade,
    message: error.message,
  });
});

worker.on("stalled", (jobId) => {
  logger.warn("Dispatch job stalled", { jobId });
});

module.exports = worker;
