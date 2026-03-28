const { Worker } = require("bullmq");
const { connectMongo } = require("../config/mongo");
const { createRedisConnection } = require("../config/redis");
const { logger } = require("../config/logger");
const { QueueNames, ReconciliationJobs } = require("../utils/constants");
const recoveryService = require("../services/recovery.service");

async function startReconciliationWorker() {
  await connectMongo();

  const worker = new Worker(
    QueueNames.RECONCILIATION,
    async (job) => {
      logger.info("Reconciliation job started", {
        name: job.name,
        id: job.id,
        attemptsMade: job.attemptsMade
      });

      switch (job.name) {
        case ReconciliationJobs.STALE_DRIVER_SCAN:
          return recoveryService.scanStaleDriversAndRecover();
        case ReconciliationJobs.DISPATCH_RECOVERY_SCAN:
          return recoveryService.recoverDispatchingRides();
        case ReconciliationJobs.REHYDRATE_STATE:
          return recoveryService.rebuildRedisStateFromMongo();
        default:
          logger.warn("Unknown reconciliation job", { name: job.name });
          return null;
      }
    },
    {
      connection: createRedisConnection("worker-reconciliation"),
      concurrency: 1
    }
  );

  worker.on("completed", (job) => {
    logger.info("Reconciliation job completed", { name: job.name, id: job.id });
  });

  worker.on("failed", (job, error) => {
    logger.error("Reconciliation job failed", {
      id: job && job.id,
      name: job && job.name,
      attemptsMade: job && job.attemptsMade,
      message: error.message
    });
  });

  worker.on("stalled", (jobId) => {
    logger.warn("Reconciliation job stalled", { jobId });
  });

  return worker;
}

module.exports = startReconciliationWorker;
