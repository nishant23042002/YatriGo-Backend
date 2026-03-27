const { Queue } = require("bullmq");
const { env } = require("../config/env");
const { createRedisConnection } = require("../config/redis");
const {
  DispatchJobs,
  PersistenceJobs,
  QueueNames,
  ReconciliationJobs
} = require("../utils/constants");

const dispatchConnection = createRedisConnection("queue-dispatch");
const persistenceConnection = createRedisConnection("queue-persistence");
const reconciliationConnection = createRedisConnection("queue-reconciliation");

const defaultJobOptions = {
  removeOnComplete: 500,
  removeOnFail: 500,
  attempts: 5,
  backoff: {
    type: "exponential",
    delay: 1000
  }
};

const dispatchQueue = new Queue(QueueNames.DISPATCH, {
  connection: dispatchConnection,
  defaultJobOptions
});

const persistenceQueue = new Queue(QueueNames.PERSISTENCE, {
  connection: persistenceConnection,
  defaultJobOptions
});

const reconciliationQueue = new Queue(QueueNames.RECONCILIATION, {
  connection: reconciliationConnection,
  defaultJobOptions
});

function sanitizeJobIdPart(value) {
  return String(value)
    .replace(/[:\s]/g, "_")
    .replace(/[^a-zA-Z0-9-_]/g, "-")
    .replace(/-+/g, "-")
    .replace(/_+/g, "_")
    .replace(/^[-_]+|[-_]+$/g, "") || "part";
}

function buildJobId(...parts) {
  return parts.map(sanitizeJobIdPart).join("__");
}

async function enqueueDispatchStart(rideId, version, delay = 0, reason = "STATE_CHANGE") {
  return dispatchQueue.add(
    DispatchJobs.START_DISPATCH,
    { rideId, version, reason },
    {
      jobId: buildJobId("dispatch-start", rideId, version, reason),
      delay
    }
  );
}

async function enqueueBatchTimeout(rideId, batchId, delay) {
  return dispatchQueue.add(
    DispatchJobs.BATCH_TIMEOUT,
    { rideId, batchId },
    {
      jobId: buildJobId("dispatch-timeout", rideId, batchId),
      delay
    }
  );
}

async function enqueueDriverUnavailable(rideId, driverId, reason) {
  return dispatchQueue.add(
    DispatchJobs.DRIVER_UNAVAILABLE,
    { rideId, driverId, reason },
    {
      jobId: buildJobId("dispatch-driver-unavailable", rideId, driverId, reason)
    }
  );
}

async function enqueueRideSync(rideId, version) {
  return persistenceQueue.add(
    PersistenceJobs.SYNC_RIDE,
    { rideId, version },
    {
      jobId: buildJobId("persist-ride", rideId, version)
    }
  );
}

async function enqueueDriverSync(driverId, reason, dedupeSuffix = Date.now()) {
  return persistenceQueue.add(
    PersistenceJobs.SYNC_DRIVER,
    { driverId, reason, requestedAt: new Date().toISOString() },
    {
      jobId: buildJobId("persist-driver", driverId, reason, dedupeSuffix)
    }
  );
}

async function registerRepeatableJobs() {
  await reconciliationQueue.add(
    ReconciliationJobs.STALE_DRIVER_SCAN,
    {},
    {
      jobId: ReconciliationJobs.STALE_DRIVER_SCAN,
      repeat: { every: env.driver.reconcileIntervalMs }
    }
  );

  await reconciliationQueue.add(
    ReconciliationJobs.DISPATCH_RECOVERY_SCAN,
    {},
    {
      jobId: ReconciliationJobs.DISPATCH_RECOVERY_SCAN,
      repeat: { every: env.recovery.dispatchRecoveryIntervalMs }
    }
  );

  await reconciliationQueue.add(
    ReconciliationJobs.REHYDRATE_STATE,
    {},
    {
      jobId: ReconciliationJobs.REHYDRATE_STATE
    }
  );
}

module.exports = {
  dispatchQueue,
  enqueueBatchTimeout,
  enqueueDispatchStart,
  enqueueDriverSync,
  enqueueDriverUnavailable,
  enqueueRideSync,
  persistenceQueue,
  reconciliationQueue,
  registerRepeatableJobs
};
