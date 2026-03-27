const RideStatus = Object.freeze({
  REQUESTED: "REQUESTED",
  DISPATCHING: "DISPATCHING",
  ACCEPTED: "ACCEPTED",
  ARRIVING: "ARRIVING",
  ONGOING: "ONGOING",
  COMPLETED: "COMPLETED",
  CANCELLED: "CANCELLED"
});

const DriverStatus = Object.freeze({
  ONLINE: "ONLINE",
  OFFLINE: "OFFLINE",
  BUSY: "BUSY"
});

const SocketEvents = Object.freeze({
  CUSTOMER: {
    RIDE_REQUESTED: "ride_requested",
    DRIVER_ASSIGNED: "driver_assigned",
    DRIVER_LOCATION_UPDATE: "driver_location_update",
    RIDE_STATUS_UPDATE: "ride_status_update"
  },
  DRIVER: {
    NEW_RIDE_REQUEST: "new_ride_request",
    RIDE_ASSIGNED: "ride_assigned",
    RIDE_CANCELLED: "ride_cancelled"
  }
});

const InternalSocketEvents = Object.freeze({
  SNAPSHOT: "snapshot",
  DRIVER_CONNECTION_LOST: "driver_connection_lost"
});

const QueueNames = Object.freeze({
  DISPATCH: "dispatch",
  PERSISTENCE: "persistence",
  RECONCILIATION: "reconciliation"
});

const DispatchJobs = Object.freeze({
  START_DISPATCH: "start-dispatch",
  BATCH_TIMEOUT: "batch-timeout",
  DRIVER_UNAVAILABLE: "driver-unavailable"
});

const PersistenceJobs = Object.freeze({
  SYNC_RIDE: "sync-ride",
  SYNC_DRIVER: "sync-driver"
});

const ReconciliationJobs = Object.freeze({
  STALE_DRIVER_SCAN: "stale-driver-scan",
  DISPATCH_RECOVERY_SCAN: "dispatch-recovery-scan",
  REHYDRATE_STATE: "rehydrate-state"
});

const TerminalRideStatuses = new Set([RideStatus.COMPLETED, RideStatus.CANCELLED]);

const RideTransitions = Object.freeze({
  [RideStatus.REQUESTED]: [RideStatus.DISPATCHING, RideStatus.CANCELLED],
  [RideStatus.DISPATCHING]: [RideStatus.ACCEPTED, RideStatus.CANCELLED],
  [RideStatus.ACCEPTED]: [RideStatus.ARRIVING, RideStatus.DISPATCHING, RideStatus.CANCELLED],
  [RideStatus.ARRIVING]: [RideStatus.ONGOING, RideStatus.DISPATCHING, RideStatus.CANCELLED],
  [RideStatus.ONGOING]: [RideStatus.COMPLETED, RideStatus.CANCELLED],
  [RideStatus.COMPLETED]: [],
  [RideStatus.CANCELLED]: []
});

module.exports = {
  DispatchJobs,
  DriverStatus,
  InternalSocketEvents,
  PersistenceJobs,
  QueueNames,
  ReconciliationJobs,
  RideStatus,
  RideTransitions,
  SocketEvents,
  TerminalRideStatuses
};
