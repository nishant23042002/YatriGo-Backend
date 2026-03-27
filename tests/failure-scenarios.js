const { recoveryWaitMs } = require("./lib/config");
const {
  connectSocket,
  createId,
  createRedis,
  disconnectSockets,
  emitWithAck,
  ensure,
  fetchRide,
  log,
  runStep,
  runScenario,
  sleep,
  waitForCondition,
  waitForEvent,
  waitForRideStatus
} = require("./lib/helpers");

async function duplicateAcceptScenario(redis, sockets) {
  const customerId = createId("customer-dup");
  const driverId = createId("driver-dup");
  const driverSocket = await connectSocket("DRIVER", driverId);
  const customerSocket = await connectSocket("CUSTOMER", customerId);
  sockets.push(driverSocket, customerSocket);

  await emitWithAck(driverSocket, "go_online", {
    eventId: createId("evt"),
    lat: 26.913,
    lng: 75.784
  });

  const offerPromise = waitForEvent(driverSocket, "new_ride_request");
  const ride = await emitWithAck(customerSocket, "request_ride", {
    eventId: createId("evt"),
    origin: { lat: 26.913, lng: 75.784, address: "Duplicate pickup" },
    destination: { lat: 26.92, lng: 75.79, address: "Duplicate drop" }
  });

  await offerPromise;

  const duplicateEventId = createId("dup-accept");
  const results = await runStep("send duplicate accept events", () =>
    Promise.allSettled([
      emitWithAck(driverSocket, "accept_ride", { eventId: duplicateEventId, rideId: ride.data.rideId }),
      emitWithAck(driverSocket, "accept_ride", { eventId: duplicateEventId, rideId: ride.data.rideId })
    ])
  );

  await waitForRideStatus(ride.data.rideId, "ACCEPTED");
  const successes = results.filter((entry) => entry.status === "fulfilled");
  ensure(successes.length >= 1, "Duplicate accept scenario had no successful accept");
  log("Duplicate socket event handled", {
    rideId: ride.data.rideId,
    resultStatuses: results.map((entry) => entry.status)
  });
}

async function driverDropScenario(redis, sockets) {
  const customerId = createId("customer-drop");
  const primaryDriverId = createId("driver-drop-primary");
  const backupDriverId = createId("driver-drop-backup");

  const primarySocket = await connectSocket("DRIVER", primaryDriverId);
  const backupSocket = await connectSocket("DRIVER", backupDriverId);
  const customerSocket = await connectSocket("CUSTOMER", customerId);
  sockets.push(primarySocket, backupSocket, customerSocket);

  await emitWithAck(primarySocket, "go_online", {
    eventId: createId("evt"),
    lat: 26.914,
    lng: 75.785
  });
  await emitWithAck(backupSocket, "go_online", {
    eventId: createId("evt"),
    lat: 26.9142,
    lng: 75.7852
  });

  const primaryOfferPromise = waitForEvent(primarySocket, "new_ride_request");
  const backupOfferPromise = waitForEvent(backupSocket, "new_ride_request");
  const ride = await emitWithAck(customerSocket, "request_ride", {
    eventId: createId("evt"),
    origin: { lat: 26.914, lng: 75.785, address: "Drop test pickup" },
    destination: { lat: 26.919, lng: 75.79, address: "Drop test destination" }
  });
  const rideId = ride.data.rideId;

  const firstOffer = await Promise.race([
    primaryOfferPromise.then((payload) => ({ socket: primarySocket, driverId: primaryDriverId, payload })),
    backupOfferPromise.then((payload) => ({ socket: backupSocket, driverId: backupDriverId, payload }))
  ]);

  await runStep("first notified driver goes offline", () => emitWithAck(firstOffer.socket, "go_offline", {
    eventId: createId("evt"),
    reason: "TEST_DRIVER_DROP"
  }));

  const otherDriverSocket = firstOffer.driverId === primaryDriverId ? backupSocket : primarySocket;
  const otherDriverId = firstOffer.driverId === primaryDriverId ? backupDriverId : primaryDriverId;
  const otherExistingOfferPromise =
    firstOffer.driverId === primaryDriverId ? backupOfferPromise : primaryOfferPromise;

  let otherOffer = null;
  try {
    otherOffer = await Promise.race([
      otherExistingOfferPromise,
      sleep(500).then(() => null)
    ]);
  } catch (error) {
    otherOffer = null;
  }

  if (!otherOffer || otherOffer.rideId !== rideId) {
    otherOffer = await waitForEvent(otherDriverSocket, "new_ride_request", {
      timeoutMs: recoveryWaitMs
    });
  }

  ensure(otherOffer.rideId === rideId, "Backup driver did not receive re-dispatched ride");
  await runStep("backup driver accepts re-dispatched ride", () => emitWithAck(otherDriverSocket, "accept_ride", {
    eventId: createId("evt"),
    rideId
  }));
  const acceptedRide = await waitForRideStatus(rideId, "ACCEPTED", recoveryWaitMs);
  ensure(acceptedRide.driverId === otherDriverId, "Ride was not reassigned to backup driver");
  log("Driver disconnect during dispatch recovered", {
    rideId,
    reassignedDriverId: otherDriverId
  });
}

async function redisRecoveryScenario(redis, sockets) {
  const customerId = createId("customer-recovery");
  const driverId = createId("driver-recovery");

  const driverSocket = await connectSocket("DRIVER", driverId);
  const customerSocket = await connectSocket("CUSTOMER", customerId);
  sockets.push(driverSocket, customerSocket);

  await emitWithAck(driverSocket, "go_online", {
    eventId: createId("evt"),
    lat: 26.915,
    lng: 75.786
  });

  const firstOfferPromise = waitForEvent(driverSocket, "new_ride_request");
  const ride = await emitWithAck(customerSocket, "request_ride", {
    eventId: createId("evt"),
    origin: { lat: 26.915, lng: 75.786, address: "Recovery pickup" },
    destination: { lat: 26.923, lng: 75.794, address: "Recovery drop" }
  });
  const rideId = ride.data.rideId;
  const firstOffer = await firstOfferPromise;

  await runStep("mock redis dispatch-state loss", async () => {
    await redis.hset(`ride:${rideId}:active`, {
      currentBatchId: "",
      currentBatchDrivers: "[]",
      currentBatchExpiresAt: "",
      updatedAt: new Date().toISOString()
    });
    await redis.del(`ride:${rideId}:responses`);
    await redis.del(`ride:${rideId}:notified`);
    await redis.del(`driver:${driverId}:reservation`);
  });
  log("Mocked Redis dispatch state loss", { rideId, firstBatchId: firstOffer.batchId });

  const recoveredOffer = await waitForEvent(driverSocket, "new_ride_request", {
    timeoutMs: recoveryWaitMs + 10000,
    filter: (payload) => payload.rideId === rideId && payload.batchId !== firstOffer.batchId
  });
  ensure(Boolean(recoveredOffer.batchId), "Recovered offer missing batch id");

  await runStep("driver accepts after recovery", () => emitWithAck(driverSocket, "accept_ride", {
    eventId: createId("evt"),
    rideId
  }));
  await waitForRideStatus(rideId, "ACCEPTED", recoveryWaitMs);
  log("Mock Redis restart recovery verified", {
    rideId,
    recoveredBatchId: recoveredOffer.batchId
  });
}

runScenario("failure-scenarios", async () => {
  const redis = createRedis();
  const sockets = [];

  try {
    await duplicateAcceptScenario(redis, sockets);
    await driverDropScenario(redis, sockets);
    await redisRecoveryScenario(redis, sockets);

    await waitForCondition(async () => true, "Failure scenarios complete", 50);
  } finally {
    await disconnectSockets(sockets);
    await redis.quit();
  }
});
