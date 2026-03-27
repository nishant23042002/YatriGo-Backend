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
  waitForEvent,
  waitForRideStatus
} = require("./lib/helpers");

runScenario("single-driver-flow", async () => {
  const redis = createRedis();
  const sockets = [];

  try {
    const driverId = createId("driver");
    const customerId = createId("customer");
    const driverSocket = await connectSocket("DRIVER", driverId);
    const customerSocket = await connectSocket("CUSTOMER", customerId);
    sockets.push(driverSocket, customerSocket);

    await runStep("driver goes online", () => emitWithAck(driverSocket, "go_online", {
      eventId: createId("evt"),
      lat: 26.9124,
      lng: 75.7873,
      metadata: { test: "single-driver-flow" }
    }));
    log("Driver online", { driverId });

    const newRideRequestPromise = waitForEvent(driverSocket, "new_ride_request");
    const rideRequestedResponse = await runStep("customer requests ride", () => emitWithAck(customerSocket, "request_ride", {
      eventId: createId("evt"),
      origin: { lat: 26.9124, lng: 75.7873, address: "Pickup" },
      destination: { lat: 26.922, lng: 75.79, address: "Drop" },
      metadata: { test: "single-driver-flow" }
    }));
    const rideId = rideRequestedResponse.data.rideId;
    log("Ride requested", { rideId });

    const offer = await newRideRequestPromise;
    ensure(offer.rideId === rideId, "Driver received wrong ride offer");

    await runStep("driver accepts ride", () => emitWithAck(driverSocket, "accept_ride", {
      eventId: createId("evt"),
      rideId
    }));
    log("Driver accepted ride", { rideId, driverId });

    await waitForRideStatus(rideId, "ACCEPTED");
    await emitWithAck(driverSocket, "location_heartbeat", {
      eventId: createId("evt"),
      lat: 26.915,
      lng: 75.788
    });
    await waitForEvent(customerSocket, "driver_location_update", {
      filter: (payload) => payload.rideId === rideId
    });

    await runStep("driver marks arriving", () => emitWithAck(driverSocket, "ride_arriving", {
      eventId: createId("evt"),
      rideId
    }));
    await waitForRideStatus(rideId, "ARRIVING");

    await runStep("driver starts ride", () => emitWithAck(driverSocket, "ride_started", {
      eventId: createId("evt"),
      rideId
    }));
    await waitForRideStatus(rideId, "ONGOING");

    await runStep("driver completes ride", () => emitWithAck(driverSocket, "ride_completed", {
      eventId: createId("evt"),
      rideId
    }));
    const completedRide = await waitForRideStatus(rideId, "COMPLETED");
    ensure(completedRide.driverId === driverId, "Completed ride has wrong driver");

    const customerActiveRide = await redis.get(`customer:${customerId}:activeRide`);
    ensure(!customerActiveRide, "Customer active ride pointer still exists after completion");

    const driverActiveRide = await redis.get(`driver:${driverId}:activeRide`);
    ensure(!driverActiveRide, "Driver active ride pointer still exists after completion");

    const fetchedRide = await fetchRide(rideId);
    ensure(fetchedRide.status === "COMPLETED", "Ride fetch did not return completed state");
    log("Single driver flow verified", {
      rideId,
      driverId,
      customerId
    });
  } finally {
    await disconnectSockets(sockets);
    await redis.quit();
  }
});
