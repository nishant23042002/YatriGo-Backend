const { raceDriverCount } = require("./lib/config");
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

runScenario("multi-driver-race", async () => {
  const redis = createRedis();
  const sockets = [];

  try {
    const customerId = createId("customer");
    const customerSocket = await connectSocket("CUSTOMER", customerId);
    sockets.push(customerSocket);

    const drivers = [];
    for (let index = 0; index < raceDriverCount; index += 1) {
      const driverId = createId(`driver-${index}`);
      const socket = await connectSocket("DRIVER", driverId);
      sockets.push(socket);
      drivers.push({ driverId, socket });
      await emitWithAck(socket, "go_online", {
        eventId: createId("evt"),
        lat: 26.9124 + index * 0.0002,
        lng: 75.7873 + index * 0.0002,
        metadata: { test: "multi-driver-race" }
      });
    }

    const offerPromises = drivers.map((driver) =>
      waitForEvent(driver.socket, "new_ride_request", {
        timeoutMs: 15000
      }).then((offer) => ({ driverId: driver.driverId, socket: driver.socket, offer }))
    );

    const rideResponse = await runStep("customer requests race ride", () => emitWithAck(customerSocket, "request_ride", {
      eventId: createId("evt"),
      origin: { lat: 26.9124, lng: 75.7873, address: "Race pickup" },
      destination: { lat: 26.918, lng: 75.792, address: "Race drop" },
      metadata: { test: "multi-driver-race" }
    }));
    const rideId = rideResponse.data.rideId;

    const receivedOffers = (await Promise.allSettled(offerPromises))
      .filter((entry) => entry.status === "fulfilled")
      .map((entry) => entry.value)
      .filter((entry) => entry.offer.rideId === rideId);

    ensure(receivedOffers.length >= 1, "No driver received the race test offer");
    log("Drivers received offer", {
      rideId,
      notifiedDrivers: receivedOffers.map((entry) => entry.driverId),
      totalOnlineDrivers: raceDriverCount
    });

    const acceptResults = await runStep("simultaneous driver accepts", () =>
      Promise.allSettled(
        receivedOffers.map((entry) =>
          emitWithAck(entry.socket, "accept_ride", {
            eventId: createId("evt"),
            rideId
          }).then(() => entry.driverId)
        )
      )
    );

    await waitForRideStatus(rideId, "ACCEPTED");
    const acceptedRide = await fetchRide(rideId);
    const winningDriverId = acceptedRide.driverId;
    ensure(Boolean(winningDriverId), "Race test ride does not have an assigned driver");

    const successfulAccepts = acceptResults.filter((entry) => entry.status === "fulfilled");
    ensure(successfulAccepts.length === 1, `Expected exactly one successful accept, got ${successfulAccepts.length}`);

    const conflictingAssignments = await Promise.all(
      drivers.map(async ({ driverId }) => ({
        driverId,
        activeRideId: await redis.get(`driver:${driverId}:activeRide`)
      }))
    );
    const assignedDrivers = conflictingAssignments.filter((entry) => entry.activeRideId === rideId);
    ensure(assignedDrivers.length === 1, "More than one driver is marked active for the same ride");
    ensure(assignedDrivers[0].driverId === winningDriverId, "Winning driver does not match Redis state");

    log("Multi-driver race verified", {
      rideId,
      winningDriverId,
      attemptedAccepts: receivedOffers.length
    });
  } finally {
    await disconnectSockets(sockets);
    await redis.quit();
  }
});
