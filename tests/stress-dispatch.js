const { stressDriverCount, stressRideCount } = require("./lib/config");
const {
  connectSocket,
  createId,
  createRedis,
  disconnectSockets,
  emitWithAck,
  ensure,
  fetchRide,
  log,
  request,
  runStep,
  runScenario,
  sleep,
  waitForCondition
} = require("./lib/helpers");

runScenario("stress-dispatch", async () => {
  const redis = createRedis();
  const sockets = [];

  try {
    const drivers = [];
    const acceptedRideIds = new Set();
    const driverAcceptedCounts = new Map();

    for (let index = 0; index < stressDriverCount; index += 1) {
      const driverId = createId(`stress-driver-${index}`);
      const socket = await connectSocket("DRIVER", driverId);
      sockets.push(socket);
      drivers.push({ driverId, socket, busy: false });

      socket.on("new_ride_request", async (payload) => {
        try {
          await emitWithAck(socket, "event_ack", { eventId: payload.eventId });
          if (acceptedRideIds.has(payload.rideId)) {
            return;
          }

          const shouldAccept = Math.random() < 0.9;
          await sleep(Math.floor(Math.random() * 300));

          if (shouldAccept) {
            const response = await emitWithAck(socket, "accept_ride", {
              eventId: createId("evt"),
              rideId: payload.rideId
            }).catch(() => null);

            if (response && response.data && response.data.driverId === driverId) {
              acceptedRideIds.add(payload.rideId);
              driverAcceptedCounts.set(
                driverId,
                (driverAcceptedCounts.get(driverId) || 0) + 1
              );

              await sleep(400 + Math.floor(Math.random() * 200));
              await emitWithAck(socket, "ride_started", {
                eventId: createId("evt"),
                rideId: payload.rideId
              }).catch(() => null);
              await sleep(400 + Math.floor(Math.random() * 200));
              await emitWithAck(socket, "ride_completed", {
                eventId: createId("evt"),
                rideId: payload.rideId
              }).catch(() => null);
            }
            return;
          }

          await emitWithAck(socket, "reject_ride", {
            eventId: createId("evt"),
            rideId: payload.rideId,
            reason: "TEST_REJECT"
          }).catch(() => null);
        } catch (error) {
          return null;
        }
      });

      await emitWithAck(socket, "go_online", {
        eventId: createId("evt"),
        lat: 26.91 + index * 0.0005,
        lng: 75.78 + index * 0.0005,
        metadata: { test: "stress-dispatch" }
      });
    }

    const rideRequests = [];
    for (let index = 0; index < stressRideCount; index += 1) {
      rideRequests.push(
        request("POST", "/api/customers/rides/request", {
          customerId: createId(`stress-customer-${index}`),
          origin: {
            lat: 26.91 + (index % 10) * 0.001,
            lng: 75.78 + (index % 10) * 0.001,
            address: `Stress pickup ${index}`
          },
          destination: {
            lat: 26.95 + (index % 10) * 0.001,
            lng: 75.82 + (index % 10) * 0.001,
            address: `Stress drop ${index}`
          },
          metadata: { test: "stress-dispatch", index }
        })
      );
    }

    const rides = await runStep("create concurrent ride requests", () => Promise.all(rideRequests));
    log("Stress requests created", {
      requestedRides: rides.length,
      onlineDrivers: stressDriverCount
    });

    await runStep("wait for stress rides to settle", () => waitForCondition(
      async () => {
        const fetched = await Promise.all(rides.map((ride) => fetchRide(ride.rideId)));
        const active = fetched.filter((ride) => !["COMPLETED", "CANCELLED"].includes(ride.status));
        return active.length < rides.length * 0.8 ? fetched : null;
      },
      "Stress test rides did not settle enough to validate state",
      90000
    ));

    const finalStates = await Promise.all(rides.map((ride) => fetchRide(ride.rideId)));
    const assignedByDriver = new Map();

    for (const ride of finalStates) {
      if (!ride.driverId) {
        continue;
      }

      const existing = assignedByDriver.get(ride.driverId) || [];
      existing.push(ride.rideId);
      assignedByDriver.set(ride.driverId, existing);
    }

    for (const [driverId, rideIds] of assignedByDriver.entries()) {
      ensure(
        rideIds.length <= 1 || finalStates.filter((ride) => ride.driverId === driverId && !["COMPLETED", "CANCELLED"].includes(ride.status)).length <= 1,
        `Driver ${driverId} has multiple non-terminal rides`
      );
    }

    const activeRideIds = finalStates
      .filter((ride) => !["COMPLETED", "CANCELLED"].includes(ride.status))
      .map((ride) => ride.rideId);
    const duplicateActiveAssignments = [];

    for (const rideId of activeRideIds) {
      const matches = [];
      for (const driver of drivers) {
        const activeRideId = await redis.get(`driver:${driver.driverId}:activeRide`);
        if (activeRideId === rideId) {
          matches.push(driver.driverId);
        }
      }

      if (matches.length > 1) {
        duplicateActiveAssignments.push({ rideId, matches });
      }
    }

    ensure(duplicateActiveAssignments.length === 0, "Duplicate active driver assignments detected");
    log("Stress dispatch verified", {
      requestedRides: rides.length,
      acceptedRides: acceptedRideIds.size,
      duplicateActiveAssignments: duplicateActiveAssignments.length
    });
  } finally {
    await disconnectSockets(sockets);
    await redis.quit();
  }
});
