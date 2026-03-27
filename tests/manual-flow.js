const readline = require("readline/promises");
const { stdin, stdout } = require("process");
const {
  connectSocket,
  createId,
  createRedis,
  disconnectSockets,
  emitWithAck,
  fetchRide
} = require("./lib/helpers");

function printLine(prefix, message, meta) {
  if (meta) {
    console.log(`${prefix} ${message}`, meta);
    return;
  }

  console.log(`${prefix} ${message}`);
}

function printMenu() {
  console.log("\n===== YatriGo Manual Socket Flow =====");
  console.log("1. Connect Driver");
  console.log("2. Connect Customer");
  console.log("3. Driver Go Online");
  console.log("4. Request Ride");
  console.log("5. Accept Ride");
  console.log("6. Send Location Update");
  console.log("7. Mark Arriving");
  console.log("8. Start Ride");
  console.log("9. Complete Ride");
  console.log("10. Show Current State");
  console.log("0. Exit");
}

async function showCurrentState(state, redis) {
  printLine("🧠 [STATE]", "Current identifiers", {
    driverId: state.driverId,
    customerId: state.customerId,
    rideId: state.rideId
  });

  if (state.rideId) {
    try {
      const ride = await fetchRide(state.rideId);
      printLine("🚕 [RIDE]", "Ride state", ride);
    } catch (error) {
      printLine("⚠️ [RIDE]", `Unable to fetch ride state: ${error.message}`);
    }
  }

  if (state.driverId) {
    const [driverHash, activeRideId, reservation, pendingDispatches] = await Promise.all([
      redis.hgetall(`driver:${state.driverId}:state`),
      redis.get(`driver:${state.driverId}:activeRide`),
      redis.get(`driver:${state.driverId}:reservation`),
      redis.smembers(`driver:${state.driverId}:dispatches`)
    ]);

    printLine("🚗 [DRIVER]", "Driver Redis state", {
      ...driverHash,
      activeRideId,
      reservation,
      pendingDispatches
    });
  }

  if (state.customerId) {
    const customerRideId = await redis.get(`customer:${state.customerId}:activeRide`);
    printLine("👤 [CUSTOMER]", "Customer active ride pointer", {
      customerId: state.customerId,
      activeRideId: customerRideId
    });
  }
}

function attachRealtimeLogs(socket, label, state) {
  socket.on("connect", () => {
    printLine("🚀 [SOCKET CONNECT]", `${label} connected`, {
      socketId: socket.id
    });
  });

  socket.on("disconnect", (reason) => {
    printLine("⚠️ [SOCKET DISCONNECT]", `${label} disconnected`, { reason });
  });

  socket.onAny((event, payload) => {
    printLine("📥 [EVENT RECEIVED]", `${label} -> ${event}`, payload);
    if (event === "new_ride_request" && payload && payload.rideId) {
      state.rideId = state.rideId || payload.rideId;
      state.lastOffer = payload;
    }
    if (event === "ride_assigned" && payload && payload.rideId) {
      state.rideId = payload.rideId;
    }
  });
}

async function main() {
  const rl = readline.createInterface({
    input: stdin,
    output: stdout
  });
  const redis = createRedis();
  const state = {
    driverId: null,
    customerId: null,
    rideId: null,
    driverSocket: null,
    customerSocket: null,
    lastOffer: null,
    lat: 26.9124,
    lng: 75.7873
  };

  async function ensureDriverSocket() {
    if (!state.driverSocket) {
      throw new Error("Driver is not connected yet. Choose option 1 first.");
    }
  }

  async function ensureCustomerSocket() {
    if (!state.customerSocket) {
      throw new Error("Customer is not connected yet. Choose option 2 first.");
    }
  }

  try {
    let running = true;
    while (running) {
      printMenu();
      const answer = (await rl.question("Choose an option: ")).trim();

      try {
        switch (answer) {
          case "1": {
            if (state.driverSocket) {
              printLine("⚠️ [TEST]", "Driver is already connected", {
                driverId: state.driverId
              });
              break;
            }

            state.driverId = createId("manual-driver");
            state.driverSocket = await connectSocket("DRIVER", state.driverId);
            attachRealtimeLogs(state.driverSocket, "DRIVER", state);
            printLine("🚀 [SOCKET CONNECT]", `Driver connected: ${state.driverId}`);
            break;
          }
          case "2": {
            if (state.customerSocket) {
              printLine("⚠️ [TEST]", "Customer is already connected", {
                customerId: state.customerId
              });
              break;
            }

            state.customerId = createId("manual-customer");
            state.customerSocket = await connectSocket("CUSTOMER", state.customerId);
            attachRealtimeLogs(state.customerSocket, "CUSTOMER", state);
            printLine("🚀 [SOCKET CONNECT]", `Customer connected: ${state.customerId}`);
            break;
          }
          case "3": {
            await ensureDriverSocket();
            const response = await emitWithAck(state.driverSocket, "go_online", {
              eventId: createId("evt"),
              lat: state.lat,
              lng: state.lng,
              metadata: { source: "manual-flow" }
            });
            printLine("✅ [DRIVER]", "Driver went online", response.data);
            await showCurrentState(state, redis);
            break;
          }
          case "4": {
            await ensureCustomerSocket();
            const response = await emitWithAck(state.customerSocket, "request_ride", {
              eventId: createId("evt"),
              origin: { lat: state.lat, lng: state.lng, address: "Manual pickup" },
              destination: { lat: state.lat + 0.01, lng: state.lng + 0.01, address: "Manual drop" },
              metadata: { source: "manual-flow" }
            });
            state.rideId = response.data.rideId;
            printLine("📤 [EVENT SENT]", "Ride request sent", response.data);
            await showCurrentState(state, redis);
            break;
          }
          case "5": {
            await ensureDriverSocket();
            const rideId = state.rideId || (state.lastOffer && state.lastOffer.rideId);
            if (!rideId) {
              throw new Error("No ride is available to accept yet.");
            }

            const response = await emitWithAck(state.driverSocket, "accept_ride", {
              eventId: createId("evt"),
              rideId
            });
            state.rideId = rideId;
            printLine("✅ [DISPATCH]", "Ride accepted", response.data);
            await showCurrentState(state, redis);
            break;
          }
          case "6": {
            await ensureDriverSocket();
            state.lat += 0.0008;
            state.lng += 0.0008;
            const response = await emitWithAck(state.driverSocket, "location_heartbeat", {
              eventId: createId("evt"),
              lat: state.lat,
              lng: state.lng
            });
            printLine("📤 [EVENT SENT]", "Location update sent", response.data);
            await showCurrentState(state, redis);
            break;
          }
          case "7": {
            await ensureDriverSocket();
            if (!state.rideId) {
              throw new Error("No active ride to mark arriving.");
            }

            const response = await emitWithAck(state.driverSocket, "ride_arriving", {
              eventId: createId("evt"),
              rideId: state.rideId
            });
            printLine("✅ [RIDE]", "Ride marked arriving", response.data);
            await showCurrentState(state, redis);
            break;
          }
          case "8": {
            await ensureDriverSocket();
            if (!state.rideId) {
              throw new Error("No active ride to start.");
            }

            const response = await emitWithAck(state.driverSocket, "ride_started", {
              eventId: createId("evt"),
              rideId: state.rideId
            });
            printLine("✅ [RIDE]", "Ride started", response.data);
            await showCurrentState(state, redis);
            break;
          }
          case "9": {
            await ensureDriverSocket();
            if (!state.rideId) {
              throw new Error("No active ride to complete.");
            }

            const response = await emitWithAck(state.driverSocket, "ride_completed", {
              eventId: createId("evt"),
              rideId: state.rideId
            });
            printLine("✅ [RIDE]", "Ride completed", response.data);
            await showCurrentState(state, redis);
            break;
          }
          case "10": {
            await showCurrentState(state, redis);
            break;
          }
          case "0": {
            running = false;
            break;
          }
          default:
            printLine("⚠️ [TEST]", "Invalid option. Try again.");
        }
      } catch (error) {
        printLine("⚠️ [ERROR]", error.message);
      }
    }
  } finally {
    await disconnectSockets([state.driverSocket, state.customerSocket]);
    await redis.quit();
    rl.close();
    printLine("👋 [TEST]", "Manual socket flow closed");
  }
}

main().catch((error) => {
  printLine("⚠️ [ERROR]", error.message);
  process.exit(1);
});
