// 🔥 CLEANED MANUAL TEST SCRIPT (PRODUCTION STYLE)

const readline = require("readline/promises");
const { stdin, stdout } = require("process");

const {
  connectSocket,
  createId,
  createRedis,
  disconnectSockets,
  emitWithAck,
  fetchRide,
} = require("./lib/helpers");

// ==============================
// 🧠 CLEAN LOG HELPERS
// ==============================

function log(msg) {
  console.log(msg);
}

function logSuccess(msg, meta = "") {
  console.log(`✅ ${msg} ${meta}`);
}

function logError(msg, meta = "") {
  console.log(`❌ ${msg} ${meta}`);
}

function logWarn(msg, meta = "") {
  console.log(`⚠️ ${msg} ${meta}`);
}

function summarizeAck(event, res) {
  if (!res) return `❌ ${event} no response`;

  if (!res.success) {
    return `❌ ${event} failed: ${res.error}`;
  }

  const d = res.data || {};

  return (
    `✅ ${event}` +
    (d.rideId ? ` rideId=${d.rideId}` : "") +
    (d.driverId ? ` driverId=${d.driverId}` : "") +
    (d.status ? ` status=${d.status}` : "")
  );
}

// ==============================
// 📋 MENU
// ==============================

function printMenu() {
  console.log("\n===== YatriGo Manual Flow =====");
  console.log("1. Connect Driver");
  console.log("2. Connect Customer");
  console.log("3. Go Online");
  console.log("4. Request Ride");
  console.log("5. Accept Ride");
  console.log("6. Heartbeat");
  console.log("7. Arriving");
  console.log("8. Start Ride");
  console.log("9. Complete Ride");
  console.log("10. Show State");
  console.log("11. Simulate Crash");
  console.log("12. Reconnect (Recovery)");
  console.log("0. Exit");
}

// ==============================
// 🧠 STATE VIEW (CLEAN)
// ==============================

async function showState(state, redis) {
  log(
    `🧠 driver=${state.driverId} customer=${state.customerId} ride=${state.rideId}`,
  );

  if (state.rideId) {
    const ride = await fetchRide(state.rideId);
    log(`🚕 ride status=${ride.status} driver=${ride.driverId}`);
  }

  if (state.driverId) {
    const activeRide = await redis.get(`driver:${state.driverId}:activeRide`);
    log(`🚗 driver activeRide=${activeRide || "NONE"}`);
  }

  if (state.customerId) {
    const ride = await redis.get(`customer:${state.customerId}:activeRide`);
    log(`👤 customer activeRide=${ride || "NONE"}`);
  }
}

// ==============================
// 📡 SOCKET LOGS (MINIMAL)
// ==============================

function attachLogs(socket, label, state) {
  socket.on("connect", () => {
    log(`🟢 ${label} connected`);
  });

  socket.on("disconnect", (r) => {
    logWarn(`${label} disconnected`, r);
  });

  socket.on("new_ride_request", (d) => {
    log(`🚕 OFFER rideId=${d.rideId}`);
    state.rideId = state.rideId || d.rideId;
  });

  socket.on("ride_assigned", (d) => {
    log(`✅ ASSIGNED rideId=${d.rideId}`);
    state.rideId = d.rideId;
  });

  socket.on("ride_status_update", (d) => {
    log(`📡 STATUS ${d.status} rideId=${d.rideId}`);
  });

  socket.on("snapshot", (d) => {
    log(
      `📸 SNAPSHOT status=${d?.driverState?.status} ride=${d?.ride?.status || "NONE"}`,
    );
  });
}

// ==============================
// 🚀 MAIN
// ==============================

async function main() {
  const rl = readline.createInterface({ input: stdin, output: stdout });
  const redis = createRedis();

  const state = {
    driverId: null,
    customerId: null,
    rideId: null,
    driverSocket: null,
    customerSocket: null,
    lat: 18.9582,
    lng: 72.8319,
  };

  try {
    let running = true;

    while (running) {
      printMenu();
      const ans = (await rl.question("Option: ")).trim();

      try {
        switch (ans) {
          case "1": {
            state.driverId = createId("driver");
            state.driverSocket = await connectSocket("DRIVER", state.driverId);
            attachLogs(state.driverSocket, "DRIVER", state);
            logSuccess("Driver connected", state.driverId);
            break;
          }

          case "2": {
            state.customerId = createId("customer");
            state.customerSocket = await connectSocket(
              "CUSTOMER",
              state.customerId,
            );
            attachLogs(state.customerSocket, "CUSTOMER", state);
            logSuccess("Customer connected", state.customerId);
            break;
          }

          case "3": {
            const res = await emitWithAck(state.driverSocket, "go_online", {
              eventId: createId("evt"),
              lat: state.lat,
              lng: state.lng,
            });
            log(summarizeAck("go_online", res));
            break;
          }

          case "4": {
            const res = await emitWithAck(
              state.customerSocket,
              "request_ride",
              {
                eventId: createId("evt"),
                origin: { lat: state.lat, lng: state.lng },
                destination: { lat: state.lat + 0.01, lng: state.lng + 0.01 },
              },
            );

            state.rideId = res.data.rideId;
            log(`🚕 Ride created ${state.rideId}`);
            break;
          }

          case "5": {
            const res = await emitWithAck(state.driverSocket, "accept_ride", {
              eventId: createId("evt"),
              rideId: state.rideId,
            });
            log(summarizeAck("accept_ride", res));
            break;
          }

          case "6": {
            state.lat += 0.0005;
            state.lng += 0.0005;

            await emitWithAck(state.driverSocket, "location_heartbeat", {
              eventId: createId("evt"),
              lat: state.lat,
              lng: state.lng,
            });

            log("❤️ heartbeat sent");
            break;
          }

          case "7": {
            const res = await emitWithAck(state.driverSocket, "ride_arriving", {
              eventId: createId("evt"),
              rideId: state.rideId,
            });
            log(summarizeAck("arriving", res));
            break;
          }

          case "8": {
            const res = await emitWithAck(state.driverSocket, "ride_started", {
              eventId: createId("evt"),
              rideId: state.rideId,
            });
            log(summarizeAck("started", res));
            break;
          }

          case "9": {
            const res = await emitWithAck(
              state.driverSocket,
              "ride_completed",
              {
                eventId: createId("evt"),
                rideId: state.rideId,
              },
            );
            log(summarizeAck("completed", res));
            break;
          }

          case "10": {
            await showState(state, redis);
            break;
          }

          // ==========================
          // 💥 CRASH
          // ==========================

          case "11": {
            logWarn("Simulating driver crash...");
            state.driverSocket.disconnect();
            state.driverSocket = null;
            await new Promise((r) => setTimeout(r, 5000));
            log("Driver offline simulated");
            break;
          }

          // ==========================
          // 🔁 RECONNECT TEST
          // ==========================

          case "12": {
            log("🔁 reconnecting...");

            state.driverSocket = await connectSocket("DRIVER", state.driverId);
            attachLogs(state.driverSocket, "DRIVER-RE", state);

            await new Promise((r) => state.driverSocket.once("snapshot", r));

            log("📸 snapshot received");

            if (!state.rideId) {
              logWarn("No ride to recover (expected if idle)");
              break;
            }

            const ride = await fetchRide(state.rideId);
            const activeRide = await redis.get(
              `driver:${state.driverId}:activeRide`,
            );

            if (ride.driverId !== state.driverId) {
              logError("wrong driver after recovery");
            } else if (activeRide !== state.rideId) {
              logError("active ride mismatch");
            } else {
              logSuccess("recovery success", `status=${ride.status}`);
            }

            break;
          }

          case "0":
            running = false;
            break;

          default:
            logWarn("Invalid option");
        }
      } catch (e) {
        logError(e.message);
      }
    }
  } finally {
    await disconnectSockets([state.driverSocket, state.customerSocket]);
    await redis.quit();
    rl.close();
    log("👋 exit");
  }
}

main();
