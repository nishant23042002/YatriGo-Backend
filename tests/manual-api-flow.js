const readline = require("readline/promises");
const { stdin, stdout } = require("process");
const {
  createId,
  createRedis,
  fetchRide,
  request
} = require("./lib/helpers");

function printLine(prefix, message, meta) {
  if (meta) {
    console.log(`${prefix} ${message}`, meta);
    return;
  }

  console.log(`${prefix} ${message}`);
}

function printMenu() {
  console.log("\n===== YatriGo Manual API Flow =====");
  console.log("1. Initialize Driver + Customer");
  console.log("2. Driver Go Online");
  console.log("3. Request Ride");
  console.log("4. Accept Ride");
  console.log("5. Mark Arriving");
  console.log("6. Start Ride");
  console.log("7. Complete Ride");
  console.log("8. Show Current State");
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
    lat: 26.9124,
    lng: 75.7873
  };

  try {
    let running = true;
    while (running) {
      printMenu();
      const answer = (await rl.question("Choose an option: ")).trim();

      try {
        switch (answer) {
          case "1": {
            state.driverId = createId("manual-api-driver");
            state.customerId = createId("manual-api-customer");
            printLine("🚀 [TEST]", "Initialized API actors", {
              driverId: state.driverId,
              customerId: state.customerId
            });
            break;
          }
          case "2": {
            if (!state.driverId) {
              throw new Error("Initialize actors first with option 1.");
            }

            const response = await request("POST", "/api/drivers/status/online", {
              driverId: state.driverId,
              lat: state.lat,
              lng: state.lng,
              socketId: `manual-api-${state.driverId}`,
              metadata: { source: "manual-api-flow" }
            });
            printLine("✅ [DRIVER]", "Driver went online through API", response);
            await showCurrentState(state, redis);
            break;
          }
          case "3": {
            if (!state.customerId) {
              throw new Error("Initialize actors first with option 1.");
            }

            const response = await request("POST", "/api/customers/rides/request", {
              customerId: state.customerId,
              origin: { lat: state.lat, lng: state.lng, address: "Manual API pickup" },
              destination: { lat: state.lat + 0.01, lng: state.lng + 0.01, address: "Manual API drop" },
              metadata: { source: "manual-api-flow" }
            });
            state.rideId = response.rideId;
            printLine("📤 [EVENT SENT]", "Ride requested through API", response);
            await showCurrentState(state, redis);
            break;
          }
          case "4": {
            if (!state.driverId || !state.rideId) {
              throw new Error("You need both a driver and ride before accepting.");
            }

            const response = await request("POST", `/api/drivers/rides/${state.rideId}/accept`, {
              driverId: state.driverId
            });
            printLine("✅ [DISPATCH]", "Ride accepted through API", response);
            await showCurrentState(state, redis);
            break;
          }
          case "5": {
            if (!state.rideId) {
              throw new Error("No ride available.");
            }

            const response = await request("POST", `/api/drivers/rides/${state.rideId}/arriving`, {
              driverId: state.driverId
            });
            printLine("✅ [RIDE]", "Ride marked arriving through API", response);
            await showCurrentState(state, redis);
            break;
          }
          case "6": {
            if (!state.rideId) {
              throw new Error("No ride available.");
            }

            const response = await request("POST", `/api/drivers/rides/${state.rideId}/start`, {
              driverId: state.driverId
            });
            printLine("✅ [RIDE]", "Ride started through API", response);
            await showCurrentState(state, redis);
            break;
          }
          case "7": {
            if (!state.rideId) {
              throw new Error("No ride available.");
            }

            const response = await request("POST", `/api/drivers/rides/${state.rideId}/complete`, {
              driverId: state.driverId
            });
            printLine("✅ [RIDE]", "Ride completed through API", response);
            await showCurrentState(state, redis);
            break;
          }
          case "8": {
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
    await redis.quit();
    rl.close();
    printLine("👋 [TEST]", "Manual API flow closed");
  }
}

main().catch((error) => {
  printLine("⚠️ [ERROR]", error.message);
  process.exit(1);
});
