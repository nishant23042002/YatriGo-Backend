const {
  connectSocket,
  createId,
  emitWithAck,
  waitForEvent,
  log,
} = require("./lib/helpers");

(async () => {
  console.log("🚀 CUSTOMER LIVE TEST STARTED\n");

  const customerId = createId("customer");
  const eventId = () => createId("evt");

  let socket;

  try {
    // ================================
    // 🔌 CONNECT CUSTOMER SOCKET
    // ================================
    socket = await connectSocket("CUSTOMER", customerId);
    log("Customer connected", { customerId });

    // ================================
    // 📡 LISTEN TO ALL IMPORTANT EVENTS
    // ================================
    socket.on("ride_requested", (data) => {
      console.log("\n📦 ride_requested:", data);
    });

    socket.on("driver_assigned", (data) => {
      console.log("\n🚕 driver_assigned:", data);
    });

    socket.on("driver_location_update", (data) => {
      console.log("\n📍 driver_location_update:", data);
    });

    socket.on("ride_status_update", (data) => {
      console.log("\n🔄 ride_status_update:", data);
    });

    socket.on("driver_connection_lost", (data) => {
      console.log("\n⚠️ driver_connection_lost:", data);
    });

    socket.on("snapshot", (data) => {
      console.log("\n📸 snapshot:", data);
    });

    // ================================
    // 🚗 REQUEST RIDE
    // ================================
    console.log("\n🟡 Requesting ride...");

    const response = await emitWithAck(socket, "request_ride", {
      eventId: eventId(),
      origin: {
        lat: 26.9124,
        lng: 75.7873,
        address: "Pickup Location",
      },
      destination: {
        lat: 26.922,
        lng: 75.79,
        address: "Drop Location",
      },
      metadata: {
        source: "customer-live-test",
      },
    });

    const rideId = response.data.rideId;

    console.log("\n✅ Ride Created:", rideId);

    // ================================
    // ⏳ WAIT FOR DRIVER ASSIGNMENT
    // ================================
    console.log("\n⏳ Waiting for driver to accept...");

    await waitForEvent(socket, "driver_assigned", {
      filter: (payload) => payload.rideId === rideId,
      timeout: 30000,
    });

    console.log("\n🎉 Driver assigned successfully!");

    console.log("\n🧭 Now control flow from DRIVER APP:");
    console.log("➡ Accept ride");
    console.log("➡ Send location");
    console.log("➡ Start ride");
    console.log("➡ Complete ride\n");
  } catch (err) {
    console.error("\n❌ ERROR:", err.message || err);
  }
})();
