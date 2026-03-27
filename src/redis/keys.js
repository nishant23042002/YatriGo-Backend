const keys = {
  activeRides: () => "rides:active",
  availableDrivers: () => "drivers:available",
  busyDrivers: () => "drivers:busy",
  onlineDrivers: () => "drivers:online",
  availableDriversGeo: () => "drivers:geo:available",
  socketPubSubChannel: () => "socket:broadcast",
  rideHash: (rideId) => `ride:${rideId}:active`,
  rideTimeline: (rideId) => `ride:${rideId}:timeline`,
  rideNotifiedDrivers: (rideId) => `ride:${rideId}:notified`,
  rideResponses: (rideId) => `ride:${rideId}:responses`,
  rideLock: (rideId) => `lock:ride:${rideId}`,
  driverHash: (driverId) => `driver:${driverId}:state`,
  driverHeartbeat: (driverId) => `driver:${driverId}:heartbeat`,
  driverLock: (driverId) => `lock:driver:${driverId}`,
  driverActiveRide: (driverId) => `driver:${driverId}:activeRide`,
  driverPendingDispatches: (driverId) => `driver:${driverId}:dispatches`,
  driverReservation: (driverId) => `driver:${driverId}:reservation`,
  customerActiveRide: (customerId) => `customer:${customerId}:activeRide`,
  customerLock: (customerId) => `lock:customer:${customerId}`,
  socketEventAck: (eventId) => `socket:event:ack:${eventId}`
};

module.exports = { keys };
