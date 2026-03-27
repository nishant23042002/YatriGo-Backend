const ACCEPT_RIDE_SCRIPT = `
local rideKey = KEYS[1]
local driverKey = KEYS[2]
local availableSet = KEYS[3]
local availableGeo = KEYS[4]
local busySet = KEYS[5]
local notifiedSet = KEYS[6]
local responsesHash = KEYS[7]
local driverActiveRideKey = KEYS[8]
local pendingDispatchesKey = KEYS[9]
local driverReservationKey = KEYS[10]

local driverId = ARGV[1]
local rideId = ARGV[2]
local acceptedAt = ARGV[3]
local reservationToken = ARGV[4]

if redis.call("exists", rideKey) == 0 then
  return -1
end

local status = redis.call("hget", rideKey, "status")
if status ~= "DISPATCHING" then
  return -2
end

local assignedDriverId = redis.call("hget", rideKey, "driverId")
if assignedDriverId and assignedDriverId ~= "" then
  return -3
end

local currentBatchId = redis.call("hget", rideKey, "currentBatchId")
if not currentBatchId or currentBatchId == "" then
  return -6
end

local currentBatchExpiresAt = redis.call("hget", rideKey, "currentBatchExpiresAt")
if currentBatchExpiresAt and currentBatchExpiresAt ~= "" and currentBatchExpiresAt < acceptedAt then
  return -7
end

if redis.call("sismember", notifiedSet, driverId) == 0 then
  return -4
end

local responseState = redis.call("hget", responsesHash, driverId)
if responseState ~= ("PENDING:" .. currentBatchId) then
  return -8
end

local currentReservation = redis.call("get", driverReservationKey)
if currentReservation ~= reservationToken then
  return -9
end

local driverStatus = redis.call("hget", driverKey, "status")
local driverActiveRideId = redis.call("get", driverActiveRideKey)
if driverStatus ~= "ONLINE" or (driverActiveRideId and driverActiveRideId ~= "" and driverActiveRideId ~= rideId) then
  return -5
end

redis.call("hset", rideKey,
  "status", "ACCEPTED",
  "driverId", driverId,
  "acceptedAt", acceptedAt,
  "updatedAt", acceptedAt,
  "currentBatchId", "",
  "currentBatchDrivers", "[]",
  "currentBatchExpiresAt", ""
)
redis.call("hincrby", rideKey, "version", 1)

redis.call("hset", driverKey,
  "status", "BUSY",
  "activeRideId", rideId,
  "updatedAt", acceptedAt
)
redis.call("srem", availableSet, driverId)
redis.call("zrem", availableGeo, driverId)
redis.call("sadd", busySet, driverId)
redis.call("set", driverActiveRideKey, rideId)
redis.call("hset", responsesHash, driverId, "ACCEPTED")
redis.call("srem", pendingDispatchesKey, rideId)
redis.call("del", driverReservationKey)

return tonumber(redis.call("hget", rideKey, "version"))
`;

module.exports = {
  ACCEPT_RIDE_SCRIPT
};
