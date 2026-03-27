# YatriGo Flow Diagrams

This file describes the live control flow of the current YatriGo backend using text-based diagrams. The goal is to make debugging and incident analysis faster.

## 1. Single Driver Flow

```text
Customer socket/API -> request_ride
  ->
Optional auth gate:
  if AUTH_ENABLED=true verify JWT and map token -> actorId
  if AUTH_ENABLED=false trust provided role/actorId for dev/test
  ->
Rate limit gate:
  increment rate-limit:customer:request-ride:{customerId}
  reject before ride creation if threshold exceeded
  ->
ride-state.service.requestRide(customerId, origin, destination)
  ->
Acquire lock:customer:{customerId}
  ->
Check customer:{customerId}:activeRide
  ->
Compute estimatedDistanceKm and estimatedDurationMin
Compute estimatedEtaMinutes
Compute estimatedFare and normalized rideType
  ->
Create ride:{rideId}:active status=REQUESTED version=1
Store estimatedDistanceKm / estimatedDurationMin / estimatedEtaMinutes / estimatedFare / rideType
Create customer:{customerId}:activeRide = rideId
Add rideId to rides:active
Append ride:{rideId}:timeline -> RIDE_REQUESTED
Queue sync-ride
Publish customer event ride_requested with ETA and fare estimate
Queue start-dispatch
  ->
dispatch.worker consumes start-dispatch
  ->
dispatch.service.startDispatch(rideId)
  ->
Acquire lock:ride:{rideId}
  ->
REQUESTED -> DISPATCHING
Append timeline -> DISPATCH_STARTED
Queue sync-ride
Publish customer event ride_status_update
  ->
Query drivers:geo:available by origin and radius
  ->
Skip drivers currently under reject cooldown when checking reservable pool
  ->
Reserve driver using driver:{driverId}:reservation = rideId:batchId (NX, PX)
  ->
Update ride:{rideId}:active:
  currentBatchId
  currentBatchDrivers
  currentBatchExpiresAt
  version++
Update ride:{rideId}:notified
Update ride:{rideId}:responses = PENDING:{batchId}
Update driver:{driverId}:dispatches += rideId
Queue batch-timeout
Publish driver event new_ride_request
  ->
Driver socket -> accept_ride
  ->
dispatch.service.acceptRide(rideId, driverId)
  ->
Redis Lua script validates:
  ride exists
  ride status is DISPATCHING
  no driver already assigned
  batch still active
  driver was notified
  response still PENDING for current batch
  reservation token matches
  driver still ONLINE and not active on another ride
  ->
Lua writes atomically:
  ride status=ACCEPTED
  ride driverId=driverId
  ride acceptedAt=now
  clear current batch fields
  version++
  driver status=BUSY
  driver activeRideId=rideId
  remove driver from drivers:available
  remove driver from drivers:geo:available
  add driver to drivers:busy
  set driver:{driverId}:activeRide = rideId
  responses[driverId] = ACCEPTED
  remove rideId from driver:{driverId}:dispatches
  delete driver:{driverId}:reservation
  ->
dispatch.service post-accept work:
  release other driver reservations
  clear other driver pending membership
  append timeline -> DRIVER_ACCEPTED
  queue sync-ride
  queue sync-driver
  publish customer event driver_assigned with driverProfile and trip estimate context
  publish customer event ride_status_update
  publish driver event ride_assigned
  publish losing drivers event ride_cancelled
  ->
Driver socket/API -> location_heartbeat
  ->
driver-state.service.heartbeat
  ->
Refresh driver:{driverId}:heartbeat TTL
Update driver:{driverId}:state location
  ->
If ride status in ACCEPTED/ARRIVING/ONGOING:
  publish customer event driver_location_update
  ->
Driver socket/API -> ride_arriving
  ->
Ride ACCEPTED -> ARRIVING
Append timeline -> RIDE_ARRIVING
Queue sync-ride
Publish customer ride_status_update
  ->
Driver socket/API -> ride_started
  ->
Ride ARRIVING -> ONGOING
Append timeline -> RIDE_ONGOING
Queue sync-ride
Publish customer ride_status_update
  ->
Driver socket/API -> ride_completed
  ->
Compute actualDistanceKm and actualDurationMin
Compute finalFare / billing / commissionAmount / driverEarning
  ->
Ride ONGOING -> COMPLETED
Append timeline -> RIDE_COMPLETED
cleanupTerminalRideKeys:
  remove rideId from rides:active
  delete customer:{customerId}:activeRide
  expire ride hash/timeline/notified/responses
  clear driver pending dispatch membership
  release driver activeRide pointer
  move driver BUSY -> ONLINE or keep OFFLINE if driver already offline
  queue sync-driver
queue sync-ride
publish customer ride_status_update with final billing
```

### Notes

- Redis is authoritative during the entire active lifecycle.
- MongoDB is updated asynchronously by the persistence worker.
- Customer tracking depends on driver heartbeat plus ride status eligibility.

## 2. Multi-Driver Race Condition Flow

```text
Ride is DISPATCHING
  ->
Batch sent to driver A, B, C, D, E
  ->
All drivers receive new_ride_request
  ->
Multiple drivers emit accept_ride almost simultaneously
  ->
Driver entrypoint rate limit is checked:
  rate-limit:driver:accept-ride:{driverId}
  ->
Each accept call runs the same Redis Lua script
  ->
First script that reaches Redis while all guards are valid:
  sets ride status = ACCEPTED
  sets ride driverId = winner
  clears current batch
  sets winner status = BUSY
  deletes winner reservation
  version++
  returns new version
  ->
Every later script sees one of:
  ride status != DISPATCHING
  assigned driver already set
  current batch cleared
  reservation mismatch
  response no longer PENDING
  ->
Later scripts return negative rejection code
  ->
dispatch.service throws RIDE_ACCEPT_REJECTED
  ->
Only one driver gets ride_assigned
Other notified drivers get ride_cancelled
```

### Why race is prevented

```text
Protection layer 1:
  driver reservation key prevents a driver from being offered to two rides

Protection layer 2:
  ride responses hash requires exact PENDING:{batchId}

Protection layer 3:
  currentBatchId/currentBatchExpiresAt must still be valid

Protection layer 4:
  Lua script makes ride assignment + driver BUSY transition atomic

Protection layer 5:
  losing driver reservations are explicitly released after winner selection

Protection layer 6:
  driver reject cooldown can temporarily remove recently rejecting drivers
  from the reservable candidate pool in later rounds
```

### Important debug points

- `ride:{rideId}:active`
- `ride:{rideId}:responses`
- `ride:{rideId}:notified`
- `driver:{driverId}:reservation`
- `driver:{driverId}:activeRide`

## 3. No Driver Available Flow

```text
Ride REQUESTED
  ->
Dispatch round 1
  radius = INITIAL_SEARCH_RADIUS_KM
  ->
GEOSEARCH finds no reservable drivers
  ->
If dispatchRound < DISPATCH_MAX_ROUNDS
and radius < MAX_SEARCH_RADIUS_KM
  ->
Increase round
Increase radius = min(radius * 2, MAX_SEARCH_RADIUS_KM)
Append timeline -> DISPATCH_EXPANDED
Queue sync-ride
Queue delayed start-dispatch
  ->
Dispatch round 2 / 3 / ...
  ->
Still no reservable drivers
  ->
When max round reached or max radius reached
  ->
Ride status -> CANCELLED
Set cancellation reason = NO_DRIVERS_AVAILABLE
Append timeline -> DISPATCH_EXHAUSTED
cleanupTerminalRideKeys
Queue sync-ride
Publish customer ride_status_update with dispatchOutcome=NO_DRIVERS_AVAILABLE
```

### Notes

- YatriGo does not spam all drivers at once.
- Search radius grows gradually.
- A ride becomes terminal only after expansion is exhausted.

## 4. Heartbeat Failure Flow

```text
Driver is ONLINE or BUSY
  ->
driver:{driverId}:heartbeat exists with TTL
  ->
Driver stops sending heartbeat
  ->
driver:{driverId}:heartbeat expires
  ->
Reconciliation worker runs stale-driver-scan
  ->
driver-state.service.scanStaleDrivers()
  ->
Driver found in drivers:online but heartbeat key missing
  ->
driver-state.service.goOffline(reason=HEARTBEAT_TIMEOUT)
  ->
Remove driver from:
  drivers:online
  drivers:available
  drivers:busy
  drivers:geo:available
Delete reservation key
Keep historical state in driver:{driverId}:state as OFFLINE
  ->
recovery.service.handleDriverDrop(...)
  ->
Case A: driver had pending dispatches
  ->
Each affected ride calls handleDriverRejection(...)
  ->
Batch may continue or redispatch
  ->
Case B: driver owned ride in ACCEPTED or ARRIVING
  ->
ride-state.service.requeueRideAfterDriverLoss(...)
  ->
Ride ACCEPTED/ARRIVING -> DISPATCHING
Reset driverId and acceptedAt
Clear notified/responses state
Queue sync-ride
Publish ride_status_update with requeuedReason
Queue delayed redispatch
  ->
Case C: driver owned ride in ONGOING
  ->
Customer receives driver_connection_lost
Ride remains ongoing for operational follow-up
```

### Notes

- The current implementation only auto-redispatches safely before trip start.
- Ongoing ride disconnection is surfaced to the customer instead of silently reassigning mid-trip.

## 5. Driver Disconnect Flow

```text
Driver socket disconnects
  ->
Socket layer logs disconnect reason
  ->
Redis state is NOT immediately destroyed by socket disconnect alone
  ->
If driver reconnects before heartbeat expiry
  ->
New socket connects with same actorId
  ->
driver-state.service.restoreDriverSession(driverId, socketId)
  ->
Acquire lock:driver:{driverId}
  ->
If driver state exists and status != OFFLINE
  ->
Update socketId
Refresh lastHeartbeatAt
Refresh updatedAt
Persist driver realtime state
Queue sync-driver
Emit driver snapshot
  ->
Driver resumes without corrupting BUSY/ONLINE state
```

### Why state is preserved

```text
Socket presence is not treated as the only source of truth.
Heartbeat key and driver hash determine health and status.
This allows short network drops to recover cleanly.
```

## 6. Redis Crash / Recovery Flow

```text
Redis process restarts or in-memory realtime state is lost
  ->
API server starts
  ->
src/server.js bootstrap:
  connect Mongo
  ping Redis
  rebuildRedisStateFromMongo()
  register repeatable jobs
  start socket server
  ->
recovery.service.rebuildRedisStateFromMongo()
  ->
Load non-terminal rides from Mongo Ride collection
For each ride:
  recreate ride:{rideId}:active
  restore ETA / fare / billing metadata from Mongo snapshot
  recreate customer:{customerId}:activeRide
  add rideId to rides:active
  recreate ride:{rideId}:notified from Mongo dispatch history
  recreate ride:{rideId}:timeline
  if ride.driverId exists:
    set driver:{driverId}:activeRide
    mark driver BUSY
    add to drivers:busy
  ->
Load non-offline drivers from Mongo Driver collection
For each driver without active ride key:
  recreate driver:{driverId}:state
  if ONLINE and no active ride:
    add to drivers:online
    add to drivers:available
    GEOADD into drivers:geo:available
  if BUSY or activeRideId exists:
    add to drivers:busy
    remove from available pools
  ->
Repeatable reconciliation jobs start running
  ->
dispatch-recovery-scan checks rides:active
  ->
If dispatching ride has no current batch:
  queue start-dispatch
If current batch expired:
  queue batch-timeout
  ->
Dispatch resumes from recovered state
```

### Recovery limitations

```text
Mongo is persistence/history, not a perfect realtime mirror.
Recovery rebuilds enough state to continue operating,
but short-lived ephemeral details may be reconstructed conservatively.
```

## 7. Manual Socket Test Flow (`tests/manual-flow.js`)

```text
Developer runs node tests/manual-flow.js
  ->
Menu shows numbered actions
  ->
1 Connect Driver
  open socket auth { role: DRIVER, actorId }
  ->
2 Connect Customer
  open socket auth { role: CUSTOMER, actorId }
  ->
3 Driver Go Online
  emit go_online with lat/lng
  server may verify JWT if AUTH_ENABLED=true
  ->
4 Request Ride
  emit request_ride with origin/destination/eventId
  ->
Server checks customer request rate limit
Server creates ride with ETA and fare estimate
Server starts dispatch
  ->
Driver receives new_ride_request
  ->
5 Accept Ride
  emit accept_ride
  server may apply driver accept rate limit
  ->
6 Send Location Update
  emit location_heartbeat
  customer receives driver_location_update with timestamp
  ->
7 Mark Arriving
  emit ride_arriving
  ->
8 Start Ride
  emit ride_started
  ->
9 Complete Ride
  emit ride_completed
  ->
10 Show Current State
  fetch ride via API
  inspect driver/customer Redis pointers
```

## 8. Manual API Test Flow (`tests/manual-api-flow.js`)

```text
Developer runs node tests/manual-api-flow.js
  ->
1 Initialize Driver + Customer IDs
  ->
2 Driver Go Online
  POST /api/drivers/status/online
  ->
3 Request Ride
  POST /api/customers/rides/request
  ->
  controller may verify JWT and request rate limit first
  ->
4 Accept Ride
  POST /api/drivers/rides/:rideId/accept
  ->
  controller may verify JWT and accept rate limit first
  ->
5 Mark Arriving
  POST /api/drivers/rides/:rideId/arriving
  ->
6 Start Ride
  POST /api/drivers/rides/:rideId/start
  ->
7 Complete Ride
  POST /api/drivers/rides/:rideId/complete
  ->
8 Show Current State
  fetch ride and inspect Redis
```

## 9. Persistence Flow

```text
Ride or driver state changes in Redis
  ->
enqueueRideSync / enqueueDriverSync
  ->
Persistence worker consumes job
  ->
For rides:
  read ride hash
  read ride timeline
  read ride notified drivers
  read ETA / pricing / billing fields
  upsert Mongo Ride
  ->
For drivers:
  read driver hash
  read enriched driver profile snapshot fields when present
  upsert Mongo Driver
```

### Why this matters

```text
The API does not wait for Mongo to confirm writes.
Redis stays fast and authoritative for active operations.
Mongo becomes the durable record and recovery source.
```

## 10. Observability Checklist During Incident Debugging

```text
If dispatch seems stuck:
  inspect ride active hash
  inspect currentBatchId/currentBatchExpiresAt
  inspect ride responses hash
  inspect driver reservations
  inspect dispatch worker logs

If double assignment is suspected:
  inspect driver activeRide keys across candidate drivers
  inspect ride responses for ACCEPTED/TIMEOUT/REJECTED mix
  inspect ride version changes
  inspect Lua acceptance rejections in logs

If reconnect behavior looks wrong:
  inspect driver state hash socketId/status
  inspect heartbeat key existence
  inspect snapshot emissions
```
