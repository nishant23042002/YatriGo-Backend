# YatriGo Architecture Deep Dive

## Scope

This document covers the application-owned files in the YatriGo repository:

- Top-level runtime/config files
- All files under `src/`
- All files under `tests/`

It intentionally excludes `node_modules/`, which is third-party code rather than YatriGo system logic.

## System Overview

YatriGo is a Redis-first ride-hailing backend designed for a small-town deployment model where:

- connectivity is weak,
- drivers disconnect and reconnect often,
- traffic is moderate but spiky,
- dispatch correctness matters more than extreme throughput.

Core design choices:

- Redis is the source of truth for all active rides and active driver state.
- MongoDB stores persistent ride/driver history and acts as recovery input after process or Redis failure.
- BullMQ decouples dispatch, persistence, and reconciliation from request/ack latency.
- Socket.IO provides low-latency realtime customer and driver updates.
- Locks, idempotency keys, and one Lua script are used to prevent double assignment.
- ETA, pricing, billing, auth, and rate limits are added as additive layers around the existing Redis-first flow.

## Recent Production Layer Additions

- ETA and Haversine-based trip estimation at ride creation
- Estimated fare and final billing calculation
- Customer-facing driver profile enrichment
- Notification abstraction on top of the existing socket publisher
- Optional JWT auth for API and sockets
- Redis-backed rate limiting
- Driver reject cooldown to reduce repeat dispatch spam
- ETA and pricing are computed as additive ride metadata in Redis, not as a replacement for dispatch state.
- Billing is finalized on ride completion and persisted asynchronously to MongoDB.
- Optional JWT auth and Redis-backed rate limits protect entrypoints without changing dispatch internals.

## Runtime Architecture

### Primary runtime surfaces

- HTTP API: Express routes under `src/routes/` and controllers under `src/controllers/`
- Optional auth layer: middleware under `src/middleware/`
- Realtime interface: Socket handlers under `src/socket/`
- Domain logic: Services under `src/services/`
- Realtime state store: Redis keys defined in `src/redis/keys.js`
- Background execution: BullMQ queues in `src/queues/index.js` and workers under `src/workers/`
- Persistence: MongoDB models under `src/models/`

### Core control loops

1. Customer requests ride
2. ETA, distance, ride type, and estimated fare are created with the ride in Redis
3. Dispatch job is enqueued
4. Dispatch worker selects nearby available drivers using Redis GEO
5. Drivers are notified in batches
6. One driver atomically wins acceptance
7. Customer notifications are enriched with driver profile and ride billing context
8. Final fare, commission, and driver earning are written when the ride completes
9. Persistence worker copies Redis state to MongoDB
10. Reconciliation workers heal stale drivers and broken dispatch state

## Redis Design Deep Dive

### Driver state keys

- `driver:{driverId}:state`
  - Redis hash
  - Stores current driver realtime snapshot: `status`, `lat`, `lng`, `socketId`, `activeRideId`, `lastHeartbeatAt`, `updatedAt`, `metadata`
  - Owned mainly by `driver-state.service.js`
- `driver:{driverId}:heartbeat`
  - Redis string with TTL
  - Presence of this key means the driver heartbeat is still alive
  - Expiry is used by stale-driver reconciliation
- `driver:{driverId}:activeRide`
  - Redis string
  - Fast lookup for the ride currently owned by the driver
- `driver:{driverId}:dispatches`
  - Redis set
  - Ride IDs currently pending for that driver during dispatch
- `driver:{driverId}:reservation`
  - Redis string with TTL
  - Temporary reservation token `rideId:batchId`
  - Prevents a driver from being offered to multiple rides at the same time
- `drivers:online`
  - Redis set of all online drivers
- `drivers:available`
  - Redis set of available drivers
- `drivers:busy`
  - Redis set of busy drivers
- `drivers:geo:available`
  - Redis GEO sorted set
  - Spatial index for dispatch candidate search

### Ride state keys

- `ride:{rideId}:active`
  - Redis hash
  - Main realtime ride record
  - Fields include status, customer/driver IDs, dispatch round, search radius, current batch, acceptedAt, metadata, version, ETA estimate, and billing fields
- `ride:{rideId}:timeline`
  - Redis list
  - Ordered lifecycle events appended as JSON strings
- `ride:{rideId}:notified`
  - Redis set
  - Drivers who were notified for the ride across dispatch rounds
- `ride:{rideId}:responses`
  - Redis hash
  - Per-driver response state such as `PENDING:{batchId}`, `TIMEOUT`, `REJECTED`, `ACCEPTED`
- `rides:active`
  - Redis set
  - All rides that are not terminal
- `customer:{customerId}:activeRide`
  - Redis string
  - Current active ride pointer for a customer

### Locks

- `lock:ride:{rideId}`
  - Used around dispatch and ride transitions
  - Prevents concurrent mutation of the same ride
- `lock:driver:{driverId}`
  - Used around driver online/offline/heartbeat/session restore
  - Prevents driver state races
- `lock:customer:{customerId}`
  - Used when creating rides
  - Prevents a customer from opening multiple active rides concurrently

### Socket and idempotency keys

- `socket:event:ack:{eventId}`
  - Records that a client acknowledged a pushed socket event
  - Used for observability and reconnect/debug tracing
- `idempotency:{scope}:{key}`
  - Created by `markIdempotent`
  - Used for socket inbound dedupe and socket publish dedupe

### Operational protection keys

- `rate-limit:customer:request-ride:{customerId}`
  - Redis string counter with expiry
  - Throttles repeated ride request creation attempts
- `rate-limit:driver:accept-ride:{driverId}`
  - Redis string counter with expiry
  - Throttles repeated accept attempts at the entrypoint
- `ops:driver:reject-cooldown:{driverId}`
  - Redis string with expiry
  - Temporary cooldown applied after driver rejection to reduce dispatch spam

### TTL strategy

- `driver:{driverId}:heartbeat`
  - Short TTL
  - Expires inactive drivers out of the “healthy” pool
- `driver:{driverId}:reservation`
  - Dispatch response-window TTL
  - Automatically frees a driver if dispatch gets stuck
- `ride:{rideId}:active`, `ride:{rideId}:timeline`, `ride:{rideId}:notified`, `ride:{rideId}:responses`
  - Refreshed during active ride mutations
  - Retained for a shorter terminal TTL after completion/cancellation
- `socket:event:ack:{eventId}`
  - Short-lived observability record
- `idempotency:*`
  - Medium-lived dedupe window

### Why this Redis layout works

- Hashes hold mutable snapshots cheaply.
- Sets provide fast membership checks for availability pools.
- GEO index supports nearest-driver search.
- Ride-local sets and hashes isolate dispatch state per ride.
- Expiring reservation and heartbeat keys prevent stale active state from living forever.

## Socket Event Map

### Customer -> backend

- `request_ride`
  - Emitted by customer socket
  - Handled in `src/socket/customer.socket.js`
  - Payload:
    - `origin`
    - `destination`
    - `metadata`
    - optional `eventId`
  - Side effects:
    - rate limit check
    - creates ride in Redis
    - computes ride type, distance, ETA, and estimated fare
    - schedules persistence and dispatch
    - joins customer socket to `ride:{rideId}` room
- `cancel_ride`
  - Emitted by customer
  - Cancels a non-terminal ride
- `subscribe_ride`
  - Emitted by customer
  - Joins ride room and returns current ride snapshot
- `event_ack`
  - Generic client acknowledgment of pushed events

### Backend -> customer

- `ride_requested`
  - Emitted after ride creation
- `driver_assigned`
  - Emitted after atomic driver acceptance
  - Customer payload is enriched with driver profile when available
- `driver_location_update`
  - Emitted from driver heartbeat when ride is trackable
  - Payload now includes `timestamp` plus optional `speed` and `route`
- `ride_status_update`
  - Emitted on lifecycle changes and redispatch/recovery changes
  - Completion payload can include final billing
- `snapshot`
  - Emitted on reconnect/init when a customer already has an active ride
  - Customer payload is enriched with driver profile and pricing fields
- `driver_connection_lost`
  - Emitted if an ongoing ride loses driver connectivity

### Driver -> backend

- `go_online`
  - Puts driver into Redis online/available or online/busy depending on active ride pointer
- `go_offline`
  - Removes driver from pools and triggers recovery handling
- `location_heartbeat`
  - Refreshes heartbeat TTL and optionally pushes location to customer
- `accept_ride`
  - Atomic ride acceptance path
- `reject_ride`
  - Marks a driver response as rejected and may advance dispatch
- `ride_arriving`
  - Transition `ACCEPTED -> ARRIVING`
- `ride_started`
  - Transition `ARRIVING -> ONGOING`
- `ride_completed`
  - Transition `ONGOING -> COMPLETED`
- `event_ack`
  - Generic client acknowledgment of pushed events

### Backend -> driver

- `new_ride_request`
  - Dispatch offer with `rideId`, `batchId`, trip coordinates, `expiresAt`, `ackRequired`
- `ride_assigned`
  - Emitted to winning driver after acceptance
- `ride_cancelled`
  - Emitted to losing drivers or when the ride is cancelled
- `snapshot`
  - Emitted on reconnect/init with driver state and active ride snapshot

## File-by-File Deep Dive

## package.json

### Purpose

Declares the Node.js package, runtime dependencies, and the operational commands used by developers.

### How It Fits

- Layer: repository root / toolchain entrypoint
- Called by: npm
- Calls: server bootstrap, workers, and test scripts through npm scripts

### Key Contents

- `start`, `dev`
- `workers`, `worker:*`
- `test:*` scripts for manual and automated validation

### Redis Usage

None directly.

### Failure Handling

If scripts fail, npm surfaces process exit codes. No application logic is owned here.

## package-lock.json

### Purpose

Pins dependency versions so engineers run consistent builds locally and in deployment.

### How It Fits

- Layer: dependency lock artifact
- Called by: npm install

### Key Contents

- Exact transitive dependency graph for BullMQ, Redis clients, Socket.IO, Mongoose, Express, and utilities

### Redis Usage

None directly.

### Failure Handling

Prevents dependency drift; if corrupted, installs become inconsistent.

## .env.example

### Purpose

Documents the supported environment variables and safe defaults for local and production deployment.

### How It Fits

- Layer: operator/developer configuration template
- Used by: `src/config/env.js`, test config, deployment tooling

### Key Values

- Dispatch search radius, batching, retry delay, max rounds
- Driver heartbeat TTL and reconciliation intervals
- Redis TTL windows
- Socket ACK TTL
- `APP_MODE` and `LOG_LEVEL`

### Redis Usage

Indirect: configures timeouts, TTLs, lock windows, and batch timing.

### Failure Handling

Bad values fall back through `readNumber`/`readString` defaults in `env.js`.

## .env

### Purpose

Local environment override file for a concrete workstation or deployment environment.

### How It Fits

- Layer: local runtime config
- Used by: `dotenv` in `src/config/env.js` and `tests/lib/config.js`

### Key Contents

Runtime-specific values only. The schema mirrors `.env.example`.

### Redis Usage

Indirect via connection URL and TTL/timing values.

### Failure Handling

Invalid or missing values typically fall back in `env.js`. Secret or host issues surface at connection time.

## README.md

### Purpose

Developer-facing quickstart and operational guide.

### How It Fits

- Layer: top-level developer documentation
- Used by: engineers onboarding or running tests

### Key Contents

- Project overview
- Run instructions
- Testing commands
- Socket event list
- Redis inspection commands

### Redis Usage

None directly, but documents inspection of Redis state.

### Failure Handling

None. It is informational.

## src/app.js

### Purpose

Creates the Express application with middleware, health endpoint, routes, and error handling.

### How It Fits

- Layer: API composition
- Called by: `src/server.js`
- Calls: customer and driver route modules

### Key Functions

- `createApp()`
  - Input: none
  - Output: configured Express app
  - Side effects: mounts middleware and route trees

### Redis Usage

None directly.

### Failure Handling

Delegates route errors to the shared `failure` responder.

## src/server.js

### Purpose

Bootstraps the full API/socket runtime.

### How It Fits

- Layer: server process entrypoint
- Called by: `npm start`, `npm run dev`
- Calls:
  - Mongo connect
  - Redis ping
  - Redis rebuild from Mongo
  - repeatable job registration
  - socket server creation

### Key Functions

- `bootstrap()`
  - Input: none
  - Output: running HTTP + Socket.IO server
  - Side effects:
    - opens MongoDB connection
    - validates Redis
    - rehydrates Redis state
    - registers repeatable reconciliation jobs
    - starts HTTP listener

### Redis Usage

- `redis.ping()` health check
- indirect through recovery and queue registration

### Failure Handling

Any bootstrap error is logged and causes process exit. This is intentional to avoid starting in a half-ready state.

## src/config/env.js

### Purpose

Centralizes environment parsing and defaulting.

### How It Fits

- Layer: configuration
- Called by: almost every runtime layer

### Key Functions

- `readNumber(name, fallback)`
- `readString(name, fallback)`
- exported `env` object

### Redis Usage

Indirect. Supplies:

- Redis URL
- TTL values
- lock TTL
- dispatch timing
- heartbeat windows

### Failure Handling

Malformed numeric values degrade to safe defaults rather than crashing startup.

## src/config/logger.js

### Purpose

Provides structured, category-aware, colored console logging.

### How It Fits

- Layer: observability utility
- Called by: services, socket layer, workers, startup

### Key Functions

- `normalizeMeta`
- `inferCategory`
- `inferIcon`
- `format`
- `shouldLog`
- `logger.info/warn/error/debug`
- `logger.child`

### Redis Usage

None directly.

### Failure Handling

Minimal risk. Logging failure would only affect observability, not state.

## src/config/mongo.js

### Purpose

Owns Mongoose connection lifecycle and ensures only one connection attempt runs at a time.

### How It Fits

- Layer: persistence infrastructure
- Called by: `src/server.js`, persistence worker, reconciliation worker

### Key Functions

- `connectMongo()`
  - Input: none
  - Output: mongoose connection
  - Side effects: opens MongoDB connection, logs success

### Redis Usage

None.

### Failure Handling

Failed connect propagates to caller, causing process bootstrap or worker startup to fail fast.

## src/config/redis.js

### Purpose

Creates named Redis connections for the app, socket pub/sub, and queue/workers.

### How It Fits

- Layer: realtime infrastructure
- Called by: app runtime, queues, workers, socket adapter

### Key Functions

- `createRedisConnection(name)`
  - Input: logical connection name
  - Output: `IORedis` client
  - Side effects: attaches error logging

### Redis Usage

Owns all Redis client creation.

### Failure Handling

Logs connection-level Redis failures. Does not implement reconnect policy itself beyond ioredis defaults.

## src/controllers/customer.controller.js

### Purpose

Maps HTTP customer requests to ride domain services.

### How It Fits

- Layer: controller
- Called by: `src/routes/customer.routes.js`
- Calls: `ride-state.service.js`

### Key Functions

- `requestRide(req, res)`
  - Calls `rideStateService.requestRide`
- `cancelRide(req, res)`
  - Calls `rideStateService.cancelRide`
- `getRide(req, res)`
  - Calls `getRide` and `getRideTimeline`
  - Throws `RIDE_NOT_FOUND` if Redis has no active record

### Redis Usage

Indirect via ride service.

### Failure Handling

Domain errors bubble to Express error middleware and become JSON API errors.

## src/controllers/driver.controller.js

### Purpose

Maps HTTP driver requests to driver, dispatch, recovery, and ride services.

### How It Fits

- Layer: controller
- Called by: `src/routes/driver.routes.js`
- Calls:
  - `driver-state.service.js`
  - `dispatch.service.js`
  - `ride-state.service.js`
  - `recovery.service.js`
  - `socket-publisher.service.js`

### Key Functions

- `goOnline`
- `goOffline`
  - Also calls `handleDriverDrop` so offline state impacts dispatch/ride recovery
- `heartbeat`
  - Updates driver state and may emit live location to customer
- `acceptRide`
- `rejectRide`
- `markArriving`
- `startRide`
- `completeRide`

### Redis Usage

Indirect via services.

### Failure Handling

Propagates domain errors. The controller does not retry on its own.

## src/routes/customer.routes.js

### Purpose

Defines customer HTTP endpoints.

### How It Fits

- Layer: routing
- Called by: Express app
- Calls: customer controller functions through `asyncHandler`

### Key Endpoints

- `POST /api/customers/rides/request`
- `POST /api/customers/rides/:rideId/cancel`
- `GET /api/customers/rides/:rideId`

### Redis Usage

None directly.

### Failure Handling

`asyncHandler` forwards async exceptions.

## src/routes/driver.routes.js

### Purpose

Defines driver HTTP endpoints.

### How It Fits

- Layer: routing
- Called by: Express app
- Calls: driver controller functions

### Key Endpoints

- `POST /api/drivers/status/online`
- `POST /api/drivers/status/offline`
- `POST /api/drivers/location`
- `POST /api/drivers/rides/:rideId/accept`
- `POST /api/drivers/rides/:rideId/reject`
- `POST /api/drivers/rides/:rideId/arriving`
- `POST /api/drivers/rides/:rideId/start`
- `POST /api/drivers/rides/:rideId/complete`

### Redis Usage

None directly.

### Failure Handling

Same as customer routes.

## src/models/User.js

### Purpose

Persistent user identity model for customers, drivers, or admins.

### How It Fits

- Layer: MongoDB persistence model
- Called by: currently not central to active flow, but available for broader platform integration

### Key Fields

- `userId`
- `name`
- `phone`
- `role`

### Redis Usage

None.

### Failure Handling

Mongo validation and uniqueness constraints apply on writes.

## src/models/Driver.js

### Purpose

Persistent driver profile and last known state history.

### How It Fits

- Layer: MongoDB persistence model
- Called by: persistence worker and recovery service

### Key Fields

- `driverId`
- `userId`
- `vehicle`
- `status`
- `lastKnownLocation`
- `activeRideId`
- `metadata`

### Redis Usage

None directly.

### Failure Handling

Persistence worker retries via BullMQ if upsert fails.

## src/models/Ride.js

### Purpose

Persistent ride history model.

### How It Fits

- Layer: MongoDB persistence model
- Called by: persistence worker and recovery service

### Key Fields

- ride identity and actors
- lifecycle `status`
- pickup/drop points
- dispatch subdocument
- cancellation info
- timeline array
- metadata

### Redis Usage

None directly.

### Failure Handling

BullMQ retries persistence if upsert fails.

## src/queues/index.js

### Purpose

Defines BullMQ queues and all enqueue helpers.

### How It Fits

- Layer: async orchestration
- Called by: services and server bootstrap
- Calls: BullMQ queue add operations

### Key Functions

- `enqueueDispatchStart`
- `enqueueBatchTimeout`
- `enqueueDriverUnavailable`
- `enqueueRideSync`
- `enqueueDriverSync`
- `registerRepeatableJobs`
- `sanitizeJobIdPart` / `buildJobId`
  - Ensures BullMQ-safe job IDs

### Redis Usage

Indirect through BullMQ Redis-backed queues.

### Failure Handling

- Queue add errors propagate to caller
- Job defaults include retries and exponential backoff
- Repeatable jobs are registered during startup

## src/redis/keys.js

### Purpose

Single source of truth for Redis key naming.

### How It Fits

- Layer: Redis schema helper
- Called by: almost every service, socket module, worker, and recovery path

### Key Functions

Named key builders for:

- ride keys
- driver keys
- locks
- room broadcast channel
- socket ACK storage

### Redis Usage

This file defines the Redis namespace itself.

### Failure Handling

None directly. Consistency here is critical because key drift would fragment state.

## src/redis/scripts.js

### Purpose

Stores Lua scripts executed atomically by Redis.

### How It Fits

- Layer: Redis atomic mutation primitive
- Called by: `dispatch.service.js`

### Key Contents

- `ACCEPT_RIDE_SCRIPT`
  - Verifies:
    - ride exists
    - ride is `DISPATCHING`
    - ride has no assigned driver yet
    - current batch still exists
    - batch has not expired
    - driver was actually notified
    - driver response is still `PENDING:{batchId}`
    - driver reservation token matches
    - driver state is still `ONLINE`
    - driver is not already on another ride
  - Mutates:
    - ride hash to `ACCEPTED`
    - driver hash to `BUSY`
    - availability/busy sets
    - geo index
    - active ride pointer
    - response hash
    - pending dispatch set
    - reservation key

### Redis Usage

Critical atomic update path for zero double assignment.

### Failure Handling

Returns negative codes rather than partial writes. Caller translates those into a 409-style domain error.

## src/services/dispatch.service.js

### Purpose

Owns dispatch batching, expansion, timeout handling, rejection handling, and atomic ride acceptance.

### How It Fits

- Layer: core dispatch domain service
- Called by:
  - dispatch worker
  - driver controller/socket handlers
  - recovery service
- Calls:
  - ride state service
  - driver state service
  - queue helpers
  - socket publisher
  - Redis Lua script

### Key Functions

- `startDispatch(rideId)`
  - Ride lock wrapper around `dispatchUnderLock`
- `dispatchUnderLock(rideId)`
  - Converts `REQUESTED -> DISPATCHING`
  - heals expired current batch
  - selects nearest available drivers
  - reserves drivers
  - either:
    - sends a batch,
    - expands search radius,
    - or cancels as `NO_DRIVERS_AVAILABLE`
- `reserveDriversForBatch(rideId, batchId, candidateDriverIds)`
  - Attempts `NX PX` reservations per driver
- `handleBatchTimeout(rideId, batchId)`
  - Marks pending drivers as timed out
  - clears batch
  - redispatches under lock
- `handleDriverRejection(rideId, driverId, reason)`
  - Marks rejection, clears pending membership, may trigger next dispatch step
- `acceptRide(rideId, driverId)`
  - Runs Lua atomic acceptance
  - releases losing driver reservations
  - emits customer and driver assignment events
- `markDriverUnavailableDuringDispatch`
  - Alias to rejection path for recovery/offline handling

### Redis Usage

- Reads/writes:
  - `ride:{rideId}:active`
  - `ride:{rideId}:notified`
  - `ride:{rideId}:responses`
  - `driver:{driverId}:dispatches`
  - `driver:{driverId}:reservation`
  - `drivers:available`
  - `drivers:busy`
  - `drivers:geo:available`
  - `lock:ride:{rideId}`
  - `driver:{driverId}:activeRide`

### Key Lifecycle Notes

- `ride:{rideId}:responses` starts with `PENDING:{batchId}`, then becomes `TIMEOUT`, rejection reason, or `ACCEPTED`
- `driver:{driverId}:reservation` exists only for the dispatch response window
- `ride:{rideId}:notified` survives across rounds so the same ride does not repeatedly spam the same drivers unless state is reset

### Failure Handling

- Uses ride lock for serialized ride mutation
- Queue retries for dispatch jobs
- Re-dispatches after timeout or full-batch rejection
- If lock is busy, throws a retryable 409 domain error
- Atomic acceptance protects against concurrent driver accepts and late accepts

## src/services/driver-state.service.js

### Purpose

Owns realtime driver presence, location, availability, reservations, and heartbeat health.

### How It Fits

- Layer: realtime driver state service
- Called by:
  - driver controller
  - driver socket handlers
  - dispatch service
  - recovery service
  - persistence worker
- Calls:
  - queue helper for Mongo sync
  - lock helper

### Key Functions

- `getDriverState(driverId)`
  - Reads `driver:{id}:state`
- `persistDriverRealtimeState(driverId, state)`
  - Canonical writer for driver hash, pools, heartbeat key, activeRide pointer, GEO index
- `goOnline`
- `heartbeat`
- `goOffline`
- `restoreDriverSession`
  - Rebinds socket ID after reconnect without corrupting BUSY/ONLINE state
- `getNearestAvailableDrivers(origin, radiusKm, count)`
  - GEOSEARCH against `drivers:geo:available`
- `scanStaleDrivers()`
  - Walks `drivers:online` and expires missing heartbeats
- `isDriverReservable(driverId)`
  - Checks existence, heartbeat, state, active ride, and reservation presence
- `cleanupGhostAvailability(driverId)`
  - Cleans orphan driver pool entries
- `canTrackLocation(status)`
  - Allows location pushing only for `ACCEPTED`, `ARRIVING`, `ONGOING`

### Redis Usage

- `driver:{driverId}:state`
- `driver:{driverId}:heartbeat`
- `driver:{driverId}:activeRide`
- `driver:{driverId}:dispatches`
- `driver:{driverId}:reservation`
- `drivers:online`
- `drivers:available`
- `drivers:busy`
- `drivers:geo:available`
- `lock:driver:{driverId}`

### Failure Handling

- Driver mutations are lock-protected
- Stale or ghost drivers are removed from pools
- Mongo persistence is async and retried via queue
- Missing driver heartbeat causes offline transition and later recovery handling

## src/services/ride-state.service.js

### Purpose

Owns ride creation, ride transitions, ride timeline, customer active pointer, terminal cleanup, and redispatch after driver loss.

### How It Fits

- Layer: realtime ride state machine service
- Called by:
  - customer controller/socket
  - driver controller/socket
  - dispatch service
  - recovery service
  - persistence worker
- Calls:
  - queue helpers
  - socket publisher
  - lock helper

### Key Functions

- `requestRide({ customerId, origin, destination, metadata })`
  - Acquires customer lock
  - rejects second active ride
  - creates ride hash
  - sets customer active ride pointer
  - adds ride to `rides:active`
  - appends timeline
  - queues persistence
  - emits `ride_requested`
  - queues dispatch start
- `transitionRide(rideId, nextStatus, options)`
  - Shared transition engine with transition validation
  - appends timeline
  - emits status update
  - handles terminal cleanup
- `cancelRide`
- `markDriverArriving`
- `startRide`
- `completeRide`
- `appendTimelineEvent`
- `getRide`
- `getRideTimeline`
- `updateRideHash`
- `cleanupTerminalRideKeys`
  - removes ride from active set
  - deletes customer active pointer
  - expires ride keys
  - clears pending dispatch membership
  - releases current batch reservations
  - returns driver to ONLINE or keeps OFFLINE depending on driver state
- `releaseDriverAvailability`
- `requeueRideAfterDriverLoss`
  - Resets dispatch fields and queues redispatch when assigned driver disappears before ride starts

### Redis Usage

- `ride:{rideId}:active`
- `ride:{rideId}:timeline`
- `ride:{rideId}:notified`
- `ride:{rideId}:responses`
- `customer:{customerId}:activeRide`
- `rides:active`
- `driver:{driverId}:activeRide`
- `driver:{driverId}:dispatches`
- `driver:{driverId}:reservation`
- `drivers:available`
- `drivers:busy`
- `drivers:geo:available`
- `lock:ride:{rideId}`
- `lock:customer:{customerId}`

### Failure Handling

- Customer and ride locks prevent duplicate creation and transition races
- Invalid transition throws `INVALID_RIDE_TRANSITION`
- Terminal cleanup ensures pool consistency even after cancellation/completion
- Redispatch after driver loss is intentionally limited to pre-start states

## src/services/recovery.service.js

### Purpose

Owns failure recovery and state rehydration behavior.

### How It Fits

- Layer: recovery / reconciliation domain service
- Called by:
  - driver offline flows
  - reconciliation worker
  - server bootstrap

### Key Functions

- `handleDriverDrop({ driverId, activeRideId, pendingRideIds, reason })`
  - Rejects driver out of pending dispatches
  - requeues accepted/arriving rides
  - emits connection-lost event for ongoing rides
- `scanStaleDriversAndRecover()`
  - Combines stale-driver scan with drop handling
- `recoverDispatchingRides()`
  - Walks `rides:active`
  - if dispatching ride has no batch, queues `start-dispatch`
  - if batch expired, queues timeout job
- `rebuildRedisStateFromMongo()`
  - Rehydrates active rides and non-offline drivers into Redis

### Redis Usage

- Reads and repairs:
  - `rides:active`
  - ride hashes/timelines/notified sets
  - driver hash and active ride keys
  - availability/busy sets
  - GEO index

### Failure Handling

- Recovers ghost ride IDs
- Restores redis state from Mongo on startup or repeatable job
- Limits redispatch to ride statuses where reassignment is safe

## src/services/socket-publisher.service.js

### Purpose

Publishes fanout events into Redis pub/sub, with dedupe protection.

### How It Fits

- Layer: realtime outbound event publisher
- Called by: ride, driver, dispatch, and recovery services
- Calls:
  - Redis idempotency helper
  - Redis pub/sub publish

### Key Functions

- `publish(message, dedupeKey)`
- `emitToRoom(room, event, payload, dedupeKey)`
- `emitRideRequested`
- `emitRideStatusUpdate`
- `emitDriverAssigned`
- `emitDriverLocationUpdate`
- `emitNewRideRequest`
- `emitRideAssignedToDriver`
- `emitRideCancelledToDriver`
- `emitSnapshot`
- `emitDriverConnectionLost`

### Redis Usage

- `socket:broadcast`
- `idempotency:socket-publish:{key}`

### Failure Handling

- Duplicate publish attempts are dropped by idempotency key rather than emitted twice
- Publish errors propagate to caller

## src/socket/index.js

### Purpose

Creates the Socket.IO server, authenticates sockets, installs socket debugging, subscribes to Redis pub/sub, and fans out outbound events.

### How It Fits

- Layer: realtime ingress/egress gateway
- Called by: `src/server.js`
- Calls:
  - driver/customer socket registration
  - driver session restore
  - Redis pub/sub fanout

### Key Functions

- `createSocketServer(httpServer)`
  - Builds Socket.IO server
  - installs Redis adapter
  - authenticates `role` and `actorId`
  - registers role-specific handlers
  - processes `event_ack`
  - subscribes to `socket:broadcast`

### Redis Usage

- `socket:broadcast`
- `socket:event:ack:{eventId}`

### Failure Handling

- Rejects auth missing `role` or `actorId`
- Logs and disconnects failed connection init
- Stores ACK failures as non-fatal response errors
- Redis pub/sub fanout parse failures are logged, not retried in-place

## src/socket/helpers.js

### Purpose

Provides shared inbound socket idempotency and ACK helpers.

### How It Fits

- Layer: socket processing utility
- Called by: `customer.socket.js`, `driver.socket.js`
- Calls: Redis idempotency helper

### Key Functions

- `ackSuccess`
  - Returns stable `{ success: true, data: ... }` response shape
- `ackFailure`
- `validateInboundEvent`
  - Uses `payload.eventId` if available
  - otherwise generates per-socket fallback idempotency key
- `handleSocketEvent`
  - wraps validation, handler execution, and ACK logic

### Redis Usage

- `idempotency:socket-inbound:{key}`

### Failure Handling

- Duplicate event policy is mode-dependent
- In test mode, duplicate inbound events are tolerated and ACKed successfully
- In production mode, duplicate events are rejected

## src/socket/debug.js

### Purpose

Provides payload sanitization and socket-level debug middleware.

### How It Fits

- Layer: observability helper
- Called by: socket server and helper layer

### Key Functions

- `sanitizePayload`
  - redacts token/password-like fields
  - truncates deeply nested or huge payloads
- `socketMeta`
  - normalizes socket logging metadata
- `installSocketDebugging`
  - logs inbound and outbound socket events

### Redis Usage

None directly.

### Failure Handling

Observability only. Intended to be non-invasive.

## src/socket/customer.socket.js

### Purpose

Registers all customer-originated socket events and sends reconnect snapshots.

### How It Fits

- Layer: socket customer adapter
- Called by: `src/socket/index.js`
- Calls: ride state service

### Key Functions

- `registerCustomerSocket(socket)`
  - binds `request_ride`, `cancel_ride`, `subscribe_ride`
- `sendCustomerSnapshot(socket)`
  - if customer has active ride pointer, emits current ride snapshot and joins ride room

### Redis Usage

- reads `customer:{customerId}:activeRide`
- indirect ride service writes

### Failure Handling

- Socket handler errors are returned via ACK
- Snapshot silently no-ops if no active ride exists

## src/socket/driver.socket.js

### Purpose

Registers all driver-originated socket events and sends reconnect snapshots.

### How It Fits

- Layer: socket driver adapter
- Called by: `src/socket/index.js`
- Calls:
  - driver state service
  - dispatch service
  - ride state service
  - recovery service
  - socket publisher

### Key Functions

- `registerDriverSocket(socket)`
  - binds online/offline/heartbeat/accept/reject/arriving/started/completed
- `pushLocationIfNeeded(driverState)`
  - emits `driver_location_update` only when ride status is trackable
- `sendDriverSnapshot(socket)`
  - emits current driver state and active ride

### Redis Usage

- indirect via services
- snapshot reads `driver:{driverId}:activeRide`

### Failure Handling

- ACK wrapper returns structured error responses
- offline path explicitly triggers recovery handling

## src/utils/async-handler.js

### Purpose

Express async wrapper so controllers can throw normally.

### How It Fits

- Layer: API utility
- Called by: route definitions

### Key Functions

- `asyncHandler(handler)`

### Redis Usage

None.

### Failure Handling

Forwards errors to Express middleware.

## src/utils/constants.js

### Purpose

Holds shared enums, queue names, socket event names, and ride transition rules.

### How It Fits

- Layer: shared domain constants
- Called by: services, workers, socket publisher

### Key Contents

- `RideStatus`
- `DriverStatus`
- `SocketEvents`
- `InternalSocketEvents`
- `QueueNames`
- `DispatchJobs`
- `PersistenceJobs`
- `ReconciliationJobs`
- `TerminalRideStatuses`
- `RideTransitions`

### Redis Usage

Indirect; defines legal state transitions around Redis data.

### Failure Handling

Transition validation depends on `RideTransitions`.

## src/utils/errors.js

### Purpose

Defines the shared `AppError` class.

### How It Fits

- Layer: error contract
- Called by: services and controllers

### Key Functions

- `AppError(message, statusCode, code)`

### Redis Usage

None.

### Failure Handling

Standardizes API/socket failure payloads and HTTP status codes.

## src/utils/idempotency.js

### Purpose

Provides a small Redis-backed idempotency primitive.

### How It Fits

- Layer: Redis coordination utility
- Called by:
  - socket inbound handler dedupe
  - socket publish dedupe

### Key Functions

- `markIdempotent(redis, scope, key, ttlSeconds)`
  - `SET NX EX`
  - returns whether the operation is first-seen

### Redis Usage

- `idempotency:{scope}:{key}`

### Failure Handling

Redis failure propagates to caller. Duplicate detection relies on Redis availability.

## src/utils/lock.js

### Purpose

Provides Redis-based mutual exclusion with token-safe release.

### How It Fits

- Layer: concurrency utility
- Called by: ride, driver, and dispatch services

### Key Functions

- `acquireLock(redis, key, ttlMs)`
- `releaseLock(redis, key, token)`
- `withLock(redis, key, ttlMs, fn)`
- inline `RELEASE_LOCK_SCRIPT`

### Redis Usage

- `lock:ride:{rideId}`
- `lock:driver:{driverId}`
- `lock:customer:{customerId}`
- `driver:{driverId}:reservation` also uses the same token-safe release helper

### Failure Handling

- If lock cannot be acquired, caller gets `null` and usually converts that into a retry-style 409
- Token-safe release avoids deleting someone else’s lock

## src/utils/response.js

### Purpose

Defines the uniform HTTP response envelope.

### How It Fits

- Layer: API utility
- Called by: controllers and app error middleware

### Key Functions

- `success(res, data, statusCode)`
- `failure(res, error)`

### Redis Usage

None.

### Failure Handling

Shapes errors into `{ success: false, error: { code, message } }`.

## src/utils/serializers.js

### Purpose

Converts JSON and numeric fields between Redis string storage and JavaScript objects/numbers.

### How It Fits

- Layer: serialization utility
- Called by: ride/driver services and recovery

### Key Functions

- `stringify`
- `parseJson`
- `toNumber`

### Redis Usage

Indirect but central to how complex objects are stored inside hashes and lists.

### Failure Handling

- `parseJson` falls back safely rather than crashing on bad JSON
- `toNumber` falls back on invalid numeric strings

## src/workers/index.js

### Purpose

Starts all workers in one process.

### How It Fits

- Layer: worker process entrypoint
- Called by: `npm run workers`
- Calls:
  - dispatch worker
  - persistence worker
  - reconciliation worker

### Key Functions

- `start()`
  - pings Redis first
  - loads workers

### Redis Usage

- readiness `PING`

### Failure Handling

Process exits on startup failure.

## src/workers/dispatch.worker.js

### Purpose

Consumes dispatch queue jobs.

### How It Fits

- Layer: background worker
- Called by: BullMQ
- Calls: dispatch service

### Key Functions

- Worker job switch:
  - `start-dispatch`
  - `batch-timeout`
  - `driver-unavailable`

### Redis Usage

Indirect through queue and dispatch service.

### Failure Handling

- Job retries handled by BullMQ
- `failed`, `completed`, and `stalled` events logged

## src/workers/persistence.worker.js

### Purpose

Copies realtime Redis snapshots into MongoDB.

### How It Fits

- Layer: background persistence worker
- Called by: BullMQ
- Calls:
  - Mongo models
  - ride and driver services

### Key Functions

- `syncRide(rideId)`
  - reads ride snapshot, timeline, notified drivers from Redis
  - upserts Mongo `Ride`
- `syncDriver(driverId)`
  - reads driver snapshot from Redis
  - upserts Mongo `Driver`
- `startPersistenceWorker()`

### Redis Usage

- `ride:{rideId}:active`
- `ride:{rideId}:timeline`
- `ride:{rideId}:notified`
- `driver:{driverId}:state`

### Failure Handling

- BullMQ retries persistence jobs
- Upsert means reruns are idempotent enough for this system shape

## src/workers/reconciliation.worker.js

### Purpose

Runs periodic cleanup and recovery jobs.

### How It Fits

- Layer: background reconciliation worker
- Called by: BullMQ repeatable jobs
- Calls: recovery service

### Key Functions

- job switch:
  - `stale-driver-scan`
  - `dispatch-recovery-scan`
  - `rehydrate-state`

### Redis Usage

Indirect through recovery service.

### Failure Handling

- Retries through BullMQ
- Logs job failures and stalls

## src/middleware/auth.middleware.js

### Purpose

Provides optional JWT verification for HTTP and socket entrypoints without breaking existing development and test flows.

### How It Fits

- Layer: middleware / security
- Called by:
  - `src/app.js`
  - `src/socket/index.js`

### Key Functions

- `authenticateHttp(expectedRole)`
  - Express middleware
  - verifies bearer tokens only when `AUTH_ENABLED=true`
- `resolveSocketActor(handshakeAuth)`
  - resolves socket identity from raw `role/actorId` in compatibility mode
  - resolves socket identity from JWT claims when auth is enabled

### Redis Usage

None directly.

### Failure Handling

- Throws explicit auth errors for missing, invalid, expired, or role-mismatched tokens
- Behaves as a compatibility pass-through when auth is disabled

## src/services/eta.service.js

### Purpose

Calculates trip distance and ETA estimates.

### How It Fits

- Layer: additive product service
- Called by: `src/services/ride-state.service.js`

### Key Functions

- `haversineDistanceKm(origin, destination)`
- `estimateEtaMinutes(distanceKm, rideType)`
- `estimateTrip({ origin, destination, rideType })`

### Redis Usage

Indirect. Estimated outputs are written into the ride hash during ride creation.

### Failure Handling

- Pure computation with config-backed defaults
- No external dependency is introduced

## src/services/pricing.service.js

### Purpose

Calculates estimated fare at ride creation and final billing at ride completion.

### How It Fits

- Layer: additive product service
- Called by: `src/services/ride-state.service.js`

### Key Functions

- `estimateFare({ distanceKm, durationMin, rideType, waitingMin })`
- `calculateFinalFare({ ride, actualDistanceKm, actualDurationMin, waitingMin })`

### Redis Usage

Indirect. Pricing output is stored in the ride hash and later persisted to MongoDB.

### Failure Handling

- Pure computation
- Uses config defaults and ride-type normalization to stay deterministic

## src/services/driver-profile.service.js

### Purpose

Loads customer-facing driver profile data from MongoDB for notification enrichment and ride history snapshots.

### How It Fits

- Layer: enrichment helper
- Called by:
  - `src/services/notification.service.js`
  - `src/workers/persistence.worker.js`

### Key Functions

- `getDriverProfileSnapshot(driverId)`

### Redis Usage

None directly.

### Failure Handling

- Returns partial or null profile data if persistence does not yet contain full driver profile fields

## src/services/notification.service.js

### Purpose

Wraps the existing socket publisher with a higher-level notification abstraction and customer payload enrichment.

### How It Fits

- Layer: additive realtime abstraction
- Called by:
  - `src/services/ride-state.service.js`
  - `src/services/dispatch.service.js`
  - `src/controllers/driver.controller.js`
  - `src/socket/customer.socket.js`
  - `src/socket/driver.socket.js`

### Key Functions

- `buildCustomerRidePayload(ride, extra)`
- `notifyRideRequested`
- `notifyRideStatusUpdate`
- `notifyDriverAssigned`
- `notifyDriverLocationUpdate`
- `notifyDriverArriving`
- `notifyRideStarted`
- `notifyRideCompleted`
- `notifyRideCancelled`
- `emitCustomerSnapshot`
- `notifyPushPlaceholder`

### Redis Usage

Indirect through the wrapped socket publisher and its pub/sub fanout.

### Failure Handling

- Keeps the existing socket system as the delivery mechanism
- Push notification support is currently a non-blocking placeholder

## src/services/rate-limit.service.js

### Purpose

Adds Redis-backed throttling for externally exposed ride request and accept entrypoints.

### How It Fits

- Layer: platform protection
- Called by:
  - `src/controllers/customer.controller.js`
  - `src/controllers/driver.controller.js`
  - `src/socket/customer.socket.js`
  - `src/socket/driver.socket.js`

### Key Functions

- `enforceRideRequestRateLimit(customerId)`
- `enforceRideAcceptRateLimit(driverId)`

### Redis Usage

- `rate-limit:customer:request-ride:{customerId}`
- `rate-limit:driver:accept-ride:{driverId}`

### Failure Handling

- Throws 429-style domain errors at the entrypoint
- Does not alter ride or dispatch state if blocked

## src/services/driver-experience.service.js

### Purpose

Applies a short cooldown after driver rejection to reduce repeated offer spam.

### How It Fits

- Layer: additive dispatch-adjacent protection
- Called by:
  - `src/services/dispatch.service.js`
  - `src/services/driver-state.service.js`

### Key Functions

- `applyRejectCooldown(driverId, reason)`
- `isDriverOnRejectCooldown(driverId)`
- `getRejectCooldownRemainingMs(driverId)`

### Redis Usage

- `ops:driver:reject-cooldown:{driverId}`

### Failure Handling

- Fully additive behavior
- If cooldown is absent or expired, the existing reservable-driver flow continues normally

## tests/lib/config.js

### Purpose

Supplies test harness config with defaults tuned for test mode.

### How It Fits

- Layer: test utility
- Called by: `tests/lib/helpers.js`

### Key Values

- API and socket URLs
- ACK and event timeouts
- retry counts
- race/stress sizes
- recovery wait windows

### Redis Usage

Indirect via test Redis connection URL.

### Failure Handling

Invalid numeric config falls back to defaults.

## tests/lib/helpers.js

### Purpose

Shared test harness utilities for sockets, HTTP, retries, timing, and assertions.

### How It Fits

- Layer: test utility
- Called by: all manual and automated test scripts

### Key Functions

- `createId`
  - generates Redis/BullMQ-safe test IDs
- `connectSocket`
- `emitWithAck`
- `waitForEvent`
- `waitForRideStatus`
- `request`
- `createRedis`
- `disconnectSockets`
- `runScenario`
- `runStep`
- `retryAsync`
- `ackEvent`

### Redis Usage

- direct Redis connection for inspections
- indirect through app behavior

### Failure Handling

- Retries connection and ACKs in test mode
- `event_ack` failures are softened in test mode
- Scenario wrapper exits with clear PASS/FAIL signal

## tests/single-driver-flow.js

### Purpose

Validates the happy-path socket lifecycle with one driver and one customer.

### How It Fits

- Layer: automated integration test
- Calls:
  - socket events
  - ride fetch API
  - direct Redis inspection

### Flow Covered

- driver connect
- customer connect
- driver online
- ride request
- driver offer
- accept
- heartbeat/location update
- arriving
- started
- completed
- Redis cleanup checks

### Redis Usage

- checks:
  - `customer:{customerId}:activeRide`
  - `driver:{driverId}:activeRide`

### Failure Handling

Assertion failure exits the script with FAIL.

## tests/multi-driver-race.js

### Purpose

Validates that multiple concurrent driver accepts still result in one winner only.

### How It Fits

- Layer: automated race-condition test
- Calls:
  - multiple driver sockets
  - concurrent `accept_ride`
  - ride fetch API
  - Redis active ride inspection

### Flow Covered

- many drivers online
- one ride requested
- all notified drivers accept concurrently
- only one successful ACK should survive

### Redis Usage

- checks `driver:{driverId}:activeRide` across all drivers

### Failure Handling

Fails if more than one accept succeeds or Redis marks multiple drivers on the same ride.

## tests/stress-dispatch.js

### Purpose

Exercises the dispatch system under bursty multi-driver, multi-ride load.

### How It Fits

- Layer: automated stress/integration test
- Calls:
  - many driver sockets
  - many customer ride requests via HTTP
  - randomized accept/reject behavior
  - Redis duplicate assignment checks

### Flow Covered

- 50+ rides and 25+ drivers by default
- random accept/reject decisions
- random start/complete follow-through

### Redis Usage

- reads `driver:{driverId}:activeRide` across all stress drivers

### Failure Handling

Fails if duplicate active driver assignments are detected or if a driver has overlapping non-terminal rides.

## tests/failure-scenarios.js

### Purpose

Runs targeted failure/recovery scenarios.

### How It Fits

- Layer: automated failure test suite
- Calls: socket flows, Redis direct mutation, ride fetch API

### Key Scenarios

- `duplicateAcceptScenario`
  - duplicate `eventId` on `accept_ride`
  - validates idempotent handling
- `driverDropScenario`
  - first-notified driver goes offline
  - verifies backup-driver reassignment
- `redisRecoveryScenario`
  - intentionally damages dispatch keys
  - verifies dispatch recovery emits a new offer

### Redis Usage

- direct mutation in recovery scenario:
  - `ride:{rideId}:active`
  - `ride:{rideId}:responses`
  - `ride:{rideId}:notified`
  - `driver:{driverId}:reservation`

### Failure Handling

Scenario assertions fail the test if recovery does not happen inside the configured wait window.

## tests/manual-flow.js

### Purpose

Interactive socket-based CLI for stepping through the ride lifecycle manually.

### How It Fits

- Layer: manual developer tool
- Calls:
  - customer and driver socket events
  - ride fetch API
  - direct Redis inspections

### Key Functions

- `printMenu`
- `showCurrentState`
- `attachRealtimeLogs`
- `main`

### Menu Behavior

- `1` connect driver socket
- `2` connect customer socket
- `3` emit `go_online`
- `4` emit `request_ride`
- `5` emit `accept_ride`
- `6` emit `location_heartbeat`
- `7` emit `ride_arriving`
- `8` emit `ride_started`
- `9` emit `ride_completed`
- `10` inspect ride, driver, and customer pointers in Redis/API

### Redis Usage

- `driver:{driverId}:state`
- `driver:{driverId}:activeRide`
- `driver:{driverId}:reservation`
- `driver:{driverId}:dispatches`
- `customer:{customerId}:activeRide`

### Failure Handling

Interactive errors are caught and printed without crashing the menu loop immediately.

## tests/manual-api-flow.js

### Purpose

Interactive REST-based CLI that validates the same lifecycle without relying on sockets.

### How It Fits

- Layer: manual developer tool
- Calls:
  - HTTP controller endpoints
  - ride fetch API
  - direct Redis inspections

### Key Functions

- `printMenu`
- `showCurrentState`
- `main`

### Menu Behavior

- initialize synthetic IDs
- go online via HTTP
- request ride via HTTP
- accept/arriving/start/complete via HTTP
- inspect current Redis/API state

### Redis Usage

- same direct inspection keys as the socket manual flow for driver and ride debugging

### Failure Handling

Like `manual-flow.js`, errors are printed and the session stays interactive.

## Manual and Automated Test Flow Notes

### `manual-flow.js` option mapping

- Connect Driver
  - opens Socket.IO connection with `role=DRIVER`
- Connect Customer
  - opens Socket.IO connection with `role=CUSTOMER`
- Driver Go Online
  - triggers driver state creation and availability indexing
- Request Ride
  - creates ride in Redis and starts dispatch
- Accept Ride
  - exercises the atomic Lua accept path
- Send Location Update
  - refreshes heartbeat and may push customer location update
- Mark Arriving / Start Ride / Complete Ride
  - exercises ride transition state machine
- Show Current State
  - lets developers compare API-visible ride state with raw Redis keys

### Automated suite responsibilities

- `single-driver-flow.js`
  - happy-path lifecycle
- `multi-driver-race.js`
  - concurrent accept atomicity
- `stress-dispatch.js`
  - burst and stability behavior
- `failure-scenarios.js`
  - duplicate events, drop recovery, dispatch-state repair

## Debugging Guidance

### Where to start when a ride gets stuck

1. Check `ride:{rideId}:active`
2. Check `ride:{rideId}:responses`
3. Check `ride:{rideId}:notified`
4. Check `driver:{driverId}:reservation`
5. Check `driver:{driverId}:dispatches`
6. Check whether a dispatch worker logged `start-dispatch` or `batch-timeout`

### Where to start when a driver looks ghost-online

1. Check `driver:{driverId}:heartbeat`
2. Check `driver:{driverId}:state`
3. Check pool membership:
   - `drivers:online`
   - `drivers:available`
   - `drivers:busy`
   - `drivers:geo:available`
4. Review stale-driver reconciliation logs

### Where to start when customer tracking is broken

1. Confirm ride status is one of `ACCEPTED`, `ARRIVING`, `ONGOING`
2. Confirm driver heartbeat is updating
3. Confirm location payload is being emitted
4. Confirm customer socket is joined to `customer:{customerId}` room
5. Inspect `socket:event:ack:{eventId}` if pushed events are suspected to be ignored
