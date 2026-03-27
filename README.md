# YatriGo Backend Developer Guide

## Project Overview

YatriGo is a production-grade ride-hailing backend built for a small-town deployment model where internet quality is inconsistent, drivers reconnect often, and dispatch reliability matters more than raw scale.

Technology stack:

- Node.js + Express
- Redis for active ride and driver realtime state
- MongoDB for persistence and history
- Socket.IO for realtime communication
- BullMQ for async dispatch, persistence, and reconciliation jobs

What the system does:

- Accepts ride requests from customers
- Tracks driver availability and live location
- Dispatches rides in batches to nearby drivers
- Prevents double assignment using Redis atomic guards
- Estimates trip distance, ETA, and fare at ride creation
- Calculates final billing, commission, and driver earnings at completion
- Supports optional JWT auth and Redis-backed rate limiting
- Persists ride history asynchronously

## Architecture Summary

Main runtime pieces:

- API server: [src/server.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\server.js)
- Socket server: [src/socket/index.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\socket\index.js)
- Dispatch service: [src/services/dispatch.service.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\services\dispatch.service.js)
- Ride state service: [src/services/ride-state.service.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\services\ride-state.service.js)
- Driver state service: [src/services/driver-state.service.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\services\driver-state.service.js)
- Pricing service: [src/services/pricing.service.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\services\pricing.service.js)
- ETA service: [src/services/eta.service.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\services\eta.service.js)
- Notification service: [src/services/notification.service.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\services\notification.service.js)
- Auth middleware: [src/middleware/auth.middleware.js](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\middleware\auth.middleware.js)
- Workers: [src/workers](C:\Users\USER\OneDrive\Desktop\YatriGo backend\src\workers)

High-level flow:

1. Customer requests a ride
2. ETA and fare estimate are written to Redis with the ride
3. Dispatch worker selects and reserves nearby drivers
4. Driver accepts through an atomic Redis guard
5. Ride lifecycle progresses with enriched customer notifications
6. Final billing is calculated on completion
7. MongoDB is updated asynchronously by workers

## How to Run

### 1. Setup Environment

Copy [.env.example](C:\Users\USER\OneDrive\Desktop\YatriGo backend\.env.example) to `.env`.

Recommended local debug setup:

```env
APP_MODE=test
LOG_LEVEL=DEBUG
PORT=4000
MONGO_URI=mongodb://127.0.0.1:27017/yatrigo
REDIS_URL=redis://127.0.0.1:6379
SOCKET_CORS_ORIGIN=*
AUTH_ENABLED=false
AUTH_JWT_SECRET=change-me
PRICING_BASE_FARE=40
PRICING_PER_KM=12
PRICING_PER_MINUTE=2
ETA_AVERAGE_SPEED_KMPH=28
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Start MongoDB

```bash
mongod
```

### 4. Start Redis

```bash
redis-server
```

### 5. Start the API Server

```bash
npm run dev
```

### 6. Start Workers

Open another terminal:

```bash
npm run workers
```

### 7. Verify Health

```bash
curl http://127.0.0.1:4000/health
```

## Product Layer Additions

The backend now includes additive production-readiness layers without changing the Redis-first dispatch architecture:

- ETA + distance estimation on ride creation
- Fare estimation and final billing
- Driver profile enrichment in customer-facing ride payloads
- Enriched tracking payloads with `timestamp`, optional `speed`, and optional `route`
- Optional JWT auth for HTTP and sockets
- Redis-backed rate limits for `request_ride` and `accept_ride`
- Driver reject cooldown to reduce dispatch spam

## Manual Testing Guide

### Interactive CLI

Command:

```bash
node tests/manual-flow.js
```

What it does:

- Connects real driver and customer sockets
- Lets you step through the ride lifecycle manually
- Shows responses after each action
- Prints current ride and driver Redis state on demand

Menu:

```text
1. Connect Driver
2. Connect Customer
3. Driver Go Online
4. Request Ride
5. Accept Ride
6. Send Location Update
7. Mark Arriving
8. Start Ride
9. Complete Ride
10. Show Current State
0. Exit
```

When to use it:

- To understand the full socket-driven lifecycle
- To watch dispatch happen step-by-step
- To manually inspect ride and driver state during development

### API Testing

Command:

```bash
node tests/manual-api-flow.js
```

What it does:

- Uses REST endpoints only
- Avoids socket complexity
- Lets you step through the same basic lifecycle using controller routes

When to use it:

- To debug controllers and service calls directly
- To verify lifecycle transitions without relying on realtime clients
- To compare API behavior against socket behavior

Difference from socket testing:

- `manual-flow.js` exercises realtime socket interactions
- `manual-api-flow.js` exercises REST controllers directly

## Automated Testing Guide

### Single Driver Test

```bash
npm run test:single-driver
```

Validates:

- Basic customer-driver lifecycle
- Realtime status propagation
- Completion cleanup

### Multi-Driver Race Test

```bash
npm run test:multi-driver-race
```

Validates:

- Concurrent accept attempts
- Atomic assignment
- Only one driver wins

### Stress Test

```bash
npm run test:stress
```

Validates:

- Burst ride creation
- Randomized accept/reject behavior
- Redis state stability under load

### Failure Test

```bash
npm run test:failures
```

Validates:

- Duplicate socket events
- Driver drop during dispatch
- Recovery from damaged Redis dispatch state

## Socket Events Reference

### Driver Events Sent To Backend

- `go_online`
- `go_offline`
- `location_heartbeat`
- `accept_ride`
- `reject_ride`
- `ride_arriving`
- `ride_started`
- `ride_completed`
- `event_ack`

### Driver Events Sent To Client

- `new_ride_request`
- `ride_assigned`
- `ride_cancelled`
- `snapshot`

### Customer Events Sent To Backend

- `request_ride`
- `cancel_ride`
- `subscribe_ride`
- `event_ack`

### Customer Events Sent To Client

- `ride_requested`
- `driver_assigned`
- `driver_location_update`
- `ride_status_update`
- `snapshot`
- `driver_connection_lost`

Customer payloads may now include:

- `rideType`
- `estimatedDistanceKm`
- `estimatedDurationMin`
- `estimatedEtaMinutes`
- `estimatedFare`
- `finalFare`
- `billing`
- `commissionAmount`
- `driverEarning`
- `driverProfile`
- optional `notificationType`

## Redis Key Design

Ride keys:

- `ride:{rideId}:active`
- `ride:{rideId}:timeline`
- `ride:{rideId}:notified`
- `ride:{rideId}:responses`
- `rides:active`

Driver keys:

- `driver:{driverId}:state`
- `driver:{driverId}:heartbeat`
- `driver:{driverId}:activeRide`
- `driver:{driverId}:dispatches`
- `driver:{driverId}:reservation`

Dispatch and coordination keys:

- `drivers:geo:available`
- `drivers:available`
- `drivers:busy`
- `drivers:online`
- `lock:ride:{rideId}`
- `lock:driver:{driverId}`
- `lock:customer:{customerId}`
- `socket:event:ack:{eventId}`

Operational protection keys:

- `rate-limit:customer:request-ride:{customerId}`
- `rate-limit:driver:accept-ride:{driverId}`
- `ops:driver:reject-cooldown:{driverId}`

## Debug Logs Guide

YatriGo now uses human-readable runtime logs with category prefixes.

Example style:

```js
console.log("🚀 [SOCKET] Socket connected", { role, actorId, socketId });
console.log("📥 [SOCKET] Event received", { eventName, payload });
console.log("📤 [SOCKET] Event sent", { eventName, payload });
console.log("🧠 [DISPATCH] Dispatch started", { rideId });
console.log("✅ [RIDE] Ride assigned to driver", { rideId, driverId });
console.log("⚠️ [REDIS] Lock busy", { key });
```

Prefix meanings:

| Prefix | Meaning |
| --- | --- |
| 🚀 | connection or socket lifecycle |
| 📥 | incoming event |
| 📤 | outgoing event |
| 🧠 | system logic |
| ✅ | success |
| ⚠️ | warning or error |

Log categories:

- `SOCKET`
- `DISPATCH`
- `REDIS`
- `RIDE`
- `DRIVER`
- `TEST`

What gets logged:

- Socket auth success/failure
- Socket connect/disconnect
- Incoming socket events
- Outgoing socket events
- Dispatch start, timeout, radius expansion, assignment
- Rate limit rejections and auth failures at entrypoints
- Ride state updates
- Driver state changes
- Redis lock events

What does not get logged aggressively:

- Every Redis read
- Every queue payload field
- Raw spam-level traffic

This keeps logs readable while still making lifecycle problems visible.

## How To Debug System Flow Quickly

For a new developer, the fastest path is:

1. Start server and workers with `APP_MODE=test` and `LOG_LEVEL=DEBUG`
2. Run `node tests/manual-flow.js`
3. Connect driver and customer
4. Trigger ride request and accept it
5. Use `Show Current State` after each step
6. Watch server logs for `SOCKET`, `DISPATCH`, `RIDE`, and `DRIVER` categories
7. Confirm `estimatedFare`, `estimatedEtaMinutes`, and `driverProfile` are present where expected

## Useful Redis Inspection Commands

```bash
redis-cli HGETALL ride:<rideId>:active
redis-cli LRANGE ride:<rideId>:timeline 0 -1
redis-cli HGETALL driver:<driverId>:state
redis-cli GET driver:<driverId>:activeRide
redis-cli GET driver:<driverId>:reservation
redis-cli SMEMBERS rides:active
redis-cli SMEMBERS drivers:available
redis-cli ZRANGE drivers:geo:available 0 -1
redis-cli GET rate-limit:customer:request-ride:<customerId>
redis-cli GET rate-limit:driver:accept-ride:<driverId>
redis-cli TTL ops:driver:reject-cooldown:<driverId>
```

## Useful Commands

```bash
npm run dev
npm run workers
npm run test:single-driver
npm run test:multi-driver-race
npm run test:stress
npm run test:failures
npm run test:manual-flow
npm run test:manual-api-flow
```
