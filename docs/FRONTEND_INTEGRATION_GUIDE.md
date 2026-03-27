# YatriGo Frontend Integration Guide

## Purpose

This guide explains how to connect a React Native or mobile customer app to the current YatriGo backend without changing backend logic.

It is written for engineers integrating a real mobile app against the existing production-grade backend behavior.

## 1. System Readiness Check

This section evaluates the backend as it exists today.

### ✅ READY

- Redis is the source of truth for active rides and driver state.
- Customer socket auth contract is stable:
  - `role: "CUSTOMER"`
  - `actorId: customerId`
  - optional JWT token flow when `AUTH_ENABLED=true`
- Customer request flow is implemented on sockets:
  - `request_ride`
  - `cancel_ride`
  - `subscribe_ride`
- Customer outbound events are implemented and consistent:
  - `ride_requested`
  - `driver_assigned`
  - `driver_location_update`
  - `ride_status_update`
  - `snapshot`
  - `driver_connection_lost`
- ACK response shape is stable for success:
  - `{ success: true, data: {...} }`
- Ride lifecycle transitions are guarded on the backend and not free-form.
- ETA and pricing estimation are now available at ride creation time:
  - `estimatedDistanceKm`
  - `estimatedDurationMin`
  - `estimatedEtaMinutes`
  - `estimatedFare`
- Customer-facing ride payloads now support driver profile enrichment:
  - `driverProfile.name`
  - `driverProfile.phone`
  - `driverProfile.vehicleNumber`
  - `driverProfile.vehicleType`
- Driver location payloads now include tracking-friendly fields:
  - `timestamp`
  - optional `speed`
  - optional `route`
- Dispatch reliability is strong for small-town scale:
  - nearest-driver lookup
  - batched notifications
  - retries with radius expansion
  - atomic single-driver acceptance
  - recovery after stale batch state
- Reconnect support already exists:
  - a customer with an active ride can receive a `snapshot` on reconnect
  - frontend can also use `subscribe_ride`
- HTTP fallback exists for direct ride fetch:
  - `GET /api/customers/rides/:rideId`

### ⚠️ NEEDS IMPROVEMENT

- Redis eviction policy is not enforced by the app itself.
  - The code uses TTLs correctly for many keys.
  - But Redis server-level eviction policy must be configured safely outside the app.
  - Recommended production setting:
    - avoid global eviction for critical state if possible
    - if eviction must exist, do not use a policy that can randomly evict active ride state
  - This is an operations requirement before real mobile rollout.
- Authentication is optional, not mandatory.
  - The backend now supports JWT-based auth for HTTP and sockets.
  - But production rollout still requires a real identity issuer and token distribution flow.
- Customer integration is safe for one active ride at a time.
  - The backend enforces one active ride per customer.
  - Frontend should not assume it can open parallel bookings from the same account.
- Driver profile enrichment depends on persisted driver profile data.
  - If driver documents do not contain name/phone/vehicle details yet, customer payloads will still work but enriched fields may be `null`.
- Ongoing-ride disconnect handling is partial.
  - If a driver disconnects during `ONGOING`, the customer gets `driver_connection_lost`.
  - The app must handle that explicitly in UI.

## Is Backend Safe To Integrate Right Now?

### Short answer

Yes, for controlled integration and staging validation.

### Before exposing to real users

These items should be treated as pre-production blockers:

1. Plug the optional JWT auth layer into a real identity system.
2. Confirm Redis server eviction policy is safe for active ride state.
3. Validate production mobile reconnect behavior on weak networks.
4. Confirm the frontend gracefully handles delayed assignment, no-driver cancellation, and driver disconnect warnings.

## 2. Customer App Integration Flow

This section describes the exact customer-app flow from the frontend point of view.

## 2.1 Connect Socket

### Example

```js
import { io } from "socket.io-client";

const socket = io(BASE_URL, {
  transports: ["websocket"],
  auth: {
    role: "CUSTOMER",
    actorId: customerId
  }
});
```

When auth is enabled:

```js
const socket = io(BASE_URL, {
  transports: ["websocket"],
  auth: {
    role: "CUSTOMER",
    token: accessToken
  }
});
```

### Why `role` is required

The backend uses `role` during socket auth to decide:

- which room to join,
- which event handlers to register,
- which snapshot logic to run.

If `role` is missing, the connection is rejected.

### Why `actorId` must be stable

The backend uses `actorId` as the customer identity key for:

- room membership: `customer:{customerId}`
- active ride pointer: `customer:{customerId}:activeRide`
- reconnect snapshot lookup

If the app reconnects with a different `actorId`, the backend will treat it as a different customer and the app may not recover the active ride correctly.

### React Native recommendations

- Keep `customerId` tied to the authenticated user session.
- Reuse the same socket for the full customer session.
- Reconnect automatically on network loss.
- Reattach all event listeners after app cold start.

## 2.2 Request Ride

### Example

```js
const eventId = `evt-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

socket.emit(
  "request_ride",
  {
    eventId,
    origin: {
      lat: 26.9124,
      lng: 75.7873,
      address: "Pickup point"
    },
    destination: {
      lat: 26.922,
      lng: 75.79,
      address: "Drop point"
    },
    metadata: {
      source: "mobile-app"
    }
  },
  (response) => {
    if (!response?.success) {
      console.log("Ride request failed", response?.error);
      return;
    }

    console.log("Ride request success", response.data);
  }
);
```

### Required payload

- `eventId: string`
  - strongly recommended
  - used for idempotency and duplicate-event handling
- `origin`
  - `lat: number`
  - `lng: number`
  - optional `address: string`
- `destination`
  - `lat: number`
  - `lng: number`
  - optional `address: string`

### Optional payload

- `metadata`
  - `rideType` is supported and defaults to `STANDARD`

### Expected ACK response

```json
{
  "success": true,
  "data": {
    "rideId": "string",
    "customerId": "string",
    "driverId": null,
    "status": "REQUESTED",
    "origin": {
      "lat": 26.9124,
      "lng": 75.7873,
      "address": "Pickup point"
    },
    "destination": {
      "lat": 26.922,
      "lng": 75.79,
      "address": "Drop point"
    },
    "dispatchRound": 0,
    "searchRadiusKm": 2,
    "rideType": "STANDARD",
    "estimatedDistanceKm": 1.31,
    "estimatedDurationMin": 3,
    "estimatedEtaMinutes": 3,
    "estimatedFare": {
      "currency": "INR",
      "rideType": "STANDARD",
      "distanceKm": 1.31,
      "durationMin": 3,
      "waitingMin": 0,
      "baseFare": 40,
      "distanceCharge": 15.72,
      "durationCharge": 6,
      "waitingCharge": 0,
      "minimumFare": 60,
      "minimumApplied": false,
      "totalFare": 61.72,
      "commissionPercent": 20,
      "commissionAmount": 12.34,
      "driverEarning": 49.38
    },
    "currentBatchId": null,
    "currentBatchDrivers": [],
    "currentBatchExpiresAt": null,
    "acceptedAt": null,
    "cancellation": null,
    "createdAt": "ISO string",
    "updatedAt": "ISO string",
    "metadata": {},
    "version": 1
  }
}
```

### Error cases

- duplicate active ride for the customer
- socket auth missing or invalid
- malformed payload
- transient internal failure

### Frontend rule

Do not assume driver assignment is immediate after `request_ride` ACK. The ACK means the ride exists. Assignment happens asynchronously through later socket events.

## 2.3 Listen to Events

The frontend must listen to all customer events before sending `request_ride`.

### Required listeners

#### `ride_requested`

Purpose:

- confirms the backend published the ride creation event to the customer room

Frontend action:

- store ride snapshot
- set UI to `Searching` or `Finding driver`

#### `driver_assigned`

Purpose:

- tells the customer that a driver has successfully won the ride

Frontend action:

- store `driverId`
- store `driverProfile`
- update UI to assigned state
- start showing driver tracking screen

#### `driver_location_update`

Purpose:

- live map updates while the ride is trackable

Frontend action:

- update driver marker on map
- update ETA if your mobile app computes one
- prefer `timestamp` over local receipt time

#### `ride_status_update`

Purpose:

- canonical lifecycle status updates

Frontend action:

- update UI state machine
- persist latest ride status in local state/store
- optionally read `notificationType` for UX copy such as:
  - `DRIVER_ARRIVING`
  - `RIDE_STARTED`
  - `RIDE_COMPLETED`
  - `RIDE_CANCELLED`

#### `snapshot`

Purpose:

- reconnect or cold reconnect recovery for active customer ride

Frontend action:

- hydrate current ride screen from snapshot
- re-enter the correct UI state without waiting for a new ride event

#### `driver_connection_lost`

Purpose:

- warning event when driver disconnects during an ongoing ride

Frontend action:

- show warning banner or modal
- keep ride screen active
- instruct customer that connection is being restored

## 2.4 Suggested Listener Setup

```js
function registerCustomerSocketListeners(socket, handlers) {
  socket.on("ride_requested", handlers.onRideRequested);
  socket.on("driver_assigned", handlers.onDriverAssigned);
  socket.on("driver_location_update", handlers.onDriverLocationUpdate);
  socket.on("ride_status_update", handlers.onRideStatusUpdate);
  socket.on("snapshot", handlers.onSnapshot);
  socket.on("driver_connection_lost", handlers.onDriverConnectionLost);
}
```

## 2.5 Optional Ride Subscription

The backend supports:

```js
socket.emit(
  "subscribe_ride",
  { rideId, eventId },
  (response) => {
    console.log(response);
  }
);
```

Use this when:

- the app opens directly into an existing ride screen,
- the app has a stored `rideId`,
- you want a direct ride fetch over socket after reconnect.

## 2.6 Cancel Ride

### Example

```js
socket.emit(
  "cancel_ride",
  {
    eventId,
    rideId,
    reason: "USER_CANCELLED"
  },
  (response) => {
    console.log(response);
  }
);
```

### Frontend expectation

- cancellation is asynchronous but usually fast
- final UI confirmation should come from:
  - ACK success, and
  - `ride_status_update` showing `CANCELLED`

## 2.7 UI State Mapping

| Backend State | Frontend UI |
| --- | --- |
| REQUESTED | Searching |
| DISPATCHING | Finding driver |
| ACCEPTED | Driver assigned |
| ARRIVING | Driver arriving |
| ONGOING | Ride in progress |
| COMPLETED | Ride finished |
| CANCELLED | Ride cancelled |

### Important UI rule

Use `ride_status_update` as the source of truth for status rendering. Do not derive lifecycle state only from `driver_assigned`.

## 3. Full End-to-End Flow

```text
User clicks "Book Ride"
  ->
Frontend connects socket as CUSTOMER with stable actorId
  ->
Frontend registers listeners:
  ride_requested
  driver_assigned
  driver_location_update
  ride_status_update
  snapshot
  driver_connection_lost
  ->
Frontend emits request_ride with eventId, origin, destination
  ->
Backend ACK returns success with ride snapshot and rideId
  ->
Ride snapshot already contains estimated distance, ETA, and estimated fare
  ->
Backend writes ride to Redis
  ->
Backend starts dispatch
  ->
Driver(s) receive new_ride_request
  ->
One driver accepts
  ->
Backend emits driver_assigned to customer
Backend emits ride_status_update with ACCEPTED
  ->
Frontend stores rideId, driverId, driverProfile, estimated fare, and ETA
Frontend opens tracking UI
  ->
Driver sends location heartbeat
  ->
Backend emits driver_location_update
  ->
Frontend updates map in real time
  ->
Driver marks arriving
  ->
Frontend receives ride_status_update = ARRIVING
  ->
Driver starts trip
  ->
Frontend receives ride_status_update = ONGOING
  ->
Driver completes trip
  ->
Frontend receives ride_status_update = COMPLETED
  ->
Frontend closes ride flow and clears active local ride state
```

## 4. Integration Test Plan

This should be run before connecting a real mobile build to staging or production.

## 4.1 Basic Flow Test

### Checklist

- Connect customer socket successfully
- Connect driver test client successfully
- Driver goes online
- Customer requests ride
- Customer receives ACK with `rideId`
- Driver receives `new_ride_request`
- Driver accepts
- Customer receives:
  - `driver_assigned`
  - `ride_status_update` with `ACCEPTED`
- Customer receives enriched driver profile in assigned payload when available
- Driver sends heartbeat
- Customer receives `driver_location_update`
- Ride moves through:
  - `ARRIVING`
  - `ONGOING`
  - `COMPLETED`

### Expected behavior

- No duplicate active ride is created
- Only one driver is assigned
- UI transitions correctly on status updates
- After completion, customer should not remain stuck on active ride screen

## 4.2 Edge Case Test: No Driver Available

### Checklist

- Start backend with no available drivers
- Customer requests ride
- Wait through dispatch rounds

### Expected behavior

- ACK still succeeds because ride is created
- Customer may see:
  - `REQUESTED`
  - `DISPATCHING`
  - eventually `CANCELLED`
- Final cancellation reason is system-driven because no driver was found
- UI must not hang forever in “Finding driver”

## 4.3 Edge Case Test: Driver Disconnect Before Trip Starts

### Checklist

- Create ride
- Driver accepts
- Driver goes offline before ride starts

### Expected behavior

- Ride may be requeued to dispatch again
- Customer should receive updated status transitions
- Frontend must tolerate reassignment delay

## 4.4 Edge Case Test: Ride Cancelled by Customer

### Checklist

- Customer requests ride
- Customer cancels before completion

### Expected behavior

- ACK for cancellation succeeds
- Customer receives `ride_status_update` with `CANCELLED`
- UI exits active ride flow

## 4.5 Edge Case Test: Network Reconnect

### Checklist

- Start a ride
- Put mobile app in airplane mode briefly
- Reconnect

### Expected behavior

- Socket reconnects with same `actorId`
- App receives `snapshot` if customer still has an active ride
- If app stores `rideId`, it can also call `subscribe_ride`
- UI restores current ride state without requiring a new booking

## 4.6 Edge Case Test: Driver Disconnect During Ongoing Ride

### Checklist

- Move ride to `ONGOING`
- Disconnect driver socket / stop heartbeat

### Expected behavior

- Customer may receive `driver_connection_lost`
- App should show a warning, not silently close the ride
- UI should remain in ride-in-progress context until a later status update or manual support flow

## 5. Debugging Guide

## 5.1 If Ride Is Not Created

Check:

1. Did `request_ride` ACK return?
2. Does ACK contain `success: true` and `data.rideId`?
3. Did the socket connect with:
   - `role: "CUSTOMER"`
   - stable `actorId`
4. Check backend logs for:
   - socket auth success/failure
   - event received: `request_ride`
   - ride request persisted to Redis
   - rate limit rejection if too many requests were sent

Frontend tip:

- Never access `response.data.rideId` without checking `response?.success`.

## 5.2 If No Driver Is Assigned

Check backend:

1. Is any driver online?
2. Is `drivers:available` populated?
3. Is `drivers:geo:available` populated?
4. Did dispatch start?
5. Were `new_ride_request` events sent?
6. Did any driver accept?

Check frontend:

1. Is the app listening for `driver_assigned`?
2. Is the app listening for `ride_status_update`?
3. Is UI incorrectly assuming assignment should happen immediately?

## 5.3 If No Live Tracking Appears

Check backend:

1. Is driver sending `location_heartbeat`?
2. Is ride status one of:
   - `ACCEPTED`
   - `ARRIVING`
   - `ONGOING`
3. Are `driver_location_update` events being emitted?
4. Does the payload include `timestamp` and usable coordinates?

Check frontend:

1. Is listener for `driver_location_update` attached before ride acceptance?
2. Is map state updating from the event payload?
3. Is the app dropping events after reconnect?

## 5.4 If UI Is Stuck

Check:

1. Is `ride_status_update` being received?
2. Is frontend mapping backend state to UI correctly?
3. Is local state being overwritten by stale cached values?
4. Did reconnect happen without re-registering socket listeners?

Recommended fallback:

- Fetch `GET /api/customers/rides/:rideId` and reconcile UI from backend state.

## 5.5 If Reconnect Is Broken

Check:

1. Does the app reconnect with the same `actorId`?
2. Does the backend emit `snapshot`?
3. Is the app handling `snapshot` and replacing stale local ride state?
4. Is the app accidentally creating a new socket without cleaning old listeners?

## 5.6 Backend Log Categories To Watch

- `SOCKET`
- `DISPATCH`
- `RIDE`
- `DRIVER`
- `REDIS`

Most useful events during frontend integration:

- socket auth attempt/success/failure
- event received
- event sent
- dispatch started
- driver notified
- ride assigned
- ride state transitioned

## 6. Socket Event Contract

This section documents the practical contract a customer mobile app should follow.

## 6.1 Connection Auth Payload

```json
{
  "role": "CUSTOMER",
  "actorId": "customer-123"
}
```

When auth is enabled, `token` can be sent instead of trusting raw `actorId`:

```json
{
  "role": "CUSTOMER",
  "token": "jwt-token"
}
```

## 6.2 `request_ride`

### Direction

Customer -> backend

### Payload

```json
{
  "eventId": "string",
  "origin": {
    "lat": 26.9124,
    "lng": 75.7873,
    "address": "Pickup point"
  },
  "destination": {
    "lat": 26.922,
    "lng": 75.79,
    "address": "Drop point"
  },
  "metadata": {
    "source": "mobile-app",
    "rideType": "STANDARD"
  }
}
```

### ACK success shape

```json
{
  "success": true,
  "data": {
    "rideId": "string",
    "customerId": "string",
    "driverId": null,
    "status": "REQUESTED",
    "rideType": "STANDARD",
    "estimatedDistanceKm": 1.31,
    "estimatedDurationMin": 3,
    "estimatedEtaMinutes": 3,
    "estimatedFare": {
      "currency": "INR",
      "totalFare": 61.72
    }
  }
}
```

### ACK failure shape

```json
{
  "success": false,
  "error": "string"
}
```

## 6.3 `cancel_ride`

### Direction

Customer -> backend

### Payload

```json
{
  "eventId": "string",
  "rideId": "string",
  "reason": "USER_CANCELLED"
}
```

## 6.4 `subscribe_ride`

### Direction

Customer -> backend

### Payload

```json
{
  "eventId": "string",
  "rideId": "string"
}
```

## 6.5 `ride_requested`

### Direction

Backend -> customer

### Payload

Ride snapshot. Minimum fields frontend should expect:

```json
{
  "rideId": "string",
  "customerId": "string",
  "driverId": null,
  "status": "REQUESTED",
  "origin": {
    "lat": 26.9124,
    "lng": 75.7873
  },
  "destination": {
    "lat": 26.922,
    "lng": 75.79
  },
  "version": 1,
  "eventId": "string"
}
```

## 6.6 `driver_assigned`

### Direction

Backend -> customer

### Payload

Current ride snapshot with assigned driver.

Minimum fields frontend should use:

```json
{
  "rideId": "string",
  "driverId": "string",
  "status": "ACCEPTED",
  "estimatedDistanceKm": 1.31,
  "estimatedDurationMin": 3,
  "estimatedEtaMinutes": 3,
  "estimatedFare": {
    "currency": "INR",
    "totalFare": 61.72
  },
  "driverProfile": {
    "driverId": "string",
    "name": "string or null",
    "phone": "string or null",
    "vehicleNumber": "string or null",
    "vehicleType": "string or null"
  },
  "eventId": "string"
}
```

## 6.7 `driver_location_update`

### Direction

Backend -> customer

### Payload

```json
{
  "rideId": "string",
  "driverId": "string",
  "lat": 26.915,
  "lng": 75.788,
  "at": "ISO string",
  "timestamp": "ISO string",
  "speed": null,
  "route": null,
  "eventId": "string"
}
```

## 6.8 `ride_status_update`

### Direction

Backend -> customer

### Payload

Current ride snapshot. Minimum frontend fields:

```json
{
  "rideId": "string",
  "driverId": "string or null",
  "status": "REQUESTED | DISPATCHING | ACCEPTED | ARRIVING | ONGOING | COMPLETED | CANCELLED",
  "estimatedDistanceKm": 1.31,
  "estimatedDurationMin": 3,
  "estimatedEtaMinutes": 3,
  "estimatedFare": {
    "currency": "INR",
    "totalFare": 61.72
  },
  "finalFare": {
    "currency": "INR",
    "totalFare": 74.5
  },
  "billing": {
    "currency": "INR",
    "totalFare": 74.5
  },
  "notificationType": "DRIVER_ARRIVING | RIDE_STARTED | RIDE_COMPLETED | RIDE_CANCELLED | undefined",
  "driverProfile": {
    "driverId": "string",
    "name": "string or null",
    "phone": "string or null",
    "vehicleNumber": "string or null",
    "vehicleType": "string or null"
  },
  "version": 2,
  "eventId": "string"
}
```

### Important note

This is the main event the UI should trust for lifecycle progression.

## 6.9 `snapshot`

### Direction

Backend -> customer

### Payload

Same general ride snapshot shape as `ride_status_update`.

Use it to rebuild app state after reconnect.

## 6.10 `driver_connection_lost`

### Direction

Backend -> customer

### Payload

```json
{
  "rideId": "string",
  "driverId": "string",
  "at": "ISO string",
  "reason": "string",
  "eventId": "string"
}
```

## 7. Common Mistakes

- Connecting socket without a stable `actorId`
- Not sending JWT token when `AUTH_ENABLED=true`
- Attaching listeners after sending `request_ride`
- Not sending `eventId`
- Assuming `request_ride` ACK means driver is already assigned
- Using `driver_assigned` as the only lifecycle signal
- Not reading `estimatedFare` and `estimatedEtaMinutes` from the initial ride snapshot
- Ignoring `driverProfile` nullability
- Not handling `snapshot` on reconnect
- Not handling `driver_connection_lost`
- Not handling `CANCELLED` after dispatch exhausts
- Accessing `response.data` without checking `response.success`
- Creating multiple sockets for the same customer screen without cleanup
- Forgetting to remove old listeners on screen unmount
- Not providing HTTP fallback to fetch current ride state

## 8. Optional Improvements Before Connecting the Real App

These are recommendations only. They are not changes made by this guide.

### 8.1 Connect the Existing Auth Layer

The backend now supports optional JWT auth, but production still needs:

- token issuer
- token refresh policy
- secure mobile token storage
- mapping from token claims -> stable customer ID

### 8.2 Confirm Redis Production Policy

Operations should verify:

- Redis memory sizing
- eviction policy
- persistence mode and restart behavior
- monitoring for key eviction and memory pressure

### 8.3 Populate Driver Profile Data

Customer payloads now support:

- driver name
- driver phone
- vehicle number
- vehicle type

Make sure those values are actually populated in driver persistence, otherwise the fields will remain `null`.

### 8.4 Improve Log Correlation

For smoother mobile debugging, it would help to correlate:

- `eventId`
- `rideId`
- `customerId`
- socket ID

in every customer-facing log line.

### 8.5 Mobile Resilience Testing

Before production rollout, explicitly test:

- app background/foreground transitions
- airplane mode reconnect
- slow network ACK delays
- duplicate tap protection on “Book Ride”

## 9. Suggested React Native Integration Pattern

Use:

- socket as the realtime channel
- one local ride store in app state
- `ride_status_update` as lifecycle truth
- `snapshot` for reconnect recovery
- `GET /api/customers/rides/:rideId` as recovery fallback
- estimated fare and ETA from the initial ride snapshot
- final fare and billing from the terminal ride snapshot

### Practical frontend rule set

1. Connect socket on authenticated session start.
2. Register listeners immediately.
3. Generate a unique `eventId` for every emitted event.
4. On `request_ride` ACK success, store `rideId`.
5. Drive the ride screen from incoming lifecycle events.
6. On reconnect, wait for `snapshot`; if missing but you have `rideId`, fetch ride from HTTP.
7. Clear local active ride state only after terminal status:
   - `COMPLETED`
   - `CANCELLED`

## Final Recommendation

The backend is good enough to integrate with a staging React Native customer app now.

For real production users, do not skip:

- auth hardening
- Redis operations validation
- reconnect testing on poor mobile networks
- UI handling for delayed assignment and driver disconnect warnings

If those are handled, this backend is a solid foundation for a customer mobile integration.
