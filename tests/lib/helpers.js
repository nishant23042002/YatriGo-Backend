const http = require("http");
const https = require("https");
const { io } = require("socket.io-client");
const IORedis = require("ioredis");
const {
  ackRetries,
  ackTimeoutMs,
  apiBaseUrl,
  appMode,
  connectRetries,
  eventTimeoutMs,
  pollIntervalMs,
  redisUrl,
  socketUrl
} = require("./config");

function createId(prefix) {
  const safePrefix = String(prefix || "id")
    .replace(/[^a-zA-Z0-9-_]/g, "-")
    .replace(/-+/g, "-")
    .replace(/_+/g, "_")
    .replace(/^[-_]+|[-_]+$/g, "") || "id";

  return `${safePrefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensure(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function log(message, meta) {
  const prefix = "🧪 [TEST]";
  if (meta) {
    console.log(`${prefix} ${message} ${JSON.stringify(meta)}`);
    return;
  }

  console.log(`${prefix} ${message}`);
}

function stepStart(message, meta) {
  const prefix = "🧠 [STEP START]";
  if (meta) {
    console.log(`${prefix} ${message} ${JSON.stringify(meta)}`);
    return;
  }
  console.log(`${prefix} ${message}`);
}

function stepSuccess(message, meta) {
  const prefix = "✅ [STEP OK]";
  if (meta) {
    console.log(`${prefix} ${message} ${JSON.stringify(meta)}`);
    return;
  }
  console.log(`${prefix} ${message}`);
}

function stepFailure(message, error) {
  console.error(`⚠️ [STEP FAIL] ${message}`, error ? error.stack || error.message || error : "");
}

function markPass(message, meta) {
  const prefix = "✅ [PASS]";
  if (meta) {
    console.log(`${prefix} ${message} ${JSON.stringify(meta)}`);
    return;
  }
  console.log(`${prefix} ${message}`);
}

function markFail(message, error) {
  console.error(`⚠️ [FAIL] ${message}`, error ? error.stack || error.message || error : "");
}

function request(method, path, body) {
  return retryAsync(
    `http:${method}:${path}`,
    () => {
      const url = new URL(path, apiBaseUrl);
      const client = url.protocol === "https:" ? https : http;
      const payload = body ? JSON.stringify(body) : null;

      log("HTTP request", {
        method,
        path,
        body
      });

      return new Promise((resolve, reject) => {
        const req = client.request(
          url,
          {
            method,
            headers: {
              "Content-Type": "application/json",
              "Content-Length": payload ? Buffer.byteLength(payload) : 0
            }
          },
          (res) => {
            let data = "";
            res.on("data", (chunk) => {
              data += chunk;
            });
            res.on("end", () => {
              try {
                const parsed = data ? JSON.parse(data) : {};
                if (res.statusCode >= 400 || parsed.success === false) {
                  const message =
                    (parsed.error && parsed.error.message) ||
                    `Request failed with status ${res.statusCode}`;
                  return reject(new Error(message));
                }
                resolve(parsed.data);
              } catch (error) {
                reject(error);
              }
            });
          }
        );

        req.on("error", reject);
        if (payload) {
          req.write(payload);
        }
        req.end();
      });
    },
    appMode === "test" ? 3 : 1
  );
}

function createRedis() {
  return new IORedis(redisUrl, {
    maxRetriesPerRequest: null
  });
}

async function retryAsync(actionName, fn, retries, delayMs = 400) {
  let lastError = null;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      if (attempt > 1) {
        log(`RETRY ${actionName}`, { attempt, retries });
      }
      return await fn(attempt);
    } catch (error) {
      lastError = error;
      if (attempt >= retries) {
        throw error;
      }
      await sleep(delayMs * attempt);
    }
  }

  throw lastError;
}

async function ackEvent(socket, payload) {
  if (!payload || !payload.eventId) {
    return;
  }

  await emitWithAck(socket, "event_ack", { eventId: payload.eventId }, ackTimeoutMs).catch((error) => {
    if (appMode === "test") {
      log("WARN event_ack failed in test mode", {
        eventId: payload.eventId,
        error: error.message
      });
      return null;
    }

    throw error;
  });
}

function connectSocket(role, actorId) {
  return retryAsync(
    `connectSocket:${role}:${actorId}`,
    () =>
      new Promise((resolve, reject) => {
        log("Socket connect attempt", {
          role,
          actorId,
          socketUrl,
          appMode
        });

        const socket = io(socketUrl, {
          transports: ["websocket"],
          reconnection: true,
          auth: {
            role,
            actorId
          }
        });

        const onConnect = () => {
          cleanup();
          log("Socket connect success", { role, actorId, socketId: socket.id });
          resolve(socket);
        };

        const onError = (error) => {
          cleanup();
          socket.disconnect();
          log("Socket connect failure", {
            role,
            actorId,
            error: error.message || String(error)
          });
          reject(error instanceof Error ? error : new Error(String(error)));
        };

        const timeout = setTimeout(() => {
          cleanup();
          socket.disconnect();
          reject(new Error(`Socket connect timeout for ${role}:${actorId}`));
        }, ackTimeoutMs);

        function cleanup() {
          clearTimeout(timeout);
          socket.off("connect", onConnect);
          socket.off("connect_error", onError);
        }

        socket.once("connect", onConnect);
        socket.once("connect_error", onError);
      }),
    connectRetries
  );
}

function emitWithAck(socket, event, payload, timeoutMs = ackTimeoutMs) {
  return retryAsync(
    `emitWithAck:${event}`,
    () =>
      new Promise((resolve, reject) => {
        log("Socket emit", {
          socketId: socket.id,
          event,
          payload
        });

        let settled = false;
        const timeout = setTimeout(() => {
          if (!settled) {
            settled = true;
            reject(new Error(`ACK timeout for event ${event}`));
          }
        }, timeoutMs);

        socket.emit(event, payload, (response) => {
          if (settled) {
            return;
          }

          clearTimeout(timeout);
          settled = true;

          if (!response || response.success === false) {
            reject(new Error((response && response.error) || `ACK failed for ${event}`));
            return;
          }

          log("Socket ACK received", {
            socketId: socket.id,
            event,
            response
          });
          resolve(response);
        });
      }),
    ackRetries
  );
}

function waitForEvent(socket, event, options = {}) {
  const timeoutMs = options.timeoutMs || eventTimeoutMs;
  const filter = options.filter || (() => true);

  return new Promise((resolve, reject) => {
    const handler = async (payload) => {
      try {
        if (!filter(payload)) {
          return;
        }

        cleanup();
        log("Socket event received", {
          socketId: socket.id,
          event,
          payload
        });
        await ackEvent(socket, payload);
        resolve(payload);
      } catch (error) {
        cleanup();
        reject(error);
      }
    };

    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for ${event}`));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timeout);
      socket.off(event, handler);
    }

    socket.on(event, handler);
  });
}

async function waitForCondition(check, message, timeoutMs = eventTimeoutMs) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const value = await check();
    if (value) {
      return value;
    }
    await sleep(pollIntervalMs);
  }

  throw new Error(message);
}

async function fetchRide(rideId) {
  return request("GET", `/api/customers/rides/${rideId}`);
}

async function waitForRideStatus(rideId, expectedStatus, timeoutMs = eventTimeoutMs) {
  return waitForCondition(
    async () => {
      const ride = await fetchRide(rideId);
      return ride && ride.status === expectedStatus ? ride : null;
    },
    `Ride ${rideId} did not reach status ${expectedStatus}`,
    timeoutMs
  );
}

async function disconnectSockets(sockets) {
  await Promise.all(
    sockets.filter(Boolean).map(async (socket) => {
      try {
        socket.removeAllListeners();
        socket.disconnect();
      } catch (error) {
        return null;
      }
      return null;
    })
  );
}

async function runScenario(name, fn) {
  try {
    log(`START ${name}`);
    await fn();
    markPass(name, { mode: appMode });
    process.exit(0);
  } catch (error) {
    markFail(name, error);
    process.exit(1);
  }
}

async function runStep(name, fn) {
  stepStart(name);
  try {
    const result = await fn();
    stepSuccess(name);
    return result;
  } catch (error) {
    stepFailure(name, error);
    throw error;
  }
}

module.exports = {
  ackTimeoutMs,
  connectSocket,
  createId,
  createRedis,
  disconnectSockets,
  emitWithAck,
  ensure,
  eventTimeoutMs,
  fetchRide,
  log,
  markPass,
  request,
  runStep,
  runScenario,
  sleep,
  stepFailure,
  stepStart,
  stepSuccess,
  waitForCondition,
  waitForEvent,
  waitForRideStatus
};
