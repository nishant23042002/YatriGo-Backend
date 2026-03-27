const crypto = require("crypto");
const { env } = require("../config/env");
const { AppError } = require("../utils/errors");

function decodeBase64Url(value) {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padding = "=".repeat((4 - (normalized.length % 4 || 4)) % 4);
  return Buffer.from(`${normalized}${padding}`, "base64").toString("utf8");
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch (error) {
    return null;
  }
}

function verifyHs256Jwt(token) {
  const segments = String(token || "").split(".");
  if (segments.length !== 3) {
    throw new AppError("Invalid auth token format", 401, "INVALID_AUTH_TOKEN");
  }

  const [encodedHeader, encodedPayload, signature] = segments;
  const header = safeJsonParse(decodeBase64Url(encodedHeader));
  const payload = safeJsonParse(decodeBase64Url(encodedPayload));

  if (!header || !payload || header.alg !== "HS256") {
    throw new AppError("Unsupported auth token", 401, "INVALID_AUTH_TOKEN");
  }

  const expectedSignature = crypto
    .createHmac("sha256", env.auth.jwtSecret)
    .update(`${encodedHeader}.${encodedPayload}`)
    .digest("base64url");

  const actualBuffer = Buffer.from(signature);
  const expectedBuffer = Buffer.from(expectedSignature);
  if (
    actualBuffer.length !== expectedBuffer.length ||
    !crypto.timingSafeEqual(actualBuffer, expectedBuffer)
  ) {
    throw new AppError("Invalid auth token signature", 401, "INVALID_AUTH_TOKEN");
  }

  const now = Math.floor(Date.now() / 1000);
  if (payload.nbf && Number(payload.nbf) > now) {
    throw new AppError("Auth token not active yet", 401, "INVALID_AUTH_TOKEN");
  }
  if (payload.exp && Number(payload.exp) <= now) {
    throw new AppError("Auth token expired", 401, "AUTH_TOKEN_EXPIRED");
  }
  if (env.auth.issuer && payload.iss && payload.iss !== env.auth.issuer) {
    throw new AppError("Auth token issuer mismatch", 401, "INVALID_AUTH_TOKEN");
  }
  if (env.auth.audience && payload.aud && payload.aud !== env.auth.audience) {
    throw new AppError("Auth token audience mismatch", 401, "INVALID_AUTH_TOKEN");
  }

  return payload;
}

function normalizeAuthenticatedActor(payload, fallbackRole) {
  const actorId = payload.actorId || payload.sub || payload.userId || payload.driverId || payload.customerId;
  const role = String(payload.role || fallbackRole || "").toUpperCase();

  if (!actorId || !role) {
    throw new AppError("Auth token missing actor identity", 401, "INVALID_AUTH_TOKEN");
  }

  return {
    actorId,
    role,
    claims: payload
  };
}

function extractBearerToken(req) {
  const header = req.headers.authorization || "";
  if (!header.startsWith("Bearer ")) {
    return null;
  }

  return header.slice("Bearer ".length).trim();
}

function authenticateHttp(expectedRole) {
  return (req, _res, next) => {
    try {
      if (!env.auth.enabled) {
        return next();
      }

      const token = extractBearerToken(req);
      if (!token) {
        throw new AppError("Missing bearer token", 401, "AUTH_REQUIRED");
      }

      const auth = normalizeAuthenticatedActor(
        verifyHs256Jwt(token),
        expectedRole
      );

      if (expectedRole && auth.role !== String(expectedRole).toUpperCase()) {
        throw new AppError("Forbidden actor role", 403, "AUTH_ROLE_FORBIDDEN");
      }

      req.auth = auth;
      return next();
    } catch (error) {
      return next(error);
    }
  };
}

function resolveSocketActor(handshakeAuth = {}) {
  if (!env.auth.enabled) {
    const role = String(handshakeAuth.role || "").toUpperCase();
    const actorId = handshakeAuth.actorId;

    if (!role || !actorId) {
      throw new AppError("Socket auth requires role and actorId", 401, "SOCKET_AUTH_REQUIRED");
    }

    return {
      role,
      actorId,
      claims: null
    };
  }

  const token = handshakeAuth.token || handshakeAuth.accessToken;
  if (!token) {
    throw new AppError("Socket auth token is required", 401, "SOCKET_AUTH_REQUIRED");
  }

  return normalizeAuthenticatedActor(
    verifyHs256Jwt(token),
    handshakeAuth.role
  );
}

module.exports = {
  authenticateHttp,
  resolveSocketActor
};
