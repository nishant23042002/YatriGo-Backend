const { Schema, model } = require("mongoose");

const pointSchema = new Schema(
  {
    lat: { type: Number, required: true },
    lng: { type: Number, required: true },
    address: { type: String, default: "" }
  },
  { _id: false }
);

const timelineSchema = new Schema(
  {
    event: { type: String, required: true },
    actorType: { type: String, default: "SYSTEM" },
    actorId: { type: String, default: "" },
    at: { type: Date, required: true },
    metadata: { type: Schema.Types.Mixed, default: {} }
  },
  { _id: false }
);

const rideSchema = new Schema(
  {
    rideId: {
      type: String,
      required: true,
      unique: true,
      index: true
    },
    customerId: {
      type: String,
      required: true,
      index: true
    },
    driverId: {
      type: String,
      default: null,
      index: true
    },
    status: {
      type: String,
      required: true,
      index: true
    },
    origin: {
      type: pointSchema,
      required: true
    },
    destination: {
      type: pointSchema,
      required: true
    },
    rideType: {
      type: String,
      default: "STANDARD"
    },
    estimatedDistanceKm: {
      type: Number,
      default: 0
    },
    estimatedDurationMin: {
      type: Number,
      default: 0
    },
    estimatedEtaMinutes: {
      type: Number,
      default: 0
    },
    estimatedFare: {
      type: Schema.Types.Mixed,
      default: null
    },
    actualDistanceKm: {
      type: Number,
      default: 0
    },
    actualDurationMin: {
      type: Number,
      default: 0
    },
    finalFare: {
      type: Schema.Types.Mixed,
      default: null
    },
    billing: {
      type: Schema.Types.Mixed,
      default: null
    },
    commissionAmount: {
      type: Number,
      default: 0
    },
    driverEarning: {
      type: Number,
      default: 0
    },
    driverSnapshot: {
      type: Schema.Types.Mixed,
      default: null
    },
    dispatch: {
      round: { type: Number, default: 0 },
      radiusKm: { type: Number, default: 0 },
      currentBatchId: { type: String, default: null },
      currentBatchDrivers: { type: [String], default: [] },
      currentBatchExpiresAt: { type: Date, default: null },
      notifiedDrivers: { type: [String], default: [] }
    },
    cancellation: {
      actorType: String,
      actorId: String,
      reason: String,
      at: Date
    },
    timeline: {
      type: [timelineSchema],
      default: []
    },
    metadata: {
      type: Schema.Types.Mixed,
      default: {}
    }
  },
  {
    timestamps: true
  }
);

module.exports = model("Ride", rideSchema);
