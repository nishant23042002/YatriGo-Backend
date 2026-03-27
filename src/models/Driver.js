const { Schema, model } = require("mongoose");

const driverSchema = new Schema(
  {
    driverId: {
      type: String,
      required: true,
      unique: true,
      index: true
    },
    userId: {
      type: String,
      required: true,
      index: true
    },
    name: {
      type: String,
      default: null
    },
    phone: {
      type: String,
      default: null
    },
    vehicle: {
      type: {
        make: String,
        model: String,
        plateNumber: String,
        category: String
      },
      default: {}
    },
    status: {
      type: String,
      enum: ["ONLINE", "OFFLINE", "BUSY"],
      default: "OFFLINE"
    },
    lastKnownLocation: {
      lat: Number,
      lng: Number,
      updatedAt: Date
    },
    activeRideId: {
      type: String,
      default: null
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

module.exports = model("Driver", driverSchema);
