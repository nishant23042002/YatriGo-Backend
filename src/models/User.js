const { Schema, model } = require("mongoose");

const userSchema = new Schema(
  {
    userId: {
      type: String,
      required: true,
      unique: true,
      index: true
    },
    name: {
      type: String,
      required: true
    },
    phone: {
      type: String,
      required: true,
      unique: true
    },
    role: {
      type: String,
      enum: ["CUSTOMER", "DRIVER", "ADMIN"],
      required: true
    }
  },
  {
    timestamps: true
  }
);

module.exports = model("User", userSchema);
