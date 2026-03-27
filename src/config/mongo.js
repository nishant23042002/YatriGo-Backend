const mongoose = require("mongoose");
const { env } = require("./env");
const { logger } = require("./logger");

let connectPromise = null;

async function connectMongo() {
  if (mongoose.connection.readyState === 1) {
    return mongoose.connection;
  }

  if (connectPromise) {
    return connectPromise;
  }

  mongoose.set("strictQuery", true);
  connectPromise = mongoose.connect(env.mongoUri, {
    autoIndex: true
  });
  try {
    await connectPromise;
  } finally {
    connectPromise = null;
  }
  logger.info("MongoDB connected", { uri: env.mongoUri });
}

module.exports = { connectMongo, mongoose };
