const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const { authenticateHttp } = require("./middleware/auth.middleware");
const customerRoutes = require("./routes/customer.routes");
const driverRoutes = require("./routes/driver.routes");
const { failure } = require("./utils/response");

function createApp() {
  const app = express();

  app.use(helmet());
  app.use(cors());
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.json({ success: true, message: "ok" });
  });

  app.use("/api/customers", authenticateHttp("CUSTOMER"), customerRoutes);
  app.use("/api/drivers", authenticateHttp("DRIVER"), driverRoutes);

  app.use((error, _req, res, _next) => failure(res, error));

  return app;
}

module.exports = { createApp };
