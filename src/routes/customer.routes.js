const express = require("express");
const { asyncHandler } = require("../utils/async-handler");
const customerController = require("../controllers/customer.controller");

const router = express.Router();

router.post("/rides/request", asyncHandler(customerController.requestRide));
router.post("/rides/:rideId/cancel", asyncHandler(customerController.cancelRide));
router.get("/rides/:rideId", asyncHandler(customerController.getRide));

module.exports = router;
