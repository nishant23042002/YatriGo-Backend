const express = require("express");
const { asyncHandler } = require("../utils/async-handler");
const driverController = require("../controllers/driver.controller");

const router = express.Router();

router.post("/status/online", asyncHandler(driverController.goOnline));
router.post("/status/offline", asyncHandler(driverController.goOffline));
router.post("/location", asyncHandler(driverController.heartbeat));
router.post("/rides/:rideId/accept", asyncHandler(driverController.acceptRide));
router.post("/rides/:rideId/reject", asyncHandler(driverController.rejectRide));
router.post("/rides/:rideId/arriving", asyncHandler(driverController.markArriving));
router.post("/rides/:rideId/start", asyncHandler(driverController.startRide));
router.post("/rides/:rideId/complete", asyncHandler(driverController.completeRide));

module.exports = router;
