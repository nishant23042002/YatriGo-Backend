const express = require("express");
const { estimate } = require("../controllers/pricing.controller");

const router = express.Router();

router.post("/estimate", estimate);

module.exports = router;
