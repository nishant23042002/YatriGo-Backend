const Driver = require("../models/Driver");

async function getDriverProfileSnapshot(driverId) {
  if (!driverId) {
    return null;
  }

  const driver = await Driver.findOne({ driverId }).lean().catch(() => null);
  const metadata = (driver && driver.metadata) || {};
  const vehicle = (driver && driver.vehicle) || {};

  return {
    driverId,
    name: (driver && driver.name) || metadata.name || null,
    phone: (driver && driver.phone) || metadata.phone || null,
    vehicleNumber: vehicle.plateNumber || metadata.vehicleNumber || null,
    vehicleType: vehicle.category || metadata.vehicleType || null,
    vehicle: {
      make: vehicle.make || metadata.vehicleMake || null,
      model: vehicle.model || metadata.vehicleModel || null,
      plateNumber: vehicle.plateNumber || metadata.vehicleNumber || null,
      category: vehicle.category || metadata.vehicleType || null
    }
  };
}

module.exports = {
  getDriverProfileSnapshot
};
