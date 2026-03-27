function success(res, data, statusCode = 200) {
  return res.status(statusCode).json({
    success: true,
    data
  });
}

function failure(res, error) {
  return res.status(error.statusCode || 500).json({
    success: false,
    error: {
      code: error.code || "INTERNAL_SERVER_ERROR",
      message: error.message || "Unexpected error"
    }
  });
}

module.exports = { success, failure };
