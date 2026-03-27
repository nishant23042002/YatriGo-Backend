function stringify(value) {
  return JSON.stringify(value || {});
}

function parseJson(value, fallback = null) {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value);
  } catch (error) {
    return fallback;
  }
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

module.exports = {
  parseJson,
  stringify,
  toNumber
};
