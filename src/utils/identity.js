function normalizeId(id) {
  if (!id) throw new Error("Invalid id");
  return String(id).trim();
}

module.exports = { normalizeId };
