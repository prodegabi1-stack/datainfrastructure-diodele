export function computeSampleSliceMetrics(slice, speedKmh) {
  if (!slice || slice.length === 0) {
    return {
      g_lat: 0,
      g_lon: 0,
      abs_g_lat: 0,
      g_brake: 0,
      total_g: 0,
      is_corner_apex: 0,
      speed_kmh: speedKmh || 0,
    };
  }

  const len = slice.length;
  const avgAx = slice.reduce((s, p) => s + (p.ax || 0), 0) / len;
  const avgAy = slice.reduce((s, p) => s + (p.ay || 0), 0) / len;
  const avgAz = slice.reduce((s, p) => s + (p.az || 0), 0) / len;
  const avgGz = slice.reduce((s, p) => s + Math.abs(p.gz || 0), 0) / len;

  const totalG = Math.sqrt(avgAx * avgAx + avgAy * avgAy + avgAz * avgAz);
  const absGLat = Number(Math.abs(avgAx).toFixed(3));
  const gBrake = avgAy < 0 ? Number(Math.abs(avgAy).toFixed(3)) : 0;
  const isCornerApex = absGLat > 0.35 || avgGz > 0.35 ? 1 : 0;

  return {
    g_lat: Number(avgAx.toFixed(3)),
    g_lon: Number(avgAy.toFixed(3)),
    abs_g_lat: absGLat,
    g_brake: gBrake,
    total_g: Number(totalG.toFixed(3)),
    is_corner_apex: isCornerApex,
    speed_kmh: speedKmh || 0,
  };
}

export function haversineMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}
