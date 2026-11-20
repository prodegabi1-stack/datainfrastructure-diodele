CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  device_id TEXT,
  track_name TEXT,
  created_at TEXT
);

CREATE TABLE IF NOT EXISTS laps (
  lap_id TEXT PRIMARY KEY,
  session_id TEXT,
  lap_number INTEGER,
  duration_ms INTEGER,
  max_speed_kmh REAL,
  avg_speed_kmh REAL,
  max_g_lat REAL,
  max_g_brake REAL
);

CREATE TABLE IF NOT EXISTS telemetry_points (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lap_id TEXT,
  lap_offset_ms INTEGER,
  lat REAL,
  lon REAL,
  speed_kmh REAL,
  bearing REAL,
  g_lat REAL,
  g_lon REAL,
  is_corner_apex INTEGER
);

CREATE INDEX IF NOT EXISTS idx_lap_time ON telemetry_points(lap_id, lap_offset_ms);
