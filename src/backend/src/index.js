import { computeSampleSliceMetrics, haversineMeters } from "./metrics.js";

export class TelemetryHub {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.webClients = new Set();
    this.sessions = new Map();
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const role = url.searchParams.get("role") || "phone";

    if (role === "web") {
      this.webClients.add(server);
      server.addEventListener("close", () => this.webClients.delete(server));
      return new Response(null, { status: 101, webSocket: client });
    }

    let activeSessionId = url.searchParams.get("session") || `ses_${Date.now()}`;
    const defaultTrack = url.searchParams.get("track") || "Default-Track";
    const defaultDevice = url.searchParams.get("device") || "S23-Driver";

    server.addEventListener("message", async (event) => {
      try {
        const data = JSON.parse(event.data);

        if (data.session_id) {
          activeSessionId = data.session_id;
        }

        if (data.event_type === "STOP_SESSION") {
          await this.finalizeSession(activeSessionId);
          return;
        }

        if (data.event_type === "telemetry_batch" && Array.isArray(data.samples) && data.samples.length > 0) {
          const trackName = data.track_name || defaultTrack;
          const deviceId = data.device_id || defaultDevice;
          await this.processBatch(data, activeSessionId, trackName, deviceId);
        }
      } catch (err) {
        console.error("WS error:", err);
      }
    });

    server.addEventListener("close", async () => {
      await this.finalizeSession(activeSessionId);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  async processBatch(data, sessionId, trackName, deviceId) {
    let ses = this.sessions.get(sessionId);
    const firstSampleTs = data.samples[0].ts;
    const lastSampleTs = data.samples[data.samples.length - 1].ts;

    if (!ses) {
      await this.env.DB.prepare(
        "INSERT OR IGNORE INTO sessions (session_id, device_id, track_name, created_at) VALUES (?, ?, ?, ?)"
      )
        .bind(sessionId, deviceId, trackName, new Date().toISOString())
        .run();

      ses = {
        sessionId,
        trackName,
        deviceId,
        startLat: data.gps?.lat || 0,
        startLon: data.gps?.lon || 0,
        lapNumber: 1,
        lapStartTime: firstSampleTs,
        lastSampleTs: lastSampleTs,
        leftStartZone: false,
        lapMaxSpeed: 0,
        lapSpeedSum: 0,
        lapPointCount: 0,
        lapMaxGLat: 0,
        lapMaxGBrake: 0,
        pointsBuffer: [],
      };
      this.sessions.set(sessionId, ses);
    }

    const gps = data.gps || { lat: 0, lon: 0, speed: 0, bearing: 0 };
    const speedKmh = Number((gps.speed * 3.6).toFixed(1));

    if (ses.startLat === 0 && gps.lat !== 0 && gps.lon !== 0) {
      ses.startLat = gps.lat;
      ses.startLon = gps.lon;
    }

    const chunkSize = Math.max(1, Math.floor(data.samples.length / 4));
    let latestFrame = null;

    for (let i = 0; i < data.samples.length; i += chunkSize) {
      const slice = data.samples.slice(i, i + chunkSize);
      if (slice.length === 0) continue;

      const m = computeSampleSliceMetrics(slice, speedKmh);

      if (speedKmh > ses.lapMaxSpeed) ses.lapMaxSpeed = speedKmh;
      ses.lapSpeedSum += speedKmh;
      ses.lapPointCount += 1;

      if (m.abs_g_lat > ses.lapMaxGLat) ses.lapMaxGLat = m.abs_g_lat;
      if (m.g_brake > ses.lapMaxGBrake) ses.lapMaxGBrake = m.g_brake;

      const sliceEndTs = slice[slice.length - 1].ts;
      ses.lastSampleTs = sliceEndTs;

      const lapId = `${ses.sessionId}_lap_${String(ses.lapNumber).padStart(2, "0")}`;
      const lapOffsetMs = Math.max(0, sliceEndTs - ses.lapStartTime);

      const point = {
        lap_id: lapId,
        lap_offset_ms: lapOffsetMs,
        lat: gps.lat,
        lon: gps.lon,
        speed_kmh: speedKmh,
        bearing: gps.bearing,
        g_lat: m.g_lat,
        g_lon: m.g_lon,
        is_corner_apex: m.is_corner_apex,
      };

      ses.pointsBuffer.push(point);
      latestFrame = { ...point, total_g: m.total_g };
    }

    if (ses.startLat !== 0 && gps.lat !== 0) {
      const dist = haversineMeters(ses.startLat, ses.startLon, gps.lat, gps.lon);
      if (dist > 25) {
        ses.leftStartZone = true;
      } else if (dist < 15 && ses.leftStartZone && lastSampleTs - ses.lapStartTime > 10000) {
        await this.saveCompletedLap(ses, lastSampleTs);
        ses.lapNumber += 1;
        ses.lapStartTime = lastSampleTs;
        ses.leftStartZone = false;
        ses.lapMaxSpeed = 0;
        ses.lapSpeedSum = 0;
        ses.lapPointCount = 0;
        ses.lapMaxGLat = 0;
        ses.lapMaxGBrake = 0;
      }
    }

    if (ses.pointsBuffer.length >= 20) {
      await this.flushPointsToD1(ses);
    }

    if (latestFrame) {
      this.broadcastToWeb({
        type: "LIVE_FRAME",
        status: "STREAMING",
        session_id: ses.sessionId,
        device_id: ses.deviceId,
        track_name: ses.trackName,
        current_lap: ses.lapNumber,
        current_lap_time_ms: Math.max(0, lastSampleTs - ses.lapStartTime),
        frame: latestFrame,
      });
    }
  }

  async flushPointsToD1(ses) {
    if (ses.pointsBuffer.length === 0) return;
    const toInsert = [...ses.pointsBuffer];
    ses.pointsBuffer = [];

    const stmt = this.env.DB.prepare(
      "INSERT INTO telemetry_points (lap_id, lap_offset_ms, lat, lon, speed_kmh, bearing, g_lat, g_lon, is_corner_apex) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    await this.env.DB.batch(
      toInsert.map((p) =>
        stmt.bind(p.lap_id, p.lap_offset_ms, p.lat, p.lon, p.speed_kmh, p.bearing, p.g_lat, p.g_lon, p.is_corner_apex)
      )
    );
  }

  async saveCompletedLap(ses, endTs) {
    await this.flushPointsToD1(ses);
    const lapId = `${ses.sessionId}_lap_${String(ses.lapNumber).padStart(2, "0")}`;
    const durationMs = Math.max(0, endTs - ses.lapStartTime);
    const avgSpeed = ses.lapPointCount > 0
      ? Number((ses.lapSpeedSum / ses.lapPointCount).toFixed(1))
      : 0;

    await this.env.DB.prepare(
      "INSERT OR REPLACE INTO laps (lap_id, session_id, lap_number, duration_ms, max_speed_kmh, avg_speed_kmh, max_g_lat, max_g_brake) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
      .bind(
        lapId,
        ses.sessionId,
        ses.lapNumber,
        durationMs,
        ses.lapMaxSpeed,
        avgSpeed,
        Number(ses.lapMaxGLat.toFixed(2)),
        Number(ses.lapMaxGBrake.toFixed(2))
      )
      .run();
  }

  async finalizeSession(sessionId) {
    const ses = this.sessions.get(sessionId);
    if (!ses) return;
    await this.saveCompletedLap(ses, ses.lastSampleTs);
    this.sessions.delete(sessionId);
    this.broadcastToWeb({ type: "SESSION_ENDED", session_id: sessionId });
  }

  broadcastToWeb(msg) {
    const text = JSON.stringify(msg);
    for (const client of this.webClients) {
      try {
        client.send(text);
      } catch {
        this.webClients.delete(client);
      }
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    if (url.pathname === "/ws") {
      const id = env.TELEMETRY_HUB.idFromName("global-hub");
      return env.TELEMETRY_HUB.get(id).fetch(request);
    }

    if (url.pathname === "/api/sessions") {
      const { results } = await env.DB.prepare(
        "SELECT * FROM sessions ORDER BY created_at DESC LIMIT 50"
      ).all();
      return Response.json(results, { headers: corsHeaders });
    }

    const lapsMatch = url.pathname.match(/^\/api\/sessions\/([^/]+)\/laps$/);
    if (lapsMatch) {
      const sessionId = decodeURIComponent(lapsMatch[1]);
      const { results } = await env.DB.prepare(
        "SELECT * FROM laps WHERE session_id = ? ORDER BY lap_number ASC"
      )
        .bind(sessionId)
        .all();
      return Response.json(results, { headers: corsHeaders });
    }

    const telemetryMatch = url.pathname.match(/^\/api\/sessions\/([^/]+)\/telemetry$/);
    if (telemetryMatch) {
      const sessionId = decodeURIComponent(telemetryMatch[1]);
      const { results } = await env.DB.prepare(
        `SELECT tp.*, l.lap_number 
         FROM telemetry_points tp 
         JOIN laps l ON tp.lap_id = l.lap_id 
         WHERE l.session_id = ? 
         ORDER BY l.lap_number ASC, tp.lap_offset_ms ASC`
      )
        .bind(sessionId)
        .all();
      return Response.json(results, { headers: corsHeaders });
    }

    const lapPointsMatch = url.pathname.match(/^\/api\/laps\/([^/]+)\/telemetry$/);
    if (lapPointsMatch) {
      const lapId = decodeURIComponent(lapPointsMatch[1]);
      const { results } = await env.DB.prepare(
        "SELECT * FROM telemetry_points WHERE lap_id = ? ORDER BY lap_offset_ms ASC"
      )
        .bind(lapId)
        .all();
      return Response.json(results, { headers: corsHeaders });
    }

    return new Response("F1 Telemetry API Online", { headers: corsHeaders });
  },
};
