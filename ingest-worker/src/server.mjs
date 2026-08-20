/**
 * vanicall ingest worker.
 *
 * Pulls an RTSP source and republishes it into a vanicall room over WHIP. This exists because a
 * browser cannot do it: RTSP is not a browser protocol and ffmpeg cannot run in a page, so the
 * "paste a URL and click Stream" flow needs a process somewhere that can hold an ffmpeg open.
 *
 * Deliberately dumb. It knows nothing about rooms, ownership, or the database — the caller mints
 * an ingest credential against the vanicall API first (which is where ownership is actually
 * enforced) and hands the worker only `{ rtspUrl, whipUrl, token }`. That keeps authorization in
 * one place and means a compromised worker can publish, but cannot create or read anything.
 *
 * REACHABILITY: this can only pull cameras it can route to. Deployed to Fly, that means publicly
 * reachable RTSP only — a 192.168.x.x camera is invisible from the cloud no matter what the UI
 * does. For LAN cameras, run this same service on that network and point the app at it.
 */
import http from "node:http";
import { randomUUID } from "node:crypto";
import jwt from "jsonwebtoken";
import { spawnPublish } from "@vanicall/ingest";

const PORT = Number(process.env.PORT ?? 8090);
const JWT_SECRET = process.env.JWT_SECRET;
/** Only WHIP URLs on this origin are accepted, so the worker can't be used as an open relay. */
const VANICALL_API_URL = (process.env.VANICALL_API_URL ?? "https://vanicall.fly.dev").replace(/\/+$/, "");
/** Browsers calling this cross-origin. Comma-separated, or `*`. */
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS ?? "*").split(",").map((s) => s.trim());
/** A ceiling on concurrent ffmpeg processes, since each is real CPU on a small VM. */
const MAX_JOBS = Number(process.env.MAX_JOBS ?? 4);

if (!JWT_SECRET) {
  console.error("JWT_SECRET is required (the same secret the vanicall API signs with)");
  process.exit(1);
}

/** Restart backoff for a source that drops — cameras reboot, links flap. */
const RESTART_BASE_MS = 2000;
const RESTART_MAX_MS = 30000;
const MAX_RESTARTS = 100;

/**
 * Scale-to-zero. When the worker has held zero jobs for this long, it stops its own Fly machine so
 * nothing bills while idle. It wakes on the next request via `auto_start_machines`.
 *
 * This is done in the app rather than by Fly's connection-based autostop on purpose: a running
 * ffmpeg holds no inbound HTTP connection, so Fly would see an idle machine and stop it mid-stream,
 * killing every live feed. Keying the decision on `jobs.size` instead means a machine only ever
 * stops when it genuinely has no streams. Set IDLE_SHUTDOWN_MS=0 to disable (e.g. when running the
 * worker locally, off Fly).
 */
const IDLE_SHUTDOWN_MS = Number(process.env.IDLE_SHUTDOWN_MS ?? 5 * 60 * 1000);
// Poll often enough to notice the idle window promptly, but never more than every 30s in prod.
const IDLE_CHECK_MS = Math.max(1000, Math.min(30 * 1000, Math.floor(IDLE_SHUTDOWN_MS / 3) || 1000));

/**
 * Stop a stream once its room has had zero human viewers for this long. A room whose only members
 * are feeds is one nobody is watching, so publishing into it just burns CPU and keeps the worker
 * awake. The grace absorbs a viewer refreshing or briefly dropping. Set HUMANLESS_STOP_MS=0 to
 * disable (a feed then runs until explicitly stopped, regardless of audience).
 *
 * Only applies to streams whose /publish carried a `roomId` (the UI always sends one). Without it
 * the worker can't ask about the room, so the stream just runs — a safe default.
 */
const HUMANLESS_STOP_MS = Number(process.env.HUMANLESS_STOP_MS ?? 90 * 1000);
// Poll frequently enough that the grace window is the thing that decides timing, not the interval.
const PRESENCE_CHECK_MS = Math.max(1000, Math.min(20 * 1000, Math.floor(HUMANLESS_STOP_MS / 3) || 1000));

/** jobId -> job */
const jobs = new Map();

/** Epoch ms when the worker last had at least one job (or booted). Drives the idle shutdown. */
let lastActiveAt = Date.now();
const markActive = () => { lastActiveAt = Date.now(); };

const nowIso = () => new Date().toISOString();

/** RTSP URLs routinely carry `user:pass@`. Never echo those back or log them. */
function redactRtsp(url) {
  try {
    const u = new URL(url);
    if (u.username) u.username = "***";
    if (u.password) u.password = "***";
    return u.toString();
  } catch {
    return "rtsp://<unparseable>";
  }
}

function publicJob(j) {
  return {
    jobId: j.jobId,
    name: j.name,
    rtspUrl: redactRtsp(j.rtspUrl),
    roomId: j.roomId ?? null,
    status: j.status,
    restarts: j.restarts,
    startedAt: j.startedAt,
    lastError: j.lastError ?? null,
  };
}

function startFfmpeg(job) {
  if (job.stopped) return;

  job.status = "connecting";
  const pub = spawnPublish(job.rtspUrl, job.whipUrl, job.token, {
    inputArgs: ["-rtsp_transport", "tcp"],
    realtime: false,
    stats: true,
    videoBitrate: job.videoBitrate,
    onLog: (line) => {
      // ffmpeg's stderr is the only real signal we get about the source.
      if (/frame=|speed=/.test(line)) {
        job.status = "publishing";
        return;
      }
      if (/error|failed|denied|refused|timed out/i.test(line)) {
        job.lastError = line.slice(0, 300);
        console.warn("[" + job.jobId + "] " + line);
      }
    },
  });
  job.pub = pub;

  pub.done.then(
    () => {
      if (job.stopped) {
        job.status = "stopped";
        return;
      }
      // The source ended on its own (a file, or a camera that closed the connection).
      scheduleRestart(job, "source ended");
    },
    (e) => {
      if (job.stopped) {
        job.status = "stopped";
        return;
      }
      job.lastError = String(e && e.message ? e.message : e).split("\n")[0].slice(0, 300);
      scheduleRestart(job, job.lastError);
    },
  );
}

function scheduleRestart(job, why) {
  if (job.stopped) return;
  if (job.restarts >= MAX_RESTARTS) {
    job.status = "failed";
    job.lastError = "gave up after " + MAX_RESTARTS + " restarts: " + why;
    console.error("[" + job.jobId + "] " + job.lastError);
    return;
  }
  job.restarts += 1;
  // Exponential backoff. This also rides out the ~60s during which the API still considers the
  // previous publish live after a hard failure — until its reaper frees the slot a re-publish is
  // rejected with 409, so retrying is the correct response rather than an error.
  const delay = Math.min(RESTART_BASE_MS * Math.pow(2, job.restarts - 1), RESTART_MAX_MS);
  job.status = "reconnecting";
  console.warn("[" + job.jobId + "] restart " + job.restarts + " in " + delay + "ms — " + why);
  job.restartTimer = setTimeout(() => startFfmpeg(job), delay);
}

function stopJob(job) {
  job.stopped = true;
  if (job.restartTimer) clearTimeout(job.restartTimer);
  try {
    if (job.pub) job.pub.stop();
  } catch {
    // Already gone.
  }
  job.status = "stopped";
}

// ---------------- HTTP ----------------

function cors(req, res) {
  const origin = req.headers.origin;
  let allow = null;
  if (ALLOWED_ORIGINS.includes("*")) allow = "*";
  else if (origin && ALLOWED_ORIGINS.includes(origin)) allow = origin;
  if (allow) res.setHeader("Access-Control-Allow-Origin", allow);
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Authorization,Content-Type");
  res.setHeader("Vary", "Origin");
}

function send(res, code, body) {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}

/** The caller must be a logged-in vanicall user; verified with the API's own signing secret. */
function authed(req, res) {
  const header = req.headers.authorization || "";
  const token = header.replace(/^Bearer /, "");
  if (!token) {
    send(res, 401, { error: "unauthorized" });
    return null;
  }
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch {
    send(res, 401, { error: "unauthorized" });
    return null;
  }
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (c) => {
      body += c;
      if (body.length > 1e6) reject(new Error("body too large"));
    });
    req.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        reject(e);
      }
    });
  });
}

const server = http.createServer(async (req, res) => {
  cors(req, res);
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, "http://" + req.headers.host);
  const path = url.pathname;

  if (path === "/health") {
    send(res, 200, { status: "ok", jobs: jobs.size, selfStop: selfStopStatus });
    return;
  }

  if (path === "/publish" && req.method === "POST") {
    if (!authed(req, res)) return;

    let body;
    try {
      body = await readJson(req);
    } catch {
      send(res, 400, { error: "invalid json" });
      return;
    }

    const { rtspUrl, whipUrl, token, name, roomId, videoBitrate } = body;

    // Only rtsp(s). Without this the worker would happily read file:// or http:// on its own host.
    if (typeof rtspUrl !== "string" || !/^rtsps?:\/\//i.test(rtspUrl)) {
      send(res, 400, { error: "rtspUrl must start with rtsp:// or rtsps://" });
      return;
    }
    // Pin the publish target so this can't be pointed at an arbitrary host.
    if (typeof whipUrl !== "string" || !whipUrl.startsWith(VANICALL_API_URL + "/whip/")) {
      send(res, 400, { error: "whipUrl must be on " + VANICALL_API_URL });
      return;
    }
    if (typeof token !== "string" || !token) {
      send(res, 400, { error: "token is required" });
      return;
    }

    const live = Array.from(jobs.values()).filter((j) => !j.stopped).length;
    if (live >= MAX_JOBS) {
      send(res, 429, { error: "worker at capacity (" + MAX_JOBS + " concurrent streams)" });
      return;
    }

    const job = {
      jobId: randomUUID(),
      name: typeof name === "string" && name ? name : "RTSP source",
      rtspUrl,
      whipUrl,
      token,
      roomId: typeof roomId === "string" ? roomId : null,
      videoBitrate: typeof videoBitrate === "string" ? videoBitrate : undefined,
      status: "starting",
      restarts: 0,
      stopped: false,
      startedAt: nowIso(),
      // Epoch ms since the room was first seen with no humans (null = humans present / not yet
      // checked). When this exceeds HUMANLESS_STOP_MS the stream is stopped as unwatched.
      humanlessSince: null,
    };
    jobs.set(job.jobId, job);
    markActive();
    console.log("[" + job.jobId + "] publishing " + redactRtsp(rtspUrl) + " -> " + whipUrl);
    startFfmpeg(job);
    send(res, 202, publicJob(job));
    return;
  }

  if (path === "/jobs" && req.method === "GET") {
    if (!authed(req, res)) return;
    send(res, 200, { jobs: Array.from(jobs.values()).map(publicJob) });
    return;
  }

  const jobMatch = path.match(/^\/jobs\/([0-9a-fA-F-]{36})$/);
  if (jobMatch) {
    if (!authed(req, res)) return;
    const job = jobs.get(jobMatch[1]);
    if (!job) {
      send(res, 404, { error: "no such job" });
      return;
    }
    if (req.method === "GET") {
      send(res, 200, publicJob(job));
      return;
    }
    if (req.method === "DELETE") {
      stopJob(job);
      console.log("[" + job.jobId + "] stopped by request");
      // Keep it listed briefly so the UI can show the final state, then forget it.
      setTimeout(() => jobs.delete(job.jobId), 60000);
      send(res, 200, publicJob(job));
      return;
    }
  }

  send(res, 404, { error: "not found" });
});

/**
 * Whether idle self-stop can actually work, surfaced in /health so a wrong or missing FLY_API_TOKEN
 * is caught by a curl at deploy time instead of silently at the first idle window five minutes in.
 *   armed         — token present and accepted by the Fly API; scale-to-zero will work
 *   token-invalid — a token is set but the Fly API rejected it (typo / wrong org / bad scope)
 *   no-token      — running on Fly but FLY_API_TOKEN is unset; will NOT scale down
 *   off-fly       — not on a Fly machine (local dev); self-stop falls back to process exit
 *   disabled      — IDLE_SHUTDOWN_MS=0
 *   checking      — preflight not finished yet
 */
let selfStopStatus = "checking";

/**
 * Preflight the self-stop path at boot: a read-only GET on this machine with the token. It proves
 * the exact credential and permission the later stop call needs, without stopping anything.
 */
async function verifySelfStop() {
  if (IDLE_SHUTDOWN_MS <= 0) { selfStopStatus = "disabled"; return; }

  const machineId = process.env.FLY_MACHINE_ID;
  const appName = process.env.FLY_APP_NAME;
  const token = process.env.FLY_API_TOKEN;

  if (!machineId || !appName) { selfStopStatus = "off-fly"; return; }
  if (!token) {
    selfStopStatus = "no-token";
    console.warn("FLY_API_TOKEN is not set — this worker will NOT scale to zero. See README.");
    return;
  }
  try {
    const r = await fetch(
      "https://api.machines.dev/v1/apps/" + appName + "/machines/" + machineId,
      { headers: { Authorization: "Bearer " + token } },
    );
    if (r.ok) {
      selfStopStatus = "armed";
      console.log("self-stop armed (FLY_API_TOKEN verified)");
    } else {
      selfStopStatus = "token-invalid";
      console.error(
        "FLY_API_TOKEN was REJECTED by the Fly API (HTTP " + r.status + ") — self-stop will not " +
          "work. Recreate it: fly tokens create deploy -a " + appName,
      );
    }
  } catch (e) {
    // Network hiccup at boot shouldn't be reported as a bad token; leave it to retry via the log.
    selfStopStatus = "checking";
    console.warn("self-stop preflight could not reach the Fly API: " + (e && e.message ? e.message : e));
  }
}

server.listen(PORT, () => {
  console.log("ingest worker on :" + PORT + "  (publish target: " + VANICALL_API_URL + ")");
  void verifySelfStop();
});

// Stop every publish cleanly on shutdown so each sends its WHIP DELETE and the tiles disappear
// immediately, rather than lingering until the API's reaper notices.
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    console.log(sig + " — stopping " + jobs.size + " job(s)");
    for (const job of jobs.values()) stopJob(job);
    setTimeout(() => process.exit(0), 1500);
  });
}

// ---------------- Scale to zero ----------------

/**
 * Stop this Fly machine when it has been idle long enough. Fly then keeps it stopped until a
 * request arrives (auto_start_machines), so an idle worker costs nothing.
 *
 * Uses the Fly Machines API rather than process.exit(): a machine deployed via `fly deploy`
 * restarts its process on exit, which would just loop instead of scaling down. Stopping the
 * machine is the operation that actually parks it. FLY_MACHINE_ID and FLY_APP_NAME are injected by
 * Fly; FLY_API_TOKEN must be provided as a secret. Off Fly (local dev), none of that is set and
 * this falls back to a plain exit.
 */
async function stopSelfIfIdle() {
  if (IDLE_SHUTDOWN_MS <= 0) return;
  if (jobs.size > 0) { markActive(); return; }
  if (Date.now() - lastActiveAt < IDLE_SHUTDOWN_MS) return;

  const machineId = process.env.FLY_MACHINE_ID;
  const appName = process.env.FLY_APP_NAME;
  const token = process.env.FLY_API_TOKEN;

  // Off Fly (local dev): a plain exit is the right "scale to zero".
  if (!machineId || !appName) {
    console.log("idle — exiting");
    process.exit(0);
  }

  // On Fly with no usable token, stopping is impossible AND exiting would just restart-loop (Fly
  // restarts a cleanly-exited service machine every IDLE_SHUTDOWN_MS). Stay up instead; /health
  // already reports this as no-token / token-invalid so it's visible.
  if (!token) return;

  console.log("idle for " + Math.round(IDLE_SHUTDOWN_MS / 1000) + "s — stopping machine " + machineId);
  try {
    const r = await fetch(
      "https://api.machines.dev/v1/apps/" + appName + "/machines/" + machineId + "/stop",
      { method: "POST", headers: { Authorization: "Bearer " + token } },
    );
    // On success Fly stops the machine and this process is killed; reaching here means it didn't.
    if (!r.ok) {
      selfStopStatus = "token-invalid";
      console.warn("self-stop failed: HTTP " + r.status + " " + (await r.text()).slice(0, 200));
    }
  } catch (e) {
    console.warn("self-stop request errored: " + (e && e.message ? e.message : e));
  }
}

setInterval(() => { void stopSelfIfIdle(); }, IDLE_CHECK_MS).unref();

// ---------------- Stop streams nobody is watching ----------------

/**
 * For each live job that carried a roomId, ask the API how many humans are in that room. Once a
 * room has been humanless for HUMANLESS_STOP_MS, stop the stream (a real stop — it will not
 * restart), which then lets the worker go idle and scale down.
 *
 * A failed presence check is treated as "unknown", not "empty": the humanless timer resets, so a
 * transient API blip never reaps a watched stream. Ingests are excluded from the count server-side,
 * so a room containing only feeds correctly reads as zero humans.
 */
async function stopUnwatchedStreams() {
  if (HUMANLESS_STOP_MS <= 0) return;

  for (const job of jobs.values()) {
    if (job.stopped || !job.roomId) continue;
    try {
      const r = await fetch(VANICALL_API_URL + "/rooms/" + encodeURIComponent(job.roomId) + "/presence");
      if (!r.ok) { job.humanlessSince = null; continue; }
      const { humans } = await r.json();

      if (humans > 0) {
        job.humanlessSince = null;
        continue;
      }
      // No humans. Start (or continue) the countdown.
      if (job.humanlessSince == null) job.humanlessSince = Date.now();
      else if (Date.now() - job.humanlessSince >= HUMANLESS_STOP_MS) {
        console.log("[" + job.jobId + "] no viewers for " + Math.round(HUMANLESS_STOP_MS / 1000) + "s — stopping");
        stopJob(job);
        setTimeout(() => jobs.delete(job.jobId), 60000);
      }
    } catch {
      // Could not reach the API — treat as unknown, not empty.
      job.humanlessSince = null;
    }
  }
}

setInterval(() => { void stopUnwatchedStreams(); }, PRESENCE_CHECK_MS).unref();
