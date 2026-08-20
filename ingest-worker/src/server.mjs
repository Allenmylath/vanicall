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

/** jobId -> job */
const jobs = new Map();

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
    send(res, 200, { status: "ok", jobs: jobs.size });
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
    };
    jobs.set(job.jobId, job);
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

server.listen(PORT, () => {
  console.log("ingest worker on :" + PORT + "  (publish target: " + VANICALL_API_URL + ")");
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
