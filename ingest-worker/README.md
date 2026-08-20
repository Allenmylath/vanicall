# vanicall ingest worker

Pulls an RTSP source and republishes it into a vanicall room over WHIP.

This exists because a browser can't do it: RTSP isn't a browser protocol and ffmpeg can't run in a
page. The "paste a URL and click Stream" flow needs a process that can hold an ffmpeg open, and
this is it.

## What it can reach

**It can only pull cameras it can route to.** Deployed to Fly, that means publicly reachable RTSP
only — a `192.168.x.x` camera is invisible from the cloud no matter what the UI does. There is no
way around this short of a tunnel or VPN.

For cameras on a home or office network, run this same service **on that network**:

```bash
cd ingest-worker
JWT_SECRET=<same secret the API signs with> npm start
```

and point the web app at it with `VITE_INGEST_WORKER_URL=http://localhost:8090`. The app also
degrades gracefully: with no worker configured it shows a ready-to-paste ffmpeg command instead.

## Design

Deliberately dumb. It knows nothing about rooms, ownership, or the database. The caller mints an
ingest credential against the vanicall API first — which is where ownership is actually enforced —
and hands the worker only `{ rtspUrl, whipUrl, token }`. Authorization stays in one place, and a
compromised worker can publish but cannot create or read anything.

## API

All routes except `/health` require `Authorization: Bearer <vanicall JWT>`, verified with the same
`JWT_SECRET` the API signs with.

| Method | Path | Notes |
| --- | --- | --- |
| `GET` | `/health` | No auth |
| `POST` | `/publish` | `{ rtspUrl, whipUrl, token, name?, roomId?, videoBitrate? }` → `202` + job |
| `GET` | `/jobs` | All jobs |
| `GET` | `/jobs/:id` | One job |
| `DELETE` | `/jobs/:id` | Stop; ffmpeg sends the WHIP DELETE on its way out |

Job status: `starting` → `connecting` → `publishing`, plus `reconnecting`, `stopped`, `failed`.

A dropped source is retried with exponential backoff (2s → 30s, up to 100 times), because cameras
reboot and links flap. That backoff also rides out the ~60s during which the API still considers
the previous publish live after a hard failure — until its reaper frees the slot, a re-publish is
rejected with 409, so retrying is correct rather than an error.

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `JWT_SECRET` | — | **Required.** Same secret the vanicall API signs with |
| `VANICALL_API_URL` | `https://vanicall.fly.dev` | Only WHIP URLs on this origin are accepted |
| `PORT` | `8090` | |
| `ALLOWED_ORIGINS` | `*` | CORS; comma-separated. Tighten in production |
| `MAX_JOBS` | `4` | Concurrent ffmpeg processes |

## Guards

- Input must be `rtsp://` or `rtsps://`, so the worker can't be made to read `file://` or reach
  into its own network.
- `whipUrl` must be on `VANICALL_API_URL`, so it can't be used as an open relay.
- RTSP credentials (`user:pass@`) are redacted from all logs and API responses.
- Runs as a non-root user, since ffmpeg is executing caller-influenced URLs.

## Deploying

```bash
fly launch --no-deploy --copy-config -c ingest-worker/fly.toml --name vanicall-ingest
fly secrets set -a vanicall-ingest JWT_SECRET="$(fly ssh console -a vanicall -C 'printenv JWT_SECRET')"
fly deploy -c ingest-worker/fly.toml --dockerfile ingest-worker/Dockerfile .
```

Deploy from the **repo root** — the image needs `../ingest-lib`.

### ffmpeg version

The image pulls a **static ffmpeg 7.1 build**, not Debian's. Bookworm ships 5.1, which has no WHIP
muxer at all — `apt-get install ffmpeg` produces a worker that cannot publish. The Dockerfile
asserts the muxer is present at build time so this fails loudly rather than at runtime.

### Why a separate Fly app

The main `vanicall` app is a small JSON relay with `auto_stop_machines` enabled — it does no media
and no crypto. ffmpeg is the opposite: sustained CPU, and a machine that must not stop while a
stream is open. Sharing the app would undo that right-sizing and let autostop kill live feeds.
