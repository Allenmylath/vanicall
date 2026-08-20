# vanicall

A Rust (axum) signaling/API server for WebRTC calls backed by the
**Cloudflare Realtime SFU** (`rtc.live.cloudflare.com/v1`) and Postgres (Neon).

## Endpoints

| Method | Path                      | Description                                          |
| ------ | ------------------------- | ---------------------------------------------------- |
| GET    | `/health`                 | Health check (used by Fly.io)                        |
| POST   | `/login`                  | Login/create user, returns a JWT                     |
| POST   | `/rooms`                  | Create a (logical) room                              |
| GET    | `/rooms/:id/config`       | Public room config (`name`, `e2ee`) — no auth        |
| POST   | `/rooms/:id/ingests`      | Mint an external-source publish credential (owner)   |
| GET    | `/rooms/:id/ingests`      | List a room's ingests (owner)                        |
| DELETE | `/ingests/:id`            | Revoke an ingest, cutting off any live publish       |
| POST   | `/whip/:ingest_id`        | WHIP publish — SDP offer in, SDP answer out          |
| DELETE | `/whip/:ingest_id/:res`   | WHIP teardown                                        |
| GET    | `/usage`                  | Authed user's billed usage (Bearer token)            |
| GET    | `/ws`                     | WebSocket signaling (JWT)                            |

## Signaling protocol (`/ws`)

Follows Cloudflare's documented session lifecycle. **Each client gets one
session = one PeerConnection**, created **server-side** on connect — the client
never sends its own session id, so it can't act on a session it doesn't own.

On connect the server replies:

```json
{ "action": "session_created", "cf_session_id": "<id>" }
```

Then the client drives publish/subscribe over the socket (all act on the
server-owned session):

```jsonc
// Publish local tracks: client offer -> CF answer
{ "action": "publish", "sdp": "<offer-sdp>",
  "tracks": [{ "mid": "0", "trackName": "mic" }, { "mid": "1", "trackName": "cam" }] }
// -> { "action": "publish_answer", "sessionDescription": {…}, "tracks": [...] }

// Subscribe to remote tracks (published by other sessions) -> CF offer
{ "action": "subscribe",
  "tracks": [{ "sessionId": "<other-cf-session>", "trackName": "mic" }] }
// -> { "action": "subscribe_offer", "sessionDescription": {…},
//      "requiresImmediateRenegotiation": true }

// Finish a renegotiation: client answer
{ "action": "renegotiate", "sdp": "<answer-sdp>" }
// -> { "action": "renegotiated" }
```

### Presence / rooms

Cloudflare's SFU has no built-in rooms, so the server keeps an in-memory roster
and fans out join/leave/publish events. After `session_created`, the client
joins a room (created via `POST /rooms`):

```jsonc
{ "action": "join", "room_id": "<uuid>" }
```

It immediately gets the current roster, then live events as peers come and go:

```jsonc
// current participants (excludes you), sent once on join
{ "action": "roster", "peers": [
  { "cf_session_id": "...", "user_name": "alice", "tracks": ["mic", "cam"] } ] }

// a new participant joined
{ "action": "peer_joined", "cf_session_id": "...", "user_name": "bob", "tracks": [] }
// a participant published/updated tracks  -> now you can `subscribe` to them
{ "action": "peer_published", "cf_session_id": "...", "tracks": ["mic"] }
// a participant disconnected
{ "action": "peer_left", "cf_session_id": "..." }
```

Typical flow: `join` → for each peer in `roster`/`peer_published`, send a
`subscribe` for their `cf_session_id` + `trackName`, then answer the resulting
`subscribe_offer` with `renegotiate`. When you `publish`, everyone else in the
room gets a `peer_published` and subscribes to you. Presence is in-memory per
server instance — fine for a single Fly machine; for multiple `web` machines
you'd back it with Redis/postgres `LISTEN`/NOTIFY.

## External video ingest (WHIP)

Non-browser sources — RTSP cameras, ffmpeg, OBS, file pumps — publish into a room
over **WHIP**, so nothing has to implement the WebSocket protocol above. WHIP is
one exchange (POST an SDP offer, get an SDP answer, DELETE to stop), which maps
1:1 onto Cloudflare's `tracks/new` with `location: "local"`. Media flows encoder
↔ Cloudflare directly; this server only relays SDP, exactly as it does for
browsers.

An ingest appears in the roster as a normal member with `"role": "ingest"`, and
publishes as `mic` + `cam` (or `screen`) — the track names the web client
subscribes to. Peers see it as another tile with no client-side changes.

```bash
# 1. Create a room that accepts external sources, and mint a credential for it.
ROOM=$(curl -s -X POST "$API/rooms" -H "Authorization: Bearer $JWT" \
  -H 'content-type: application/json' \
  -d '{"name":"Front door","e2ee":false}' | jq -r .room_id)

curl -s -X POST "$API/rooms/$ROOM/ingests" -H "Authorization: Bearer $JWT" \
  -H 'content-type: application/json' -d '{"name":"Front door cam"}'
# { "ingest_id": "...", "whip_url": "https://…/whip/<id>", "token": "…", "expires_at": "…" }

# 2. Point any encoder at it. Requires ffmpeg 7.0+ (older builds have no WHIP muxer).
ffmpeg -re -i rtsp://192.168.1.9/stream \
  -c:v libx264 -profile:v baseline -tune zerolatency -bf 0 -g 60 -pix_fmt yuv420p \
  -c:a libopus -ar 48000 -ac 2 -b:a 64k \
  -f whip -authorization "$TOKEN" "$WHIP_URL"
```

[`ingest-lib/`](ingest-lib/) wraps both steps (`@vanicall/ingest`), including
spawning ffmpeg with the right flags.

**Ingest requires `e2ee: false` on the room.** An encoder cannot join the room's
MLS group, and browsers drop frames they can't decrypt, so a plaintext publisher
would render as static in every tile. `POST /rooms/:id/ingests` returns **409**
for an encrypted room, so ingest can never silently downgrade one; the web client
reads `GET /rooms/:id/config` before connecting and shows a "Not encrypted" badge.

Ingest tokens are scoped to one room and one track name and expire after 30 days;
only their SHA-256 is stored, so the plaintext is returned once at mint. Cloudflare
usage is billed to the room owner via a normal `sessions` row.

Because an encoder holds no connection to this server, a crashed one can't be
detected the way the WebSocket heartbeat detects a vanished browser. A per-publish
reaper polls Cloudflare's session state and tears the ingest down when it reports
the session gone (`410`), with a hard max lifetime as a backstop.

## Billing (GB usage)

Billing uses **Cloudflare's own usage analytics** as the source of truth, so the
numbers match what Cloudflare bills you for and clients can't tamper with them.
It runs as a **separate reconciler process** ([`src/bin/reconcile.rs`](src/bin/reconcile.rs)) —
the signaling server does no byte counting.

**How it splits:**

- **Signaling server** records only the mapping it alone knows: which user used
  which Cloudflare `cf_session_id`, plus `started_at` / `ended_at`.
- **Reconciler** finds ended-but-unbilled sessions and queries Cloudflare's
  `callsUsageAdaptiveGroups` GraphQL dataset (filtered by `sessionId`) for the
  real `egressBytes` + `ingressBytes`, then writes them to the session and adds
  them to the user's running total.

**Data model** (see [`schema.sql`](schema.sql)):

- `sessions` — one row per call: `user_id`, `cf_session_id`, `started_at`,
  `ended_at`, `egress_bytes`, `ingress_bytes`, `bytes_used` (the billed total),
  derived `gb_used`, and `reconciled_at` (set once billed → guarantees each call
  is billed exactly once).
- `users.total_bytes` — running total, incremented by the reconciler.
  `users.total_gb` is a derived column (`total_bytes / 1e9`).

Bytes are the integer source of truth; GB is computed, so there's no rounding
drift. `1 GB = 1,000,000,000 bytes`.

**Why a separate process / delay:** Cloudflare's analytics is delayed and
adaptively sampled. The reconciler waits `RECONCILE_SETTLE_SECS` (default 5 min)
after a call ends before trusting the numbers, then bills it.

**Run the reconciler:**

```bash
# Continuous (loops every RECONCILE_INTERVAL_SECS) — how fly.toml runs it:
RECONCILE_INTERVAL_SECS=600 cargo run --bin reconcile

# One-shot (single pass then exit) — for a cron / Fly scheduled machine:
cargo run --bin reconcile
```

On Fly.io it runs as a **scheduled machine** (hourly, one-shot) so it costs nothing between runs,
rather than an always-on loop. Create it once, in the same sitting as the deploy that removes the
old always-on process group (until you do, billing does not run):

```bash
fly deploy      # this destroys the old always-on reconciler machine
fly machine run . /usr/local/bin/reconcile   --schedule hourly --vm-size shared-cpu-1x --vm-memory 512 -a vanicall
```

Calls are then billed within the hour instead of within 10 minutes. That is harmless — Cloudflare's
usage analytics settles slowly anyway, which is why each run waits `RECONCILE_SETTLE_SECS` before
trusting a call's numbers.

**Read a user's bill:**

```bash
curl https://<app>.fly.dev/usage -H "Authorization: Bearer <jwt>"
# { "user_id": "...", "total_bytes": 1234567890, "total_gb": 1.23456789 }
```

> Per-user accuracy assumes each participant uses their own Cloudflare session
> (the normal Calls pattern), since attribution is by `sessionId`.

## Configuration

All secrets come from environment variables (see [`.env.example`](.env.example)):

- `DATABASE_URL` — Postgres connection string
- `JWT_SECRET` — secret for signing JWTs
- `CF_APP_ID`, `CF_APP_SECRET` — Cloudflare Realtime app credentials, used for
  the SFU session/track API at `rtc.live.cloudflare.com`
- `CF_ACCOUNT_ID` — Cloudflare account id (reconciler / analytics only)
- `CF_ANALYTICS_API_TOKEN` — Cloudflare token with **Account Analytics**
  permission (used by the reconciler only)
- `PUBLIC_BASE_URL` — absolute origin this server is reachable at. Only used to
  build the WHIP URL handed to encoders, which can't resolve a relative path.
  Defaults to `https://vanicall.fly.dev`; set it for local dev or another host.
- `PORT` — listen port (Fly.io sets this automatically; defaults to `8080`)
- `RECONCILE_*` — reconciler tuning (see [`.env.example`](.env.example))

## Local development

```bash
# 1. Set env vars (e.g. export from .env)
# 2. Apply DB schema
psql "$DATABASE_URL" -f schema.sql
# 3. Run
cargo run
```

## Deploy to Fly.io

```bash
# Install flyctl, then authenticate
fly auth login

# First time: create the app (don't deploy yet)
fly launch --no-deploy --copy-config --name vanicall

# Set secrets (never commit these)
fly secrets set \
  DATABASE_URL="postgres://...:...@ep-xxx.neon.tech/neondb?sslmode=require" \
  JWT_SECRET="$(openssl rand -hex 32)" \
  CF_APP_ID="..." \
  CF_APP_SECRET="..." \
  CF_ACCOUNT_ID="..." \
  CF_ANALYTICS_API_TOKEN="..."

# Apply the database schema once
psql "$DATABASE_URL" -f schema.sql

# Deploy (creates both the `web` and `reconciler` machines)
fly deploy
```

`fly.toml` defines the `web` process (the signaling/REST server, on `$PORT`
with HTTPS/WSS and a `/health` check), which scales to zero when idle. The
billing reconciler is a separate scheduled machine (see the billing section
above) rather than a process group, so it too runs only when it has work.
