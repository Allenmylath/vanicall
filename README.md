# vanicall

A Rust (axum) signaling/API server for WebRTC calls backed by the
**Cloudflare Realtime SFU** (`rtc.live.cloudflare.com/v1`) and Postgres (Neon).

## Endpoints

| Method | Path      | Description                                    |
| ------ | --------- | ---------------------------------------------- |
| GET    | `/health` | Health check (used by Fly.io)                  |
| POST   | `/login`  | Login/create user, returns a JWT               |
| POST   | `/rooms`  | Create a (logical) room                        |
| GET    | `/usage`  | Authed user's billed usage (Bearer token)      |
| GET    | `/ws`     | WebSocket signaling (JWT)                      |

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

On Fly.io it runs automatically as the `reconciler` process group (see
[`fly.toml`](fly.toml)); no extra command needed after `fly deploy`.

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

`fly.toml` defines two process groups from one image: `web` (the signaling/REST
server, listening on `$PORT` with HTTPS/WSS and a `/health` check) and
`reconciler` (the billing job). Both come up on `fly deploy`.
