-- Database schema for vanicall.
-- Apply once against your Postgres (e.g. Neon):
--   psql "$DATABASE_URL" -f schema.sql
--
-- Safe to re-run: tables use IF NOT EXISTS, and the billing columns on `users`
-- are added with ALTER ... IF NOT EXISTS so existing databases upgrade cleanly.

-- Byte counters are the integer source of truth for billing.
-- GB is a derived (generated) column so there is no floating-point drift:
--   1 GB = 1,000,000,000 bytes (decimal GB, as used for billing/metering).

CREATE TABLE IF NOT EXISTS users (
    id   UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Upgrade existing `users` tables with the billing counter + derived GB.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS total_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS total_gb DOUBLE PRECISION
        GENERATED ALWAYS AS (total_bytes::double precision / 1000000000) STORED;

-- Rooms are just a logical grouping. Cloudflare sessions are per-client
-- (created in the WS flow), so a room no longer owns a cf_session_id.
CREATE TABLE IF NOT EXISTS rooms (
    id         UUID PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drop the obsolete per-room session column on existing databases.
ALTER TABLE rooms DROP COLUMN IF EXISTS cf_session_id;

-- Room ownership + soft-delete, so a host can list and remove their own rooms.
-- `owner_user_id` is nullable for legacy rooms created before ownership existed.
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id);
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_rooms_owner ON rooms (owner_user_id) WHERE deleted_at IS NULL;

-- One row per WebSocket/signaling connection (i.e. one call session).
-- The signaling server only fills in user_id, cf_session_id, started_at, ended_at.
-- The byte columns are filled in later by the reconciler from Cloudflare's
-- usage analytics (src/bin/reconcile.rs) — billing is decoupled from signaling.
CREATE TABLE IF NOT EXISTS sessions (
    id            UUID PRIMARY KEY,
    user_id       UUID NOT NULL REFERENCES users(id),
    cf_session_id TEXT,                              -- filled in when the first SDP offer arrives
    egress_bytes  BIGINT NOT NULL DEFAULT 0,         -- Cloudflare -> client (from CF analytics)
    ingress_bytes BIGINT NOT NULL DEFAULT 0,         -- client -> Cloudflare (from CF analytics)
    bytes_used    BIGINT NOT NULL DEFAULT 0,         -- egress + ingress, the billed total
    gb_used       DOUBLE PRECISION
                      GENERATED ALWAYS AS (bytes_used::double precision / 1000000000) STORED,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at      TIMESTAMPTZ,                       -- NULL while the session is still open
    reconciled_at TIMESTAMPTZ                        -- set once billed; guarantees bill-once
);

-- Tie a session to the room it joined and capture the display name shown to peers
-- (this is what records guest names in a host's call history).
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS room_id UUID REFERENCES rooms(id);
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS display_name TEXT;
CREATE INDEX IF NOT EXISTS idx_sessions_room_id ON sessions (room_id);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions (user_id);
-- Lets the reconciler cheaply find ended-but-unbilled sessions.
CREATE INDEX IF NOT EXISTS idx_sessions_unreconciled
    ON sessions (ended_at)
    WHERE reconciled_at IS NULL;
