use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

// Cloudflare Realtime SFU session/track API base.
// NOTE: this is NOT api.cloudflare.com — that host is only for app management.
const CF_SFU_BASE: &str = "https://rtc.live.cloudflare.com/v1";

// WebSocket heartbeat. The browser WebSocket API gives pages no way to send pings, so a call where
// everyone is merely listening produces zero inbound frames. Without a heartbeat a peer that
// vanishes (laptop closed, network dropped) is only noticed when TCP eventually gives up — minutes
// during which the roster still advertises a member other clients try to subscribe to.
//
// We ping every interval; if a ping is still unanswered when the next tick fires, the peer is gone.
// Detection therefore takes between one and two intervals. Browsers reply to ping frames
// automatically, so this needs no client-side support.
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

// How long a minted ingest token stays usable. Long enough for a camera to be configured once and
// left alone for a while, short enough that a token leaked into a shell history or a device config
// stops working. Rotate by minting a new ingest.
const INGEST_TOKEN_TTL_HOURS: i64 = 24 * 30;

// An ingest has no socket, so nothing tells us when its encoder dies — a `kill -9`ed ffmpeg would
// otherwise leave a ghost tile in the roster forever. We poll Cloudflare for the session's track
// state on this interval and reap once the track is definitively gone (see `spawn_ingest_reaper`).
const INGEST_REAP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

// Backstop for the reaper: even if Cloudflare keeps reporting a track as live, a publish is torn
// down after this long. Re-POST to the WHIP endpoint to continue; encoders reconnect on their own.
const INGEST_MAX_LIFETIME: std::time::Duration = std::time::Duration::from_secs(12 * 60 * 60);

// Roster roles. Clients use these to label a tile and to skip host controls on a feed that has no
// human behind it to obey them.
const ROLE_HUMAN: &str = "human";
const ROLE_INGEST: &str = "ingest";

// --- Configuration (loaded from environment) ---
#[derive(Clone)]
struct Config {
    // App ID goes in the SFU URL path; App Secret is the Bearer token for it.
    cf_app_id: String,
    cf_app_secret: String,
    jwt_secret: String,
    // Absolute origin this server is reachable at. Only used to build the WHIP URL handed to
    // encoders, which cannot resolve a relative path.
    public_base_url: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            cf_app_id: require_env("CF_APP_ID"),
            cf_app_secret: require_env("CF_APP_SECRET"),
            jwt_secret: require_env("JWT_SECRET"),
            public_base_url: env::var("PUBLIC_BASE_URL")
                .unwrap_or_else(|_| "https://vanicall.fly.dev".to_string())
                .trim_end_matches('/')
                .to_string(),
        }
    }
}

fn require_env(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Missing required environment variable: {key}"))
}

// --- App State ---
#[derive(Clone)]
struct AppState {
    db: PgPool,
    config: Arc<Config>,
    presence: Arc<Presence>,
    // Shared, pooled HTTP client for the Cloudflare SFU API. Reused across calls so each request
    // doesn't pay a fresh TLS handshake, and bounded by a timeout so a stalled CF response can't
    // wedge a signaling task forever. (reqwest::Client is internally Arc'd — cloning is cheap.)
    http: reqwest::Client,
    // Currently-publishing WHIP ingests, keyed by ingest id. Unlike a browser, an encoder holds no
    // connection to this server between the initial POST and the final DELETE, so the state that a
    // WebSocket's stack frame would normally own has to live here instead.
    //
    // A `None` value is a reservation held across the Cloudflare round-trip in `whip_publish`, so
    // two encoders racing to publish on the same ingest can't both win. Presence of the key — not
    // its value — is what "already publishing" means.
    ingests: Arc<Mutex<HashMap<Uuid, Option<LiveIngest>>>>,
}

/// A WHIP publish that is currently live. Everything needed to tear it down from any of the three
/// paths that can end it: the encoder's `DELETE`, an owner revoking the ingest, or the reaper.
#[derive(Clone)]
struct LiveIngest {
    /// Nonce in the `Location` URL. Checked on `DELETE` so a stale resource URL from a previous
    /// publish can't tear down the current one.
    resource: String,
    cf_session_id: String,
    room_id: String,
    /// `sessions` row id, for stamping `ended_at`.
    billing_id: Uuid,
    /// `(mid, trackName)` for each published track. Mids close them, names identify them in the
    /// roster and in Cloudflare's session state.
    tracks: Vec<(String, String)>,
}

// --- Presence ---
// In-memory roster of who is in each room. Cloudflare's SFU has no concept of
// rooms, so the app layer tracks participants and broadcasts join/leave/publish
// events over the WebSocket so clients can discover and subscribe to each other.

#[derive(Clone)]
struct MemberInfo {
    user_name: String,
    cf_session_id: String,
    tracks: Vec<String>,
    // `ROLE_HUMAN` for a browser on a WebSocket, `ROLE_INGEST` for an external encoder publishing
    // over WHIP. Ingests have no socket and no client code, so they can neither send nor act on
    // relay messages (host controls, E2EE key exchange).
    role: &'static str,
}

struct Room {
    members: HashMap<String, MemberInfo>, // keyed by cf_session_id
    // Per-member delivery channel (the member's socket-writer queue). Fan-out pushes directly here
    // rather than through a lossy broadcast bus: MLS control messages (Welcome/Commit) MUST NOT be
    // dropped, or a peer never keys and is stuck "negotiating" forever. Unbounded + FIFO per
    // recipient guarantees reliable, ordered delivery, which is what the E2EE handshake relies on.
    senders: HashMap<String, mpsc::UnboundedSender<Message>>,
}

impl Room {
    /// Send a pre-serialized payload to every member except `origin`.
    fn fanout(&self, origin: &str, payload: &str) {
        for (sid, tx) in &self.senders {
            if sid != origin {
                let _ = tx.send(Message::Text(payload.to_string()));
            }
        }
    }
}

#[derive(Default)]
struct Presence {
    rooms: Mutex<HashMap<String, Room>>,
}

impl Presence {
    /// Add a member to a room. Returns the existing roster (for the newcomer). `tx` is the
    /// member's socket-writer queue, registered for reliable fan-out. Announces the join to
    /// everyone else.
    fn join(
        &self,
        room_id: &str,
        member: MemberInfo,
        tx: mpsc::UnboundedSender<Message>,
    ) -> Vec<MemberInfo> {
        let mut rooms = self.rooms.lock().unwrap();
        let room = rooms.entry(room_id.to_string()).or_insert_with(|| Room {
            members: HashMap::new(),
            senders: HashMap::new(),
        });

        let existing: Vec<MemberInfo> = room.members.values().cloned().collect();

        let payload = serde_json::json!({
            "action": "peer_joined",
            "cf_session_id": member.cf_session_id,
            "user_name": member.user_name,
            "tracks": member.tracks,
            "role": member.role,
        })
        .to_string();
        room.fanout(&member.cf_session_id, &payload);

        room.senders.insert(member.cf_session_id.clone(), tx);
        room.members.insert(member.cf_session_id.clone(), member);
        existing
    }

    /// Record a member's published tracks and announce them to the room.
    fn publish(&self, room_id: &str, cf_session_id: &str, tracks: Vec<String>) {
        let mut rooms = self.rooms.lock().unwrap();
        if let Some(room) = rooms.get_mut(room_id) {
            if let Some(m) = room.members.get_mut(cf_session_id) {
                m.tracks = tracks.clone();
            }
            let payload = serde_json::json!({
                "action": "peer_published",
                "cf_session_id": cf_session_id,
                "tracks": tracks,
            })
            .to_string();
            room.fanout(cf_session_id, &payload);
        }
    }

    /// Fan out an opaque payload to everyone in a room. Used for E2EE control messages: the
    /// server never parses or stores `payload`, it only relays it. `origin` is the sender's
    /// cf_session_id so the echo is skipped.
    fn relay(&self, room_id: &str, origin: &str, payload: String) {
        let rooms = self.rooms.lock().unwrap();
        if let Some(room) = rooms.get(room_id) {
            room.fanout(origin, &payload);
        }
    }

    /// Remove a member, announce the departure, and drop the room if now empty.
    fn leave(&self, room_id: &str, cf_session_id: &str) {
        let mut rooms = self.rooms.lock().unwrap();
        if let Some(room) = rooms.get_mut(room_id) {
            room.members.remove(cf_session_id);
            room.senders.remove(cf_session_id);
            let payload = serde_json::json!({
                "action": "peer_left",
                "cf_session_id": cf_session_id,
            })
            .to_string();
            room.fanout(cf_session_id, &payload);
            if room.members.is_empty() {
                rooms.remove(room_id);
            }
        }
    }
}

// --- Data Structures ---
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    name: String,
    exp: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoginRequest {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoginResponse {
    token: String,
    user_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateRoomRequest {
    name: String,
    /// Defaults to true, so a caller that doesn't know about the flag gets an encrypted room.
    /// Pass false to allow external ingest — see `create_ingest`.
    #[serde(default = "default_true")]
    e2ee: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomResponse {
    room_id: String,
    e2ee: bool,
}

#[derive(Debug, Deserialize)]
struct CreateIngestRequest {
    /// Display name for the feed in the roster, e.g. "Front door camera".
    name: String,
    /// Track name the feed publishes video as. Only `cam` and `screen` are ever subscribed to by
    /// the web client, so anything else would be announced and then silently ignored.
    #[serde(default = "default_track_name")]
    track_name: String,
}

fn default_track_name() -> String {
    "cam".to_string()
}

// --- Main Entry Point ---
#[tokio::main]
async fn main() {
    // Load .env for local dev; a no-op in production (no file present).
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .init();

    tracing::info!("Starting server...");

    let config = Config::from_env();
    let database_url = require_env("DATABASE_URL");

    // 1. Connect to Postgres (e.g. Neon)
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to the database");

    tracing::info!("Connected to Postgres!");

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .expect("failed to build HTTP client");

    let state = AppState {
        db,
        config: Arc::new(config),
        presence: Arc::new(Presence::default()),
        http,
        ingests: Arc::new(Mutex::new(HashMap::new())),
    };

    // 2. Setup CORS so the frontend can talk to the API.
    //    Tighten `allow_origin` to your real frontend origin in production.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
        // WHIP puts the resource URL in `Location`; a browser-based encoder can't read it without
        // this. Native encoders (ffmpeg, OBS) aren't subject to CORS and don't need it.
        .expose_headers(Any);

    // 3. Build Router
    let app = Router::new()
        .route("/health", get(health))
        .route("/login", post(login))
        .route("/rooms", get(list_rooms).post(create_room))
        .route("/rooms/:id", delete(delete_room))
        .route("/rooms/:id/config", get(room_config))
        .route("/rooms/:id/sessions", get(room_sessions))
        .route("/rooms/:id/ingests", get(list_ingests).post(create_ingest))
        .route("/ingests/:id", delete(revoke_ingest))
        // WHIP (WebRTC-HTTP Ingestion Protocol) — how external encoders publish. Authenticated by
        // the ingest's own bearer token, not a JWT: these are called by ffmpeg/OBS/a camera, which
        // have no notion of a vanicall login.
        .route("/whip/:ingest_id", post(whip_publish))
        .route("/whip/:ingest_id/:resource", delete(whip_delete))
        .route("/usage", get(get_usage))
        .route("/ws", get(ws_handler))
        .layer(cors)
        .with_state(state);

    // Fly.io provides the port via $PORT; default to 8080 locally.
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = format!("0.0.0.0:{port}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!("Server running on http://{addr}");
    axum::serve(listener, app).await.unwrap();
}

// --- Health check (used by Fly.io) ---
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

// --- REST API: Authentication ---
async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> impl IntoResponse {
    let user_name = payload.name.clone();

    // 1. Check if user exists
    let existing_user: Option<(Uuid,)> = sqlx::query_as("SELECT id FROM users WHERE name = $1")
        .bind(&user_name)
        .fetch_optional(&state.db)
        .await
        .unwrap();

    // 2. Determine user_id (login or create)
    let user_id = match existing_user {
        Some((id,)) => id, // User exists
        None => {
            // Create new user
            let new_id = Uuid::new_v4();
            sqlx::query("INSERT INTO users (id, name) VALUES ($1, $2)")
                .bind(new_id)
                .bind(&user_name)
                .execute(&state.db)
                .await
                .unwrap();
            new_id
        }
    };

    // 3. Generate JWT
    let exp = (Utc::now() + Duration::hours(24)).timestamp() as usize;
    let claims = Claims {
        sub: user_id.to_string(),
        name: user_name,
        exp,
    };

    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.config.jwt_secret.as_ref()),
    )
    .unwrap();

    Json(LoginResponse {
        token,
        user_id: user_id.to_string(),
    })
}

// --- REST API: Room Creation ---
// A room is just a logical grouping. Per Cloudflare's model a *session* maps to
// one client's PeerConnection, so sessions are created per-client in the WS flow
// (handle_socket), not per-room.
async fn create_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CreateRoomRequest>,
) -> axum::response::Response {
    // Creating (hosting) a room requires auth; the creator becomes the owner.
    let owner_id = match authed_user_id(&headers, &state.config.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    let room_id = Uuid::new_v4();
    if let Err(e) =
        sqlx::query("INSERT INTO rooms (id, name, owner_user_id, e2ee) VALUES ($1, $2, $3, $4)")
            .bind(room_id)
            .bind(&payload.name)
            .bind(owner_id)
            .bind(payload.e2ee)
            .execute(&state.db)
            .await
    {
        tracing::error!("create_room failed: {e}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "could not create room" })),
        )
            .into_response();
    }

    Json(RoomResponse { room_id: room_id.to_string(), e2ee: payload.e2ee }).into_response()
}

/// Public room configuration, fetched by clients before they connect.
///
/// Unauthenticated on purpose: guests join by link and have no token until they log in, but the
/// client must know whether the room is end-to-end encrypted *before* it constructs its
/// `CallClient` — `e2ee` is a constructor option there, so it cannot arrive over the WebSocket.
/// Nothing here is sensitive: the caller already holds the room id, and the name is on the
/// invite page anyway.
async fn room_config(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Ok(room_id) = Uuid::parse_str(&id) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid room id" })))
            .into_response();
    };

    let row: Option<(String, bool)> =
        sqlx::query_as("SELECT name, e2ee FROM rooms WHERE id = $1 AND deleted_at IS NULL")
            .bind(room_id)
            .fetch_optional(&state.db)
            .await
            .unwrap_or(None);

    match row {
        Some((name, e2ee)) => Json(serde_json::json!({
            "room_id": room_id.to_string(),
            "name": name,
            "e2ee": e2ee,
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "room not found" })),
        )
            .into_response(),
    }
}

/// List the rooms owned by the authenticated user, with a little call history rollup.
async fn list_rooms(State(state): State<AppState>, headers: HeaderMap) -> axum::response::Response {
    let owner_id = match authed_user_id(&headers, &state.config.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    let rows = sqlx::query_as::<_, (Uuid, String, String, bool, i64, Option<String>)>(
        "SELECT r.id, r.name, r.created_at::text, r.e2ee, \
                COUNT(s.id) AS session_count, MAX(s.started_at)::text AS last_active \
         FROM rooms r LEFT JOIN sessions s ON s.room_id = r.id \
         WHERE r.owner_user_id = $1 AND r.deleted_at IS NULL \
         GROUP BY r.id, r.name, r.created_at, r.e2ee \
         ORDER BY COALESCE(MAX(s.started_at), r.created_at) DESC",
    )
    .bind(owner_id)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => {
            let rooms: Vec<_> = rows
                .into_iter()
                .map(|(id, name, created_at, e2ee, session_count, last_active)| {
                    serde_json::json!({
                        "room_id": id.to_string(),
                        "name": name,
                        "created_at": created_at,
                        "e2ee": e2ee,
                        "session_count": session_count,
                        "last_active": last_active,
                    })
                })
                .collect();
            Json(serde_json::json!({ "rooms": rooms })).into_response()
        }
        Err(e) => {
            tracing::error!("list_rooms failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "could not list rooms" })),
            )
                .into_response()
        }
    }
}

/// Soft-delete a room the caller owns. Idempotent; only the owner can delete.
async fn delete_room(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> axum::response::Response {
    let owner_id = match authed_user_id(&headers, &state.config.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let Ok(room_id) = Uuid::parse_str(&id) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid room id" }))).into_response();
    };

    let res = sqlx::query(
        "UPDATE rooms SET deleted_at = now() \
         WHERE id = $1 AND owner_user_id = $2 AND deleted_at IS NULL",
    )
    .bind(room_id)
    .bind(owner_id)
    .execute(&state.db)
    .await;

    match res {
        Ok(r) if r.rows_affected() > 0 => Json(serde_json::json!({ "deleted": true })).into_response(),
        Ok(_) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "room not found or not yours" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!("delete_room failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "could not delete room" })),
            )
                .into_response()
        }
    }
}

/// Past sessions (call history) for a room the caller owns, including the display names that
/// attended — which is how guest names are surfaced after the fact.
async fn room_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> axum::response::Response {
    let owner_id = match authed_user_id(&headers, &state.config.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let Ok(room_id) = Uuid::parse_str(&id) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid room id" }))).into_response();
    };

    // Ownership check before exposing any history.
    let owns: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM rooms WHERE id = $1 AND owner_user_id = $2 AND deleted_at IS NULL")
            .bind(room_id)
            .bind(owner_id)
            .fetch_optional(&state.db)
            .await
            .unwrap_or(None);
    if owns.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "room not found or not yours" })),
        )
            .into_response();
    }

    let rows = sqlx::query_as::<_, (Option<String>, Option<String>, String, Option<String>)>(
        "SELECT display_name, cf_session_id, started_at::text, ended_at::text \
         FROM sessions WHERE room_id = $1 ORDER BY started_at DESC",
    )
    .bind(room_id)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => {
            let sessions: Vec<_> = rows
                .into_iter()
                .map(|(display_name, cf_session_id, started_at, ended_at)| {
                    serde_json::json!({
                        "display_name": display_name,
                        "cf_session_id": cf_session_id,
                        "started_at": started_at,
                        "ended_at": ended_at,
                    })
                })
                .collect();
            Json(serde_json::json!({ "sessions": sessions })).into_response()
        }
        Err(e) => {
            tracing::error!("room_sessions failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "could not load sessions" })),
            )
                .into_response()
        }
    }
}

/// Validate the bearer JWT and return the authenticated user's id, or an error response.
fn authed_user_id(
    headers: &HeaderMap,
    secret: &str,
) -> Result<Uuid, (StatusCode, Json<serde_json::Value>)> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    let claims = validate_jwt(token, secret).map_err(|_| {
        (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "unauthorized" })))
    })?;
    Uuid::parse_str(&claims.sub).map_err(|_| {
        (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid token subject" })))
    })
}

// --- External video ingest (WHIP) ---
//
// Lets a non-browser source — an RTSP camera, ffmpeg, OBS, a file pump — publish into a room. The
// source speaks WHIP (WebRTC-HTTP Ingestion Protocol), which is a single exchange: POST an SDP
// offer, get an SDP answer, DELETE to stop. That maps 1:1 onto Cloudflare's `tracks/new` with
// `location: "local"`, so this is signaling translation and nothing more — media still flows
// encoder <-> Cloudflare directly and never touches this server, exactly as it does for browsers.
//
// Why WHIP rather than an SDK speaking our WebSocket protocol: every encoder already implements
// it (ffmpeg 7.0+ `-f whip`, OBS 30+, GStreamer `whipsink`), so an integrator installs nothing.
// It is also stateless HTTP — no heartbeat, no reconnect, and none of the strict request
// serialization the WS protocol's single-slot response waiter demands of its clients.
//
// The catch is E2EE: an encoder cannot join the room's MLS group, and browsers drop unkeyed
// frames, so a plaintext publisher would decode as garbage in every tile. Ingest is therefore
// only permitted in rooms created with `e2ee = false`, checked both at mint and at publish.

/// Track names the web client will actually subscribe to for video. Publishing under any other
/// name gets the feed announced in the roster and then ignored forever, which is a confusing
/// failure, so we reject it up front instead.
const INGEST_VIDEO_TRACK_NAMES: [&str; 2] = ["cam", "screen"];

/// Track name for the audio side of a feed. The web client always pulls this one.
const INGEST_AUDIO_TRACK_NAME: &str = "mic";

fn bearer_token(headers: &HeaderMap) -> &str {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

/// Only the digest is stored, so a database dump doesn't yield usable publish credentials.
/// Plain equality is fine for comparing digests — an attacker who could exploit the timing would
/// still need a preimage of the hash, not just the hash.
fn hash_ingest_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn mint_ingest_token() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn err(status: StatusCode, msg: &str) -> axum::response::Response {
    (status, Json(serde_json::json!({ "error": msg }))).into_response()
}

/// Mint a publish credential for a room. Owner-only, and refused outright for an encrypted room —
/// this is the single point where ingest could otherwise weaken a room's guarantees, so it fails
/// closed rather than silently downgrading peers who believe the call is end-to-end encrypted.
async fn create_ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(payload): Json<CreateIngestRequest>,
) -> axum::response::Response {
    let owner_id = match authed_user_id(&headers, &state.config.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let Ok(room_id) = Uuid::parse_str(&id) else {
        return err(StatusCode::BAD_REQUEST, "invalid room id");
    };
    if !INGEST_VIDEO_TRACK_NAMES.contains(&payload.track_name.as_str()) {
        return err(StatusCode::BAD_REQUEST, "track_name must be 'cam' or 'screen'");
    }

    let room: Option<(bool,)> = sqlx::query_as(
        "SELECT e2ee FROM rooms WHERE id = $1 AND owner_user_id = $2 AND deleted_at IS NULL",
    )
    .bind(room_id)
    .bind(owner_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let Some((e2ee,)) = room else {
        return err(StatusCode::NOT_FOUND, "room not found or not yours");
    };
    if e2ee {
        return err(
            StatusCode::CONFLICT,
            "room is end-to-end encrypted; external sources cannot join its MLS group. \
             Create the room with e2ee: false to accept ingest.",
        );
    }

    let ingest_id = Uuid::new_v4();
    let token = mint_ingest_token();
    let expires_at = Utc::now() + Duration::hours(INGEST_TOKEN_TTL_HOURS);

    if let Err(e) = sqlx::query(
        "INSERT INTO ingests (id, room_id, user_id, name, track_name, token_hash, expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(ingest_id)
    .bind(room_id)
    .bind(owner_id)
    .bind(&payload.name)
    .bind(&payload.track_name)
    .bind(hash_ingest_token(&token))
    .bind(expires_at)
    .execute(&state.db)
    .await
    {
        tracing::error!("create_ingest failed: {e}");
        return err(StatusCode::INTERNAL_SERVER_ERROR, "could not create ingest");
    }

    // `token` is returned exactly once — only its digest is stored.
    Json(serde_json::json!({
        "ingest_id": ingest_id.to_string(),
        "room_id": room_id.to_string(),
        "name": payload.name,
        "track_name": payload.track_name,
        "whip_url": format!("{}/whip/{}", state.config.public_base_url, ingest_id),
        "token": token,
        "expires_at": expires_at.to_rfc3339(),
    }))
    .into_response()
}

/// List a room's un-revoked ingests, with whether each is publishing right now.
async fn list_ingests(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> axum::response::Response {
    let owner_id = match authed_user_id(&headers, &state.config.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let Ok(room_id) = Uuid::parse_str(&id) else {
        return err(StatusCode::BAD_REQUEST, "invalid room id");
    };

    let rows = sqlx::query_as::<_, (Uuid, String, String, String, String)>(
        "SELECT i.id, i.name, i.track_name, i.created_at::text, i.expires_at::text \
         FROM ingests i JOIN rooms r ON r.id = i.room_id \
         WHERE i.room_id = $1 AND r.owner_user_id = $2 AND i.revoked_at IS NULL \
         ORDER BY i.created_at DESC",
    )
    .bind(room_id)
    .bind(owner_id)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => {
            let live = state.ingests.lock().unwrap();
            let ingests: Vec<_> = rows
                .into_iter()
                .map(|(iid, name, track_name, created_at, expires_at)| {
                    serde_json::json!({
                        "ingest_id": iid.to_string(),
                        "name": name,
                        "track_name": track_name,
                        "created_at": created_at,
                        "expires_at": expires_at,
                        "publishing": matches!(live.get(&iid), Some(Some(_))),
                    })
                })
                .collect();
            Json(serde_json::json!({ "ingests": ingests })).into_response()
        }
        Err(e) => {
            tracing::error!("list_ingests failed: {e}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "could not list ingests")
        }
    }
}

/// Revoke an ingest and cut off any publish it currently has in flight.
async fn revoke_ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> axum::response::Response {
    let owner_id = match authed_user_id(&headers, &state.config.jwt_secret) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let Ok(ingest_id) = Uuid::parse_str(&id) else {
        return err(StatusCode::BAD_REQUEST, "invalid ingest id");
    };

    let res = sqlx::query(
        "UPDATE ingests SET revoked_at = now() \
         WHERE id = $1 AND revoked_at IS NULL \
           AND room_id IN (SELECT id FROM rooms WHERE owner_user_id = $2)",
    )
    .bind(ingest_id)
    .bind(owner_id)
    .execute(&state.db)
    .await;

    match res {
        Ok(r) if r.rows_affected() > 0 => {
            // Revoking blocks the *next* publish; this ends the current one.
            teardown_ingest(&state, ingest_id, None, true).await;
            Json(serde_json::json!({ "revoked": true })).into_response()
        }
        Ok(_) => err(StatusCode::NOT_FOUND, "ingest not found or not yours"),
        Err(e) => {
            tracing::error!("revoke_ingest failed: {e}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "could not revoke ingest")
        }
    }
}

/// Pull the `(kind, mid)` of each media section out of an SDP offer.
///
/// Cloudflare needs a mid for every track it is told about, and the mids are assigned by the
/// encoder, so they have to be read back out of its offer. Only the first `a=mid:` after each
/// `m=` line belongs to that section, which is what `kind.take()` enforces.
fn parse_offer_mids(sdp: &str) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    let mut kind: Option<&'static str> = None;
    for line in sdp.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("m=") {
            kind = match rest.split_whitespace().next() {
                Some("audio") => Some("audio"),
                Some("video") => Some("video"),
                _ => None, // application/data sections carry no track we publish
            };
        } else if let Some(mid) = line.strip_prefix("a=mid:") {
            if let Some(k) = kind.take() {
                out.push((k, mid.trim().to_string()));
            }
        }
    }
    out
}

/// The ingest row plus the state of the room it points at, as needed to authorize a publish.
type IngestRow = (Uuid, Uuid, String, String, String, DateTime<Utc>, Option<DateTime<Utc>>, bool, Option<DateTime<Utc>>);

/// Look up an ingest and check its bearer token. Returns the row or the response to send back.
async fn authorize_ingest(
    state: &AppState,
    ingest_id: Uuid,
    token: &str,
) -> Result<IngestRow, axum::response::Response> {
    let row: Option<IngestRow> = sqlx::query_as(
        "SELECT i.room_id, i.user_id, i.name, i.track_name, i.token_hash, \
                i.expires_at, i.revoked_at, r.e2ee, r.deleted_at \
         FROM ingests i JOIN rooms r ON r.id = i.room_id WHERE i.id = $1",
    )
    .bind(ingest_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    // A bad id and a bad token are both reported as 401, so the endpoint doesn't confirm which
    // ingest ids exist to someone guessing.
    let Some(row) = row else {
        return Err(err(StatusCode::UNAUTHORIZED, "unauthorized"));
    };
    if token.is_empty() || row.4 != hash_ingest_token(token) {
        return Err(err(StatusCode::UNAUTHORIZED, "unauthorized"));
    }
    if row.6.is_some() {
        return Err(err(StatusCode::FORBIDDEN, "ingest revoked"));
    }
    if row.5 <= Utc::now() {
        return Err(err(StatusCode::FORBIDDEN, "ingest token expired"));
    }
    if row.8.is_some() {
        return Err(err(StatusCode::NOT_FOUND, "room deleted"));
    }
    // The room could have been flipped to encrypted after the token was minted.
    if row.7 {
        return Err(err(StatusCode::CONFLICT, "room is end-to-end encrypted"));
    }
    Ok(row)
}

/// `POST /whip/:ingest_id` — the WHIP endpoint. Body is an SDP offer; the reply is an SDP answer
/// plus a `Location` header the encoder later DELETEs to stop publishing.
async fn whip_publish(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    offer: String,
) -> axum::response::Response {
    let Ok(ingest_id) = Uuid::parse_str(&id) else {
        return err(StatusCode::UNAUTHORIZED, "unauthorized");
    };
    let row = match authorize_ingest(&state, ingest_id, bearer_token(&headers)).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    // Claim the publish slot before the Cloudflare round-trip, so two encoders pointed at the same
    // ingest can't both establish a session (which would leave one of them orphaned and billing).
    {
        let mut live = state.ingests.lock().unwrap();
        if live.contains_key(&ingest_id) {
            return err(StatusCode::CONFLICT, "ingest is already publishing");
        }
        live.insert(ingest_id, None);
    }

    match whip_start(&state, ingest_id, row, &offer).await {
        Ok(resp) => resp,
        Err(resp) => {
            state.ingests.lock().unwrap().remove(&ingest_id);
            resp
        }
    }
}

/// The body of `whip_publish`, split out so every failure path releases the reserved slot.
async fn whip_start(
    state: &AppState,
    ingest_id: Uuid,
    row: IngestRow,
    offer: &str,
) -> Result<axum::response::Response, axum::response::Response> {
    let (room_id, user_id, name, video_track_name, _, _, _, _, _) = row;

    // Map the offer's media sections onto the track names the web client subscribes to. Extra
    // sections of a kind we've already mapped are dropped: two tracks can't share a name, and no
    // encoder we target sends more than one of each.
    let mut tracks: Vec<(String, String)> = Vec::new();
    for (kind, mid) in parse_offer_mids(offer) {
        let track_name = match kind {
            "audio" => INGEST_AUDIO_TRACK_NAME.to_string(),
            _ => video_track_name.clone(),
        };
        if tracks.iter().any(|(_, n)| *n == track_name) {
            continue;
        }
        tracks.push((mid, track_name));
    }
    if tracks.is_empty() {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "offer contains no audio or video track with a mid",
        ));
    }

    let cf_session_id = cf_new_session(&state.http, &state.config, &ingest_id.to_string())
        .await
        .map_err(|e| {
            tracing::error!("ingest {ingest_id}: sessions/new failed: {e}");
            err(StatusCode::BAD_GATEWAY, "could not create media session")
        })?;

    let body = serde_json::json!({
        "sessionDescription": { "type": "offer", "sdp": offer },
        "tracks": tracks.iter().map(|(mid, track_name)| serde_json::json!({
            "location": "local",
            "mid": mid,
            "trackName": track_name,
        })).collect::<Vec<_>>(),
    });

    let res = cf_tracks_new(&state.http, &state.config, &cf_session_id, body)
        .await
        .map_err(|e| {
            tracing::error!("ingest {ingest_id}: tracks/new failed: {e}");
            // The orphaned CF session has no PeerConnection and times out on its own.
            err(StatusCode::BAD_GATEWAY, "sfu rejected the offer")
        })?;

    let Some(answer) = res["sessionDescription"]["sdp"].as_str() else {
        tracing::error!("ingest {ingest_id}: no answer sdp in tracks/new response: {res}");
        return Err(err(StatusCode::BAD_GATEWAY, "sfu returned no answer"));
    };
    let answer = answer.to_string();

    // Open a billing row so the reconciler attributes this feed's Cloudflare usage to the room
    // owner. Unlike the WS path, room and display name are known up front.
    let billing_id = Uuid::new_v4();
    if let Err(e) = sqlx::query(
        "INSERT INTO sessions (id, user_id, cf_session_id, room_id, display_name) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(billing_id)
    .bind(user_id)
    .bind(&cf_session_id)
    .bind(room_id)
    .bind(&name)
    .execute(&state.db)
    .await
    {
        tracing::error!("ingest {ingest_id}: could not open billing session: {e}");
        return Err(err(StatusCode::INTERNAL_SERVER_ERROR, "could not open session"));
    }

    // An ingest has no socket, but `Presence` fans out to every member unconditionally. Give it a
    // channel that is drained and discarded so the queue can't grow without bound; the drain task
    // ends by itself when `leave` drops the sender.
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let room_key = room_id.to_string();
    let track_names: Vec<String> = tracks.iter().map(|(_, n)| n.clone()).collect();
    state.presence.join(
        &room_key,
        MemberInfo {
            user_name: name.clone(),
            cf_session_id: cf_session_id.clone(),
            tracks: Vec::new(),
            role: ROLE_INGEST,
        },
        tx,
    );
    // Announce the tracks separately, mirroring the browser's join-then-publish order — this is
    // the event that puts the feed on everyone's stage.
    state.presence.publish(&room_key, &cf_session_id, track_names);

    let resource = Uuid::new_v4().to_string();
    let location = format!(
        "{}/whip/{}/{}",
        state.config.public_base_url, ingest_id, resource
    );

    state.ingests.lock().unwrap().insert(
        ingest_id,
        Some(LiveIngest {
            resource: resource.clone(),
            cf_session_id: cf_session_id.clone(),
            room_id: room_key,
            billing_id,
            tracks,
        }),
    );
    spawn_ingest_reaper(state.clone(), ingest_id, resource);

    tracing::info!("ingest {ingest_id} ({name}) publishing to room {room_id} (cf_session {cf_session_id})");

    Ok((
        StatusCode::CREATED,
        [
            (header::CONTENT_TYPE, "application/sdp"),
            (header::LOCATION, location.as_str()),
        ],
        answer,
    )
        .into_response())
}

/// `DELETE /whip/:ingest_id/:resource` — the encoder stopping cleanly.
async fn whip_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, resource)): Path<(String, String)>,
) -> axum::response::Response {
    let Ok(ingest_id) = Uuid::parse_str(&id) else {
        return err(StatusCode::UNAUTHORIZED, "unauthorized");
    };
    // Revoked or expired tokens are still allowed to *stop* a publish — refusing would strand a
    // live feed that we would rather see torn down.
    let token = bearer_token(&headers);
    let row: Option<(String,)> = sqlx::query_as("SELECT token_hash FROM ingests WHERE id = $1")
        .bind(ingest_id)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);
    match row {
        Some((hash,)) if !token.is_empty() && hash == hash_ingest_token(token) => {}
        _ => return err(StatusCode::UNAUTHORIZED, "unauthorized"),
    }

    teardown_ingest(&state, ingest_id, Some(&resource), true).await;
    // WHIP wants DELETE to be idempotent, so a repeat after the reaper already cleaned up is a 200.
    StatusCode::OK.into_response()
}

/// End a live publish: close the Cloudflare tracks, drop the roster entry, and close the billing
/// row. Safe to call for an ingest that isn't publishing.
///
/// `expect_resource` guards against a stale `Location` URL from a previous publish tearing down
/// the current one; pass `None` to end whatever is live regardless.
///
/// `close_tracks` is false when the caller already knows Cloudflare has dropped the session —
/// closing tracks on it would just 410 and log a warning for what is the normal crash path.
async fn teardown_ingest(
    state: &AppState,
    ingest_id: Uuid,
    expect_resource: Option<&str>,
    close_tracks: bool,
) -> bool {
    let live = {
        let mut map = state.ingests.lock().unwrap();
        let matches = match map.get(&ingest_id) {
            Some(Some(l)) => expect_resource.is_none_or(|r| r == l.resource),
            // `Some(None)` is a publish still mid-handshake; it cleans up its own reservation.
            _ => false,
        };
        if matches {
            map.remove(&ingest_id).flatten()
        } else {
            None
        }
    };
    let Some(live) = live else { return false };

    // Best effort: if this fails, the session dies on its own once the encoder stops sending.
    // `force` because there is no PeerConnection left to renegotiate with.
    if close_tracks {
        let body = serde_json::json!({
            "tracks": live.tracks.iter().map(|(mid, _)| serde_json::json!({ "mid": mid })).collect::<Vec<_>>(),
            "force": true,
        });
        if let Err(e) = cf_tracks_close(&state.http, &state.config, &live.cf_session_id, body).await {
            tracing::warn!("ingest {ingest_id}: tracks/close failed: {e}");
        }
    }

    state.presence.leave(&live.room_id, &live.cf_session_id);

    if let Err(e) = sqlx::query("UPDATE sessions SET ended_at = now() WHERE id = $1")
        .bind(live.billing_id)
        .execute(&state.db)
        .await
    {
        tracing::error!("ingest {ingest_id}: failed to close session {}: {e}", live.billing_id);
    }

    tracing::info!("ingest {ingest_id} stopped (cf_session {})", live.cf_session_id);
    true
}

/// Watch a live publish and tear it down once its encoder is gone.
///
/// A browser that vanishes is caught by the WebSocket heartbeat, but an ingest holds no connection
/// to us at all — a `kill -9`ed ffmpeg sends no DELETE and would otherwise leave a permanent ghost
/// tile in the roster. So we ask Cloudflare instead, and only act on a definitive answer: a failed
/// poll is not evidence the encoder died, and killing a healthy feed over a network blip is worse
/// than reaping one interval late. `INGEST_MAX_LIFETIME` is the backstop if we never get one.
fn spawn_ingest_reaper(state: AppState, ingest_id: Uuid, resource: String) {
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + INGEST_MAX_LIFETIME;
        let mut consecutive_dead = 0u8;

        loop {
            tokio::time::sleep(INGEST_REAP_INTERVAL).await;

            // Someone else (a DELETE, a revoke) already ended this publish, or a newer one has
            // taken the slot — either way this reaper is done.
            let (cf_session_id, track_names) = {
                let map = state.ingests.lock().unwrap();
                match map.get(&ingest_id) {
                    Some(Some(l)) if l.resource == resource => (
                        l.cf_session_id.clone(),
                        l.tracks.iter().map(|(_, n)| n.clone()).collect::<Vec<_>>(),
                    ),
                    _ => return,
                }
            };

            if tokio::time::Instant::now() >= deadline {
                tracing::info!("ingest {ingest_id} hit max lifetime; tearing down");
                teardown_ingest(&state, ingest_id, Some(&resource), true).await;
                return;
            }

            match cf_get_session(&state.http, &state.config, &cf_session_id).await {
                // Unambiguous: Cloudflare has torn the session down. A crashed encoder cannot come
                // back into it either — reconnecting means a fresh WHIP POST, which is blocked
                // while this publish holds the slot — so reap now instead of waiting out another
                // interval to confirm something that can't change.
                SessionPoll::Gone => {
                    tracing::info!("ingest {ingest_id}: session gone; tearing down");
                    teardown_ingest(&state, ingest_id, Some(&resource), false).await;
                    return;
                }
                SessionPoll::Alive(v) => {
                    if session_tracks_are_dead(&v, &track_names) {
                        consecutive_dead += 1;
                    } else {
                        consecutive_dead = 0;
                    }
                }
                SessionPoll::Unknown(e) => {
                    tracing::warn!("ingest {ingest_id}: session poll failed: {e}")
                }
            }

            // The session is up but reports no live track of ours. That's a softer signal than a
            // gone session, so require two in a row before ending a broadcast on it.
            if consecutive_dead >= 2 {
                tracing::info!("ingest {ingest_id}: tracks inactive; tearing down");
                teardown_ingest(&state, ingest_id, Some(&resource), true).await;
                return;
            }
        }
    });
}

/// Whether a session-state response says none of our tracks are still going.
///
/// Deliberately conservative: an unrecognized shape reads as alive, and `waiting` (the track is
/// negotiated but packets haven't arrived yet) counts as alive too — that is exactly the state an
/// encoder sits in while it is starting up.
fn session_tracks_are_dead(state: &serde_json::Value, track_names: &[String]) -> bool {
    if state["errorCode"].as_str().is_some_and(|c| !c.is_empty()) {
        return true;
    }
    let Some(tracks) = state["tracks"].as_array() else {
        return false;
    };
    !tracks.iter().any(|t| {
        let name = t["trackName"].as_str().unwrap_or_default();
        let status = t["status"].as_str().unwrap_or("active");
        track_names.iter().any(|n| n == name) && status != "inactive"
    })
}

// --- REST API: Billing / Usage ---
// Returns the authenticated user's total billed usage.
// Auth: `Authorization: Bearer <jwt>`.
async fn get_usage(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> (StatusCode, Json<serde_json::Value>) {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    let claims = match validate_jwt(token, &state.config.jwt_secret) {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            );
        }
    };

    let user_id = match Uuid::parse_str(&claims.sub) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid token subject" })),
            );
        }
    };

    let row: Option<(i64,)> = sqlx::query_as("SELECT total_bytes FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);

    match row {
        Some((total_bytes,)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "user_id": user_id.to_string(),
                "total_bytes": total_bytes,
                "total_gb": total_bytes as f64 / 1_000_000_000.0,
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "user not found" })),
        ),
    }
}

// --- WebSocket: WebRTC Signaling ---
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // 1. Extract JWT from WebSocket Subprotocol header
    let token = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // 2. Validate JWT
    let claims = match validate_jwt(&token, &state.config.jwt_secret) {
        Ok(claims) => claims,
        Err(_) => {
            return axum::response::Response::builder()
                .status(401)
                .body("Unauthorized".into())
                .unwrap();
        }
    };

    // 3. Upgrade to WebSocket. We MUST echo the offered subprotocol back: a
    // browser that opened `new WebSocket(url, token)` rejects the handshake if
    // the server's 101 response doesn't confirm a subprotocol, closing the
    // socket right after connecting. Echoing the token satisfies that rule.
    ws.protocols([token])
        .on_upgrade(move |socket| handle_socket(socket, state, claims))
}

async fn handle_socket(socket: WebSocket, state: AppState, claims: Claims) {
    let user_name = claims.name.clone();

    // The JWT `sub` carries the user's UUID (set in `login`).
    let user_id = match Uuid::parse_str(&claims.sub) {
        Ok(id) => id,
        Err(_) => {
            tracing::warn!("ws connection with invalid user id in token: {}", claims.sub);
            return;
        }
    };

    // Split the socket so a dedicated writer task can interleave direct replies
    // (signaling answers) with broadcast presence events. All outgoing messages
    // go through `out_tx`.
    let (mut ws_sink, mut ws_stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();
    let writer = tokio::spawn(async move {
        while let Some(m) = out_rx.recv().await {
            if ws_sink.send(m).await.is_err() {
                break;
            }
        }
    });

    // Per Cloudflare's model, each client gets exactly ONE session (= one
    // PeerConnection). We create it server-side so the client can never operate
    // on a session it doesn't own. We pass the user id as `correlationId` purely
    // for traceability in Cloudflare's tooling.
    let cf_session_id = match cf_new_session(&state.http, &state.config, &user_id.to_string()).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("failed to create CF session for {user_name}: {e}");
            let err = serde_json::json!({ "error": "failed to create media session" });
            let _ = out_tx.send(Message::Text(err.to_string()));
            writer.abort();
            return;
        }
    };

    // Open the billing/session row with the CF session already attached. The
    // reconciler attributes Cloudflare usage to this user via cf_session_id.
    let billing_id = Uuid::new_v4();
    if let Err(e) =
        sqlx::query("INSERT INTO sessions (id, user_id, cf_session_id) VALUES ($1, $2, $3)")
            .bind(billing_id)
            .bind(user_id)
            .bind(&cf_session_id)
            .execute(&state.db)
            .await
    {
        tracing::error!("failed to open billing session: {e}");
        writer.abort();
        return;
    }

    tracing::info!(
        "{} connected (cf_session {}, billing {})",
        user_name,
        cf_session_id,
        billing_id
    );

    // Tell the client which session is theirs. The client never sends its own
    // session id back — the server always uses `cf_session_id` for this socket.
    let _ = out_tx.send(Message::Text(
        serde_json::json!({
            "action": "session_created",
            "cf_session_id": cf_session_id,
        })
        .to_string(),
    ));

    // Set once the client joins a room; presence events flow only after that.
    let mut room_id: Option<String> = None;

    // Heartbeat state. `interval`'s first tick resolves immediately, so consume it here rather than
    // pinging the instant the socket opens.
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.tick().await;
    let mut awaiting_pong = false;

    loop {
        let msg = tokio::select! {
            incoming = ws_stream.next() => match incoming {
                Some(Ok(m)) => m,
                // Stream ended or errored: the peer is gone either way.
                _ => break,
            },
            _ = heartbeat.tick() => {
                if awaiting_pong {
                    tracing::info!(
                        "{} timed out (no pong within {}s, cf_session {})",
                        user_name,
                        HEARTBEAT_INTERVAL.as_secs(),
                        cf_session_id
                    );
                    break;
                }
                awaiting_pong = true;
                // A send error means the writer task is gone, i.e. the socket is already closed.
                if out_tx.send(Message::Ping(Vec::new())).is_err() {
                    break;
                }
                continue;
            }
        };

        // Any inbound frame — pong, text, anything — proves the peer is still there.
        awaiting_pong = false;

        let Message::Text(text) = msg else { continue };
        let Ok(json_msg) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };

        match json_msg["action"].as_str() {
            // Join a room's presence channel. Sends the newcomer the current
            // roster, then streams join/leave/publish events from other peers.
            Some("join") => {
                let Some(rid) = json_msg["room_id"].as_str() else {
                    let _ = out_tx.send(Message::Text(
                        serde_json::json!({ "error": "join requires room_id" }).to_string(),
                    ));
                    continue;
                };
                if room_id.is_some() {
                    continue; // already joined; ignore
                }

                let member = MemberInfo {
                    user_name: user_name.clone(),
                    cf_session_id: cf_session_id.clone(),
                    tracks: Vec::new(),
                    role: ROLE_HUMAN,
                };
                // Register our socket-writer queue so the room can deliver presence/relay events
                // straight to us (lossless, FIFO) — no separate forwarder task or broadcast bus.
                let existing = state.presence.join(rid, member, out_tx.clone());

                // Record which room this session joined + the display name shown to peers, so the
                // owner's call history captures who attended (guests included). Spawned: it's pure
                // bookkeeping the client doesn't wait on, so it must not delay the roster (and thus
                // the E2EE bootstrap that fires on it) or block the read loop on a DB round-trip.
                if let Ok(rid_uuid) = Uuid::parse_str(rid) {
                    let db = state.db.clone();
                    let display = user_name.clone();
                    tokio::spawn(async move {
                        if let Err(e) = sqlx::query(
                            "UPDATE sessions SET room_id = $1, display_name = $2 WHERE id = $3",
                        )
                        .bind(rid_uuid)
                        .bind(&display)
                        .bind(billing_id)
                        .execute(&db)
                        .await
                        {
                            tracing::error!("failed to record room/display_name for session: {e}");
                        }
                    });
                }

                // Send the current roster (excludes self).
                let _ = out_tx.send(Message::Text(
                    serde_json::json!({
                        "action": "roster",
                        "peers": existing.iter().map(|m| serde_json::json!({
                            "cf_session_id": m.cf_session_id,
                            "user_name": m.user_name,
                            "tracks": m.tracks,
                            "role": m.role,
                        })).collect::<Vec<_>>(),
                    })
                    .to_string(),
                ));

                room_id = Some(rid.to_string());
            }

            // Signaling actions all target the server-owned cf_session_id. Each hits the Cloudflare
            // API, which can take seconds — so we run it in a spawned task instead of awaiting it
            // inline. Awaiting inline parks this connection's whole read loop, which would stall any
            // following message (notably an `e2ee_relay` key package) behind the CF round-trip and
            // leave a peer stuck "negotiating". The client serializes its own signaling (one
            // outstanding request at a time), so spawning never races two CF mutations for a session.
            Some("publish") => {
                let http = state.http.clone();
                let config = state.config.clone();
                let presence = state.presence.clone();
                let sid = cf_session_id.clone();
                let rid = room_id.clone();
                let req = json_msg.clone();
                let tx = out_tx.clone();
                tokio::spawn(async move {
                    let reply = handle_signal(&http, &config, &sid, &req).await;
                    // On a successful publish, announce our tracks to the room.
                    if reply.get("action").and_then(|a| a.as_str()) == Some("publish_answer") {
                        if let Some(rid) = &rid {
                            let tracks = track_names(&req);
                            presence.publish(rid, &sid, tracks);
                        }
                    }
                    let _ = tx.send(Message::Text(reply.to_string()));
                });
            }

            // End-to-end encryption control messages. The server is a zero-trust relay: it
            // never parses `msg`, only fans it out to the rest of the room, stamping the
            // authoritative sender so clients can't spoof `from`. MLS ordering is preserved by
            // the per-room broadcast channel.
            Some("e2ee_relay") => {
                if let Some(rid) = &room_id {
                    let payload = serde_json::json!({
                        "action": "e2ee_relay",
                        "from": cf_session_id,
                        "msg": json_msg.get("msg").cloned().unwrap_or(serde_json::Value::Null),
                    })
                    .to_string();
                    state.presence.relay(rid, &cf_session_id, payload);
                }
            }

            // subscribe / unsubscribe / update / renegotiate — same as publish, spawned so the CF
            // round-trip never blocks this connection's read loop (see the note on `publish`).
            _ => {
                let http = state.http.clone();
                let config = state.config.clone();
                let sid = cf_session_id.clone();
                let req = json_msg.clone();
                let tx = out_tx.clone();
                tokio::spawn(async move {
                    let reply = handle_signal(&http, &config, &sid, &req).await;
                    let _ = tx.send(Message::Text(reply.to_string()));
                });
            }
        }
    }

    // --- Cleanup on disconnect ---
    if let Some(rid) = &room_id {
        state.presence.leave(rid, &cf_session_id);
    }
    writer.abort();

    // Stamp the end of the call so the reconciler knows the billing window.
    if let Err(e) = sqlx::query("UPDATE sessions SET ended_at = now() WHERE id = $1")
        .bind(billing_id)
        .execute(&state.db)
        .await
    {
        tracing::error!("failed to close session {billing_id}: {e}");
    }
    tracing::info!("{} disconnected (cf_session {})", user_name, cf_session_id);
}

/// Extract the `trackName`s from a publish message's `tracks` array.
fn track_names(msg: &serde_json::Value) -> Vec<String> {
    msg["tracks"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["trackName"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Handle one signaling message against this socket's own Cloudflare session,
/// following the documented session lifecycle. Returns the JSON reply to send.
///
/// Inbound message shapes (all operate on the server-owned `cf_session_id`):
///   { "action": "publish",     "sdp": "<offer>",  "tracks": [{ "mid", "trackName" }] }
///   { "action": "subscribe",   "tracks": [{ "sessionId", "trackName" }] }   // remote tracks
///   { "action": "renegotiate", "sdp": "<answer>" }
async fn handle_signal(
    client: &reqwest::Client,
    config: &Config,
    cf_session_id: &str,
    msg: &serde_json::Value,
) -> serde_json::Value {
    match msg["action"].as_str() {
        // Publish local tracks: client sends an offer; CF returns an answer.
        Some("publish") => {
            let sdp = msg["sdp"].as_str().unwrap_or_default();
            let tracks: Vec<serde_json::Value> = msg["tracks"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|t| {
                            Some(serde_json::json!({
                                "location": "local",
                                "mid": t["mid"].as_str()?,
                                "trackName": t["trackName"].as_str()?,
                            }))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let body = serde_json::json!({
                "sessionDescription": { "type": "offer", "sdp": sdp },
                "tracks": tracks,
            });

            match cf_tracks_new(client, config, cf_session_id, body).await {
                Ok(resp) => serde_json::json!({
                    "action": "publish_answer",
                    "sessionDescription": resp["sessionDescription"],
                    "tracks": resp["tracks"],
                }),
                Err(e) => serde_json::json!({ "error": format!("publish failed: {e}") }),
            }
        }

        // Subscribe to remote tracks (published by other sessions). CF returns an
        // offer; if renegotiation is required the client must answer via "renegotiate".
        Some("subscribe") => {
            let tracks: Vec<serde_json::Value> = msg["tracks"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|t| {
                            Some(serde_json::json!({
                                "location": "remote",
                                "sessionId": t["sessionId"].as_str()?,
                                "trackName": t["trackName"].as_str()?,
                            }))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let body = serde_json::json!({ "tracks": tracks });

            match cf_tracks_new(client, config, cf_session_id, body).await {
                Ok(resp) => serde_json::json!({
                    "action": "subscribe_offer",
                    "sessionDescription": resp["sessionDescription"],
                    "requiresImmediateRenegotiation": resp["requiresImmediateRenegotiation"],
                    "tracks": resp["tracks"],
                }),
                Err(e) => serde_json::json!({ "error": format!("subscribe failed: {e}") }),
            }
        }

        // Stop pulling remote tracks (e.g. a peer left the visible "stage"). The client sets the
        // relevant transceivers inactive and sends an offer; CF closes the tracks server-side —
        // which is what actually frees the egress we'd otherwise pay for — and answers. Direction
        // mirrors `publish` (client offers, CF answers).
        Some("unsubscribe") => {
            let sdp = msg["sdp"].as_str().unwrap_or_default();
            let tracks: Vec<serde_json::Value> = msg["tracks"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|t| Some(serde_json::json!({ "mid": t["mid"].as_str()? })))
                        .collect()
                })
                .unwrap_or_default();

            let body = serde_json::json!({
                "tracks": tracks,
                "sessionDescription": { "type": "offer", "sdp": sdp },
                "force": false,
            });

            match cf_tracks_close(client, config, cf_session_id, body).await {
                Ok(resp) => serde_json::json!({
                    "action": "unsubscribe_answer",
                    "sessionDescription": resp["sessionDescription"],
                    "requiresImmediateRenegotiation": resp["requiresImmediateRenegotiation"],
                    "tracks": resp["tracks"],
                }),
                Err(e) => serde_json::json!({ "error": format!("unsubscribe failed: {e}") }),
            }
        }

        // Change the simulcast layer the SFU forwards for already-pulled remote tracks (e.g. a tile
        // shrank to a thumbnail, so request the low `q` layer). A remote pulled track is identified
        // by location/sessionId/trackName/mid together (matching CF's own echo-simulcast example);
        // no SDP, and no renegotiation for a preferredRid change — the SFU just switches layers.
        Some("update") => {
            let tracks: Vec<serde_json::Value> = msg["tracks"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|t| {
                            let sc = &t["simulcast"];
                            Some(serde_json::json!({
                                "location": t["location"].as_str().unwrap_or("remote"),
                                "sessionId": t["sessionId"].as_str()?,
                                "trackName": t["trackName"].as_str()?,
                                "mid": t["mid"].as_str()?,
                                "simulcast": {
                                    "preferredRid": sc["preferredRid"].as_str()?,
                                    "priorityOrdering": sc["priorityOrdering"].as_str().unwrap_or("asciibetical"),
                                    "ridNotAvailable": sc["ridNotAvailable"].as_str().unwrap_or("asciibetical"),
                                },
                            }))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let body = serde_json::json!({ "tracks": tracks });

            match cf_tracks_update(client, config, cf_session_id, body).await {
                Ok(resp) => serde_json::json!({
                    "action": "update_result",
                    "requiresImmediateRenegotiation": resp["requiresImmediateRenegotiation"],
                    "tracks": resp["tracks"],
                }),
                Err(e) => serde_json::json!({ "error": format!("update failed: {e}") }),
            }
        }

        // Complete a renegotiation: client sends its answer.
        Some("renegotiate") => {
            let sdp = msg["sdp"].as_str().unwrap_or_default();
            let body = serde_json::json!({
                "sessionDescription": { "type": "answer", "sdp": sdp },
            });
            match cf_renegotiate(client, config, cf_session_id, body).await {
                Ok(()) => serde_json::json!({ "action": "renegotiated" }),
                Err(e) => serde_json::json!({ "error": format!("renegotiate failed: {e}") }),
            }
        }

        other => serde_json::json!({
            "error": format!("unknown action: {}", other.unwrap_or("<missing>"))
        }),
    }
}

// --- Helper: JWT Validation ---
fn validate_jwt(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_ref()),
        &Validation::default(),
    )?;
    Ok(token_data.claims)
}

// --- Helper: Cloudflare Realtime SFU API ---
// All calls hit https://rtc.live.cloudflare.com/v1/apps/{appId}/... and
// authenticate with the App Secret as a Bearer token.

/// POST /apps/{appId}/sessions/new — create a new session (one per client).
/// Body is optional; we send none. Returns the new `sessionId`.
async fn cf_new_session(
    client: &reqwest::Client,
    config: &Config,
    correlation_id: &str,
) -> Result<String, String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/new?correlationId={}",
        config.cf_app_id,
        urlencoding(correlation_id),
    );
    let res = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", config.cf_app_secret))
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !res.status().is_success() {
        return Err(format!(
            "sessions/new HTTP {}: {}",
            res.status(),
            res.text().await.unwrap_or_default()
        ));
    }

    let body: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;
    body["sessionId"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("no sessionId in response: {body}"))
}

/// POST /apps/{appId}/sessions/{sessionId}/tracks/new — add local or remote tracks.
async fn cf_tracks_new(
    client: &reqwest::Client,
    config: &Config,
    session_id: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/{}/tracks/new",
        config.cf_app_id, session_id
    );
    cf_send(client, config, reqwest::Method::POST, &url, Some(body)).await
}

/// Outcome of polling a session's state. Keeping "Cloudflare says it's gone" apart from "we
/// couldn't ask Cloudflare" is the whole point: the first is grounds to reap an ingest, the second
/// is not, and collapsing them either strands ghost feeds or kills healthy ones on a blip.
enum SessionPoll {
    Alive(serde_json::Value),
    Gone,
    Unknown(String),
}

/// GET /apps/{appId}/sessions/{sessionId} — current track state for a session.
///
/// Used only by the ingest reaper: an encoder has no connection to us, so Cloudflare is the only
/// thing that knows whether it is still sending. Deliberately does NOT go through `cf_send`:
/// Cloudflare reports a disconnected session as `410 Gone`, and `cf_send` flattens every non-2xx
/// into an error string indistinguishable from a timeout, which would discard the clearest signal
/// we get.
async fn cf_get_session(client: &reqwest::Client, config: &Config, session_id: &str) -> SessionPoll {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/{}",
        config.cf_app_id, session_id
    );
    let res = match client
        .get(&url)
        .header("Authorization", format!("Bearer {}", config.cf_app_secret))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return SessionPoll::Unknown(e.to_string()),
    };

    let status = res.status().as_u16();
    // 410 is what Cloudflare returns for "the PeerConnection is disconnected"; 404 covers a
    // session that has already been cleaned up on their side.
    if status == 410 || status == 404 {
        return SessionPoll::Gone;
    }
    if !res.status().is_success() {
        return SessionPoll::Unknown(format!(
            "HTTP {status}: {}",
            res.text().await.unwrap_or_default()
        ));
    }
    match res.json::<serde_json::Value>().await {
        Ok(v) => SessionPoll::Alive(v),
        Err(e) => SessionPoll::Unknown(e.to_string()),
    }
}

/// PUT /apps/{appId}/sessions/{sessionId}/tracks/close — stop sending/receiving the given tracks.
async fn cf_tracks_close(
    client: &reqwest::Client,
    config: &Config,
    session_id: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/{}/tracks/close",
        config.cf_app_id, session_id
    );
    cf_send(client, config, reqwest::Method::PUT, &url, Some(body)).await
}

/// PUT /apps/{appId}/sessions/{sessionId}/tracks/update — change a pulled track's simulcast layer.
/// No SDP / renegotiation for a `preferredRid` change; the SFU just switches which layer it forwards.
async fn cf_tracks_update(
    client: &reqwest::Client,
    config: &Config,
    session_id: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/{}/tracks/update",
        config.cf_app_id, session_id
    );
    cf_send(client, config, reqwest::Method::PUT, &url, Some(body)).await
}

/// PUT /apps/{appId}/sessions/{sessionId}/renegotiate — finish renegotiation.
async fn cf_renegotiate(
    client: &reqwest::Client,
    config: &Config,
    session_id: &str,
    body: serde_json::Value,
) -> Result<(), String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/{}/renegotiate",
        config.cf_app_id, session_id
    );
    cf_send(client, config, reqwest::Method::PUT, &url, Some(body)).await.map(|_| ())
}

/// Shared request helper for the authenticated JSON SFU endpoints.
async fn cf_send(
    client: &reqwest::Client,
    config: &Config,
    method: reqwest::Method,
    url: &str,
    body: Option<serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let mut req = client
        .request(method, url)
        .header("Authorization", format!("Bearer {}", config.cf_app_secret));
    if let Some(b) = body {
        req = req.json(&b);
    }

    let res = req.send().await.map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        return Err(format!(
            "CF API HTTP {}: {}",
            res.status(),
            res.text().await.unwrap_or_default()
        ));
    }
    res.json().await.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A verbatim offer from `ffmpeg -f whip` (8.1.1), captured against a local listener. Kept
    /// literal so a change to the parser is checked against what an encoder really sends rather
    /// than against a hand-written approximation.
    const FFMPEG_WHIP_OFFER: &str = "\
v=0\r
o=FFmpeg 4489045141692799359 2 IN IP4 127.0.0.1\r
s=FFmpegPublishSession\r
t=0 0\r
a=group:BUNDLE 0 1\r
a=extmap-allow-mixed\r
a=msid-semantic: WMS\r
m=audio 9 UDP/TLS/RTP/SAVPF 111\r
c=IN IP4 0.0.0.0\r
a=setup:passive\r
a=mid:0\r
a=sendonly\r
a=msid:FFmpeg audio\r
a=rtcp-mux\r
a=rtpmap:111 opus/48000/2\r
m=video 9 UDP/TLS/RTP/SAVPF 106 105\r
c=IN IP4 0.0.0.0\r
a=setup:passive\r
a=mid:1\r
a=sendonly\r
a=msid:FFmpeg video\r
a=rtcp-mux\r
a=rtpmap:106 H264/90000\r
a=rtpmap:105 rtx/90000\r
";

    #[test]
    fn parses_mids_from_a_real_ffmpeg_offer() {
        assert_eq!(
            parse_offer_mids(FFMPEG_WHIP_OFFER),
            vec![("audio", "0".to_string()), ("video", "1".to_string())]
        );
    }

    #[test]
    fn ignores_session_level_lines_and_data_sections() {
        // `a=msid-semantic` sits before any m= line and must not be mistaken for a mid, and a
        // data channel section carries no track we publish.
        let sdp = "v=0\na=msid-semantic: WMS\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\na=mid:2\n";
        assert!(parse_offer_mids(sdp).is_empty());
    }

    #[test]
    fn takes_only_the_first_mid_of_a_section() {
        let sdp = "m=video 9 RTP/SAVPF 96\na=mid:7\na=mid:8\n";
        assert_eq!(parse_offer_mids(sdp), vec![("video", "7".to_string())]);
    }

    #[test]
    fn session_is_dead_only_on_a_definitive_signal() {
        let names = vec!["cam".to_string(), "mic".to_string()];

        let active = serde_json::json!({ "tracks": [{ "trackName": "cam", "status": "active" }] });
        assert!(!session_tracks_are_dead(&active, &names));

        // An encoder that has negotiated but not yet sent packets sits in `waiting` — reaping it
        // would kill every feed during startup.
        let waiting = serde_json::json!({ "tracks": [{ "trackName": "cam", "status": "waiting" }] });
        assert!(!session_tracks_are_dead(&waiting, &names));

        let inactive = serde_json::json!({ "tracks": [{ "trackName": "cam", "status": "inactive" }] });
        assert!(session_tracks_are_dead(&inactive, &names));

        // Our track is gone from the session entirely.
        let gone = serde_json::json!({ "tracks": [] });
        assert!(session_tracks_are_dead(&gone, &names));

        assert!(session_tracks_are_dead(&serde_json::json!({ "errorCode": "nope" }), &names));

        // An unrecognized shape must not be read as death.
        assert!(!session_tracks_are_dead(&serde_json::json!({}), &names));
    }

    #[test]
    fn ingest_token_hash_is_stable_and_not_the_token() {
        let token = mint_ingest_token();
        assert_eq!(token.len(), 64);
        assert_eq!(hash_ingest_token(&token), hash_ingest_token(&token));
        assert_ne!(hash_ingest_token(&token), token);
        assert_ne!(mint_ingest_token(), mint_ingest_token());
    }
}

/// Minimal percent-encoding for the `correlationId` query value (UUIDs are
/// already safe, but guard against anything unexpected).
fn urlencoding(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}
