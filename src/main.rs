use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

// Cloudflare Realtime SFU session/track API base.
// NOTE: this is NOT api.cloudflare.com — that host is only for app management.
const CF_SFU_BASE: &str = "https://rtc.live.cloudflare.com/v1";

// --- Configuration (loaded from environment) ---
#[derive(Clone)]
struct Config {
    // App ID goes in the SFU URL path; App Secret is the Bearer token for it.
    cf_app_id: String,
    cf_app_secret: String,
    jwt_secret: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            cf_app_id: require_env("CF_APP_ID"),
            cf_app_secret: require_env("CF_APP_SECRET"),
            jwt_secret: require_env("JWT_SECRET"),
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
}

/// A presence event fanned out to everyone in a room. `origin` is the
/// cf_session_id of the member the event is about, so a member's own forwarder
/// can skip echoing events back to itself.
#[derive(Clone)]
struct PresenceEvent {
    origin: String,
    payload: String,
}

struct Room {
    tx: broadcast::Sender<PresenceEvent>,
    members: HashMap<String, MemberInfo>, // keyed by cf_session_id
}

#[derive(Default)]
struct Presence {
    rooms: Mutex<HashMap<String, Room>>,
}

impl Presence {
    /// Add a member to a room. Returns the existing roster (for the newcomer)
    /// and a receiver for future events. Announces the join to everyone else.
    fn join(&self, room_id: &str, member: MemberInfo) -> (Vec<MemberInfo>, broadcast::Receiver<PresenceEvent>) {
        let mut rooms = self.rooms.lock().unwrap();
        let room = rooms.entry(room_id.to_string()).or_insert_with(|| Room {
            tx: broadcast::channel(256).0,
            members: HashMap::new(),
        });

        let existing: Vec<MemberInfo> = room.members.values().cloned().collect();
        let rx = room.tx.subscribe();

        let payload = serde_json::json!({
            "action": "peer_joined",
            "cf_session_id": member.cf_session_id,
            "user_name": member.user_name,
            "tracks": member.tracks,
        })
        .to_string();
        let _ = room.tx.send(PresenceEvent {
            origin: member.cf_session_id.clone(),
            payload,
        });

        room.members.insert(member.cf_session_id.clone(), member);
        (existing, rx)
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
            let _ = room.tx.send(PresenceEvent {
                origin: cf_session_id.to_string(),
                payload,
            });
        }
    }

    /// Remove a member, announce the departure, and drop the room if now empty.
    fn leave(&self, room_id: &str, cf_session_id: &str) {
        let mut rooms = self.rooms.lock().unwrap();
        if let Some(room) = rooms.get_mut(room_id) {
            room.members.remove(cf_session_id);
            let payload = serde_json::json!({
                "action": "peer_left",
                "cf_session_id": cf_session_id,
            })
            .to_string();
            let _ = room.tx.send(PresenceEvent {
                origin: cf_session_id.to_string(),
                payload,
            });
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
}

#[derive(Debug, Serialize, Deserialize)]
struct RoomResponse {
    room_id: String,
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

    let state = AppState {
        db,
        config: Arc::new(config),
        presence: Arc::new(Presence::default()),
    };

    // 2. Setup CORS so the frontend can talk to the API.
    //    Tighten `allow_origin` to your real frontend origin in production.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // 3. Build Router
    let app = Router::new()
        .route("/health", get(health))
        .route("/login", post(login))
        .route("/rooms", post(create_room))
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
    Json(payload): Json<CreateRoomRequest>,
) -> impl IntoResponse {
    let room_id = Uuid::new_v4();

    sqlx::query("INSERT INTO rooms (id, name) VALUES ($1, $2)")
        .bind(room_id)
        .bind(&payload.name)
        .execute(&state.db)
        .await
        .unwrap();

    Json(RoomResponse {
        room_id: room_id.to_string(),
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
        .unwrap_or("");

    // 2. Validate JWT
    let claims = match validate_jwt(token, &state.config.jwt_secret) {
        Ok(claims) => claims,
        Err(_) => {
            return axum::response::Response::builder()
                .status(401)
                .body("Unauthorized".into())
                .unwrap();
        }
    };

    // 3. Upgrade to WebSocket
    ws.on_upgrade(move |socket| handle_socket(socket, state, claims))
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
    let cf_session_id = match cf_new_session(&state.config, &user_id.to_string()).await {
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
    let mut forwarder: Option<tokio::task::JoinHandle<()>> = None;

    while let Some(Ok(msg)) = ws_stream.next().await {
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
                };
                let (existing, mut rx) = state.presence.join(rid, member);

                // Send the current roster (excludes self).
                let _ = out_tx.send(Message::Text(
                    serde_json::json!({
                        "action": "roster",
                        "peers": existing.iter().map(|m| serde_json::json!({
                            "cf_session_id": m.cf_session_id,
                            "user_name": m.user_name,
                            "tracks": m.tracks,
                        })).collect::<Vec<_>>(),
                    })
                    .to_string(),
                ));

                // Forward future room events to this client (skipping our own).
                let fwd_tx = out_tx.clone();
                let me = cf_session_id.clone();
                forwarder = Some(tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(ev) => {
                                if ev.origin != me
                                    && fwd_tx.send(Message::Text(ev.payload)).is_err()
                                {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }));

                room_id = Some(rid.to_string());
            }

            // Signaling actions all target the server-owned cf_session_id.
            Some("publish") => {
                let reply = handle_signal(&state.config, &cf_session_id, &json_msg).await;
                // On a successful publish, announce our tracks to the room.
                if reply.get("action").and_then(|a| a.as_str()) == Some("publish_answer") {
                    if let Some(rid) = &room_id {
                        let tracks = track_names(&json_msg);
                        state.presence.publish(rid, &cf_session_id, tracks);
                    }
                }
                let _ = out_tx.send(Message::Text(reply.to_string()));
            }

            _ => {
                let reply = handle_signal(&state.config, &cf_session_id, &json_msg).await;
                let _ = out_tx.send(Message::Text(reply.to_string()));
            }
        }
    }

    // --- Cleanup on disconnect ---
    if let Some(rid) = &room_id {
        state.presence.leave(rid, &cf_session_id);
    }
    if let Some(f) = forwarder {
        f.abort();
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

            match cf_tracks_new(config, cf_session_id, body).await {
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

            match cf_tracks_new(config, cf_session_id, body).await {
                Ok(resp) => serde_json::json!({
                    "action": "subscribe_offer",
                    "sessionDescription": resp["sessionDescription"],
                    "requiresImmediateRenegotiation": resp["requiresImmediateRenegotiation"],
                    "tracks": resp["tracks"],
                }),
                Err(e) => serde_json::json!({ "error": format!("subscribe failed: {e}") }),
            }
        }

        // Complete a renegotiation: client sends its answer.
        Some("renegotiate") => {
            let sdp = msg["sdp"].as_str().unwrap_or_default();
            let body = serde_json::json!({
                "sessionDescription": { "type": "answer", "sdp": sdp },
            });
            match cf_renegotiate(config, cf_session_id, body).await {
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
async fn cf_new_session(config: &Config, correlation_id: &str) -> Result<String, String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/new?correlationId={}",
        config.cf_app_id,
        urlencoding(correlation_id),
    );
    let res = reqwest::Client::new()
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
    config: &Config,
    session_id: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/{}/tracks/new",
        config.cf_app_id, session_id
    );
    cf_send(config, reqwest::Method::POST, &url, Some(body)).await
}

/// PUT /apps/{appId}/sessions/{sessionId}/renegotiate — finish renegotiation.
async fn cf_renegotiate(
    config: &Config,
    session_id: &str,
    body: serde_json::Value,
) -> Result<(), String> {
    let url = format!(
        "{CF_SFU_BASE}/apps/{}/sessions/{}/renegotiate",
        config.cf_app_id, session_id
    );
    cf_send(config, reqwest::Method::PUT, &url, Some(body)).await.map(|_| ())
}

/// Shared request helper for the authenticated JSON SFU endpoints.
async fn cf_send(
    config: &Config,
    method: reqwest::Method,
    url: &str,
    body: Option<serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let mut req = reqwest::Client::new()
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
