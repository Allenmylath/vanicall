//! Billing reconciler.
//!
//! Runs independently of the signaling server. It finds call sessions that have
//! ended but not yet been billed, asks Cloudflare's GraphQL Analytics API how
//! many bytes each `cf_session_id` actually used (the `callsUsageAdaptiveGroups`
//! dataset, broken down by `sessionId`), and writes the authoritative figure
//! into the `sessions` row + the user's running `total_bytes`.
//!
//! Cloudflare's analytics is delayed and adaptively sampled, so we wait a
//! settle period after a call ends before trusting the numbers, and we bill
//! each session exactly once (guarded by `reconciled_at`).
//!
//! Run modes:
//!   - one-shot:   default — does a single pass and exits (good for a cron /
//!                 Fly scheduled machine).
//!   - continuous: set RECONCILE_INTERVAL_SECS to loop forever (good for a
//!                 long-running Fly process group).

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::env;

const CF_GRAPHQL_URL: &str = "https://api.cloudflare.com/client/v4/graphql";

struct Settings {
    account_tag: String,
    analytics_token: String,
    /// How long to wait after a call ends before the CF numbers are trusted.
    settle: Duration,
    /// After this long, accept whatever CF returns (even zero) and stop retrying.
    give_up_after: Duration,
    /// Max sessions to process per pass.
    batch: i64,
    /// If set, loop forever sleeping this many seconds between passes.
    interval_secs: Option<u64>,
}

impl Settings {
    fn from_env() -> Self {
        Self {
            account_tag: req("CF_ACCOUNT_ID"),
            analytics_token: req("CF_ANALYTICS_API_TOKEN"),
            settle: Duration::seconds(env_i64("RECONCILE_SETTLE_SECS", 300)),
            give_up_after: Duration::seconds(env_i64("RECONCILE_GIVE_UP_SECS", 86_400)),
            batch: env_i64("RECONCILE_BATCH", 200),
            interval_secs: env::var("RECONCILE_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }
}

fn req(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Missing required environment variable: {key}"))
}

fn env_i64(key: &str, default: i64) -> i64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[derive(sqlx::FromRow)]
struct PendingSession {
    id: uuid::Uuid,
    user_id: uuid::Uuid,
    cf_session_id: String,
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let settings = Settings::from_env();
    let database_url = req("DATABASE_URL");

    let db = PgPoolOptions::new()
        .max_connections(3)
        .connect(&database_url)
        .await
        .expect("Failed to connect to the database");

    let client = reqwest::Client::new();

    match settings.interval_secs {
        None => {
            let n = run_pass(&db, &client, &settings).await;
            tracing::info!("reconciled {n} session(s); exiting (one-shot mode)");
        }
        Some(secs) => {
            tracing::info!("reconciler running continuously every {secs}s");
            loop {
                let n = run_pass(&db, &client, &settings).await;
                tracing::info!("pass complete: reconciled {n} session(s)");
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            }
        }
    }
}

/// Process one batch of ended-but-unbilled sessions. Returns how many were billed.
async fn run_pass(db: &PgPool, client: &reqwest::Client, s: &Settings) -> u64 {
    let cutoff = Utc::now() - s.settle;

    let pending: Vec<PendingSession> = match sqlx::query_as(
        r#"
        SELECT id, user_id, cf_session_id, started_at, ended_at
        FROM sessions
        WHERE reconciled_at IS NULL
          AND ended_at IS NOT NULL
          AND ended_at <= $1
          AND cf_session_id IS NOT NULL
        ORDER BY ended_at ASC
        LIMIT $2
        "#,
    )
    .bind(cutoff)
    .bind(s.batch)
    .fetch_all(db)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!("failed to load pending sessions: {e}");
            return 0;
        }
    };

    let mut billed = 0u64;
    for session in pending {
        match reconcile_one(db, client, s, &session).await {
            Ok(true) => billed += 1,
            Ok(false) => {} // data not ready yet; will retry next pass
            Err(e) => tracing::error!("session {} failed: {e}", session.id),
        }
    }
    billed
}

/// Returns Ok(true) if the session was billed, Ok(false) if we should retry later.
async fn reconcile_one(
    db: &PgPool,
    client: &reqwest::Client,
    s: &Settings,
    session: &PendingSession,
) -> Result<bool, String> {
    // Pad the window slightly: analytics buckets by minute and edges can land
    // just outside the exact call timestamps.
    let start = session.started_at - Duration::seconds(60);
    let end = session.ended_at + Duration::seconds(120);

    let usage = query_cf_usage(client, s, &session.cf_session_id, start, end).await?;

    match usage {
        Some((egress, ingress)) => {
            bill(db, session, egress, ingress).await?;
            tracing::info!(
                "billed session {} (user {}): {} egress + {} ingress bytes",
                session.id,
                session.user_id,
                egress,
                ingress
            );
            Ok(true)
        }
        None => {
            // No data from Cloudflare yet. If the call is old enough, accept zero
            // and stop retrying; otherwise leave it for a later pass.
            if Utc::now() - session.ended_at > s.give_up_after {
                bill(db, session, 0, 0).await?;
                tracing::warn!(
                    "session {} aged out with no CF usage data; billed as 0",
                    session.id
                );
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

/// Write the byte totals to the session and the user, atomically and exactly once.
async fn bill(db: &PgPool, session: &PendingSession, egress: i64, ingress: i64) -> Result<(), String> {
    let total = egress.saturating_add(ingress);

    let mut tx = db.begin().await.map_err(|e| e.to_string())?;

    // `reconciled_at IS NULL` in the WHERE makes this idempotent even if two
    // passes race: only the first update touches the row.
    let updated = sqlx::query(
        r#"
        UPDATE sessions
        SET egress_bytes = $1, ingress_bytes = $2, bytes_used = $3, reconciled_at = now()
        WHERE id = $4 AND reconciled_at IS NULL
        "#,
    )
    .bind(egress)
    .bind(ingress)
    .bind(total)
    .bind(session.id)
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;

    if updated.rows_affected() == 1 {
        sqlx::query("UPDATE users SET total_bytes = total_bytes + $1 WHERE id = $2")
            .bind(total)
            .bind(session.user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
    }

    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Query Cloudflare's `callsUsageAdaptiveGroups` for one session's byte totals.
/// Returns None if Cloudflare has no rows for this session in the window yet.
async fn query_cf_usage(
    client: &reqwest::Client,
    s: &Settings,
    session_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Option<(i64, i64)>, String> {
    const QUERY: &str = r#"
        query($acct: String!, $start: Time!, $end: Time!, $sid: String!) {
          viewer {
            accounts(filter: { accountTag: $acct }) {
              callsUsageAdaptiveGroups(
                limit: 1000
                filter: { datetime_geq: $start, datetime_leq: $end, sessionId: $sid }
              ) {
                sum { egressBytes ingressBytes }
              }
            }
          }
        }
    "#;

    let body = serde_json::json!({
        "query": QUERY,
        "variables": {
            "acct": s.account_tag,
            "start": start.to_rfc3339_opts(SecondsFormat::Secs, true),
            "end": end.to_rfc3339_opts(SecondsFormat::Secs, true),
            "sid": session_id,
        }
    });

    let res = client
        .post(CF_GRAPHQL_URL)
        .header("Authorization", format!("Bearer {}", s.analytics_token))
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !res.status().is_success() {
        return Err(format!(
            "CF GraphQL HTTP {}: {}",
            res.status(),
            res.text().await.unwrap_or_default()
        ));
    }

    let json: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;

    if let Some(errors) = json.get("errors").filter(|e| !e.is_null()) {
        return Err(format!("CF GraphQL errors: {errors}"));
    }

    let groups = &json["data"]["viewer"]["accounts"][0]["callsUsageAdaptiveGroups"];
    let Some(rows) = groups.as_array() else {
        return Ok(None);
    };
    if rows.is_empty() {
        return Ok(None);
    }

    let mut egress = 0i64;
    let mut ingress = 0i64;
    for row in rows {
        egress += row["sum"]["egressBytes"].as_i64().unwrap_or(0);
        ingress += row["sum"]["ingressBytes"].as_i64().unwrap_or(0);
    }

    Ok(Some((egress, ingress)))
}
