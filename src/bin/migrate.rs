//! Applies schema.sql to the database. Idempotent (schema uses IF NOT EXISTS).
//! Usage: `cargo run --bin migrate`

use sqlx::postgres::PgPoolOptions;
use std::env;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let schema = std::fs::read_to_string("schema.sql").expect("could not read schema.sql");

    println!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("failed to connect");
    println!("Connected. Applying schema.sql...");

    // raw_sql runs multiple semicolon-separated statements via the simple protocol.
    sqlx::raw_sql(&schema)
        .execute(&pool)
        .await
        .expect("failed to apply schema");

    println!("Schema applied successfully.");
}
