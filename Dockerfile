# ---- Build stage ----
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

# Cache dependencies: build a dummy bin against Cargo.toml first.
COPY Cargo.toml ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real application.
COPY src ./src
# Touch main.rs so cargo rebuilds it (the dummy was cached above).
RUN touch src/main.rs \
    && cargo build --release

# ---- Runtime stage ----
FROM debian:bookworm-slim AS runtime

# ca-certificates is required for outbound TLS (Cloudflare API, Neon Postgres).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user.
RUN useradd --create-home --uid 10001 appuser
USER appuser
WORKDIR /home/appuser

# Ship both binaries: the signaling/web server and the billing reconciler.
COPY --from=builder /app/target/release/vanicall /usr/local/bin/vanicall
COPY --from=builder /app/target/release/reconcile /usr/local/bin/reconcile

EXPOSE 8080
ENV PORT=8080

# Default command is the web server; the reconciler process overrides this
# (see [processes] in fly.toml).
CMD ["vanicall"]
