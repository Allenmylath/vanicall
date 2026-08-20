# Build context is the REPO ROOT, not this directory — the worker depends on ../ingest-lib:
#   docker build -f ingest-worker/Dockerfile .
# fly.toml sets this via build.dockerfile from the root.

FROM node:22-bookworm-slim AS ffmpeg
# Debian bookworm ships ffmpeg 5.1, which has NO WHIP muxer — `apt-get install ffmpeg` yields a
# worker that cannot publish at all. Pull a static build instead.
#
# Pinned to the release series (n8.1), not to a dated autobuild tag: BtbN deletes old autobuild
# tags, so a dated URL eventually 404s and breaks rebuilds of an otherwise untouched deployment.
# This asset name is stable and stays on 8.1.x, which is the version the ingest path was verified
# against. Bump deliberately; the assertion below is what stops a bad swap reaching production.
ARG FFMPEG_URL=https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-n8.1-latest-linux64-gpl-8.1.tar.xz
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl xz-utils ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "$FFMPEG_URL" -o /tmp/ffmpeg.tar.xz \
    && mkdir -p /tmp/ff \
    && tar -xJf /tmp/ffmpeg.tar.xz -C /tmp/ff --strip-components=1 \
    && install -m755 /tmp/ff/bin/ffmpeg /usr/local/bin/ffmpeg \
    && install -m755 /tmp/ff/bin/ffprobe /usr/local/bin/ffprobe \
    && rm -rf /tmp/ffmpeg.tar.xz /tmp/ff \
    && ffmpeg -hide_banner -muxers | grep -q " whip" \
    && echo "WHIP muxer present"

FROM node:22-bookworm-slim AS deps
WORKDIR /app
# The SDK is a file: dependency, so it has to be present AND built before the worker installs.
COPY ingest-lib/package.json ingest-lib/tsconfig.json ./ingest-lib/
COPY ingest-lib/src ./ingest-lib/src
RUN cd ingest-lib && npm install --silent && npm run build
COPY ingest-worker/package.json ./ingest-worker/
RUN cd ingest-worker && npm install --omit=dev --silent

FROM node:22-bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ffmpeg /usr/local/bin/ffmpeg /usr/local/bin/ffmpeg
COPY --from=ffmpeg /usr/local/bin/ffprobe /usr/local/bin/ffprobe
WORKDIR /app
COPY --from=deps /app/ingest-lib ./ingest-lib
COPY --from=deps /app/ingest-worker/node_modules ./ingest-worker/node_modules
COPY ingest-worker/package.json ./ingest-worker/
COPY ingest-worker/src ./ingest-worker/src
WORKDIR /app/ingest-worker
ENV NODE_ENV=production
ENV PORT=8090
EXPOSE 8090
# Run as the node user — ffmpeg is executing attacker-influenced URLs, so don't hand it root.
USER node
CMD ["node", "src/server.mjs"]
