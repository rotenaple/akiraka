# ── Stage 1: Get Akari binary ───────────────────────────────
FROM akiraka-akari:latest AS akari-source

# ── Stage 2: Build Akiraka (Node.js) ────────────────────────
FROM node:20-slim AS akiraka-builder
WORKDIR /build
COPY package.json tsconfig.json ./
RUN npm install --omit=optional
COPY src ./src
RUN npm run build
RUN npm prune --production --omit=optional

# ── Stage 3: Bundled Runtime ────────────────────────────────
# Use debian:sid-slim to match the glibc version used by akari
FROM debian:sid-slim

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    nodejs \
    bash && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Akari binary
COPY --from=akari-source /usr/local/bin/akari /usr/local/bin/akari
RUN chmod +x /usr/local/bin/akari

# Copy Akiraka
COPY --from=akiraka-builder /build/node_modules ./node_modules
COPY --from=akiraka-builder /build/dist ./dist
COPY --from=akiraka-builder /build/package.json ./package.json
COPY data/issues_cache.txt /app/data/issues_cache.txt
COPY config /app/config
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh && sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh

VOLUME ["/data"]

ENV NODE_ENV=production
ENV CACHE_FILE=/app/data/issues_cache.txt
ENV AKARI_LOG=/data/akari_events.jsonl
ENV CTE_LOG=/data/events.enriched.cte.jsonl

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
