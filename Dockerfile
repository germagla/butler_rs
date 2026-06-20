# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates chromium \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 butler \
    && mkdir -p /data/artifacts/runs \
    && chown -R butler:butler /data

ENV HEADLESS=true
ENV CHROME=/usr/bin/chromium
ENV ARTIFACT_DIR=/data/artifacts/runs

WORKDIR /app
COPY --from=builder /app/target/release/butler_rs /usr/local/bin/butler_rs

USER butler
CMD ["butler_rs"]
