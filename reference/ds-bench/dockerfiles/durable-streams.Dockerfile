# Build context is the server-rust CRATE dir (DS_RUST_REPO/packages/server-rust).
# It is a standalone crate (own Cargo.toml, no workspace / path deps), so the crate
# dir alone is a self-sufficient — and far smaller — context than the whole monorepo.
# RUST_VERSION is overridable — the `telemetry` feature pulls tonic 0.14 which needs
# rustc >= 1.88; the default production set builds on 1.86.
ARG RUST_VERSION=1.86
FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /src
COPY . .
# FEATURES is overridable so a bench/diagnostic image can add `telemetry`. The only
# build feature the bench needs is `tier` (S3 cold-tier → MinIO). `gcloud builds
# submit --tag` passes no build-arg, so this default must match the server checkout.
ARG FEATURES=tier
RUN cargo build --release --features ${FEATURES}
RUN cp target/release/durable-streams-server /durable-streams-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /durable-streams-server /usr/local/bin/durable-streams-server
EXPOSE 4438
ENTRYPOINT ["durable-streams-server"]
