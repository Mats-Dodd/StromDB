# Build context is the ds-bench/ crate directory.
# ds-bench is edition 2024 -> needs Rust >= 1.86.
FROM rust:1.86-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release
RUN cp target/release/ds-bench /ds-bench

FROM debian:bookworm-slim
# ca-certificates for TLS; `mc` (MinIO client) so the fleet/coordinator can
# upload/download HDR+JSON results to in-cluster MinIO (cross-node merge);
# procps (pgrep) so this image can ALSO host the metrics sidecar locally.
# mc is fetched for the IMAGE's own arch (dpkg --print-architecture -> arm64|amd64)
# so a native arm64 build (local kind on Apple Silicon) and an amd64 build
# (Cloud Build for GKE) both get a runnable binary.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl procps \
    && ARCH="$(dpkg --print-architecture)" \
    && curl -fsSL "https://dl.min.io/client/mc/release/linux-${ARCH}/mc" -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc && rm -rf /var/lib/apt/lists/*
COPY --from=builder /ds-bench /usr/local/bin/ds-bench
ENTRYPOINT ["ds-bench"]
