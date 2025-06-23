# ---- Build Stage ----
FROM rust:1.85 as builder
WORKDIR /app

# Install build dependencies
RUN apt-get update

# Copy manifests and source
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

# Build the swift bot binary (release)
RUN cargo build --release

# ---- Runtime Stage ----
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/filler ./filler

EXPOSE 9898
ENV METRICS_PORT=9898

ENTRYPOINT ["./filler"] 