# ---- Build Stage ----
FROM rust:1.87.0 AS builder
WORKDIR /app

ENV CARGO_DRIFT_FFI_PATH="/usr/local/lib"

# 1. Cache dependencies first
RUN apt-get update && apt-get install jq -y && rustup component add rustfmt
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
#libdrift
RUN SO_URL=$(curl -s https://api.github.com/repos/drift-labs/drift-ffi-sys/releases/latest | jq -r '.assets[] | select(.name=="libdrift_ffi_sys.so") | .browser_download_url') &&\
  curl -L -o libdrift_ffi_sys.so "$SO_URL" &&\
  cp libdrift_ffi_sys.so $CARGO_DRIFT_FFI_PATH

RUN cargo build --release && rm -rf src

# 2. Copy actual code and rebuild
COPY . .
RUN cargo build --release

# ---- Runtime Stage ----
FROM debian:bookworm-slim
COPY --from=builder  /usr/local/lib/libdrift_ffi_sys.so /lib/
COPY --from=builder /app/target/release/filler /usr/local/bin/filler

EXPOSE 9898
ENV METRICS_PORT=9898

ENTRYPOINT ["./filler"] 