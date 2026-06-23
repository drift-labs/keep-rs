# ---- Build Stage ----
FROM rust:1.91.1 AS builder
WORKDIR /app

ENV CARGO_DRIFT_FFI_PATH="/usr/local/lib"

# 1. Cache dependencies first
RUN apt-get update && apt-get install jq -y && rustup component add rustfmt

# TODO: docker layer cache
# COPY Cargo.toml Cargo.lock ./
# RUN mkdir src && echo "fn main() {}" > src/main.rs

# install libdrift
RUN SO_URL=$(curl -s https://api.github.com/repos/velocity-exchange/drift-ffi-sys/releases/latest | jq -r '.assets[] | select(.name=="libdrift_ffi_sys.so") | .browser_download_url') &&\
    curl -L -o libdrift_ffi_sys.so "$SO_URL" &&\
    cp libdrift_ffi_sys.so $CARGO_DRIFT_FFI_PATH

# 2. Copy actual code and build.
# protocol-v2-shadow (the `drift` crate) and drift-rs are consumed as path-dep
# siblings of keep-rs. The build context is the parent dir holding all three;
# CI checks them out side by side (see .github/workflows). Preserve the
# relative layout so Cargo's `../drift-rs` / `../protocol-v2-shadow` resolve.
COPY protocol-v2-shadow ./protocol-v2-shadow
COPY drift-rs ./drift-rs
COPY keep-rs ./keep-rs

WORKDIR /app/keep-rs
RUN cargo build --release

# ---- Runtime Stage ----
FROM debian:bookworm-slim
# RUN apt-get update && apt-get install -y ca-certificates lldb
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /usr/local/lib/libdrift_ffi_sys.so /lib/
# COPY --from=builder /app/keep-rs/target/release/keeprs /usr/local/bin/keeprs-real
COPY --from=builder /app/keep-rs/target/release/keeprs /usr/local/bin/keeprs

# RUN echo '#!/bin/bash\nexec lldb -o "run" -o "bt all" -o "quit" -- /usr/local/bin/keeprs-real "$@"' > /usr/local/bin/keeprs && \
#     chmod +x /usr/local/bin/keeprs

EXPOSE 9898
ENV METRICS_PORT=9898

ENTRYPOINT ["/usr/local/bin/keeprs"]
