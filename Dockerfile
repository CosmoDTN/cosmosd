# 1.88: bp7 is edition 2024 and uses let chains, which need Rust 1.88.
# It declares no rust-version, so Cargo.toml sets rust-version to match.
FROM rust:1.88-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# include_str!/include_bytes! embed the phone player page and hls.js.
COPY assets ./assets
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ffmpeg python3 python3-pip ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/cosmosd /usr/local/bin/cosmosd
EXPOSE 7777 7778
ENTRYPOINT ["cosmosd"]
CMD ["serve"]
