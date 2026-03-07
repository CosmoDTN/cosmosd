FROM rust:1.77-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ffmpeg python3 python3-pip ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/cosmosd /usr/local/bin/cosmosd
EXPOSE 7777 7778
ENTRYPOINT ["cosmosd"]
CMD ["serve"]
