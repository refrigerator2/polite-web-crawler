FROM rust:1.95.0 AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    cmake \
    g++ \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

COPY . .

RUN cargo build --release --target x86_64-unknown-linux-musl -p crawler-worker

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/crawler-worker /usr/local/bin/crawler-worker

ENTRYPOINT ["crawler-worker"]
