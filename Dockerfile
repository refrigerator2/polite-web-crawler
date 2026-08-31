FROM rust:1.80-slim as builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

ARG SERVICE_NAME

RUN cargo build --release --package ${SERVICE_NAME}

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG SERVICE_NAME

COPY --from=builder /app/target/release/${SERVICE_NAME} /app/service

CMD ["/app/service"]
