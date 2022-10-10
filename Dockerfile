FROM rust:1.64 AS chef
WORKDIR /app


FROM chef AS builder
RUN apt-get update \
    && apt-get install -y libclang-dev cmake libsasl2-dev clang
# Build dependencies - this is the caching Docker layer!
RUN rustup  component add rustfmt
# Build application
COPY ./src src
COPY ./Cargo.lock Cargo.lock
COPY ./Cargo.toml Cargo.toml
RUN cargo build --release

FROM debian:sid-slim as release
WORKDIR /app

RUN apt-get update &&  apt-get install -y --no-install-recommends    openssl ca-certificates  libsasl2-2 \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/
COPY --from=builder app/target/release/ton-kafka-producer /usr/local/bin/ton-kafka-producer
RUN chmod +x /usr/local/bin/ton-kafka-producer
ENTRYPOINT ["ton-kafka-producer"]
