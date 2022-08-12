FROM lukemathwalker/cargo-chef:latest AS chef
WORKDIR app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

RUN apt-get update \
    && apt-get install -y libclang-dev cmake libsasl2-dev
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN apt-get update \
    && apt-get install -y libclang-dev cmake libsasl2-dev clang
RUN rustup  component add rustfmt
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY ./src src
COPY ./Cargo.lock Cargo.lock
COPY ./Cargo.toml Cargo.toml
RUN cargo build --release

FROM debian:11.0-slim as prod
RUN apt-get update &&  apt-get install -y --no-install-recommends    openssl ca-certificates  libsasl2-2 \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/
COPY --from=builder app/target/release/ton-kafka-producer /ton-kafka-producer
RUN chmod +x /ton-kafka-producer
ENV RUST_LOG='info,tonswap_trade=debug'
CMD ["/ton-kafka-producer","--config", "config.yml", "--global-config", "ton-global.config.json"]
