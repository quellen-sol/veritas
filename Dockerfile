FROM lukemathwalker/cargo-chef:latest-rust-1.82-slim-bullseye AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev git openssh-client libclang-dev cmake g++ protobuf-compiler
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

FROM chef AS planner
COPY ./.cargo/config.toml ./.cargo/config.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./crates ./crates
RUN --mount=type=ssh cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json
COPY ./.cargo/config.toml ./.cargo/config.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./crates ./crates
# Build for release
RUN --mount=type=ssh cargo build --release --bin veritas

FROM debian:bullseye-slim AS runtime
RUN apt update && apt-get install -y ca-certificates curl
COPY --from=builder /app/target/release/veritas .
CMD ["./veritas"]
