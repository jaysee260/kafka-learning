# syntax=docker/dockerfile:1
# One small image that holds the Rust home services. The hub
# (deploy/opsbox/docker-compose.yml) uses it for mining-watch and
# mining-alerts.

# ---- Build: compile and test with the full Rust toolchain ----
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
# The two cache mounts keep downloaded crates and compiled code between
# builds, so only what changed is rebuilt. The first build takes several
# minutes, mostly compiling the Kafka and ZeroMQ C libraries.
# A failing test fails the build, so a broken image is never produced.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo test --release --locked \
    && cargo build --release --locked \
    && cp target/release/mining_watch target/release/mining_alerts /usr/local/bin/

# ---- Run: just the two programs, plus ssh for reading the Pi ----
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/mining_watch /usr/local/bin/mining_alerts /usr/local/bin/

# Don't run as root (mining-watch overrides this, see the compose file).
USER nobody
CMD ["mining_alerts"]
