FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
    && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* && useradd -r -u 10001 crabbian
COPY --from=build /app/target/release/crabbian-watcher /usr/local/bin/crabbian-watcher
USER crabbian
ENV CRABBIAN_BIND=0.0.0.0:8080 LOG_LEVEL=info LOG_PROCESS=crabbian
EXPOSE 8080
CMD ["crabbian-watcher"]
