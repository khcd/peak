FROM rust:1.93-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs && cargo build --release && rm -rf src target/release/telemetry-ingest
COPY src ./src
# Docker preserves source mtimes on COPY. Refresh the crate root so Cargo cannot reuse the
# temporary dependency-cache binary created above.
RUN touch src/main.rs && cargo build --release

FROM alpine:3.22
RUN addgroup -S telemetry && adduser -S telemetry -G telemetry
WORKDIR /app
COPY config.json ./config.json
COPY tenants ./tenants
COPY --from=build /app/target/release/telemetry-ingest /usr/local/bin/telemetry-ingest
USER telemetry
EXPOSE 8081
CMD ["/usr/local/bin/telemetry-ingest"]
