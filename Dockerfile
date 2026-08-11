FROM rust:1.93-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN cargo build --release

FROM alpine:3.22
RUN addgroup -S telemetry && adduser -S telemetry -G telemetry
WORKDIR /app
COPY config.json ./config.json
COPY --from=build /app/target/release/planar-telemetry-ingest /usr/local/bin/planar-telemetry-ingest
USER telemetry
EXPOSE 8081
CMD ["/usr/local/bin/planar-telemetry-ingest"]
