FROM rust:1.93-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs && cargo build --release && rm -rf src target/release/peak
COPY src ./src
# Docker preserves source mtimes on COPY. Refresh the crate root so Cargo cannot reuse the
# temporary dependency-cache binary created above.
RUN touch src/main.rs && cargo build --release

FROM alpine:3.22
RUN addgroup -S telemetry && adduser -S telemetry -G telemetry
RUN mkdir -p /var/lib/peak && chown telemetry:telemetry /var/lib/peak
WORKDIR /app
COPY config.json ./config.json
COPY tenants ./tenants
COPY --from=build /app/target/release/peak /usr/local/bin/peak
USER telemetry
EXPOSE 8081
CMD ["/usr/local/bin/peak"]
