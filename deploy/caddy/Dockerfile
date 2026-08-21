# Stock Caddy has no rate limiting, so the edge proxy is built with the rate-limit module.
# Without this the `rate_limit` directive in deploy/Caddyfile fails to parse and Caddy will not
# start -- the two files change together.
FROM caddy:2.10-builder-alpine AS build
RUN xcaddy build --with github.com/mholt/caddy-ratelimit

FROM caddy:2.11-alpine
COPY --from=build /usr/bin/caddy /usr/bin/caddy
