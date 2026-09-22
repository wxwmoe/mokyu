# syntax=docker/dockerfile:1
FROM rust:1.96.0-bookworm AS build
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY migrations ./migrations
COPY src ./src
COPY web ./web
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --bin media-gateway \
    && cp target/release/media-gateway /media-gateway

FROM debian:bookworm-slim
ENV MALLOC_MMAP_THRESHOLD_=131072
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /media-gateway /usr/local/bin/media-gateway
RUN mkdir -p /data/multipart /data/chunks /run/media-gateway /config \
    && chown -R 10001:10001 /data /run/media-gateway \
    && printf '#!/bin/sh\nexec media-gateway cli "$@"\n' > /usr/local/bin/cli \
    && chmod 755 /usr/local/bin/cli
USER 10001:10001
EXPOSE 9000 9001 9002
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD cli status >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/media-gateway"]
CMD ["serve"]
