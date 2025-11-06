# syntax=docker/dockerfile:1.7

ARG BUILDER_IMAGE=rustlang/rust:nightly
ARG RUNTIME_IMAGE=debian:stable-slim
ARG RUNTIME_PACKAGES="ca-certificates curl tzdata libstdc++6 libsnappy1v5 liblz4-1 libzstd1 zlib1g libbz2-1.0 xz-utils"
ARG BUILD_PACKAGES="build-essential clang cmake pkg-config libsnappy-dev liblz4-dev libzstd-dev libbz2-dev zlib1g-dev capnproto"

FROM ${BUILDER_IMAGE} as builder

ARG BUILD_PACKAGES
ARG RUNTIME_PACKAGES

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    $BUILD_PACKAGES \
    $RUNTIME_PACKAGES \
 && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto

RUN cargo fetch --locked

COPY src ./src

RUN cargo build --release --locked

RUN find target/release -maxdepth 1 -type f -executable -print -exec strip {} \; || true

ARG RUNTIME_IMAGE=debian:stable-20251103-slim
FROM ${RUNTIME_IMAGE}

ARG RUNTIME_PACKAGES

LABEL org.opencontainers.image.source="https://github.com/thachp/eventdbx" \
      org.opencontainers.image.description="EventDBX server and CLI" \
      org.opencontainers.image.licenses="MIT"

ENV EVENTDBX_DATA_DIR=/var/lib/eventdbx
ENV HOME=${EVENTDBX_DATA_DIR}

# Secrets are loaded from env vars at runtime; supply them with `docker run -e ...` when needed:
#   EVENTDBX_DATA_ENCRYPTION_KEY – 32-byte base64 string encrypting on-disk data
#   EVENTDBX_AUTH_PRIVATE_KEY – base64 Ed25519 private key used for JWT signing
#   EVENTDBX_AUTH_PUBLIC_KEY – matching Ed25519 public key distributed to verifiers

RUN apt-get update \
 && apt-get install -y --no-install-recommends $RUNTIME_PACKAGES \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder --chmod=755 /app/target/release/dbx /usr/local/bin/dbx

RUN useradd --system --home "${EVENTDBX_DATA_DIR}" --shell /usr/sbin/nologin eventdbx \
 && mkdir -p "${EVENTDBX_DATA_DIR}" "${EVENTDBX_DATA_DIR}/.eventdbx/logs" /var/log/eventdbx \
 && chown -R eventdbx:eventdbx "${EVENTDBX_DATA_DIR}" /var/log/eventdbx

USER eventdbx
WORKDIR ${EVENTDBX_DATA_DIR}
VOLUME ["${EVENTDBX_DATA_DIR}"]

EXPOSE 7070 6363

ENTRYPOINT ["dbx"]
CMD ["start", "--foreground"]
