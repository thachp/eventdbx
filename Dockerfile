# syntax=docker/dockerfile:1.7

ARG BUILDER_IMAGE=rustlang/rust:nightly
ARG RUNTIME_IMAGE=debian:stable-slim
ARG RUNTIME_PACKAGES="ca-certificates tzdata libstdc++6 libsnappy1v5 liblz4-1 libzstd1 zlib1g libbz2-1.0 xz-utils"
ARG BUILD_PACKAGES="build-essential clang cmake pkg-config libsnappy-dev liblz4-dev libzstd-dev libbz2-dev zlib1g-dev capnproto"
ARG APP_USER=eventdbx
ARG APP_GROUP=eventdbx
ARG APP_UID=65532
ARG APP_GID=65532

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

ARG RUNTIME_IMAGE=debian:stable-slim
FROM ${RUNTIME_IMAGE}

ARG RUNTIME_PACKAGES
ARG APP_USER
ARG APP_GROUP
ARG APP_UID
ARG APP_GID

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

COPY --from=builder --chown=${APP_UID}:${APP_GID} --chmod=750 /app/target/release/dbx /usr/local/bin/dbx

RUN groupadd --system --gid "${APP_GID}" "${APP_GROUP}" \
 && useradd --system \
    --home "${EVENTDBX_DATA_DIR}" \
    --shell /usr/sbin/nologin \
    --uid "${APP_UID}" \
    --gid "${APP_GID}" \
    "${APP_USER}" \
 && install -d -m 750 -o "${APP_USER}" -g "${APP_GROUP}" \
      "${EVENTDBX_DATA_DIR}" \
      "${EVENTDBX_DATA_DIR}/.eventdbx/logs" \
      /var/log/eventdbx \
 && chown -R "${APP_USER}:${APP_GROUP}" "${EVENTDBX_DATA_DIR}" /var/log/eventdbx

USER ${APP_USER}
WORKDIR ${EVENTDBX_DATA_DIR}
VOLUME ["${EVENTDBX_DATA_DIR}"]

EXPOSE 7070 6363

ENTRYPOINT ["dbx"]
CMD ["start", "--foreground"]
