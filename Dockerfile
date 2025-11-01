# syntax=docker/dockerfile:1.7

FROM debian:bookworm-slim

LABEL org.opencontainers.image.source="https://github.com/thachp/eventdbx" \
      org.opencontainers.image.description="EventDBX server and CLI" \
      org.opencontainers.image.licenses="MIT"

ENV EVENTDBX_DATA_DIR=/var/lib/eventdbx
ENV HOME=${EVENTDBX_DATA_DIR}

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tzdata \
    libstdc++6 \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    zlib1g \
    libbz2-1.0 \
    xz-utils \
 && rm -rf /var/lib/apt/lists/*

# Install the published EventDBX binary, then upgrade to the latest available release.
RUN set -eux; \
    curl --proto '=https' --tlsv1.2 -LsSf \
      https://github.com/thachp/eventdbx/releases/download/v3.9.12/eventdbx-installer.sh \
      | sh; \
    install -Dm755 "$HOME/.cargo/bin/dbx" /usr/local/bin/dbx; \
    dbx upgrade; \
    rm -rf "$HOME/.cargo"
RUN useradd --system --home "${EVENTDBX_DATA_DIR}" --shell /usr/sbin/nologin eventdbx \
 && mkdir -p "${EVENTDBX_DATA_DIR}" "${EVENTDBX_DATA_DIR}/.eventdbx/logs" /var/log/eventdbx \
 && chown -R eventdbx:eventdbx "${EVENTDBX_DATA_DIR}" /var/log/eventdbx

USER eventdbx
WORKDIR ${EVENTDBX_DATA_DIR}
VOLUME ["${EVENTDBX_DATA_DIR}"]

EXPOSE 7070 6363

ENTRYPOINT ["dbx"]
CMD ["start", "--foreground"]
