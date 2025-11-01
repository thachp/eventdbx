FROM rust:1.80-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    pkg-config \
    libssl-dev \
    libpq-dev \
    build-essential \
    clang \
    libclang-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
