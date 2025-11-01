# Backend Benchmarks (Docker)

This directory contains a reproducible environment that spins up Postgres,
MongoDB, SQL Server, and the EventDBX benchmark runner inside containers so
each service receives the same resource envelope (CPU, memory, and tmpfs-backed
storage). Use it when you want to compare `get`, `apply`, `patch`, and `list`
operations across the supported databases without worrying about host-level
variance.

## Prerequisites

- Docker Engine 24+ and Docker Compose plugin 2.21+.
- 8 GB of free RAM to accommodate the containers and tmpfs mounts.
- Optional: pre-built Rust dependencies cached in `~/.cargo` to shorten the
  first build.

## Quick Start

```bash
cd benchmarks
# Launch the database containers and run the benchmark inside the bench runner.
docker compose up --abort-on-container-exit bench
```

Compose will:

1. Start `postgres`, `mongodb`, and `mssql` with the same memory cap and tmpfs
   storage size (configurable via environment variables described below).
2. Build the lightweight Rust image defined in `Dockerfile`.
3. Mount the repository into `/workspace` and execute:

   ```bash
   cargo bench --bench backend_comparison --features bench-all
   ```

4. Stream the Criterion output to your terminal.

All containers are stopped automatically once the benchmark finishes.

## Configurable Limits

Environment variables let you align resource budgets across every container:

| Variable             | Default | Description                                    |
| -------------------- | ------- | ---------------------------------------------- |
| `BENCH_MEMORY_LIMIT` | `4g`    | Memory limit passed to each service.           |
| `BENCH_CPU_LIMIT`    | `4`     | CPU quota (number of virtual cores).           |
| `BENCH_TMPFS_SIZE`   | `1073741824` (1 GiB) | tmpfs size for database storage. |
| `BENCH_MONGO_CACHE_GB` | `1.0` | WiredTiger cache size in GB.                   |
| `BENCH_MSSQL_PASSWORD` | `Eventdbx#Bench1` | Password for the SQL Server SA user. |

Example with a tighter envelope:

```bash
BENCH_MEMORY_LIMIT=2g \
BENCH_CPU_LIMIT=2 \
BENCH_TMPFS_SIZE=$((512 * 1024 * 1024)) \
docker compose up --abort-on-container-exit bench
```

## Result Files

Criterion writes reports under `target/criterion`. When you run inside the
container setup above, that directory lives on your host because the repository
is bind-mounted at `/workspace`. You can inspect HTML reports locally after the
run completes.

## Cleaning Up

All services run with ephemeral tmpfs volumes. To remove build artifacts from
the bench runner image, run:

```bash
docker compose down --volumes --rmi local
```

This will delete the temporary SQLite volume as well as the intermediate bench
image created during the build.
