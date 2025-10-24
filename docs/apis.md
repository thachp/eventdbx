---
title: API Reference
description: Integrate EventDBX via REST, GraphQL, gRPC, replication, and the Admin API.
nav_id: apis
---

# API Reference

EventDBX exposes multiple surfaces so you can choose the right tool for each integration. Tokens issued by the CLI are accepted everywhere unless specifically noted. REST/GraphQL/gRPC are now provided by companion binaries in the [dbx_plugins](https://github.com/thachp/dbx_plugins) workspace; the core daemon retains the admin API, replication socket, and control channel.

## REST

Run the `rest_api` plugin to expose these routes (`cargo run -p rest_api -- --bind 0.0.0.0:7070 --control 127.0.0.1:6363`). Base URL defaults to `http://localhost:7070`.

| Endpoint | Description |
| --- | --- |
| `GET /health` | Liveness probe (no auth required). |
| `GET /v1/aggregates` | List aggregates (`skip`/`take` query params). |
| `GET /v1/aggregates/{type}/{id}` | Fetch the current aggregate state. |
| `GET /v1/aggregates/{type}/{id}/events` | Stream events with pagination. |
| `POST /v1/events` | Append an event. |
| `GET /v1/aggregates/{type}/{id}/verify` | Return the aggregate's Merkle root. |

Example request:

```bash
curl -X POST \
  -H "Authorization: Bearer ${EVENTDBX_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "aggregate_type": "patient",
        "aggregate_id": "p-001",
        "event_type": "patient-updated",
        "payload": {
          "status": "inactive",
          "note": "Archived via REST"
        },
        "metadata": {
          "@audit": { "source": "rest" }
        }
      }' \
  http://localhost:7070/v1/events
```

Event writes enforce a few key rules:
- `aggregate_type` and `event_type` must be lowercase `snake_case`.
- Declare a schema for the aggregate before the first write; the initial event must end with `_created`.
- `aggregate_id` accepts letters, numbers, underscores, and hyphens (max 128 characters) with no surrounding whitespace.
- Payload JSON is limited to 256 KiB, and optional `metadata` objects (keys prefixed with `@`) are capped at 64 KiB so plugins can react without overwhelming the bus.
- `event_id` values are Snowflake IDs encoded as strings; assign each node a unique `snowflake_worker_id` (0-1023) in `config.toml` to avoid collisions when pushing or pulling events.

## GraphQL

Launch the `graphql_api` plugin (`cargo run -p graphql_api -- --bind 0.0.0.0:7071 --control 127.0.0.1:6363`). Supply the same bearer token.

```bash
curl -X POST \
  -H "Authorization: Bearer ${EVENTDBX_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "query Recent($take: Int!) { aggregates(take: $take) { aggregate_type aggregate_id version state } }",
        "variables": { "take": 5 }
      }' \
  http://localhost:7070/graphql
```

Mutations follow a similar shape:

```bash
curl -X POST \
  -H "Authorization: Bearer ${EVENTDBX_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "mutation Append($input: AppendEventInput!) { appendEvent(input: $input) { aggregate_type aggregate_id version payload } }",
        "variables": {
          "input": {
            "aggregate_type": "patient",
            "aggregate_id": "p-001",
            "event_type": "patient-updated",
            "payload": { "status": "active" }
          }
        }
      }' \
  http://localhost:7070/graphql
```

## gRPC

Launch the `grpc_api` plugin (`cargo run -p grpc_api -- --bind 0.0.0.0:7072 --control 127.0.0.1:6363`). Use `grpcurl` for quick checks:

```bash
grpcurl \
  -H "authorization: Bearer ${EVENTDBX_TOKEN}" \
  -d '{
        "aggregate_type": "patient",
        "aggregate_id": "p-001",
        "event_type": "patient-updated",
        "payload_json": "{\"status\":\"inactive\"}"
      }' \
  -plaintext 127.0.0.1:7070 dbx.api.EventService/AppendEvent
```

The service mirrors REST operations: `AppendEvent`, `ListAggregates`, `GetAggregate`, `ListEvents`, `VerifyAggregate`, and `Health`.

## Replication socket

`dbx push` and `dbx pull` connect over the Cap'n Proto socket defined in `[socket].bind_addr` (default `0.0.0.0:6363`). Remotes authenticate using pinned Ed25519 public keys:

```bash
dbx remote add standby1 10.10.0.5 \
  --public-key $(dbx remote key) \
  --port 6363
```

Dry-run pushes report pending changes without transferring events:

```bash
dbx push standby1 --dry-run --aggregate patient
```

## Admin API

Enable it with:

```bash
dbx config \
  --admin-enabled true \
  --admin-bind 127.0.0.1 \
  --admin-port 7171
```

Requests must include a bearer token with `*.*` privileges. Example session using the bootstrap token written to `~/.eventdbx/cli.token`:

```bash
curl -H "Authorization: Bearer $(cat ~/.eventdbx/cli.token)" \
  http://127.0.0.1:7171/admin/tokens

curl -X POST -H "Authorization: Bearer $(cat ~/.eventdbx/cli.token)" \
  -H "Content-Type: application/json" \
  -d '{"name":"standby1","endpoint":"tcp://10.10.0.5:6363","public_key":"BASE64"}' \
  http://127.0.0.1:7171/admin/remotes/standby1
```

Endpoints include `/admin/tokens`, `/admin/schemas`, `/admin/remotes`, and `/admin/plugins`, all mirroring the behavior of their CLI counterparts.
