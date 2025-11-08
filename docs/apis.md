---
title: API Reference
description: Integrate EventDBX via REST, GraphQL, and gRPC.
nav_id: apis
---

# API Reference

EventDBX exposes multiple surfaces so you can choose the right tool for each integration. Tokens issued by the CLI are accepted everywhere unless specifically noted. REST/GraphQL/gRPC are now provided by companion binaries in the [dbx_plugins](https://github.com/thachp/dbx_plugins) workspace.

## REST

Run the `dbx_rest_` plugin to expose these routes (`cargo run -p dbx_rest -- --bind 0.0.0.0:8080 --control 127.0.0.1:6363`). Base URL defaults to `http://localhost:8080`.

| Endpoint                                | Description                                   |
| --------------------------------------- | --------------------------------------------- |
| `GET /health`                           | Liveness probe (no auth required).            |
| `GET /v1/aggregates`                    | List aggregates (`cursor`/`take` query params). |
| `GET /v1/aggregates/{type}/{id}`        | Fetch the current aggregate state.            |
| `GET /v1/aggregates/{type}/{id}/events` | Stream events with pagination.                |
| `POST /v1/events`                       | Append an event.                              |
| `GET /v1/aggregates/{type}/{id}/verify` | Return the aggregate's Merkle root.           |

Cursor tokens mirror the CLI: aggregates use `a:<aggregate_type>:<aggregate_id>` for active data (`r:` for archived) and event cursors append the version (`a:<aggregate_type>:<aggregate_id>:<version>`). Supply the token from the final item in a page via the `cursor` query parameter to continue listing.

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
- Declare a schema for the aggregate before the first write when running with validation enabled.
- `aggregate_id` accepts letters, numbers, underscores, and hyphens (max 128 characters) with no surrounding whitespace.
- Payload JSON is limited to 256 KiB, and optional `metadata` objects (keys prefixed with `@`) are capped at 64 KiB so plugins can react without overwhelming the bus.
- `event_id` values are Snowflake IDs encoded as strings; assign each node a unique `snowflake_worker_id` (0-1023) in `config.toml` to avoid collisions across nodes.
- Plugins receive the event envelope with `metadata.event_id` as a stringified Snowflake and (optionally) an `extensions` object; ensure the [dbx_plugins](https://github.com/thachp/dbx_plugins) repo or any custom adapters tolerate the new fields.

## GraphQL

Launch the `dbx_graphql` plugin (`cargo run -p dbx_graphql -- --bind 0.0.0.0:8081 --control 127.0.0.1:6363`). Supply the same bearer token.

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

Launch the `dbx_grpc` plugin (`cargo run -p dbx_grpc -- --bind 0.0.0.0:8082 --control 127.0.0.1:6363`). Use `grpcurl` for quick checks:

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
