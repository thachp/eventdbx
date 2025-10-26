---
title: Plugin Architecture
description: Build read-side projections and integrations on top of EventDBX.
nav_id: plugins
---

# Plugin Architecture

EventDBX owns the **write** side of CQRS. Every aggregate mutation is validated, stored, and snapshotted inside the EventDBX daemon. Plugins take those events and push them into downstream systems so the **read** side can evolve independently—feed search indexes, hydrate analytics warehouses, trigger workflows, or expose REST/GraphQL/grpc APIs.

This guide explains how the plugin pipeline works, how to configure delivery modes, and how to extend EventDBX with your own connectors.

## Lifecycle at a glance

```
aggregate apply → Job queued → Plugin dispatch → Success/Retry/Dead
```

1. **Event stored** – `dbx aggregate apply` (or any write surface) appends an event. EventDBX materialises the latest state and captures the optional schema.
2. **Job enqueued** – Each enabled plugin receives a durable job in `plugin_queue.db`. Jobs survive restarts and carry metadata (plugin label, payload, attempts, retries).
3. **Dispatch** – The manager pulls pending jobs per plugin, applies exponential backoff, and hands the payload to the plugin implementation.
4. **Completion** – Successful jobs move to the `done` bucket. Failures requeue with increasing delay until they reach the configured attempt limit, after which they land in the `dead` bucket. Use `dbx queue retry` to nudge dead jobs back to life once the downstream service is healthy.

## Payload modes

Each plugin instance can request the minimal payload it needs. When you run `dbx plugin config …`, pass `--payload <mode>`:

| Mode              | Includes                                                         | Typical use case                 |
|-------------------|------------------------------------------------------------------|----------------------------------|
| `all`             | Event document, materialised state, and schema (default)         | Rich projections / migrations    |
| `event-only`      | Event document only                                              | Webhooks, streams, audit relays  |
| `state-only`      | Latest state map only                                            | Cache warmers, read replicas     |
| `schema-only`     | Aggregate schema only                                            | Schema registries                |
| `event-and-schema`| Event + schema, no state                                         | Validation services, codegen     |

Plugins receive payloads as JSON envelopes (HTTP/TCP) or Cap’n Proto messages (capnp/process). The queue persists the same envelope so you can inspect pending and dead jobs safely.

## Built-in plugin types

- **HTTP** – POSTs payloads to a remote endpoint; use headers for auth. Great for webhooks, SaaS connectors, or serverless consumers.
- **TCP** – Streams JSON lines over a raw TCP socket.
- **Cap’n Proto** – Sends Cap’n Proto messages (handy for low-latency services written in Rust/C++).
- **Log** – Writes event/state/schema data to the EventDBX log sink for debugging or audit transcripts.
- **Process** – Supervises an executable installed via `dbx plugin install`; the binary receives Cap'n Proto frames on stdin/stdout.

All of these live in the [dbx_plugins workspace](https://github.com/thachp/dbx_plugins). Use those implementations as templates for new connectors.

Need the binary running but don't want it to consume events (for example, the REST surface that simply exposes state)? Configure the process plugin with `--emit-events=false`. The supervisor still keeps the worker alive, but the queue will skip enqueuing payloads for that instance.

## Inspecting and managing the queue

```bash
dbx queue            # Show counts and detailed job listings
dbx queue retry      # Retry every dead job
dbx queue retry --event-id <job-id>  # Retry a specific dead job
dbx queue clear      # Clear dead jobs (asks for confirmation)
```

Each job prints its plugin label, status, attempt count, next retry deadline, and last error message. Jobs live in `~/.eventdbx/plugin_queue.db`; the data is JSON-encoded so you can inspect it manually if needed.

### Automatic pruning

EventDBX trims the `done` bucket in the background so completed jobs do not grow without bound. By default the daemon removes jobs after 24 hours and evaluates the queue every five minutes. Tune the policy in `config.toml`:

```toml
[plugin_queue.prune]
# Drop done jobs older than a day (set to 0 to disable age-based pruning).
done_ttl_secs = 86400
# Minimum delay between pruning passes. Values lower than 60 seconds are rounded up.
interval_secs = 300
# Optional ceiling for the number of done jobs to retain. Omit to keep all jobs within the TTL.
max_done_jobs = 500
```

Use `dbx queue clear-done --older-than-hours <hours>` for an ad-hoc cleanup when you need to reclaim space immediately.

## Extending EventDBX

You can extend EventDBX in two ways:

1. **Custom process plugin** – Package your service as an executable, install it with `dbx plugin install`, and configure it with `dbx plugin config process … --payload …`. Your binary speaks the same Cap’n Proto protocol as the built-in process plugin.
2. **Native Rust plugin** – Fork the [dbx_plugins workspace](https://github.com/thachp/dbx_plugins) and add a crate that implements the `Plugin` trait. Ship your crate (e.g., `search_api`, `sql_gateway`), then run it alongside the daemon.

Ideas:

- Push events into Apache Kafka, Amazon EventBridge, Google Pub/Sub, or Azure Event Grid.
- Maintain a document search index (OpenSearch, Meilisearch, Typesense).
- Sync aggregate state into a SQL warehouse and expose reporting endpoints.
- Derive graph views in Neo4j or TigerGraph.
- Trigger orchestration workflows, ML feature stores, or notification pipelines.

EventDBX remains the source of truth; plugins consume the append-only log and build read-friendly representations wherever your business needs them.

## Operational tips

- **Backoff & attempts** – Control maximum attempts via `plugin_max_attempts` in `config.toml`. Failed jobs back off exponentially (1s, 2s, 4s, then 10s).
- **Observability** – Combine `dbx queue` output with your plugin’s own logs/metrics to monitor throughput and retries.
- **Upgrades** – Plugins can be deployed independently. When you introduce a new connector, install it, configure payload mode, and watch the queue drain.
- **Schema changes** – If your read model depends on schemas, set the plugin to `event-and-schema` or `schema-only` so it always receives the latest contracts.

Need a refresher on CLI commands? Head back to the [CLI reference]({{ '/cli/' | relative_url }}). Ready to build? Start with the examples in `dbx_plugins` and customize from there.
