---
title: CLI Reference
description: Discover every EventDBX CLI command for servers, schemas, tokens, and aggregates.
nav_id: cli
---

# CLI Reference

The `dbx` binary manages servers, schemas, tokens, plugins, and backups. Every command accepts `--config <path>` when you need to point at a non-default configuration file. The CLI programs the _write_ side of EventDBX and orchestrates the plugin pipeline that pushes jobs to read-side services.

## Global options

```bash
dbx --help
```

Key flags:

- `--config <path>` – Override the configuration file (`~/.eventdbx/config.toml` by default).
- `--data-dir <path>` – Override the data directory for commands that access RocksDB directly.
- `--socket <addr>` – Talk to a remote daemon through its Cap'n Proto control socket (default `0.0.0.0:6363`).
- `-v/--verbose` – Increase logging.

## Server lifecycle

- `dbx start [--foreground] [--port <u16>] [--data-dir <path>] [--restrict <off|default|strict>]` – Launch the server.
- `dbx stop` – Stop the running daemon.
- `dbx status` – Display PID, uptime, and restriction mode.
- `dbx restart [start options…]` – Stop then start in one command.
- `dbx destroy [--yes]` – Remove the configuration, data directory, and PID file.

## Configuration

- `dbx config [--port <u16>] [--data-dir <path>] [--cache-threshold <usize>] [--dek <base64>] [--list-page-size <usize>] [--page-limit <usize>] [--plugin-max-attempts <u32>] [--snapshot-threshold <u64>] [--clear-snapshot-threshold]`

Run without flags to print the current configuration. The first invocation must supply `--dek` (32 bytes of base64).

## Tokens

- `dbx token generate --group <name> --user <name> [--expiration <secs>] [--limit <writes>] [--keep-alive]`
- Add `--tenant <id>` (repeatable) to restrict the token to specific tenant ids; the server rejects requests for tenants not present in the token claims.
- `dbx token list`
- `dbx token revoke --token <value>`
- `dbx token refresh --token <value> [--expiration <secs>] [--limit <writes>]`

## Tenants

- `dbx tenant assign <tenant> --shard <shard-0001>` – Pin a tenant to an explicit shard label. Omit the command to continue using hash-based placement.
- `dbx tenant unassign <tenant>` – Remove the explicit assignment so the tenant hashes across the configured shard count.
- `dbx tenant list [--shard <shard-0001>] [--json]` – Display manual assignments plus any configured storage quotas and current usage (filters by shard and supports JSON output).
- `dbx tenant stats [--json]` – Summarise explicit assignments per shard.
- `dbx tenant quota set <tenant> --max-mb <n>` – Enforce a per-tenant storage ceiling in megabytes; the daemon rejects new writes once usage reaches the limit.
- `dbx tenant quota clear <tenant>` – Remove the storage quota so the tenant can grow again.
- `dbx tenant quota recalc <tenant>` – Recompute storage usage counters by walking the tenant’s event store (handy after manual maintenance or restores).

## Schemas

- `dbx schema create <name> --events <event1,event2,...> [--snapshot-threshold <u64>]`
- `dbx schema add <name> --events <event1,event2,...>`
- `dbx schema remove <name> <event>`
- `dbx schema annotate <name> <event> [--note <text>] [--clear]`
- `dbx schema list`
- `dbx schema field <aggregate> <field> [--type <type>] [--format <format>] [--required|--not-required] [...rules]`
- `dbx schema alter <aggregate> <event> [--add <field,...>] [--remove <field,...>] [--set <field,...>] [--clear]`

`schema field` lets you manage column types, formats, substring/regex checks, numeric ranges, nested object rules, or clear them entirely (`--clear-type`, `--clear-rules`) without editing `schemas.json` by hand.

`schema alter` updates the allowed field list per event—append or remove entries incrementally, replace the list via `--set`, or empty it with `--clear`.

## Aggregates

- `dbx aggregate create --aggregate <type> --aggregate-id <id> --event <name> [--field KEY=VALUE...] [--payload <json>] [--metadata <json>] [--note <text>] [--json]`
- `dbx aggregate apply --aggregate <type> --aggregate-id <id> --event <name> [--field KEY=VALUE...] [--payload <json>] [--stage]`
- `dbx aggregate patch --aggregate <type> --aggregate-id <id> --event <name> --patch <json> [--stage] [--metadata <json>] [--note <text>]`
- `dbx aggregate list [--cursor <token>] [--take <n>] [--stage]`
- `dbx aggregate get --aggregate <type> --aggregate-id <id> [--include-events]`
- `dbx aggregate replay --aggregate <type> --aggregate-id <id> [--skip <n>] [--take <n>]`
- `dbx aggregate verify --aggregate <type> --aggregate-id <id>`
- `dbx aggregate snapshot --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `dbx aggregate archive|restore --aggregate <type> --aggregate-id <id>`
- `dbx aggregate remove --aggregate <type> --aggregate-id <id>`
- `dbx aggregate commit`
- `dbx aggregate export [<type>] [--all] --output <path> [--format csv|json] [--zip] [--pretty]`

Staging (`--stage`) records events to `~/.eventdbx/staged_events.json` until you call `dbx aggregate commit`.

Cursor pagination uses opaque-but-readable tokens. Active aggregates encode as `a:<aggregate_type>:<aggregate_id>` (archived entries use `r:`) and events append the version (`a:<aggregate_type>:<aggregate_id>:<version>`). Capture the last item from a page and feed its token back via `--cursor` to resume listing.

## Plugins & queues

- `dbx plugin install <plugin> <version> --source <path|url> [--bin <file>] [--checksum <sha256>] [--force]`
- `dbx plugin config <type> …` – Configure `tcp`, `http`, `capnp`, `log`, or `process` emitters. Every `plugin config` command accepts `--payload <all|event-only|state-only|schema-only|event-and-schema>` so each plugin receives only the portions of the job it needs. For process plugins, add `--emit-events=<true|false>` when you need the worker running but do not want it to enqueue events.
- `dbx plugin enable <name>` / `dbx plugin disable <name>`
- `dbx plugin test [<name>…]`
- `dbx plugin start <name> [--foreground]`
- `dbx plugin stop <name>`
- `dbx plugin status [<name>]`
- `dbx plugin remove <name> [--disable]`
- `dbx plugin list`
- `dbx plugin replay <plugin> <aggregate> [<aggregate_id>]`
- `dbx queue`
- `dbx queue clear`
- `dbx queue retry [--event-id <job-id>]`

EventDBX’s core store owns the _write_ side of CQRS; plugins serve the _read_ side by consuming jobs from the queue and projecting them into downstream systems (search, analytics, notification hubs, and more). Jobs are persisted in RocksDB so delivery survives restarts, and each plugin can request only the data it needs—event documents, materialised state, schemas, or combinations thereof.

See the [Plugin architecture]({{ '/plugins/' | relative_url }}) guide for deeper details, payload modes, and extension patterns.

## Replication

EventDBX can replicate an entire domain (or selected aggregates) between two nodes. Remote settings are stored per domain in `remote.json` under the domain’s data directory, encrypted when a DEK is configured.

1. Point the active domain at a remote control socket and token:

   ```bash
   dbx checkout -d example --remote replica.external:6363 --token "<remote token>"
   ```

   Re-run the command with `--remote` or `--token` to rotate either value. Use `--port` when the socket listens on a non-default port, and add `--remote-tenant <id>` when the remote server hosts multiple tenants so `dbx push`/`dbx pull` know which tenant to target.

2. Push schemas before pushing data so the destination validates events the same way:

   ```bash
   dbx push schema example
   ```

3. Synchronise domain events, optionally limiting the scope:

   ```bash
   dbx push example --concurrency 8
   dbx push example --aggregate invoice
   dbx push example --aggregate ledger --id ledger-001
   ```

   The command aborts if the remote already has more events for any aggregate, preventing unintended history rewrites.

4. Pull remote changes back into the local domain once the remote has advanced:

   ```bash
   dbx pull example
   dbx pull example --aggregate ledger
   ```

   Every pull verifies aggregate version counts and Merkle roots before importing events. Add `--concurrency <threads>` to tune throughput on large domains.

5. Automate those pulses when needed:

   ```bash
   dbx watch example --mode bidirectional --aggregate ledger --run-once
   dbx watch example --mode push --interval 120 --background
   dbx watch example --skip-if-active
   dbx watch status example
   ```

   `watch` repeats the requested direction every `--interval` seconds (default 300). Add `--run-once` to exit after one cycle, `--background` to run in the background, `--skip-if-active` to avoid overlapping cycles when another watcher is still working on that domain, and `watch status <domain>` (or `--all`) to inspect persisted state whenever you need a health check.

## Maintenance

- `dbx backup --output <path> [--force]`
- `dbx restore --input <path> [--data-dir <path>] [--force]`
- `dbx upgrade [<version>|latest] [--no-switch] [--print-only]`
- `dbx upgrade use <version> [--print-only]`
- `dbx upgrade installed [--json]`
- `dbx upgrade --suppress <version>`
- `dbx upgrade list [--limit <n>] [--json]`

`dbx upgrade` now caches every downloaded release in `~/.eventdbx/versions/<target>/<tag>/`, allowing you to install multiple versions side-by-side and activate them later with `dbx upgrade use`. The `installed` subcommand lists cached versions, and `list` marks installed entries with `[installed]` and the active version with `[active]`. Use `--no-switch` to keep the current binary in place after downloading, and `--print-only` to preview the actions without making changes.
