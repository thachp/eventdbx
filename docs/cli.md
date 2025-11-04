---
title: CLI Reference
description: Discover every EventDBX CLI command for servers, schemas, tokens, and aggregates.
nav_id: cli
---

# CLI Reference

The `dbx` binary manages servers, schemas, tokens, plugins, and backups. Every command accepts `--config <path>` when you need to point at a non-default configuration file. The CLI programs the *write* side of EventDBX and orchestrates the plugin pipeline that pushes jobs to read-side services.

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

- `dbx config [--port <u16>] [--data-dir <path>] [--cache-threshold <usize>] [--dek <base64>] [--list-page-size <usize>] [--page-limit <usize>] [--plugin-max-attempts <u32>] [--snapshot-threshold <u64>] [--clear-snapshot-threshold] [--admin-enabled <true|false>] [--admin-bind <addr>] [--admin-port <u16>]`

Run without flags to print the current configuration. The first invocation must supply `--dek` (32 bytes of base64). Admin endpoints reuse JWT bearer tokens; enable them with `--admin-enabled true` and call them with a token that carries wildcard privileges (for example, the bootstrap token at `~/.eventdbx/cli.token` or a dedicated token minted via `dbx token generate --group ops --user admin --action '*.*' --resource '*'`).

## Tokens

- `dbx token generate --group <name> --user <name> [--expiration <secs>] [--limit <writes>] [--keep-alive]`
- `dbx token list`
- `dbx token revoke --token <value>`
- `dbx token refresh --token <value> [--expiration <secs>] [--limit <writes>]`

## Schemas

- `dbx schema create <name> --events <event1,event2,...> [--snapshot-threshold <u64>]`
- `dbx schema add <name> --events <event1,event2,...>`
- `dbx schema remove <name> <event>`
- `dbx schema annotate <name> <event> [--note <text>] [--clear]`
- `dbx schema list`

## Aggregates

- `dbx aggregate create --aggregate <type> --aggregate-id <id> --event <name> [--field KEY=VALUE...] [--payload <json>] [--metadata <json>] [--note <text>] [--json]`
- `dbx aggregate apply --aggregate <type> --aggregate-id <id> --event <name> [--field KEY=VALUE...] [--payload <json>] [--stage]`
- `dbx aggregate patch --aggregate <type> --aggregate-id <id> --event <name> --patch <json> [--stage] [--metadata <json>] [--note <text>]`
- `dbx aggregate list [--skip <n>] [--take <n>] [--stage]`
- `dbx aggregate get --aggregate <type> --aggregate-id <id> [--include-events]`
- `dbx aggregate replay --aggregate <type> --aggregate-id <id> [--skip <n>] [--take <n>]`
- `dbx aggregate verify --aggregate <type> --aggregate-id <id>`
- `dbx aggregate snapshot --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `dbx aggregate archive|restore --aggregate <type> --aggregate-id <id>`
- `dbx aggregate remove --aggregate <type> --aggregate-id <id>`
- `dbx aggregate commit`
- `dbx aggregate export [<type>] [--all] --output <path> [--format csv|json] [--zip] [--pretty]`

Staging (`--stage`) records events to `~/.eventdbx/staged_events.json` until you call `dbx aggregate commit`.

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

EventDBX’s core store owns the *write* side of CQRS; plugins serve the *read* side by consuming jobs from the queue and projecting them into downstream systems (search, analytics, notification hubs, and more). Jobs are persisted in RocksDB so delivery survives restarts, and each plugin can request only the data it needs—event documents, materialised state, schemas, or combinations thereof.

See the [Plugin architecture]({{ '/plugins/' | relative_url }}) guide for deeper details, payload modes, and extension patterns.

## Maintenance

- `dbx backup --output <path> [--force]`
- `dbx restore --input <path> [--data-dir <path>] [--force]`
- `dbx upgrade [<version>|latest] [--no-switch] [--print-only]`
- `dbx upgrade use <version> [--print-only]`
- `dbx upgrade installed [--json]`
- `dbx upgrade --suppress <version>`
- `dbx upgrade list [--limit <n>] [--json]`

`dbx upgrade` now caches every downloaded release in `~/.eventdbx/versions/<target>/<tag>/`, allowing you to install multiple versions side-by-side and activate them later with `dbx upgrade use`. The `installed` subcommand lists cached versions, and `list` marks installed entries with `[installed]` and the active version with `[active]`. Use `--no-switch` to keep the current binary in place after downloading, and `--print-only` to preview the actions without making changes.

## Shortcuts for the Admin API

Once you enable the Admin API, authenticate with a root bearer token (for example, the bootstrap token at `~/.eventdbx/cli.token`) and script administrative changes with tools like `curl`:

```bash
curl -H "Authorization: Bearer $(cat ~/.eventdbx/cli.token)" \
  http://127.0.0.1:7171/admin/tokens
curl -X POST -H "Authorization: Bearer $(cat ~/.eventdbx/cli.token)" \
  -H "Content-Type: application/json" \
  -d '{"aggregate":"patient","events":["patient-added","patient-updated"]}' \
  http://127.0.0.1:7171/admin/schemas
```

Refer back to the [API reference]({{ '/apis/' | relative_url }}) for endpoint details and payload formats.
