---
title: CLI Reference
description: Discover every EventDBX CLI command for servers, schemas, tokens, and aggregates.
nav_id: cli
---

# CLI Reference

The `dbx` binary manages servers, schemas, tokens, plugins, replication, and backups. Every command accepts `--config <path>` when you need to point at a non-default configuration file.

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

- `dbx config [--port <u16>] [--data-dir <path>] [--cache-threshold <usize>] [--dek <base64>] [--list-page-size <usize>] [--page-limit <usize>] [--plugin-max-attempts <u32>] [--snapshot-threshold <u64>] [--clear-snapshot-threshold] [--admin-enabled <true|false>] [--admin-bind <addr>] [--admin-port <u16>] [--admin-master-key <secret>] [--clear-admin-master-key]`

Run without flags to print the current configuration. The first invocation must supply `--dek` (32 bytes of base64). Setting `--admin-master-key` writes an Argon2 hash of the secret and flips the Admin API online if `--admin-enabled true` is also present.

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

- `dbx aggregate apply --aggregate <type> --aggregate-id <id> --event <name> [--field KEY=VALUE...] [--payload <json>] [--patch <json>] [--stage]`
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
- `dbx plugin config <type> …` – Configure `tcp`, `http`, `grpc`, `log`, or `process` emitters.
- `dbx plugin enable <name>` / `dbx plugin disable <name>`
- `dbx plugin remove <name>`
- `dbx plugin list`
- `dbx plugin replay <plugin> <aggregate> [<aggregate_id>]`
- `dbx plugin test`
- `dbx queue`
- `dbx queue clear`
- `dbx queue retry [--event-id <uuid>]`

## Replication

- `dbx remote add <name> <ip> --public-key <base64> [--port <u16>] [--replace]`
- `dbx remote rm <name>`
- `dbx remote ls`
- `dbx remote show <name>`
- `dbx remote key [--show-path]`
- `dbx push <name> [--dry-run] [--schema] [--schema-only] [--batch-size <n>] [--aggregate <type>…] [--aggregate-id <type:id>…]`
- `dbx pull <name> [--dry-run] [--schema] [--schema-only] [--batch-size <n>] [--aggregate <type>…] [--aggregate-id <type:id>…]`

Remotes authenticate through pinned Ed25519 keys and reuse the CLI socket (`tcp://…:6363` by default).

## Maintenance

- `dbx backup --output <path> [--force]`
- `dbx restore --input <path> [--data-dir <path>] [--force]`
- `dbx upgrade [<version>|latest] [--print-only]`
- `dbx upgrade --suppress <version>`
- `dbx upgrade list [--limit <n>] [--json]`

## Shortcuts for the Admin API

Once you enable the Admin API with a master key, you can script administrative changes with tools like `curl`:

```bash
curl -H "X-Admin-Key: rotate-me-please" http://127.0.0.1:7171/admin/tokens
curl -X POST -H "X-Admin-Key: rotate-me-please" \
  -H "Content-Type: application/json" \
  -d '{"aggregate":"patient","events":["patient-added","patient-updated"]}' \
  http://127.0.0.1:7171/admin/schemas
```

Refer back to the [API reference]({{ '/apis/' | relative_url }}) for endpoint details and payload formats.
