---
title: Getting Started
description: Set up EventDBX locally, define schemas, and append your first event.
nav_id: guides
---

# Getting Started

This guide spins up EventDBX on your workstation, covers the minimal configuration (encryption and domains), and walks through staging and committing events.

## 1. Install the CLI

Use the shell installer on macOS/Linux:

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/thachp/eventdbx/releases/download/v1.13.2/eventdbx-installer.sh | sh
```

On Windows, use the matching PowerShell script from the release assets.

After installation run `dbx --version` to confirm the binary is available.

## 2. Initialize configuration

The first configuration write must include a data encryption key—32 bytes of base64. Generate one and persist the defaults:

```bash
openssl rand -base64 32 > dek.txt
dbx config --dek "$(cat dek.txt)"
```

> The configuration lives under `~/.eventdbx/config.toml` by default. Pass `--config <path>` to every command if you store it elsewhere.

Each writer process needs a distinct Snowflake worker id so generated `event_id`s stay unique across nodes. Set it once per node:

```bash
dbx config --snowflake-worker-id 7
```

Valid values range from 0–1023.

## 3. Start the server

```bash
dbx start --foreground
```

By default EventDBX stores data under `~/.eventdbx/data`. Override it with `dbx start --data-dir <path>`. The process writes a PID file so `dbx stop` can terminate it cleanly later. Public REST/GraphQL/gRPC endpoints now live in the [dbx_plugins](https://github.com/thachp/dbx_plugins) workspace—launch the `dbx_rest`, `dbx_graphql`, or `dbx_grpc` binaries beside the daemon when you need them.

## 4. Define a schema

Schemas keep payloads in check once you graduate from unstructured prototyping.

```bash
dbx schema create patient \
  --events patient-added,patient-updated \
  --snapshot-threshold 100
```

Add additional events later with `dbx schema add patient --events patient-archived`.

Schemas live alongside your data directory (default `~/.eventdbx/data/schemas.json`). Run `dbx schema <aggregate>` to print the JSON definition EventDBX enforces. You can extend it with column types, validation rules, and per-event field lists. For example:

```bash
dbx schema create person --events person_created,person_updated
dbx schema person
```

```json
{
  "person": {
    "aggregate": "person",
    "snapshot_threshold": null,
    "locked": false,
    "field_locks": [],
    "hidden": false,
    "hidden_fields": [],
    "column_types": {
      "email": { "type": "text", "required": true, "format": "email" },
      "full_name": {
        "type": "text",
        "rules": { "length": { "min": "1", "max": "64" } }
      }
    },
    "events": {
      "person_created": {
        "fields": ["first_name", "last_name"]
      },
      "person_updated": {
        "fields": []
      }
    },
    "created_at": "2025-10-26T22:25:24.028129Z",
    "updated_at": "2025-10-26T22:27:25.967615Z"
  }
}
```

- Use `column_types` to declare field data types, formats, and validation rules (length, range, regex, cross-field matches).
- Populate `events.<event>.fields` with the fields an event may touch; an empty list keeps the event permissive.
- Toggle `locked`, `field_locks`, or `hidden_fields` to freeze fields or suppress them from aggregate detail responses.

## 5. Issue a token

Automation uses bearer tokens for control-socket calls and any optional plugin surfaces.

```bash
dbx token generate \
  --group admin \
  --user jane \
  --expiration 3600
```

- Include `--tenant <id>` (repeat the flag for multiple tenants) to bind the token to specific tenant ids so the server can enforce per-tenant authorization.

The command prints a signed JWT. Export it to `EVENTDBX_TOKEN` to save retyping:

```bash
export EVENTDBX_TOKEN=<copied token>
```

## 6. Stage and commit events

Initialize the aggregate with its first event:

```bash
dbx aggregate create patient p-001 \
  --event patient_created \
  --field name="Jane Doe" \
  --field status=active
```

Append or stage additional events and commit them together:

```bash
dbx aggregate apply patient p-001 patient-updated \
  --field status=inactive \
  --stage

dbx aggregate list --stage
dbx aggregate commit
```

## 7. Query the data

Use the CLI for quick reads:

```bash
dbx aggregate get patient p-001 --include-events
```

Plugins expose REST/GraphQL/gRPC endpoints for external readers. Check the plugin README for usage examples.

## Next steps

- Explore the full [CLI reference]({{ '/cli/' | relative_url }}) for backups, plugins, and other operational tooling.
- Review the [API reference]({{ '/apis/' | relative_url }}) for REST, GraphQL, and gRPC endpoints provided by the companion plugins.
