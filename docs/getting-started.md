---
title: Getting Started
description: Set up EventDBX locally, define schemas, and append your first event.
nav_id: guides
---

# Getting Started

This guide spins up EventDBX on your workstation, covers the minimal configuration (encryption plus the Admin API), and walks through staging and committing events.

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

### Optional: enable the Admin API

Enable the `/admin` surface and seed a shared secret for automation:

```bash
dbx config \
  --admin-enabled true \
  --admin-bind 127.0.0.1 \
  --admin-port 7171 \
  --admin-master-key "rotate-me-please"
```

All Admin API calls must send `X-Admin-Key: rotate-me-please` (or the same value as a bearer token). Rotate the secret with the same flag or revoke it using `--clear-admin-master-key`.

## 3. Start the server

```bash
dbx start --foreground --api rest --api graphql --api grpc
```

By default EventDBX stores data under `~/.eventdbx/data`. Override it with `dbx start --data-dir <path>`. The process writes a PID file so `dbx stop` can terminate it cleanly later.

## 4. Define a schema

Schemas keep payloads in check once you graduate from unstructured prototyping.

```bash
dbx schema create patient \
  --events patient-added,patient-updated \
  --snapshot-threshold 100
```

Add additional events later with `dbx schema add patient --events patient-archived`.

## 5. Issue a token

Automation uses bearer tokens for REST/GraphQL/gRPC calls.

```bash
dbx token generate \
  --group admin \
  --user jane \
  --expiration 3600
```

The command prints a signed JWT. Export it to `EVENTDBX_TOKEN` to save retyping:

```bash
export EVENTDBX_TOKEN=<copied token>
```

## 6. Stage and commit events

Append an event immediately:

```bash
dbx aggregate apply patient p-001 patient-added \
  --field name="Jane Doe" \
  --field status=active
```

Or stage multiple events and commit them together:

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

Or hit the REST API:

```bash
curl \
  -H "Authorization: Bearer ${EVENTDBX_TOKEN}" \
  http://localhost:7070/v1/aggregates/patient/p-001
```

## Next steps

- Explore the full [CLI reference]({{ '/cli/' | relative_url }}) for backups, replication, plugins, and admin tooling.
- Review the [API reference]({{ '/apis/' | relative_url }}) for REST, GraphQL, gRPC, and the new Admin API endpoints.
