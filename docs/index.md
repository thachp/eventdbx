---
title: EventDBX
description: Event-sourced data with audit guarantees baked in.
nav_id: home
---

# EventDBX

EventDBX keeps every change as an immutable event so you can replay history, audit decisions, and synchronize downstream systems without duct tape. The new Admin API lets automation manage environments securely—no more handing out shell access.

## Key capabilities

- **Immutable timelines** keep the full history for every aggregate and guard it with Merkle proofs.
- **Schema-aware validation** enforces contracts per aggregate while still letting you iterate with permissive modes.
- **Secure automation** exposes an Admin API backed by an Argon2-hashed master key so you can rotate tokens, manage schemas, and update remotes over HTTP.
- **Built-in replication & plugins** stream events to standbys, services, and custom processes without extra brokers.
- **Encrypted storage** protects payloads, snapshots, and tokens once you configure a data encryption key.

## Quick start

1. **Install the CLI**

   ```bash
   curl --proto '=https' --tlsv1.2 -LsSf \
     https://github.com/thachp/eventdbx/releases/download/v1.13.2/eventdbx-installer.sh | sh
   ```

   Windows users can substitute the PowerShell installer from the same release.

2. **Start the server**

   ```bash
   dbx start --foreground --api all
   ```

   The REST and GraphQL endpoints listen on `7070` by default; the CLI socket and replication stream use `6363`.

3. **Configure the Admin API (optional but recommended)**

   ```bash
   dbx config \
     --admin-enabled true \
     --admin-bind 127.0.0.1 \
     --admin-port 7171 \
     --admin-master-key "rotate-me-please"
   ```

   Requests to `/admin/...` must include `X-Admin-Key: rotate-me-please` (or the same value as a bearer token). Rotate the secret with the same flag or revoke access via `--clear-admin-master-key`.

4. **Append your first event**

   ```bash
   dbx aggregate apply patient p-001 patient-added \
     --field name="Jane Doe" \
     --field status=active
   ```

   List aggregates with `dbx aggregate list` or query state through the REST endpoint `GET /v1/aggregates/patient/p-001`.

## What to read next

- [Getting started]({{ '/getting-started/' | relative_url }}) walks through schema setup, tokens, and common workflows.
- [CLI reference]({{ '/cli/' | relative_url }}) details every subcommand with practical examples.
- [API reference]({{ '/apis/' | relative_url }}) covers REST, GraphQL, gRPC, replication, and the Admin API routes.
