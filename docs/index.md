---
title: EventDBX
description: Event-sourced write-side storage with pluggable read models.
nav_id: home
---

# EventDBX

EventDBX keeps every change as an immutable event so you can replay history, audit decisions, and stream updates to downstream systems without duct tape. The core database handles the **write** side of CQRS; the plugin job queue fans events out to read-side services—search, analytics, notifications, custom projections—so each system can specialise without touching the write path.

## Key capabilities

- **Immutable timelines** keep the full history for every aggregate and guard it with Merkle proofs.
- **Schema-aware validation** enforces contracts per aggregate while still letting you iterate with permissive modes.
- **Plugin-powered read models** persist jobs in RocksDB, apply backoff, and deliver only the slices each plugin needs (event/state/schema) so external systems can build tailored projections.
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
   dbx start --foreground
   ```

   The daemon owns the write-side RocksDB store and exposes a Cap'n Proto control socket on `6363`. Read surfaces (REST, GraphQL, gRPC, search, etc.) run as plugins in the [dbx_plugins workspace](https://github.com/thachp/dbx_plugins); deploy the ones you need alongside the server.

3. **Append your first event**

   ```bash
   dbx aggregate create patient p-001 \
     --event patient_created \
     --field name="Jane Doe" \
     --field status=active
   ```

   List aggregates with `dbx aggregate list`. If you have a read-side plugin enabled (for example the REST API plugin), query state there as well.

## What to read next

- [Getting started]({{ '/getting-started/' | relative_url }}) walks through schema setup, tokens, and common workflows.
- [CLI reference]({{ '/cli/' | relative_url }}) details every subcommand with practical examples.
- [Plugin architecture]({{ '/plugins/' | relative_url }}) explains how the job queue works and how to build new read-side connectors.
- [API reference]({{ '/apis/' | relative_url }}) covers REST, GraphQL, and gRPC routes exposed by companion plugins.
