# EventDBX

EventDBX is a single-tenant, single-authority write-side event database.

The supported core is:

- authenticated writes
- schema enforcement from `schema.dbx`
- immutable event history
- aggregate reads
- Merkle-style integrity verification
- automatic snapshots
- a pull-based outbox for downstream replication

Removed from the supported product surface:

- multi-tenant hosting and tenant management
- built-in peer replication (`checkout`, `merge`, `push`, `pull`, `watch`)
- plugin and queue orchestration
- manual snapshot management commands
- backup, restore, and upgrade commands

## Quick Start

1. Initialize a local workspace in your project directory:

```bash
dbx init
```

This creates `./.dbx` and stores both configuration and runtime data there. Runtime commands discover the nearest `.dbx/config.toml` by walking up from the current directory, similar to Git repository discovery.
`dbx init` also emits a CLI bootstrap token and persists it under `.dbx/cli.token`; the default init-issued token expires after 86,400 seconds.
Use `--ttl` to override that bootstrap token lifetime with either raw seconds or human suffixes like `10m`, `24h`, `10d`, or `2w`.

2. Start the daemon:

```bash
dbx start --foreground
```

3. Author a schema in `schema.dbx`:

```text
aggregate order {
  snapshot_threshold = 100

  field status {
    type = "text"
  }

  event order_created {
    fields = ["status"]
  }

  event order_paid {
    fields = ["status"]
  }
}
```

4. Validate and preview the compiled runtime schema:

```bash
dbx schema validate
dbx schema show
```

`dbx schema show` is a deterministic preview of the compiled runtime shape. It intentionally normalizes volatile timestamps so unchanged source renders the same output across runs.

5. Apply the schema:

```bash
dbx schema apply
```

6. Bootstrap or mint a token:

```bash
dbx token bootstrap --stdout
dbx token generate --group ops --user alice --action aggregate.read --action aggregate.append --json
```

7. Write and read aggregates:

```bash
dbx aggregate create order order-42 --event order_created --field status=open --json
dbx aggregate apply order order-42 order_paid --field status=paid
dbx aggregate get order order-42 --include-events
dbx aggregate list order --json
dbx events order order-42 --json
dbx aggregate verify order order-42 --json
```

Automatic snapshots are still part of the core write path. Set `snapshot_threshold` in `schema.dbx`; EventDBX will create snapshots internally after qualifying writes. The standalone snapshot command surface has been removed.

## Outbox

EventDBX exposes a pull-based control-socket outbox for downstream replication.

Contract:

- `readOutbox(afterEventId?, take?) -> { eventsJson, nextAfterEventId }`
- events are committed, active-only, and globally ordered by ascending `event_id`
- `afterEventId` is exclusive
- `nextAfterEventId` is the checkpoint a replicator should persist

Recommended replication shape:

1. bootstrap a remote once with replay or export/import
2. pull local outbox batches from the authoritative EventDBX instance
3. push those events to the downstream system
4. advance the checkpoint only after successful delivery

This replaces built-in peer replication. The core daemon does not manage destination config, retries, scheduling, or bidirectional reconciliation.

## CLI Surface

Supported commands:

- `dbx init`
- `dbx start`
- `dbx stop`
- `dbx status`
- `dbx restart`
- `dbx destroy`
- `dbx config`
- `dbx token generate|list|revoke|refresh|bootstrap`
- `dbx schema validate|show|apply`
- `dbx aggregate create|apply|list|get|verify`
- `dbx events`

## Configuration

`dbx config` manages the lean runtime surface:

- data path
- HTTP/control bind settings
- paging limits
- encryption-at-rest key
- auth keys
- Snowflake worker id
- control-channel Noise toggle

The default config location is the nearest `.dbx/config.toml` in the current directory tree.

EventDBX now runs as a single-tenant core pinned to the logical domain `default`.

## Migration Note

This build refuses to start against legacy multi-domain or multi-tenant layouts.

Examples of rejected state:

- `config.toml` selecting a non-default domain
- `[tenants].multi_tenant = true`
- extra tenant/domain directories under the data root

The error is intentional. Export/import the data or run a dedicated migration before upgrading to the lean single-tenant build.
