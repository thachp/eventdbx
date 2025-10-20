# EventDBX

You'll likely enjoy this database system. Worry less about how you structure your data and focus more on your business logic.

## Overview

EventDBX is an event-sourced, key-value, write-side database system designed to provide immutable, append-only storage for events across various domains. It is ideal for applications requiring detailed audit trails for compliance, complex business processes involving states, and high data integrity levels.

## Getting Started

Follow the steps below to spin up EventDBX locally. The commands assume you installed the CLI globally; if you're contributing to the project, clone the repository and run them from the repo root instead.

1. **Install the CLI**

   ```bash
   # Install prebuilt binaries via shell script
   curl --proto '=https' --tlsv1.2 -LsSf https://github.com/thachp/eventdbx/releases/download/v1.9.15/eventdbx-installer.sh | sh
   ```

   ```bash
   # Install prebuilt binaries via powershell script
   powershell -ExecutionPolicy Bypass -c "irm https://github.com/thachp/eventdbx/releases/download/v1.9.15/eventdbx-installer.ps1 | iex"
   ```

2. **Start the server**

   ```bash
   eventdbx start --foreground
   ```

   - Omit `--foreground` to daemonise the process.
   - Use `--data-dir <path>` to override the default `$HOME/.eventdbx` directory.
   - Restriction (schema enforcement) is enabled by default; disable it with `--restrict=false` if you need a permissive environment.

- Choose the API surface with `--api rest`, `--api graphql`, `--api grpc`, or `--api all` (enable every surface). `--api grpc`/`--api all` automatically flip the gRPC listener on for the current session; persistently enable it by setting `grpc.enabled = true` in `config.toml`.

3. **Define a schema (recommended when running in restricted mode)**

   ```bash
   eventdbx schema create \
     --aggregate patient \
     --events patient-added,patient-updated \
     --snapshot-threshold 100
   ```

   Omit `--snapshot-threshold` to inherit the default configured in `config.toml` (if any).

4. **Issue a token for CLI access**

   ```bash
   eventdbx token generate --group admin --user jane --expiration 3600
   ```

5. **Append an event**
   ```bash
   eventdbx aggregate apply person p-002 patient-added \
   --field name="Jane Doe" \
   --field status=active
   ```

You now have a working EventDBX instance with an initial aggregate. Explore the [Command-Line Reference](#command-line-reference) for the full set of supported operations.

## Features

- **Flexible JSON payloads**: Events accept arbitrary JSON payloads; scalar values are normalized into strings for state tracking, while structured objects remain fully queryable.
- **Immutable Data Structure**: Once data is entered into EventDBX, it becomes immutable, meaning it cannot be altered or deleted. This characteristic is crucial for applications where the accuracy and traceability of historical data are paramount, such as medical records, financial transactions, and supply chain management. Data can be archived, moving from short-term to long-term storage, but cannot be deleted.
- **Event Sourcing and Replay**: EventDBX is built on the principle of event sourcing, storing all changes to the data as a sequence of events. This allows for the complete replay of events to reconstruct the database's state at any point in time, thereby enhancing data recovery and audit capabilities. Unlike traditional databases that execute update statements to modify data, this system is event-driven. Aggregate state changes are defined in the event object, allowing these events to be replayed at any time to reconstruct the aggregate's current state.
- **Merkle Tree Integration**: Each aggregate in EventDBX is associated with a Merkle tree of events, enabling verification of data integrity. The Merkle tree structure ensures that any data tampering can be detected, offering an additional security layer against data corruption.
- **Built-in Audit Trails**: EventDBX automatically maintains a comprehensive audit trail of all transactions, a feature invaluable for meeting compliance and regulatory requirements. It provides transparent and tamper-evident records. During audits, administrators can issue specific tokens to auditors to access and review specific aggregate instances and all relevant events associated with those instances.
- **Security with Token-Based Authorization**: EventDBX implements token-based authorization to manage database access. This approach allows for precise control over who can access and modify data, protecting against unauthorized changes.
- **Encrypted Payloads & Secrets at Rest**: Event payloads, aggregate snapshots, and `tokens.json` are encrypted transparently when a DEK is configured. Metadata such as aggregate identifiers, versions, and Merkle roots remain readable so plugins, replication, and integrity checks keep working without additional configuration.
- **Powered by RocksDB and Rust**: At its core, EventDBX utilizes RocksDB for storage, taking advantage of its high performance and efficiency. The system is developed in Rust, known for its safety, efficiency, and concurrency capabilities, ensuring that it is both rapid and dependable.

## Restriction Modes

EventDBX can run in two validation modes, tuned for different phases of development:

- **Unrestricted mode** (`--restrict=false`): Ideal for prototyping and rapid application development. Event payloads are accepted as-is with no schema definition or data-type friction, letting you iterate quickly without pre-registering aggregates, tables, or column types.
- **Restricted mode** (default): Enables schema enforcement. Once your application matures, define required fields, data types, and validation rules via `eventdbx schema …` commands. Incoming events must match the declared schema, and violations are rejected before they reach storage. You can toggle restriction per server start, so development and production can adopt different policies.

## Command-Line Reference

EventDBX ships a single `eventdbx` binary. Every command accepts an optional `--config <path>` to point at an alternate configuration file.

### Server lifecycle

- `eventdbx start [--port <u16>] [--data-dir <path>] [--foreground] [--restrict | --restrict=false]`  
  Launches the server. Schema validation is enforced by default; pass `--restrict=false` to run in permissive mode.
- `eventdbx stop`  
  Stops the running daemon referenced by the PID file.
- `eventdbx status`  
  Prints the current port, PID, uptime, and whether restriction is enabled.
- `eventdbx restart [start options…]`  
  Stops the existing daemon (if any) and restarts it with the provided options.
- `eventdbx destroy [--yes]`  
  Removes the PID file, data directory, and configuration file after confirmation (or immediately with `--yes`).

### Configuration

- `eventdbx config [--port <u16>] [--data-dir <path>] [--master-key <secret>] [--dek <secret>] [--memory-threshold <usize>] [--list-page-size <usize>] [--page-limit <usize>] [--plugin-max-attempts <u32>]`  
  Persists configuration updates. The first invocation must include both `--master-key` and `--dek`. `--list-page-size` sets the default page size for aggregate listings (default 10), `--page-limit` caps any requested page size across list and event endpoints (default 1000), and `--plugin-max-attempts` controls how many retries are attempted before an event is marked dead (default 10).

### Tokens

- `eventdbx token generate --group <name> --user <name> [--expiration <secs>] [--limit <writes>] [--keep-alive]`  
  Issues a new token tied to a Unix-style group and user.
- `eventdbx token list`  
  Lists all tokens with status, expiry, and remaining writes.
- `eventdbx token revoke --token <value>`  
  Revokes a token immediately.
- `eventdbx token refresh --token <value> [--expiration <secs>] [--limit <writes>]`  
  Extends the lifetime or write allowance of an existing token.

### Schemas

- `eventdbx schema create --aggregate <name> --events <event1,event2,...> [--snapshot-threshold <u64>]`
- `eventdbx schema add --aggregate <name> --events <event1,event2,...>`
- `eventdbx schema remove --aggregate <name> --event <name>`
- `eventdbx schema list`

Schemas are stored on disk; when the server runs with restriction enabled, incoming events must satisfy the recorded schema.

### Aggregates

- `eventdbx aggregate apply --aggregate <type> --aggregate-id <id> --event <name> --field KEY=VALUE... [--stage]`  
  Appends an event immediately—use `--stage` to queue it for a later commit.
- `eventdbx aggregate list [--skip <n>] [--take <n>] [--stage]`  
  Lists aggregates with version, Merkle root, and archive status; pass `--stage` to display queued events instead.
- `eventdbx aggregate get --aggregate <type> --aggregate-id <id> [--version <u64>] [--include-events]`
- `eventdbx aggregate replay --aggregate <type> --aggregate-id <id> [--skip <n>] [--take <n>]`
- `eventdbx aggregate verify --aggregate <type> --aggregate-id <id>`
- `eventdbx aggregate snapshot --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `eventdbx aggregate archive --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `eventdbx aggregate restore --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `eventdbx aggregate remove --aggregate <type> --aggregate-id <id>` Removes an aggregate that has no events (version still 0).
- `eventdbx aggregate commit`  
  Flushes all staged events in a single atomic transaction.
- `eventdbx aggregate export [<type>] [--all] --output <path> [--format csv|json] [--zip] [--pretty]`  
  Writes the current aggregate state (no metadata) as CSV or JSON. Exports default to one file per aggregate type; pass `--zip` to bundle the output into an archive.

Staged events are stored in `.eventdbx/staged_events.json`. Use `aggregate apply --stage` to add entries to this queue, inspect them with `aggregate list --stage`, and persist the entire batch with `aggregate commit`. Events are validated against the active schema (when restriction is enabled) during both staging and commit. The commit operation writes every pending event in one RocksDB batch, guaranteeing all-or-nothing persistence.

### Plugins

- `eventdbx plugin config tcp --name <label> --host <hostname> --port <u16> [--disable]`
- `eventdbx plugin config http --name <label> --endpoint <host|url> [--https] [--header KEY=VALUE]... [--disable]`
- `eventdbx plugin config grpc --name <label> --endpoint <host|url> [--disable]`
- `eventdbx plugin config log --name <label> --level <trace|debug|info|warn|error> [--template "text with {aggregate} {event} {id}"] [--disable]`
- `eventdbx plugin enable <label>`
- `eventdbx plugin disable <label>`
- `eventdbx plugin remove <label>`
- `eventdbx plugin test`
- `eventdbx plugin list`
- `eventdbx plugin queue`
- `eventdbx plugin queue clear`
- `eventdbx plugin queue retry [--event-id <uuid>]`
- `eventdbx plugin replay <plugin-name> <aggregate> [<aggregate_id>]`

Clearing dead entries prompts for confirmation to avoid accidental removal. Manual retries run the failed events immediately; use `--event-id` to target a specific entry.

### Replication

- `eventdbx remote add <name> <endpoint> --public-key <base64>`  
  Registers a standby and pins its Ed25519 public key.
- `eventdbx remote rm <name>`  
  Removes a configured remote.
- `eventdbx remote ls`  
  Lists remotes with their endpoints.
- `eventdbx remote show <name>`  
  Displays the endpoint and pinned key for a remote.
- `eventdbx remote key [--show-path]`  
  Prints this node's replication public key (generated on first run).
- `eventdbx remote push <name> [--dry-run] [--schema] [--schema-only] [--batch-size <n>] [--aggregate <type>...] [--aggregate-id <type:id>...]`  
  Streams local events to the remote in fast-forward mode; dry runs report pending changes.
- `eventdbx remote pull <name> [--dry-run] [--schema] [--schema-only] [--batch-size <n>] [--aggregate <type>...] [--aggregate-id <type:id>...]`  
  Fast-forwards the local node from the remote, reporting changes in dry-run mode.

Replication keys live alongside the data directory (`replication.key` / `replication.pub`) and are created automatically the first time the CLI loads configuration. The standby gRPC listener defaults to `127.0.0.1:7443`; override it in `config.toml` via `replication.bind_addr` when you expose the replica on another interface. When the HTTP server processes writes it streams committed events to every configured remote over gRPC using the pinned public keys. Use `--aggregate` repeatedly to scope push/pull to specific aggregate types when you only need to sync a subset of data, `--aggregate-id TYPE:ID` to target individual aggregates, `--schema` to copy schema definitions alongside events, and `--schema-only` to synchronize schemas without touching event data.
Use `--aggregate` repeatedly to scope push/pull to specific aggregate types when you only need to sync a subset of data.

### Maintenance

- `eventdbx backup --output <path> [--force]`  
  Creates a compressed archive with the entire EventDBX data directory and configuration. Stop the server before running a backup to avoid partial snapshots.
- `eventdbx restore --input <path> [--data-dir <path>] [--force]`  
  Restores data from a backup archive. Use `--data-dir` to override the stored location, and `--force` to overwrite non-empty destinations. The server must be stopped before restoring.

Plugins fire after every committed event to keep external systems in sync. Remaining plugins deliver events through different channels:

Failed deliveries are automatically queued and retried with exponential backoff. The server keeps attempting until the plugin succeeds or the aggregate is removed, ensuring transient outages do not drop notifications.
Use `eventdbx plugin queue` to inspect pending/dead event IDs.

Plugin configurations are stored in `.eventdbx/plugins.json`. Each plugin instance requires a unique `--name` so you can update, enable, disable, remove, or replay it later. `plugin enable` validates connectivity (creating directories, touching files, or checking network access) before marking the plugin active. Remove a plugin only after disabling it with `plugin disable <name>`. `plugin replay` resends stored events for a single aggregate instance—or every instance of a type—through the selected plugin.

- **TCP**: Writes a single-line JSON `EventRecord` to the configured socket.
- **HTTP**: POSTs the `EventRecord` JSON to an endpoint with optional headers; add `--https` during configuration to force HTTPS when the endpoint lacks a scheme.
- **gRPC**: Sends `EventRecord` batches to a remote gRPC endpoint compatible with the replication `ApplyEvents` API.
- **Log**: Emits a formatted line via `tracing` at the configured level. By default: `aggregate=<type> id=<id> event=<event>`.

Need point-in-time snapshots instead of streaming plugins? Use `eventdbx aggregate export` to capture aggregate state as CSV or JSON on demand.

Example gRPC configuration:

```bash
# point at an existing replication-compatible listener
eventdbx plugin config grpc \
  --name audit-grpc \
  --endpoint grpc://replica.internal:8800

# enable the plugin once connectivity is confirmed
eventdbx plugin enable audit-grpc

# inspect status
eventdbx plugin list
```

Example HTTP/TCP payload (`EventRecord`):

```json
{
  "aggregate_type": "patient",
  "aggregate_id": "p-001",
  "event_type": "patient-updated",
  "payload": {
    "status": "inactive",
    "comment": "Archived via API"
  },
  "metadata": {
    "event_id": "45c3013e-9b95-4ed0-9af9-1a465f81d3cf",
    "created_at": "2024-12-01T17:22:43.512345Z",
    "issued_by": {
      "group": "admin",
      "user": "jane"
    }
  },
  "version": 5,
  "hash": "cafe…",
  "merkle_root": "deadbeef…"
}
```

## REST API

The server exposes a small HTTP API (served on port `7070` by default). All endpoints require a bearer token unless otherwise noted.

| Method & Path                                               | Description                                                       |
| ----------------------------------------------------------- | ----------------------------------------------------------------- |
| `GET /health`                                               | Liveness probe (unauthenticated).                                 |
| `GET /v1/aggregates`                                        | Lists aggregates; supports `skip`/`take` query parameters.        |
| `GET /v1/aggregates/{aggregate_type}/{aggregate_id}`        | Returns the current state for a specific aggregate.               |
| `GET /v1/aggregates/{aggregate_type}/{aggregate_id}/events` | Lists events for an aggregate; supports `skip`/`take` pagination. |
| `POST /v1/events`                                           | Appends an event; aggregate identifiers are provided in the body. |
| `GET /v1/aggregates/{aggregate_type}/{aggregate_id}/verify` | Computes and returns the Merkle root for integrity verification.  |

Paginated responses cap `take` at the configurable `page_limit` (default `1000`). Adjust it with `eventdbx config --page-limit <n>` if you need larger pages.

All authenticated requests must include `Authorization: Bearer <token>` with a token issued via the CLI.

### cURL examples

```bash
# Post a JSON event (global endpoint)
curl \
  -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "aggregate_type": "patient",
        "aggregate_id": "p-001",
        "event_type": "patient-updated",
        "payload": {
          "status": "inactive",
          "meta": {"source": "api"}
        }
      }' \
  http://localhost:7070/v1/events

# Retrieve the first 10 events for an aggregate (supports `skip`/`take` up to `page_limit`)
curl \
  -H "Authorization: Bearer TOKEN" \
  "http://localhost:7070/v1/aggregates/patient/p-001/events?skip=0&take=10"


# Health check
curl http://localhost:7070/health

# List aggregates (replace TOKEN with an active value)
curl \
  -H "Authorization: Bearer TOKEN" \
  http://localhost:7070/v1/aggregates

# Append an event (global endpoint)
curl \
  -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "aggregate_type": "patient",
        "aggregate_id": "p-001",
        "event_type": "patient-updated",
        "payload": {
          "status": "inactive",
          "comment": "Archived via API"
        }
      }' \
  http://localhost:7070/v1/events

```

## gRPC API

Set `grpc.enabled = true` in `config.toml` to expose a gRPC surface (disabled by default). The listener binds to `grpc.bind_addr` (default `127.0.0.1:7442`). The service mirrors the REST operations (`AppendEvent`, `ListAggregates`, `GetAggregate`, `ListEvents`, and `VerifyAggregate`) plus a simple `Health` probe.

Example `grpcurl` invocation:

```bash
grpcurl -H "authorization: Bearer TOKEN" \
  -d '{
        "aggregate_type": "patient",
        "aggregate_id": "p-001",
        "event_type": "patient-updated",
        "payload_json": "{\"status\":\"inactive\"}"
      }' \
  -plaintext 127.0.0.1:7442 eventdbx.api.EventService/AppendEvent
```

## GraphQL API

GraphQL is served on `/graphql`, and an interactive Playground is available via `GET /graphql` or `/graphql/playground`. Supply the same bearer token header used for REST requests.

### Query example

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "query RecentAggregates($take: Int!) { aggregates(take: $take) { aggregate_type aggregate_id version merkle_root state } }",
        "variables": { "take": 5 }
      }' \
  http://localhost:7070/graphql
```

Sample response:

```json
{
  "data": {
    "aggregates": [
      {
        "aggregate_type": "patient",
        "aggregate_id": "p-001",
        "version": 3,
        "merkle_root": "8f4c3d…",
        "state": {
          "name": "Jane Doe",
          "status": "active"
        }
      }
    ]
  }
}
```

### Mutation example

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "query": "mutation Append($input: AppendEventInput!) { appendEvent(input: $input) { aggregate_type aggregate_id version payload } }",
        "variables": {
          "input": {
            "aggregate_type": "patient",
            "aggregate_id": "p-001",
            "event_type": "patient-updated",
            "payload": { "status": "inactive", "comment": "Archived via GraphQL" }
          }
        }
      }' \
  http://localhost:7070/graphql
```

The mutation triggers the same validation, replication, and plugin notifications as the REST endpoint.

## Contributing

### **Quick Start**

1. **Report Issues**: Found a bug or have a suggestion? [Open an issue](https://chat.openai.com/g/g-kvXdAN8VA-eventdbx-guide/c/72d2fa39-c3e2-4e40-bd61-5694f7b82aab#) with detailed information.
2. **Contribute Code**:
   - **Fork & Clone**: Fork the EventDBX repo and clone your fork.
   - **Branch**: Create a branch for your changes from **`develop`**.
   - **Develop**: Make your changes, adhering to our coding standards. Add or update tests as necessary.
   - **Commit**: Use [Conventional Commits](https://www.conventionalcommits.org/) for clear, structured commit messages (e.g., **`feat: add new feature`** or **`fix: correct a bug`**).
   - **Pull Request**: Submit a pull request (PR) against the **`develop`** branch of the original repo. Describe your changes and link to any related issues.

### **Guidelines**

- **Formatting**: A project-wide Prettier configuration lives at `.prettierrc.json`; use it for Markdown/JSON/YAML changes.
- **Commit linting**: Conventional Commit headers are enforced through `.commitlintrc.json`.
- **Code Review**: Your PR will be reviewed by the team. Be open to feedback and make requested adjustments.
- **Merge**: Once approved, your PR will be merged into the project, and you'll be credited as a contributor.

## License

EventDBX is licensed under the [MIT](https://github.com/thachp/eventdbx/blob/HEAD/apps/system/) License.
