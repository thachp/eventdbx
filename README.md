# EventDBX

You’ll appreciate this database system. It lets you spend less time designing schemas and more time writing code that drives your application.

## Overview

EventDBX is an event-sourced, NoSQL write-side database designed to provide immutable, append-only storage for events across various domains. It is ideal for applications requiring detailed audit trails for compliance, complex business processes involving states, and high data integrity levels. The core engine focuses on the _write_ side of CQRS—capturing and validating events, persisting aggregate state, and ensuring integrity with Merkle trees.

The companion plugin framework turns those events into durable jobs and delivers them to other services, letting external systems specialise in the _read_ side. Whether you need to hydrate a search index, stream to analytics, feed caches, or trigger workflows, plugins let you extend EventDBX without altering the write path. Each plugin chooses the payload shape it needs (event-only, state-only, schema-only, or combinations) while the queue guarantees delivery and backoff.

## Getting Started

Follow the steps below to spin up EventDBX locally. The commands assume you installed the CLI globally; if you're contributing to the project, clone the repository and run them from the repo root instead.

The CLI installs as `dbx`. Older releases exposed an `eventdbx` alias, but the primary command is now `dbx`.

1. **Install the CLI**

   ```bash
   # Install prebuilt binaries via shell script
   curl --proto '=https' --tlsv1.2 -LsSf https://github.com/thachp/eventdbx/releases/download/v1.13.2/eventdbx-installer.sh | sh
   ```

   ```bash
   # Install prebuilt binaries via powershell script
   powershell -ExecutionPolicy Bypass -c "irm https://github.com/thachp/eventdbx/releases/download/v1.13.2/eventdbx-installer.ps1 | iex"
   ```

2. **Start the server**

   ```bash
   dbx start --foreground
   ```

   - Omit `--foreground` to daemonise the process.
   - Use `--data-dir <path>` to override the default `$HOME/.eventdbx` directory.

- Restriction (schema enforcement) defaults to `default`; switch to `--restrict=off` for permissive prototyping or `--restrict=strict` to require declared schemas on every write.

- Public REST/GraphQL/gRPC surfaces now live in the [dbx_plugins workspace](https://github.com/thachp/dbx_plugins). Install the `rest`, `graphql`, or `grpc` plugin alongside the daemon to expose those endpoints. The built-in server only retains the admin API and control socket.

  ```bash
  # install the restful api plugin
  dbx plugin install rest

  # config the api to not receive events stream
  dbx plugin config process rest --emit-events false

  # start the rest endpoint, default port is 8080
  dbx plugin start rest
  ```

3. **Define a schema (recommended when running in restricted mode)**

   ```bash
   dbx schema create patient \
     --events patient_added,patient_updated \
     --snapshot-threshold 100
   ```

   Omit `--snapshot-threshold` to inherit the default configured in `config.toml` (if any).

4. **Issue a token for CLI access**

   ```bash
   dbx token generate --group admin --user jane --expiration 3600
   ```

5. **Append an event**
   - When the server is running the CLI proxies writes through the control socket automatically. Pass a token with `--token` or set `EVENTDBX_TOKEN` to reuse your own credentials; otherwise the CLI mints a short-lived token for the call.
     ```bash
     dbx aggregate apply person p-002 patient-added \
       --field name="Jane Doe" \
       --field status=active
     ```
   - If the server is stopped, the same CLI command writes to the local RocksDB store directly.

You now have a working EventDBX instance with an initial aggregate. Explore the [Command-Line Reference](#command-line-reference) for the full set of supported operations.

## Features

- **Flexible JSON payloads**: Events accept arbitrary JSON payloads; scalar values are normalized into strings for state tracking, while structured objects remain fully queryable.
- **Immutable Data Structure**: Once data is entered into EventDBX, it becomes immutable, meaning it cannot be altered or deleted. This characteristic is crucial for applications where the accuracy and traceability of historical data are paramount, such as medical records, financial transactions, and supply chain management. Data can be archived, moving from short-term to long-term storage, but cannot be deleted.
- **Event Sourcing and Replay**: EventDBX is built on the principle of event sourcing, storing all changes to the data as a sequence of events. This allows for the complete replay of events to reconstruct the database's state at any point in time, thereby enhancing data recovery and audit capabilities. Unlike traditional databases that execute update statements to modify data, this system is event-driven. Aggregate state changes are defined in the event object, allowing these events to be replayed at any time to reconstruct the aggregate's current state.
- **Merkle Tree Integration**: Each aggregate in EventDBX is associated with a Merkle tree of events, enabling verification of data integrity. The Merkle tree structure ensures that any data tampering can be detected, offering an additional security layer against data corruption.
- **Built-in Audit Trails**: EventDBX automatically maintains a comprehensive audit trail of all transactions, a feature invaluable for meeting compliance and regulatory requirements. It provides transparent and tamper-evident records. During audits, administrators can issue specific tokens to auditors to access and review specific aggregate instances and all relevant events associated with those instances.
- **Extensible Read Models via Plugins**: Every event that lands in the write-side store can be dispatched to external systems through the plugin job queue. Configure plugins to receive only the slices they care about (event, state snapshot, schema), and let specialized services—search, analytics, personalization, alerting—build optimized read models without touching the write path.
- **Security with Token-Based Authorization**: EventDBX implements token-based authorization to manage database access. Tokens are signed with an Ed25519 key pair stored under `[auth]` in `config.toml`; keep `private_key` secret and distribute `public_key` to services that need to validate them. This approach allows for precise control over who can access and modify data, protecting against unauthorized changes.
- **Encrypted Payloads & Secrets at Rest**: Event payloads, aggregate snapshots, and `tokens.json` are encrypted transparently when a DEK is configured. Metadata such as aggregate identifiers, versions, and Merkle roots remain readable so plugins, replication, and integrity checks keep working without additional configuration.
- **Powered by RocksDB and Rust**: At its core, EventDBX utilizes RocksDB for storage, taking advantage of its high performance and efficiency. The system is developed in Rust, known for its safety, efficiency, and concurrency capabilities, ensuring that it is both rapid and dependable.

## Restriction Modes

EventDBX can run in three validation modes, tuned for different phases of development:

- **Off** (`--restrict=off` or `--restrict=false`): Ideal for prototyping and rapid application development. Event payloads bypass schema validation entirely, letting you iterate quickly without pre-registering aggregates, tables, or column types.
- **Default** (`--restrict=default` or `--restrict=true`): Validates events whenever a schema exists but allows aggregates without a declared schema. This matches prior behaviour and suits teams rolling out schema enforcement incrementally.
- **Strict** (`--restrict=strict`): Requires every aggregate to have a schema before events can be appended. Missing schemas fail fast with a clear error so production environments stay aligned with their contracts.

## Command-Line Reference

EventDBX ships a single `dbx` binary. Every command accepts an optional `--config <path>` to point at an alternate configuration file.

### Server lifecycle

- `dbx start [--port <u16>] [--data-dir <path>] [--foreground] [--restrict[=<off|default|strict>]]`  
  Launches the server. The default mode validates when schemas exist; set `--restrict=off` to bypass validation or `--restrict=strict` to require schemas up front.
- `dbx stop`  
  Stops the running daemon referenced by the PID file.
- `dbx status`  
  Prints the current port, PID, uptime, and whether restriction is enabled.
- `dbx restart [start options…]`  
  Stops the existing daemon (if any) and restarts it with the provided options.
- `dbx destroy [--yes]`  
  Removes the PID file, data directory, and configuration file after confirmation (or immediately with `--yes`).

### Configuration

- `dbx config [--port <u16>] [--data-dir <path>] [--cache-threshold <usize>] [--dek <base64>] [--list-page-size <usize>] [--page-limit <usize>] [--plugin-max-attempts <u32>] [--snapshot-threshold <u64>] [--clear-snapshot-threshold] [--admin-enabled <true|false>] [--admin-bind <addr>] [--admin-port <u16>]`  
  Persists configuration updates. Run without flags to print the current settings. The first invocation must include a data encryption key via `--dek` (32 bytes of base64). `--list-page-size` sets the default page size for aggregate listings (default 10), `--page-limit` caps any requested page size across list and event endpoints (default 1000), and `--plugin-max-attempts` controls how many retries are attempted before an event is marked dead (default 10). The Admin API relies on bearer tokens; set `--admin-enabled=false` to disable the HTTP surface entirely.

### Tokens

- `dbx token generate --group <name> --user <name> [--expiration <secs>] [--limit <writes>] [--keep-alive]`  
  Issues a new token tied to a Unix-style group and user.
- `dbx token list`  
  Lists all tokens with status, expiry, and remaining writes.
- `dbx token revoke --token <value>`  
  Revokes a token immediately.
- `dbx token refresh --token <value> [--expiration <secs>] [--limit <writes>]`  
  Extends the lifetime or write allowance of an existing token.

### Schemas

- `dbx schema create <name> --events <event1,event2,...> [--snapshot-threshold <u64>]`
- `dbx schema add <name> --events <event1,event2,...>`
- `dbx schema remove <name> <event>`
- `dbx schema annotate <name> <event> [--note <text>] [--clear]`
- `dbx schema list`

Schemas are stored on disk; when restriction is `default` or `strict`, incoming events must satisfy the recorded schema (and `strict` additionally requires every aggregate to declare one).

### Aggregates

- `dbx aggregate apply --aggregate <type> --aggregate-id <id> --event <name> --field KEY=VALUE... [--payload <json>] [--patch <json>] [--stage] [--token <value>] [--note <text>]`  
  Appends an event immediately—use `--stage` to queue it for a later commit.
- `dbx aggregate list [--skip <n>] [--take <n>] [--stage]`  
  Lists aggregates with version, Merkle root, and archive status; pass `--stage` to display queued events instead.
- `dbx aggregate get --aggregate <type> --aggregate-id <id> [--version <u64>] [--include-events]`
- `dbx aggregate replay --aggregate <type> --aggregate-id <id> [--skip <n>] [--take <n>]`
- `dbx aggregate verify --aggregate <type> --aggregate-id <id>`
- `dbx aggregate snapshot --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `dbx aggregate archive --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `dbx aggregate restore --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `dbx aggregate remove --aggregate <type> --aggregate-id <id>` Removes an aggregate that has no events (version still 0).
- `dbx aggregate commit`  
  Flushes all staged events in a single atomic transaction.
- `dbx aggregate export [<type>] [--all] --output <path> [--format csv|json] [--zip] [--pretty]`  
  Writes the current aggregate state (no metadata) as CSV or JSON. Exports default to one file per aggregate type; pass `--zip` to bundle the output into an archive.

`--payload` and `--patch` are mutually exclusive. Use `--payload` when you want to supply the full document explicitly; use `--patch` for an RFC 6902 JSON Patch that the server applies before validation and persistence.

Staged events are stored in `.eventdbx/staged_events.json`. Use `aggregate apply --stage` to add entries to this queue, inspect them with `aggregate list --stage`, and persist the entire batch with `aggregate commit`. Events are validated against the active schema whenever restriction is `default` or `strict`; the strict mode also insists that a schema exists before anything can be staged. The commit operation writes every pending event in one RocksDB batch, guaranteeing all-or-nothing persistence.

### Plugins

- `dbx plugin install <plugin> <version> --source <path|url> [--bin <file>] [--checksum <sha256>] [--force]`
- `dbx plugin config tcp --name <label> --host <hostname> --port <u16> [--payload <all|event-only|state-only|schema-only|event-and-schema>] [--disable]`
- `dbx plugin config http --name <label> --endpoint <host|url> [--https] [--header KEY=VALUE]... [--payload <all|event-only|state-only|schema-only|event-and-schema>] [--disable]`
- `dbx plugin config log --name <label> --level <trace|debug|info|warn|error> [--template "text with {aggregate} {event} {id}"] [--payload <all|event-only|state-only|schema-only|event-and-schema>] [--disable]`
- `dbx plugin config process --name <instance> --plugin <id> --version <semver> [--arg <value>]... [--env KEY=VALUE]... [--working-dir <path>] [--payload <all|event-only|state-only|schema-only|event-and-schema>] [--disable]`
- `dbx plugin config capnp --name <label> --host <hostname> --port <u16> [--payload <all|event-only|state-only|schema-only|event-and-schema>] [--disable]`
- `dbx plugin enable <label>`
- `dbx plugin disable <label>`
- `dbx plugin remove <label>`
- `dbx plugin test`
- `dbx plugin list`
- `dbx queue`
- `dbx queue clear`
- `dbx queue retry [--event-id <job-id>]`
- `dbx plugin replay <plugin-name> <aggregate> [<aggregate_id>]`

Plugins consume jobs from a durable RocksDB-backed queue. EventDBX enqueues a job for every aggregate mutation, and each plugin can opt into the data it needs—event payloads, materialized state, schemas, or combinations thereof. Clearing dead entries prompts for confirmation to avoid accidental removal. Manual retries run the failed jobs immediately; use `--event-id` to target a specific entry.

### Replication

- `dbx remote add <name> <ip> --public-key <base64> [--port <n>]`  
  Registers a standby and pins its Ed25519 public key. The CLI formats the IP and port into a `tcp://` endpoint (default port `6363`).
- `dbx remote rm <name>`  
  Removes a configured remote.
- `dbx remote ls`  
  Lists remotes with their endpoints.
- `dbx remote show <name>`  
  Displays the endpoint and pinned key for a remote.
- `dbx remote key [--show-path]`  
  Prints this node's replication public key (generated on first run).
- `dbx push <name> [--dry-run] [--schema] [--schema-only] [--batch-size <n>] [--aggregate <type>...] [--aggregate-id <type:id>...]`  
  Streams local events to the remote in fast-forward mode; dry runs report pending changes.
- `dbx pull <name> [--dry-run] [--schema] [--schema-only] [--batch-size <n>] [--aggregate <type>...] [--aggregate-id <type:id>...]`  
  Fast-forwards the local node from the remote, reporting changes in dry-run mode.

Replication keys live alongside the data directory (`replication.key` / `replication.pub`) and are created automatically the first time the CLI loads configuration. The Cap'n Proto listener that powers CLI automation and replication defaults to `[socket].bind_addr` (default `0.0.0.0:6363`); point remotes at that address with a `tcp://` endpoint or override the bind address in `config.toml` when you expose the replica on another interface. The `dbx push` and `dbx pull` commands connect over this socket—no gRPC listener is required—and every session is authenticated with the remote's pinned Ed25519 public key. Use `--aggregate` repeatedly to scope push/pull to specific aggregate types when you only need to sync a subset of data, `--aggregate-id TYPE:ID` to target individual aggregates, `--schema` to copy schema definitions alongside events, and `--schema-only` to synchronize schemas without touching event data.

### Admin API

The Admin API exposes token, schema, remote, and plugin management over HTTP so automation can operate EventDBX without shell access. It is disabled by default; enable it with:

```bash
dbx config \
  --admin-enabled true \
  --admin-bind 127.0.0.1 \
  --admin-port 7171
```

Authorization uses the same JWTs as the CLI. The first server start writes a root token to `~/.eventdbx/cli.token`; reuse it (or mint a dedicated token with `dbx token generate --root`) and attach it as a bearer credential:

```bash
curl -H "Authorization: Bearer $(cat ~/.eventdbx/cli.token)" \
  http://127.0.0.1:7171/admin/remotes
```

Key routes:

| Method & Path        | Description                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------ |
| `GET /admin/tokens`  | List issued tokens; `POST` issues a new token, `/revoke` and `/refresh` manage lifecycle.                    |
| `GET /admin/schemas` | Fetch declared schemas or append new ones with `POST`.                                                       |
| `GET /admin/remotes` | Show replication remotes; `PUT /admin/remotes/{name}` upserts a remote using the same validation as the CLI. |
| `GET /admin/plugins` | Inspect plugin configuration and toggle instances with `/enable` or `/disable`.                              |

Disable access any time with `dbx config --admin-enabled false` or issue a new root token and revoke the old one via `dbx token revoke`.

### Upgrades

- `dbx upgrade [<version>|latest] [--print-only]`  
  Downloads and runs the platform-specific installer to switch binaries. The CLI looks up releases from GitHub, so you can use `latest` or supply a tag like `v1.13.2` (omitting the leading `v` also works). Versions lower than `v1.13.2` are rejected because upgrades are unsupported before that release. Use `--print-only` to show the command without executing it. Shortcut syntax `dbx upgrade@<version>` resolves through the same lookup.
- `dbx upgrade --suppress <version>`  
  Suppresses the upgrade reminder for the provided release tag. Combine it with `latest` to ignore the current release until a newer one ships. Use `dbx upgrade --clear-suppress` to re-enable reminders for all releases.
- `dbx upgrade list [--limit <n>] [--json]`  
  Queries the GitHub releases API and prints the most recent versions. The default limit is 20; use `--json` for a machine-readable list, and call `dbx upgrade@list` for the same shortcut.

> The CLI checks for new releases on startup and prints a reminder when a newer version is available. Set `DBX_NO_UPGRADE_CHECK=1` to bypass the check for automation scenarios.

### Maintenance

- `dbx backup --output <path> [--force]`  
  Creates a compressed archive with the entire EventDBX data directory and configuration. Stop the server before running a backup to avoid partial snapshots.
- `dbx restore --input <path> [--data-dir <path>] [--force]`  
  Restores data from a backup archive. Use `--data-dir` to override the stored location, and `--force` to overwrite non-empty destinations. The server must be stopped before restoring.

Plugins fire after every committed event to keep external systems in sync. Remaining plugins deliver events through different channels:

- **TCP** – Writes a single-line JSON `EventRecord` to the configured socket.
- **HTTP** – POSTs the `EventRecord` JSON to an endpoint with optional headers; add `--https` during configuration to force HTTPS when the endpoint lacks a scheme.
- **gRPC** – Sends `EventRecord` batches to a remote gRPC endpoint compatible with the replication `ApplyEvents` API.
- **Log** – Emits a formatted line via `tracing` at the configured level. By default: `aggregate=<type> id=<id> event=<event>`.
- **Process** – Launches an installed plugin binary as a supervised subprocess and streams events to it over Cap'n Proto. Use this for first-party plugins from the [`dbx_plugins`](https://github.com/thachp/dbx_plugins) workspace or your own extensions.

Process plugins are distributed as zip/tar bundles. Install them with `dbx plugin install <plugin> <version> --source <path-or-url>`—the bundle is unpacked to `~/.eventdbx/plugins/<plugin>/<version>/<target>/`, where `<target>` matches the current OS/architecture (for example, `x86_64-apple-darwin`). Official bundles live in the `dbx_plugins` releases; pass the asset URL to `--source` or point it at a local build while developing. After installation, bind the binary to an instance:

```bash
dbx plugin config process \
  --name search \
  --plugin dbx_search \
  --version 1.0.0 \
  --arg "--listen=0.0.0.0:8081" \
  --env SEARCH_CLUSTER=https://example.com

dbx plugin enable search
```

EventDBX supervises the subprocess, restarts it on failure, and delivers every `EventRecord`/aggregate snapshot over the stream. Plugins that run as independent services can continue to use the TCP/HTTP/GRPC emitters instead.

Failed deliveries are automatically queued and retried with exponential backoff. The server keeps attempting until the plugin succeeds or the aggregate is removed, ensuring transient outages do not drop notifications. Use `dbx queue` to inspect pending/dead event IDs.

Plugin configurations are stored in `.eventdbx/plugins.json`. Each plugin instance requires a unique `--name` so you can update, enable, disable, remove, or replay it later. `plugin enable` validates connectivity (creating directories, touching files, or checking network access) before marking the plugin active. Remove a plugin only after disabling it with `plugin disable <name>`. `plugin replay` resends stored events for a single aggregate instance—or every instance of a type—through the selected plugin.

Need point-in-time snapshots instead of streaming plugins? Use `dbx aggregate export` to capture aggregate state as CSV or JSON on demand.

Example gRPC configuration:

```bash
# point at an existing replication-compatible listener
dbx plugin config grpc \
  --name audit-grpc \
  --endpoint grpc://replica.internal:8800

# enable the plugin once connectivity is confirmed
dbx plugin enable audit-grpc

# inspect status
dbx plugin list
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
  "extensions": {
    "@analytics": {
      "correlation_id": "rest-1234"
    }
  },
  "metadata": {
    "event_id": "1234567890123",
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

> **Heads-up for plugin authors**  
> The [dbx_plugins](https://github.com/thachp/dbx_plugins) surfaces now receive `event_id` values as Snowflake strings and may optionally see an `extensions` object alongside `payload`. Update custom handlers to treat `metadata.event_id` as a stringified Snowflake and to ignore or consume the new `extensions` envelope as needed.

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

---

© 2025 Patrick Thach and contributors

EventDBX is open source software licensed under the [MIT License](./LICENSE).  
See the LICENSE file in the repository for details.
