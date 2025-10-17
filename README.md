# EventfulDB

Owner: Patrick Thach
Tags: Infrastructure
Author: Patrick Thach
Port: 7070
Status: In progress

## Overview

EventfulDB is an event-sourced, key-value, write-side database system designed to provide immutable, append-only storage for events across various domains. It is ideal for applications requiring detailed audit trails for compliance, complex business processes involving states, and high data integrity levels.

## Getting Started

Follow the steps below to spin up EventfulDB locally. All commands are expected to run from the repository root unless stated otherwise.

1. **Install prerequisites**
   - Rust toolchain (edition 2024) via [`rustup`](https://rustup.rs/)
   - `rocksdb` is vendored through the Rust crate, so no extra native packages are required.

2. **Clone and build**
   ```bash
   git clone https://github.com/thachp/eventful.git
   cd eventful
   cargo build
   ```

3. **(Optional) Run checks**
   ```bash
   cargo fmt -- --check
   cargo check
   cargo test    # if you add tests locally
   ```

4. **Create the initial configuration**
   The first configuration must include both a master key and a data-encryption key. These values can be any non-empty strings in development.
   ```bash
   cargo run -- config --master-key dev-master --dek dev-dek
   ```

5. **Start the server**
   ```bash
   cargo run -- start --foreground
   ```
   - Omit `--foreground` to daemonise the process.
   - Use `--data-dir <path>` to override the default `./.eventful` directory.
   - Restriction (schema enforcement) is enabled by default; disable it with `--restrict=false` if you need a permissive environment.

6. **Define a schema (recommended when running in restricted mode)**
   ```bash
   cargo run -- schema create \
     --aggregate patient \
     --events patient-added,patient-updated \
     --snapshot-threshold 100
   ```

7. **Issue a token for CLI access**
   ```bash
   cargo run -- token generate \
     --identifier-type user \
     --identifier-id admin \
     --expiration 3600
   ```

8. **Append an event**
   ```bash
   cargo run -- aggregate apply \
     --aggregate patient \
     --aggregate-id p-001 \
     --event patient-added \
     --field name="Jane Doe" \
     --field status=active
   ```

You now have a working EventfulDB instance with an initial aggregate. Explore the [Command-Line Reference](#command-line-reference) for the full set of supported operations.

## Background

My drive to create this immutable system was inspired by three key scenarios I've encountered:

- First, during my years in the healthcare industry, which involved handling sensitive patient records, I faced situations where patient information was either out of sync or had been altered/tampered with without any trace of what or who caused those changes. This could be due to system errors or user actions. I envisioned a system where patient data are immutable, ensuring that if alterations occur, there would be a way to trace what or who made these changes. There needs to be a single source of truth database system into which everyone/every system can write given the proper permissions, and where administrators cannot simply alter data without it being logged.
- Second, as a proponent of the CQRS pattern, I appreciate the separation between write and read databases. I've dealt with a replication task that involved defining numerous aggregates, loading history for each aggregate instance, creating snapshots, and managing versioning and side effects. There should be a system to handle these underlying steps, potentially saving 20% or more of production code. There are needs for a system to handle the write-side of CQRS well.
- Third, I believe a database system should excel at managing side effects. For instance, if there's a change in a patient's state, the system should notify or potentially modify the state in other systems (through integration) to mirror this change, possibly using field-by-field mapping. For example, a **`patient-updated`** event indicating a change in a patient's state should seamlessly update other systems. Specifically, changing a patient's status to deceased should automatically update the indices in OpenSearch to reflect the deceased status, employing field-by-field mapping. These side effects will be efficiently managed by plugins, enabling system administrators to enable, disable, add, or remove side effects as necessary. It should be as straightforward as mapping the fields in event X to the fields in System X and updating System X fields accordingly.

## Features

- **String-Only Storage**: EventfulDB adopts a string-only storage format, meaning all data, regardless of its original type (e.g., integers, dates), is stored as strings. This approach simplifies data handling and ensures uniformity across the database, which is particularly beneficial for data integrity and auditability.
- **Immutable Data Structure**: Once data is entered into EventfulDB, it becomes immutable, meaning it cannot be altered or deleted. This characteristic is crucial for applications where the accuracy and traceability of historical data are paramount, such as medical records, financial transactions, and supply chain management. Data can be archived, moving from short-term to long-term storage, but cannot be deleted.
- **Event Sourcing and Replay**: EventfulDB is built on the principle of event sourcing, storing all changes to the data as a sequence of events. This allows for the complete replay of events to reconstruct the database's state at any point in time, thereby enhancing data recovery and audit capabilities. Unlike traditional databases that execute update statements to modify data, this system is event-driven. Aggregate state changes are defined in the event object, allowing these events to be replayed at any time to reconstruct the aggregate's current state.
- **Merkle Tree Integration**: Each aggregate in EventualDB is associated with a Merkle tree of events, enabling verification of data integrity. The Merkle tree structure ensures that any data tampering can be detected, offering an additional security layer against data corruption.
- **Built-in Audit Trails**: EventfulDB automatically maintains a comprehensive audit trail of all transactions, a feature invaluable for meeting compliance and regulatory requirements. It provides transparent and tamper-evident records. During audits, administrators can issue specific tokens to auditors to access and review specific aggregate instances and all relevant events associated with those instances.
- **Security with Token-Based Authorization**: EventfulDB implements token-based authorization to manage database access. This approach allows for precise control over who can access and modify data, protecting against unauthorized changes. Unlike systems where a single application user account performs CRUD operations, EventualDB mandates that each user of the application obtains their own access token with a specific time horizon. For example, a doctor will receive their own access token, linked to their identifier (IAM-managed outside EventfulDB), generated by the system for each event they handle, rather than having a single application user manage all CRUD operations. This ensures the system can accurately track that it was the doctor who made changes to an aggregate state, not the application.
- **Powered by RocksDB and Rust**: At its core, EventualDB utilizes RocksDB for storage, taking advantage of its high performance and efficiency. The system is developed in Rust, known for its safety, efficiency, and concurrency capabilities, ensuring that EventualDB is both rapid and dependable.

<aside>
💡 EventfulDB is optimally employed within a microservice architecture, wherein each microservice is aligned with a distinct domain context. For example, at OM.Farm, distinct microservices are deployed for Farm Management, Livestock Management, and Livestock Leasing Agreements. Similarly, in the health sector, there could be separate microservices for managing patient demographics and patient appointments. Each microservice operates its own EventfulDB instance, tailored for its specialized purposes.

</aside>

## Command-Line Reference

EventfulDB ships a single `eventful` binary. Every command accepts an optional `--config <path>` to point at an alternate configuration file.

### Server lifecycle

- `eventful start [--port <u16>] [--data-dir <path>] [--foreground] [--restrict | --restrict=false]`  
  Launches the server. Schema validation is enforced by default; pass `--restrict=false` to run in permissive mode.
- `eventful stop`  
  Stops the running daemon referenced by the PID file.
- `eventful status`  
  Prints the current port, PID, uptime, and whether restriction is enabled.
- `eventful restart [start options…]`  
  Stops the existing daemon (if any) and restarts it with the provided options.
- `eventful destroy [--yes]`  
  Removes the PID file, data directory, and configuration file after confirmation (or immediately with `--yes`).

### Configuration

- `eventful config [--port <u16>] [--data-dir <path>] [--master-key <secret>] [--dek <secret>] [--memory-threshold <usize>]`  
  Persists configuration updates. The first invocation must include both `--master-key` and `--dek`.

### Tokens

- `eventful token generate --identifier-type <type> --identifier-id <id> [--expiration <secs>] [--limit <writes>] [--keep-alive]`  
  Issues a new token tied to an identifier.
- `eventful token list`  
  Lists all tokens with status, expiry, and remaining writes.
- `eventful token revoke --token <value>`  
  Revokes a token immediately.
- `eventful token refresh --token <value> [--expiration <secs>] [--limit <writes>]`  
  Extends the lifetime or write allowance of an existing token.

### Schemas

- `eventful schema create --aggregate <name> --events <event1,event2,...> [--snapshot-threshold <u64>]`
- `eventful schema add --aggregate <name> --events <event1,event2,...>`
- `eventful schema alter <aggregate> [--event <name>] [--snapshot-threshold <u64>] [--lock <true|false>] [--field <name>] [--add <field1,field2,...>] [--remove <field1,field2,...>]`
- `eventful schema remove --aggregate <name> --event <name>`
- `eventful schema list`
- `eventful schema show --aggregate <name>`

Schemas are stored on disk; when the server runs with restriction enabled, incoming events must satisfy the recorded schema.

### Aggregates

- `eventful aggregate apply --aggregate <type> --aggregate-id <id> --event <name> --field KEY=VALUE...`  
  Appends an event to the store (validated against the schema when restricted).
- `eventful aggregate list`  
  Lists aggregates with version, Merkle root, and archive status.
- `eventful aggregate get --aggregate <type> --aggregate-id <id> [--version <u64>] [--include-events]`
- `eventful aggregate replay --aggregate <type> --aggregate-id <id> [--skip <n>] [--take <n>]`
- `eventful aggregate verify --aggregate <type> --aggregate-id <id>`
- `eventful aggregate snapshot --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `eventful aggregate archive --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `eventful aggregate restore --aggregate <type> --aggregate-id <id> [--comment <text>]`
- `eventful aggregate commit`  
  Currently a no-op placeholder.

### Plugins

- `eventful plugin postgres --connection <connection-string> [--disable]`
- `eventful plugin sqlite --path <file> [--disable]`
- `eventful plugin csv --output-dir <dir> [--disable]`
- `eventful plugin map --plugin postgres --aggregate <name> --field <field> --datatype <sql-type>`  
  Registers Postgres column overrides; other plugin kinds reject mapping requests.

Plugins are notified after each committed event so downstream systems stay synchronized.

## Contributing

### **Quick Start**

1. **Report Issues**: Found a bug or have a suggestion? [Open an issue](https://chat.openai.com/g/g-kvXdAN8VA-eventdb-guide/c/72d2fa39-c3e2-4e40-bd61-5694f7b82aab#) with detailed information.
2. **Contribute Code**:
   - **Fork & Clone**: Fork the EventfulDB repo and clone your fork.
   - **Branch**: Create a branch for your changes from **`develop`**.
   - **Develop**: Make your changes, adhering to our coding standards. Add or update tests as necessary.
   - **Commit**: Use [Conventional Commits](https://www.conventionalcommits.org/) for clear, structured commit messages (e.g., **`feat: add new feature`** or **`fix: correct a bug`**).
   - **Pull Request**: Submit a pull request (PR) against the **`develop`** branch of the original repo. Describe your changes and link to any related issues.

### **Guidelines**

- **Code Review**: Your PR will be reviewed by the team. Be open to feedback and make requested adjustments.
- **Merge**: Once approved, your PR will be merged into the project, and you'll be credited as a contributor.

## License

EventfulDB is licensed under the [MIT](https://github.com/thachp/eventdb/blob/HEAD/apps/system/) License.
