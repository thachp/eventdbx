# Surface Extraction Plan

This note captures the shared service boundary and socket protocol changes
needed to host REST / GraphQL / gRPC APIs in the `dbx_plugins` workspace
while keeping the database core inside `eventdbx`.

## Core context

- Introduce `service::CoreContext` (done) that aggregates the `TokenManager`,
  `SchemaManager`, `EventStore`, and restrict/page sizing knobs behind cloneable
  accessors.
- `server::AppState` delegates to `CoreContext`, so the same capabilities can be
  exposed over Cap'n Proto without surfacing `Config` internals.
- Future follow‑up: lift all side-effectful actions (append, verify, etc.) into
  methods on a new `CoreService` that accepts lightweight request structs.

## Cap'n Proto control channel

- Extend the listener on port `6363` with a new message family (`ControlRequest`
  / `ControlResponse`) for authenticated plugins.
- Provide operations for:
  1. Token authorization (`authorize_action`).
  2. Aggregate reads (`list_aggregates`, `get_aggregate`,
     `list_events`, `verify_aggregate`).
  3. Event writes (`append_event`) with server-side validation and plugin
     fan-out.
  4. Schema inspection (`get_schema`, `list_hidden_fields`).
- The handshake reuses the existing `PluginEnvelope::init` so managed process
  plugins can authenticate and then upgrade the stream into a bidirectional
  RPC session.
- Responses carry JSON payloads mirroring what the current REST/GraphQL/gRPC
  layers expect, allowing short-term reuse of the serializers in the new plugin
  crates.

## Plugin workspace layout

- `dbx_plugins/common/plugin_api` gains a lightweight async client for the new
  control protocol, including helpers to build requests and handle error codes.
- Three new member crates (`plugins/rest_api`, `plugins/graphql_api`,
  `plugins/grpc_api`) depend on the control client and rehost the existing
  endpoint logic.
- Configuration changes now flow exclusively through the CLI and control
  channel; no separate admin REST router is retained in `eventdbx`.

## Extraction steps

1. Finalize the Cap'n Proto schema (`control.capnp`) and server-side handler in
   `eventdbx` that upgrades plugin connections into control sessions using the
   new request/response flow.
2. Add a `ControlClient` to `dbx_plugin_api` with async methods mirroring the
   control operations (authorize, list aggregates, append, etc.), including
   conversions into the existing DTOs used by the endpoints.
3. Port the REST handlers into `plugins/rest_api`, replacing direct `AppState`
   calls with `ControlClient` invocations and preserving request/response
   schemas for compatibility.
4. Repeat the port for GraphQL and gRPC, wiring resolvers/service methods to the
   client helper instead of `AppState` (the GraphQL schema can be moved verbatim
   once the data access calls are swapped).
5. Gate the in-repo REST/GraphQL/gRPC modules behind new Cargo features, default
   them to `false`, and update documentation to point users at the plugin
   workspace.
6. Provide launch scripts or documentation snippets demonstrating how to run the
   daemon alongside the new plugins (managed process plugins can be auto-started
   via `ProcessPlugin` once the Cap'n Proto handshake is shared).

## Feature gating & migration

- Guard the legacy REST/GraphQL/gRPC modules in `eventdbx` behind optional Cargo
  features during the transition.
- Once the plugin crates reach parity, disable the features by default and
  document how to launch the new plugins alongside the core daemon.

## Validation strategy

- Add feature-flag coverage in CI: `cargo check --no-default-features` (core
  only) and `cargo check --features legacy-surfaces` while the old modules
  remain.
- For the control channel, create integration tests that spin up the daemon,
  connect with a test client over the Cap'n Proto socket, and exercise all
  operations (list, get, append, verify). These can live under `tests/control`.
- Port the existing API smoke tests into the plugin workspace so they execute
  against the new REST/GraphQL/gRPC binaries via `cargo test` inside
  `dbx_plugins`.
- Ensure `ProcessPlugin` integration tests cover lifecycle management (install,
  restart) with the new Cap'n Proto flow to guard against regressions when
  plugins are upgraded.
