# Architecture

🚧 Work in progress 🚧 

## Goals
- Kleppmann Tree CRDT in Rust with clean traits for storage/indexing/access control.
- Runs native and WASM; embeddable as a SQLite/wa-sqlite extension.
- TypeScript interface stays stable across native/WASM/SQLite builds.
- Strong tests (unit/property/integration) and benchmarks (Rust + TS/WASM).

## Package map

This diagram is meant to answer, "What depends on what in this repo?".

Arrow direction is **depends on / uses**.
Solid arrows are runtime dependencies. Dotted arrows are build time, dev, or test connections.

```mermaid
flowchart TD
  %% Rust crates (Cargo workspace)
  subgraph Rust["Rust crates (Cargo workspace)"]
    core_crate["treecrdt-core"]
    sqlite_ext_crate["treecrdt-sqlite-ext"]
    wasm_crate["treecrdt-wasm"]
    riblt_crate["treecrdt-riblt-wasm"]

    sqlite_ext_crate --> core_crate
    wasm_crate --> core_crate
  end

  %% TypeScript packages (pnpm workspace)
  subgraph TS["TypeScript packages (pnpm workspace)"]
    iface["@treecrdt/interface"]
    discovery["@treecrdt/discovery"]
    discovery_server["@treecrdt/discovery-server-node"]
    sync_protocol["@treecrdt/sync-protocol"]
    sync_client["@treecrdt/sync"]
    sync_sqlite["@treecrdt/sync-sqlite"]
    sync_postgres["@treecrdt/sync-postgres"]
    sync_server_core["@treecrdt/sync-server-core"]
    sync_server_pg["@treecrdt/sync-server-postgres-node"]
    auth["@treecrdt/auth"]
    crypto["@treecrdt/crypto"]
    wa_vendor["@treecrdt/wa-sqlite-vendor"]
    wa["@treecrdt/wa-sqlite"]
    wasm_pkg["@treecrdt/wasm"]
    riblt_pkg["@treecrdt/riblt-wasm"]
    sqlite_node["@treecrdt/sqlite-node"]
    conformance["@treecrdt/engine-conformance"]
    bench["@treecrdt/benchmark"]
  end

  %% Runtime dependencies
  sync_protocol --> iface
  discovery_server --> discovery
  sync_protocol --> riblt_pkg
  sync_client --> sync_protocol
  sync_client --> discovery
  sync_client --> iface
  sync_client --> sync_sqlite
  sync_sqlite --> sync_protocol
  sync_sqlite --> iface
  sync_postgres --> sync_protocol
  sync_postgres --> iface
  sync_server_core --> sync_protocol
  sync_server_pg --> sync_protocol
  sync_server_pg --> sync_postgres
  sync_server_pg --> sync_server_core
  auth --> iface
  auth --> sync_protocol
  wa --> iface
  wasm_pkg --> iface
  conformance --> auth
  conformance --> sync_protocol
  conformance --> sync_sqlite
  conformance --> iface

  %% Build-time connections (how artifacts are produced)
  riblt_pkg -. wasm-pack build .-> riblt_crate
  wasm_pkg -. wasm-pack build .-> wasm_crate
  wa_vendor -. emscripten build .-> sqlite_ext_crate
  sqlite_node -. native build .-> sqlite_ext_crate
  wa -. bundles dist artifacts .-> wa_vendor

  %% Dev/test relationships (kept out of runtime deps)
  auth -. dev .-> bench
  sync_protocol -. dev .-> bench
  wasm_pkg -. dev .-> bench
  wa -. dev .-> bench
  sqlite_node -. conformance tests .-> conformance
```

## Core CRDT shape
- Operation log with `(OperationId { replica, counter }, lamport, kind)`; kinds: insert/move/delete/tombstone.
- Deterministic application rules following Kleppmann Tree CRDT; extend to support alternative tombstone semantics if needed (per linked proposal).
- Access control hooks applied before state mutation.
- Partial sync support via subtree filters + index provider for efficient fetch.

## Trait contracts (Rust)
- `Clock`: lamport/HLC pluggable (`LamportClock` provided).
- `AccessControl`: guards apply/read.
- `Storage`: append operations, load since lamport, latest_lamport.
- `IndexProvider`: optional acceleration for subtree queries and existence checks.
- These traits are the seam for SQLite/wa-sqlite implementations; extension just implements them over tables/indexes.

## WASM + TypeScript bindings
- `treecrdt-wasm`: wasm-bindgen surface mapping to `@treecrdt/interface`.
- `@treecrdt/interface`: TS types for operations, storage adapters, sync protocol, access control.
- Provide both in-memory adapter and SQLite-backed adapter (via wa-sqlite) to satisfy the interface.

## Sync engine concept
- **Protocol & runtime** (`@treecrdt/sync-protocol`, `packages/sync-protocol/protocol`): transport-agnostic peer, codecs, in-memory test helpers, and generated Protobuf types.
- **Client integration** (`@treecrdt/sync`, `packages/treecrdt-sync`): optional wiring for spec `WebSocket` clients (browser global, or `webSocketCtor` e.g. from `undici` in Node; see `packages/treecrdt-sync` README)—uses `@treecrdt/discovery` to resolve docs to websocket URLs, `@treecrdt/sync-protocol` for the wire session, and `@treecrdt/sync-sqlite` for a `SyncBackend` backed by the SQLite material layer. App code that wants full control can depend on the protocol and discovery packages only.
- Transport-agnostic: push/pull batches with causal metadata + optional subtree filters.
- Progress hooks for UI, resumable checkpoints via lamport/head.
- Access control enforced at responder using subtree filters and ACL callbacks.
- Draft protocol: [`sync/v0.md`](sync/v0.md)
