# Architecture and Delivery Plan

## Goals
- Kleppmann Tree CRDT in Rust with clean traits for storage/indexing/access control.
- Runs native and WASM; embeddable as a SQLite/wa-sqlite extension.
- TypeScript interface stays stable across native/WASM/SQLite builds.
- Strong tests (unit/property/integration) and benchmarks (Rust + TS/WASM).

## Monorepo layout (initial)
- `packages/treecrdt-core`: CRDT types, traits, reference clocks/access control, memory storage.
- `packages/treecrdt-sqlite-ext`: SQLite/wa-sqlite extension harness that will satisfy storage/index traits.
- `packages/treecrdt-wasm`: WASM bridge crate; will expose the core through wasm-bindgen and map to the TS interface.
- `packages/treecrdt-ts`: TS interface definitions for CRDT + sync protocol.
- `references/`: external implementations for inspiration only (no code copy because of licensing).

## Core CRDT shape
- Operation log with `(OperationId { replica, counter }, lamport, kind)`; kinds: insert/move/delete/tombstone.
- Deterministic application rules following Kleppmann Tree CRDT; extend to support alternative tombstone semantics if needed (per linked proposal).
- Access control hooks applied before state mutation.
- Partial sync support via subtree filters + index provider for efficient fetch.

## Trait contracts (Rust)
- `Clock`: lamport/HLC pluggable (`LamportClock` provided).
- `AccessControl`: guards apply/read.
- `Storage`: append operations, load since lamport, snapshot, latest_lamport.
- `IndexProvider`: optional acceleration for subtree queries and existence checks.
- These traits are the seam for SQLite/wa-sqlite implementations; extension just implements them over tables/indexes.

## SQLite / wa-sqlite plan
- Define op-log schema + indexes for `(lamport, node, parent)`.
- Implement `Storage` over SQLite with streaming cursors and batched writes.
- Implement `IndexProvider` using SQLite indexes for children lookups + subtree filters.
- Provide extension entrypoints (native + wasm) that instantiate `TreeCrdt` with SQLite-backed traits.
- Versioning/migrations tracked in schema table; gate CRDT version upgrades.

## WASM + TypeScript bindings
- `treecrdt-wasm`: wasm-bindgen surface mapping to `@treecrdt/interface`.
- `@treecrdt/interface`: TS types for operations, storage adapters, sync protocol, access control.
- Provide both in-memory adapter and SQLite-backed adapter (via wa-sqlite) to satisfy the interface.

## Sync engine concept
- Transport-agnostic: push/pull batches with causal metadata + optional subtree filters.
- Progress hooks for UI, resumable checkpoints via lamport/head.
- Access control enforced at responder using subtree filters and ACL callbacks.

## Testing and benchmarking
- Rust: unit tests for op semantics, property/fuzz for merge correctness, integration tests for SQLite-backed storage, WASM tests.
- TS: conformance tests against WASM bindings; mock storage for sync tests.
- Benchmarks: Criterion for Rust op throughput and merge; SQLite benchmarks for indexed fetch/partial sync; TS/WASM microbenches for browser parity.

## Near-term steps
1) Flesh out `treecrdt-core` operation semantics and data model (no storage specifics).
2) Design SQLite schema + storage/index adapters; add cdylib entrypoints for wa-sqlite.
3) Add wasm-bindgen surface + build scripts; align with TS interface.
4) Write test matrix (core, SQLite, WASM) and initial Criterion + TS benchmarks.
5) Evaluate alternative tombstone behavior from PartyKit EM proposal and gate behind feature flag.
