# Operation queries

`ops.since(lamport)` is an exclusive Lamport-threshold query over the operations currently stored:

```text
op.meta.lamport > lamport
```

The same contract applies to adapter `opsSince`, Rust `load_since`/`operations_since`, PostgreSQL
`ops_since`, WASM `opsSince`, and SQLite `treecrdt_ops_since`. Result ordering is not part of this
shared contract; only APIs that explicitly document an order guarantee one.

This is not an arrival cursor. For example, suppose `headLamport()` returns `10`. A remote operation
with Lamport `7` can arrive later, as can an operation from another replica with Lamport `10`.
`ops.since(10)` returns neither. Changing the predicate to `>=` would repeat operations at `10` but
would still miss the late operation at `7`.

Use the API that matches the job:

- `ops.all()` reads the complete operation snapshot currently stored.
- Sync reconciles operation-reference sets using RIBLT, with a direct-send fast path for tiny
  clean-slate scopes.
- `onMaterialized` observes materialization changes in the current session; it is not durable or
  resumable.
- `headLamport()` reports the current maximum for clock observation and metadata, not ingestion
  progress.

A durable arrival feed would need a backend-assigned monotonic sequence and a separate API such as
`changesAfter(sequence)`. That API is not currently provided.
