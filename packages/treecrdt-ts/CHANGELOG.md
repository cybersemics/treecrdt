# @treecrdt/interface

## 0.5.0

### Minor Changes

- b2394a9: Add `onMaterializedDurable(name, listener, { version, reset, limit, saveIntervalMs, onError })` to the SQLite-backed engines. It is `onMaterialized` that keeps its place: the listener receives every change in order and at least once, across restarts and failed deliveries, so applications no longer store a `changedSince` cursor or write the catch-up loop themselves. After a gap, changes report each node's current state. The position is kept per name in a new `treecrdt_materialization_cursors` table, and closing the client closes open subscriptions.

## 0.4.0

### Minor Changes

- fc2c6e5: Add `tree.node(id)`, a node handle that skips the existence query `tree.get(id)` runs, so reading a known node by id is one query: `await tree.node(id).children()`. A node that is not visible reads as empty: no payload, no children and no parent.

### Patch Changes

- a5d694f: Stop node reads from returning a deleted node's data. Only `tree.get(id)` checked visibility, so a handle kept across a delete still read the deleted node's payload and children, and so did adapter `treePayload` and `treeChildren`. They now check it in the same query.

## 0.3.0

### Minor Changes

- e1a6f4d: Replace keyset `treeChildrenPage` with optional index/length slices on `treeChildren` and `TreecrdtNode.children()`. Live (non-tombstoned) children are paginated in stable order.
- e69c29b: Replace the flat public `tree` surface with a lazy `get()` / `root` Node API, and split the wa-sqlite session into `operations` and `tree` sub-objects. Callers navigate with `tree.get(id)?.payload()` / `parent()` / `children()` instead of `exists` / `getPayload` / `parent` / `children`; Node handles expose canonical 32-character lowercase hex ids, root aliases resolve to the always-available root handle, and `parent()` returns `undefined` when either the node or its parent is not visible. Select browser runtime wiring via an exhaustive strategy map instead of ad hoc branching.
- be09f09: Add `tree.changedSince(since, { limit, payloads })` to the SQLite-backed engines. It returns each node whose visible state changed after a `headSeq`, with its current state and the read's `headSeq`, so applications can keep derived data in step across restarts without rebuilding it. Materialization records each changed node's `headSeq` in a new `tree_changes` table within the same commit; `since = 0` reads every node. Existing databases gain the table on open. Paged reads are resumable catch-up, not frozen snapshots: nodes changed during paging may be deferred to the next read. A `since` ahead of the database's `headSeq` is rejected. Catch-up reads scan that table, which keeps recording cheap; follow `onMaterialized` between them.
- e829a41: Remove unused legacy sync, storage, access-control, and adapter-factory declarations. Active filtered-sync contracts remain available from `@treecrdt/sync-protocol`.

### Patch Changes

- 4686794: Send SQLite operation batches as CBOR blobs instead of JSON byte arrays. Update the adapter and extension together: JSON batch input is no longer accepted. Stored operations and sync formats are unchanged.

## 0.2.0

### Minor Changes

- 2f864ec: Move local materialization write ids from the event root to each materialized change's `source.writeIds`.
- 60950b7: Add optional per-change source metadata to materialization events so apps can derive local projections like update metadata from the operation that caused a visible tree change.

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.
