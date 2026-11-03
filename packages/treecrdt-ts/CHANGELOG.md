# @treecrdt/interface

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
