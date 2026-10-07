# @treecrdt/interface

## 0.3.0

### Minor Changes

- e1a6f4d: Replace keyset `treeChildrenPage` with optional index/length slices on `treeChildren` and `TreecrdtNode.children()`. Live (non-tombstoned) children are paginated in stable order.
- e69c29b: Replace the flat public `tree` surface with a lazy `get()` / `root` Node API, and split the wa-sqlite session into `operations` and `tree` sub-objects. Callers navigate with `tree.get(id)?.payload()` / `parent()` / `children()` instead of `exists` / `getPayload` / `parent` / `children`; `parent()` returns `undefined` when either the node or its parent is not visible. Select browser runtime wiring via an exhaustive strategy map instead of ad hoc branching.
- e829a41: Remove unused legacy sync, storage, access-control, and adapter-factory declarations. Active filtered-sync contracts remain available from `@treecrdt/sync-protocol`.

## 0.2.0

### Minor Changes

- 2f864ec: Move local materialization write ids from the event root to each materialized change's `source.writeIds`.
- 60950b7: Add optional per-change source metadata to materialization events so apps can derive local projections like update metadata from the operation that caused a visible tree change.

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.
