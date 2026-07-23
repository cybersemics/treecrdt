# @treecrdt/wa-sqlite

## 0.4.0

### Minor Changes

- db8b68f: Base defensive deletion on structural subtree history and each node's current LWW payload writer, so superseded payload writes no longer restore deleted nodes. Existing materialized development databases must be reset or replayed once.

## 0.3.4

### Patch Changes

- f81a44d: Allocate local insert and move order keys correctly when adjacent prefix digits have independently ordered suffixes.

## 0.3.3

### Patch Changes

- 0a2f290: Initialize the statically linked TreeCRDT extension explicitly after opening SQLite.

## 0.3.2

### Patch Changes

- 1dd003d: Allow OPFS database paths longer than wa-sqlite's 64-byte VFS default.

## 0.3.1

### Patch Changes

- c4c58a0: Normalize cross-realm typed arrays before binding wa-sqlite blob parameters.

## 0.3.0

### Minor Changes

- 8dea846: Add Node support for in-memory WASM via `createTreecrdtClient()`, with separate browser and Node entry points resolved through package conditional exports.

### Patch Changes

- 2ed710b: Reduce wa-sqlite worker payload read copies by returning transferable binary RPC results.

## 0.2.0

### Minor Changes

- 2f864ec: Move local materialization write ids from the event root to each materialized change's `source.writeIds`.
- 9a0304d: Remove auth helpers from the wa-sqlite client surface so apps compose sqlite auth explicitly through `@treecrdt/sync-sqlite/auth`.
- 60950b7: Add optional per-change source metadata to materialization events so apps can derive local projections like update metadata from the operation that caused a visible tree change.

### Patch Changes

- Updated dependencies [2f864ec]
- Updated dependencies [60950b7]
  - @treecrdt/interface@0.2.0

## 0.1.1

### Patch Changes

- ab8ea7c: Use the any-context OPFS VFS for direct browser clients so explicit direct OPFS stores can open and survive reloads.

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.

### Patch Changes

- Updated dependencies [ed5a001]
  - @treecrdt/interface@0.1.0
  - @treecrdt/sync-sqlite@0.1.0
