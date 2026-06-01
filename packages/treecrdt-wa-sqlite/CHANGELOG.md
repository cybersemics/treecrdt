# @treecrdt/wa-sqlite

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
