# @treecrdt/sync

## 0.1.2

### Patch Changes

- Updated dependencies [d859c20]
  - @treecrdt/sync-protocol@0.1.2
  - @treecrdt/sync-sqlite@0.1.2

## 0.1.1

### Patch Changes

- 2f864ec: Move local materialization write ids from the event root to each materialized change's `source.writeIds`.
- 60950b7: Add optional per-change source metadata to materialization events so apps can derive local projections like update metadata from the operation that caused a visible tree change.
- Updated dependencies [2f864ec]
- Updated dependencies [60950b7]
  - @treecrdt/interface@0.2.0
  - @treecrdt/sync-sqlite@0.1.1
  - @treecrdt/sync-protocol@0.1.1

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.

### Patch Changes

- Updated dependencies [ed5a001]
  - @treecrdt/discovery@0.1.0
  - @treecrdt/interface@0.1.0
  - @treecrdt/sync-protocol@0.1.0
  - @treecrdt/sync-sqlite@0.1.0
