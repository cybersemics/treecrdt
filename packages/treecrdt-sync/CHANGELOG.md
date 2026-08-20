# @treecrdt/sync

## 0.2.0

### Minor Changes

- 2594f3d: Require every duplex transport to expose an idempotent `close` method and a read-only `closeSignal`, treat each transport object as one connection lifecycle, propagate malformed frames and WebSocket closure through sync sessions instead of leaving pending work unresolved, and scope protocol session routing to the originating transport. The high-level custom-transport factory now owns transport cleanup and no longer accepts a separate close callback.

### Patch Changes

- Updated dependencies [2594f3d]
  - @treecrdt/sync-protocol@0.2.0
  - @treecrdt/sync-sqlite@0.1.3

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
