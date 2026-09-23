# @treecrdt/sync-protocol

## 0.2.0

### Minor Changes

- 2594f3d: Require every duplex transport to expose an idempotent `close` method and a read-only `closeSignal`, treat each transport object as one connection lifecycle, propagate malformed frames and WebSocket closure through sync sessions instead of leaving pending work unresolved, and scope protocol session routing to the originating transport. The high-level custom-transport factory now owns transport cleanup and no longer accepts a separate close callback.

### Patch Changes

- Updated dependencies [e829a41]
  - @treecrdt/interface@0.3.0

## 0.1.2

### Patch Changes

- d859c20: Load RIBLT only when reconciliation needs it, so importing the sync protocol no longer suspends browser application startup.

## 0.1.1

### Patch Changes

- Updated dependencies [2f864ec]
- Updated dependencies [60950b7]
  - @treecrdt/interface@0.2.0

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.

### Patch Changes

- Updated dependencies [ed5a001]
  - @treecrdt/interface@0.1.0
  - @treecrdt/riblt-wasm@0.1.0
