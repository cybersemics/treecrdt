# @treecrdt/wasm

## 0.1.0

### Minor Changes

- 992283a: Use canonical VersionVector v0 bytes across storage and sync. The new `@treecrdt/wasm/codec` entry
  point lazily loads the shared Rust implementation and exposes synchronous codec methods once ready.
  Recreate development databases that contain the unreleased JSON format.

### Patch Changes

- Updated dependencies [e829a41]
  - @treecrdt/interface@0.3.0
