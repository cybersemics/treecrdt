# @treecrdt/wasm

## 0.1.0

### Minor Changes

- 992283a: Use canonical VersionVector v0 bytes across storage and sync. The new `@treecrdt/wasm/codec` entry
  point lazily loads the shared Rust implementation and exposes synchronous codec methods once ready.
  Recreate development databases that contain the unreleased JSON format.

### Patch Changes

- 1f1ef27: Apply operation batches through one WASM call, replaying retained history at most once. Decode and validate the complete batch before applying any operations.
- 78d132c: Pass operation objects and binary fields directly into WASM instead of encoding inputs as JSON and hex. Preserve the adapter API, operation-read format, and canonical version-vector bytes.
- Updated dependencies [e1a6f4d]
- Updated dependencies [e69c29b]
- Updated dependencies [e829a41]
  - @treecrdt/interface@0.3.0
