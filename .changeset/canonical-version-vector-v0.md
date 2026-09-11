---
'@treecrdt/wasm': minor
'@treecrdt/wa-sqlite': minor
---

Use canonical VersionVector v0 bytes across storage and sync. The new `@treecrdt/wasm/codec` entry
point lazily loads the shared Rust implementation and exposes synchronous codec methods once ready.
Recreate development databases that contain the unreleased JSON format.
