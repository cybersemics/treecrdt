---
'@treecrdt/wasm': minor
'@treecrdt/wa-sqlite': minor
---

Define one strict, versioned binary encoding for gap-aware version vectors and use it across the
JavaScript API, storage adapters, and runtimes. JavaScript uses `loadVersionVectorCodec()` from
`@treecrdt/wasm/codec` to lazily load the shared Rust codec in Node and browsers, then encodes and
decodes synchronously through the cached codec.
This replaces the unreleased JSON development format; recreate development databases that contain it.
