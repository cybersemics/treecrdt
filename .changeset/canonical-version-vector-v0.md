---
'@treecrdt/wasm': minor
'@treecrdt/wa-sqlite': minor
---

Define one strict, versioned binary encoding for gap-aware version vectors and use it across the
JavaScript API, storage adapters, and runtimes. JavaScript calls the shared Rust codec through
the asynchronous `@treecrdt/wasm/codec` entry point, which initializes WASM lazily in Node and browsers.
This replaces the unreleased JSON development format; recreate development databases that contain it.
