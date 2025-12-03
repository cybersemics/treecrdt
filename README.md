# treecrdt

Tree CRDT workspace targeting SQLite/wa-sqlite + WASM bindings with a shared TypeScript interface.

## Layout
- `packages/treecrdt-core`: core CRDT library with traits for storage/indexing/access control.
- `packages/treecrdt-sqlite-ext`: SQLite/wa-sqlite extension harness that will implement the core traits.
- `packages/treecrdt-wasm`: bridge for wasm-bindgen and browser/node builds.
- `packages/treecrdt-ts`: TypeScript interface definitions shared by bindings and the sync layer.
- `references/`: external implementations for inspiration only (do not copy code).

## Quick start
```
cargo check
```

TypeScript packages are scaffolded; install toolchain when ready:
```
pnpm install
pnpm --filter @treecrdt/interface build
```

## SQLite extension targets
- Default build uses `rusqlite`-backed storage for native testing.
- Extension entrypoint (for native SQLite or wa-sqlite) is behind `ext-sqlite`/`wasm-ext` features:
  - Native build only: `cargo test -p treecrdt-sqlite-ext --no-default-features --features ext-sqlite --no-run`
  - WASM/wa-sqlite will use the same entrypoint; bundling glue mirrors cr-sqlite’s `js` approach.
- For a native roundtrip test that actually loads the extension: `cargo test -p treecrdt-sqlite-ext --features "ext-sqlite rusqlite-storage" --test extension_roundtrip`
- See `docs/wa-sqlite.md` for wasm bundling notes.
