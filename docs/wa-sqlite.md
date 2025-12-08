# wa-sqlite bundling sketch

Goal: ship a single wa-sqlite WebAssembly binary that already contains the TreeCRDT SQLite extension (no `loadExtension` at runtime).

## Build
- `cd vendor/wa-sqlite && make dist`
  - The Makefile now builds `treecrdt-sqlite-ext` for `wasm32-unknown-emscripten` (`static-link` feature), links the static archive into wa-sqlite, and registers it via `sqlite3_auto_extension`.
  - Output: `vendor/wa-sqlite/dist/wa-sqlite.{mjs,wasm}` containing the extension.
- The frontend package copies those artifacts into `packages/treecrdt-wa-sqlite/public/wa-sqlite/` during `pnpm --filter @treecrdt/wa-sqlite build`.

## JS glue (similar to cr-sqlite/js)
- Import from the patched wa-sqlite build (alias configured in `vite.config.ts`):
  ```ts
  import * as SQLite from "wa-sqlite";
  import sqliteWasm from "/wa-sqlite/wa-sqlite.wasm?url"; // copied to public/

  const module = await SQLite.Factory({ wasm: sqliteWasm });
  const db = await module.open(":memory:");
  // The extension is auto-registered; no loadExtension call required.
  await db.exec("select treecrdt_version()");
  ```
- Extension SQL surface:
  - `SELECT treecrdt_version();`
  - `SELECT treecrdt_append_op(?, ?, ?, ?, ?, ?, ?, ?);`
  - `SELECT treecrdt_ops_since(? [, root]);`

## TypeScript adapter (planned)
- Provide a thin wrapper that:
  - Wraps `treecrdt_append_op`/`treecrdt_ops_since` into the `TreeCRDT` interface from `packages/treecrdt-ts`.
  - Manages Lamport/counter in JS or via wasm bindings to `treecrdt-core`.
  - Optionally replays `ops` into an in-memory wasm `TreeCrdt` instance for faster reads.

## Native testing (optional)
- The same extension works natively via `sqlite3_treecrdt_init`. The integration test (`tests/extension_roundtrip.rs`) loads it with rusqlite when `--features "ext-sqlite rusqlite-storage"` are enabled.

## Next implementation steps
- Add a table-valued function for `ops_since` (subtree filters, streaming) instead of JSON.
- Expose a “replay” hook that instantiates `treecrdt-core` on load and keeps an in-memory materialized state if desired.
- Provide a small JS loader utility that hides `loadExtension` and returns a `TreeCRDT` instance.
