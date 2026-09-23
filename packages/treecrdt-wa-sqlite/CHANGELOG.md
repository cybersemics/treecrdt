# @treecrdt/wa-sqlite

## 1.0.0

### Major Changes

- 97166eb: Reduce `createTreecrdtClient()` options to a required `docId` plus optional `persistent`, `filename`, `crossTab`, and `assetsBaseUrl` options. The public `storage` / `runtime` / nested `assets` configuration and the `TreecrdtStorage`, `TreecrdtRuntime`, and `TreecrdtAssets` types are removed. The package now selects storage and runtime internally: in-memory + direct runtime by default, OPFS + dedicated worker for `persistent: true`, and a shared worker when `crossTab: true`. Browser assets default to Vite's `BASE_URL` but the base remains overridable for custom asset routing; Vite continues to emit and resolve the WASM through its asset graph. Persistent filenames are derived deterministically from `docId` when omitted using a readable prefix and the first 16 bytes of its SHA-256 hash encoded as base64url (the derivation is exported as `opfsFilenameForDocId`). The previous `auto` default (OPFS when available) becomes in-memory; persistence is always explicit and never silently falls back — when OPFS is unavailable or fails to initialize, client creation throws with the reason. Cross-tab mode throws when `SharedWorker` is unavailable. On Node, `persistent: true` and `crossTab: true` throw; use `@treecrdt/sqlite-node` for file persistence.

### Minor Changes

- 992283a: Use canonical VersionVector v0 bytes across storage and sync. The new `@treecrdt/wasm/codec` entry
  point lazily loads the shared Rust implementation and exposes synchronous codec methods once ready.
  Recreate development databases that contain the unreleased JSON format.
- 9a0534f: Replace the custom worker RPC with Comlink and split session data access from connection lifecycle. `createWaSqliteApi` is removed; low-level callers that open a wa-sqlite handle themselves should initialize the extension, set the doc id, then pass the database to `createTreecrdtSqliteAdapter` from `@treecrdt/interface/sqlite`. `createTreecrdtClient()` is unchanged for typical apps.
- 792a8a0: Initialize the shared TreeCRDT schema through awaited SQL in one transaction,
  replacing the custom VFS retry loop. Failed initialization rolls back schema changes.
  Document ID setup shares the write transaction so concurrent opens cannot race.

  Low-level callers must now pass the connection's SQL runner as the third argument
  to `initializeTreecrdtExtension(module, handle, db, docId)`, with an optional document
  ID to initialize in the same transaction. Rebuild the vendor WASM and
  adapter together. `createTreecrdtClient()` callers need no changes.

### Patch Changes

- 49ca3c9: Reduce local insert, move, delete, and payload overhead by preparing SQLite helper statements only when each operation needs them.
- 972f402: Build the bundled wa-sqlite assets from upstream using its existing extension inputs instead of a fork.
- Updated dependencies [e829a41]
  - @treecrdt/interface@0.3.0

## 0.4.2

### Patch Changes

- 7842e93: Use the synchronous wa-sqlite build for dedicated-worker OPFS and memory databases while retaining
  the Asyncify build for direct and shared-worker OPFS access.

## 0.4.1

### Patch Changes

- 0a9e515: Keep exact version-vector metadata valid across SQLite canonical replay so subsequent local writes succeed.
- 8677f29: Emit browser WASM through Vite's asset pipeline with a content-hashed filename and avoid duplicate unhashed public copies.
- 6fe0e66: Honor the requested OPFS fallback policy in worker runtimes, close failed SQLite and OPFS resources, retry allowed memory fallback with a fresh module, and release SharedWorker ports when initialization or teardown fails.

## 0.4.0

### Minor Changes

- db8b68f: Base defensive deletion on structural subtree history and each node's current LWW payload writer, so superseded payload writes no longer restore deleted nodes. Existing materialized development databases must be reset or replayed once.

### Patch Changes

- 6afda30: Make direct and dedicated-worker client handles terminal when close or drop fails, and terminate dedicated workers when
  initialization rejects.

## 0.3.4

### Patch Changes

- f81a44d: Allocate local insert and move order keys correctly when adjacent prefix digits have independently ordered suffixes.

## 0.3.3

### Patch Changes

- 0a2f290: Initialize the statically linked TreeCRDT extension explicitly after opening SQLite.

## 0.3.2

### Patch Changes

- 1dd003d: Allow OPFS database paths longer than wa-sqlite's 64-byte VFS default.

## 0.3.1

### Patch Changes

- c4c58a0: Normalize cross-realm typed arrays before binding wa-sqlite blob parameters.

## 0.3.0

### Minor Changes

- 8dea846: Add Node support for in-memory WASM via `createTreecrdtClient()`, with separate browser and Node entry points resolved through package conditional exports.

### Patch Changes

- 2ed710b: Reduce wa-sqlite worker payload read copies by returning transferable binary RPC results.

## 0.2.0

### Minor Changes

- 2f864ec: Move local materialization write ids from the event root to each materialized change's `source.writeIds`.
- 9a0304d: Remove auth helpers from the wa-sqlite client surface so apps compose sqlite auth explicitly through `@treecrdt/sync-sqlite/auth`.
- 60950b7: Add optional per-change source metadata to materialization events so apps can derive local projections like update metadata from the operation that caused a visible tree change.

### Patch Changes

- Updated dependencies [2f864ec]
- Updated dependencies [60950b7]
  - @treecrdt/interface@0.2.0

## 0.1.1

### Patch Changes

- ab8ea7c: Use the any-context OPFS VFS for direct browser clients so explicit direct OPFS stores can open and survive reloads.

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.

### Patch Changes

- Updated dependencies [ed5a001]
  - @treecrdt/interface@0.1.0
  - @treecrdt/sync-sqlite@0.1.0
