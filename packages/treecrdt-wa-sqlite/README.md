# @treecrdt/wa-sqlite

Loader + thin helpers to use the TreeCRDT SQLite extension with wa-sqlite in the browser and Node (in-memory WASM).

## Build wa-sqlite (extension baked in)

The vendor package builds upstream wa-sqlite with TreeCRDT baked in via Makefile overrides.

```sh
pnpm --filter @treecrdt/wa-sqlite-vendor build
pnpm --filter @treecrdt/wa-sqlite build
```

The build copies wa-sqlite WASM/JS assets into `dist/wa-sqlite/` for Node and packages them for browser apps via the Vite plugin.

Low-level callers that open a wa-sqlite handle themselves must call
`initializeTreecrdtExtension(module, handle, db, docId)`, then
`createTreecrdtSqliteAdapter(db)` from `@treecrdt/interface/sqlite` (a `Database`
from this package is already a `SqliteRunner`).
`db` must be the `SqliteRunner` for that handle. Initialization registers the
functions, then initializes the shared schema and document ID in one awaited write
transaction, preventing races between connections opening the same database.
Omit `docId` only when schema initialization without a document ID is needed.
`createTreecrdtClient()` performs initialization automatically. Rebuild the vendor
WASM and adapter together.

## Browser usage

Use `createTreecrdtClient()` with a required `docId` and optional `persistent`, `filename`, and `crossTab` options. Browser apps should use `@treecrdt/wa-sqlite/vite-plugin` to copy the JS assets into `public/wa-sqlite/` (asset URLs resolve from Vite's `BASE_URL`). Vite includes the imported WASM in its asset graph and emits it with a content hash.

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite';

// In-memory (default): direct in-process runtime.
const memoryClient = await createTreecrdtClient({ docId: 'my-doc' });

// Durable: OPFS storage in a dedicated worker. Throws when OPFS is unavailable
// (e.g. missing cross-origin isolation) — there is no silent memory fallback.
const persistentClient = await createTreecrdtClient({ docId: 'my-doc', persistent: true });

// Override the OPFS filename when an existing application owns the file naming scheme.
const namedClient = await createTreecrdtClient({
  docId: 'my-doc',
  persistent: true,
  filename: '/my-existing-file.db',
});

// Share one database session between tabs through a SharedWorker.
const crossTabClient = await createTreecrdtClient({
  docId: 'my-doc',
  persistent: true,
  crossTab: true,
});
```

By default, JavaScript assets use Vite's `BASE_URL`, while Vite emits and resolves
the WASM through its asset graph. Applications with custom asset routing can
override the public asset base:

```ts
const client = await createTreecrdtClient({
  docId: 'my-doc',
  assetsBaseUrl: new URL('./vendor/', window.location.href).href,
});
```

`assetsBaseUrl` must contain the plugin-copied `wa-sqlite/` directory. Relative
asset overrides are resolved against the page URL before they are sent to a worker.

Storage, runtime, filename, and asset resolution are selected internally:

| `persistent` | `crossTab` | Storage | Runtime | Notes |
| --- | --- | --- | --- | --- |
| omitted / `false` | omitted / `false` | memory | `direct` (in-process) | data is gone when the client closes |
| `true` | omitted / `false` | OPFS | `dedicated-worker` | filename derived from `docId` with a truncated SHA-256 hash unless explicitly provided |
| omitted / `false` | `true` | memory | `shared-worker` | tabs share an in-memory database while at least one client remains connected |
| `true` | `true` | OPFS | `shared-worker` | tabs share one persistent database session |

`crossTab: true` throws when the browser does not support `SharedWorker`.

Callers only see `TreecrdtClient` (`ops` / `tree` / `local` / `onMaterialized` / `close` / `drop`).

See the [playground](../../examples/playground/README.md) for a full browser demo.

## Node usage (in-memory WASM)

On Node, `createTreecrdtClient()` runs wa-sqlite in-process with an in-memory database. `persistent: true` and `crossTab: true` throw.

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite';

const client = await createTreecrdtClient({ docId: 'my-doc' });

// ... use client.ops, client.tree, client.local, etc.

await client.close();
```

WASM assets are resolved automatically from `dist/wa-sqlite/` (or `@treecrdt/wa-sqlite-vendor` in the monorepo).

For **file-backed persistence on Node**, use [`@treecrdt/sqlite-node`](../treecrdt-sqlite-node) (native SQLite + TreeCRDT extension) instead.

## Tests

- Browser e2e: `pnpm --filter @treecrdt/wa-sqlite-demo test:e2e`
- Node unit + conformance: `pnpm --filter @treecrdt/wa-sqlite test`

## Benchmarks

```sh
pnpm --filter @treecrdt/wa-sqlite benchmark
```

Runs in-memory workloads in Node via the shared WASM loader.
