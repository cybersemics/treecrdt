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
`initializeTreecrdtExtension(module, handle)`, set the doc id, then
`createTreecrdtSqliteAdapter(db)` from `@treecrdt/interface/sqlite` (a `Database`
from this package is already a `SqliteRunner`).
`createTreecrdtClient()` does this automatically.

## Browser usage

Use `createTreecrdtClient()` with a required `docId`, optional `persistent` flag, and optional persistent database `filename`. Browser apps should use `@treecrdt/wa-sqlite/vite-plugin` to copy the JS assets into `public/wa-sqlite/` (asset URLs resolve from Vite's `BASE_URL`). Vite includes the imported WASM in its asset graph and emits it with a content hash.

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
```

Storage, runtime, filename, and asset resolution are selected internally:

| `persistent` | Storage | Runtime | Notes |
| --- | --- | --- | --- |
| omitted / `false` | memory | `direct` (in-process) | data is gone when the client closes |
| `true` | OPFS | `dedicated-worker` (Comlink over `Worker`) | filename derived from `docId` unless explicitly provided; clients using the same filename and `docId` share it |

Callers only see `TreecrdtClient` (`ops` / `tree` / `local` / `onMaterialized` / `close` / `drop`).

See the [playground](../../examples/playground/README.md) for a full browser demo.

## Node usage (in-memory WASM)

On Node, `createTreecrdtClient()` runs wa-sqlite in-process with an in-memory database. `persistent: true` throws.

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
