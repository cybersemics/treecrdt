# @treecrdt/wa-sqlite

Loader + thin helpers to use the TreeCRDT SQLite extension with wa-sqlite in the browser and Node (in-memory WASM).

## Build wa-sqlite (extension baked in)

The vendor package builds upstream wa-sqlite with TreeCRDT baked in via Makefile overrides.

```sh
pnpm --filter @treecrdt/wa-sqlite-vendor build
pnpm --filter @treecrdt/wa-sqlite build
```

The build copies wa-sqlite WASM/JS assets into `dist/wa-sqlite/` for Node and packages them for browser apps via the Vite plugin.

## Browser usage

Use `createTreecrdtClient()` with OPFS or in-memory storage. Browser apps should use `@treecrdt/wa-sqlite/vite-plugin` to copy assets into `public/wa-sqlite/`.

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite';

const client = await createTreecrdtClient({
  storage: { type: 'auto' },
  docId: 'my-doc',
});
```

See the [playground](../../examples/playground/README.md) for a full browser demo.

## Node usage (in-memory WASM)

On Node, `createTreecrdtClient()` runs wa-sqlite in-process with an in-memory database. OPFS and worker runtimes are not supported.

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite';

const client = await createTreecrdtClient({
  storage: { type: 'memory' },
  runtime: { type: 'direct' },
  docId: 'my-doc',
});

// ... use client.ops, client.tree, client.local, etc.

await client.close();
```

WASM assets are resolved automatically from `dist/wa-sqlite/` (or `@treecrdt/wa-sqlite-vendor` in the monorepo). Override with `assets.baseUrl` pointing at a filesystem directory containing the wa-sqlite artifacts.

For **file-backed persistence on Node**, use [`@treecrdt/sqlite-node`](../treecrdt-sqlite-node) (native SQLite + TreeCRDT extension) instead.

## Tests

- Browser e2e: `pnpm --filter @treecrdt/wa-sqlite-demo test:e2e`
- Node unit + conformance: `pnpm --filter @treecrdt/wa-sqlite test`

## Benchmarks

```sh
pnpm --filter @treecrdt/wa-sqlite benchmark
```

Runs in-memory workloads in Node via the shared WASM loader.
