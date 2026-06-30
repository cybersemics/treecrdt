# @treecrdt/wa-sqlite (scaffold)

Loader + thin helpers to use the TreeCRDT SQLite extension with wa-sqlite in browser/Node.

## Build wa-sqlite (extension baked in)

The vendor package builds upstream wa-sqlite with TreeCRDT baked in via Makefile overrides.

```
pnpm --filter @treecrdt/wa-sqlite-vendor build
# builds packages/treecrdt-wa-sqlite-vendor/dist (example apps copy these into public/wa-sqlite/)
```

## Create a client

Use `createTreecrdtClient` for browser and worker runtimes:

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite';

const client = await createTreecrdtClient({
  storage: { type: 'memory' },
});
```

Low-level callers that open wa-sqlite directly should call
`initializeTreecrdtExtension(module, handle)` before constructing an adapter with
`createWaSqliteApi`.

See `src/index.ts` and `src/ui/App.tsx` for helpers and a simple insert+move demo.

## OPFS single-owner WAL mode

`createTreecrdtClient` can opt in to SQLite WAL for OPFS storage:

```ts
const client = await createTreecrdtClient({
  storage: {
    type: 'opfs',
    filename: '/treecrdt.db',
    fallback: 'throw',
    writeMode: 'single-owner-wal',
  },
  runtime: { type: 'dedicated-worker' },
});
```

This mode runs `PRAGMA locking_mode=EXCLUSIVE` followed by `PRAGMA journal_mode=WAL`
on wa-sqlite's single-instance `AccessHandlePoolVFS`, then verifies that SQLite
reports `exclusive` and `wal`. It requires `runtime: { type: 'dedicated-worker' }`
or `runtime: { type: 'auto' }`.

Use it only when the application guarantees one active TreeCRDT client owns the
OPFS database file, for example a single-WebView Capacitor app. It is not safe as
a general browser multi-tab default because another independent client or worker
can block on the exclusive database owner. `AccessHandlePoolVFS` is also not
filesystem-transparent, so apps that need direct OPFS import/export should use
the default OPFS mode instead.

## OPFS write-ahead VFS mode

`writeMode: 'opfs-write-ahead'` enables wa-sqlite's experimental
`OPFSWriteAheadVFS`. It is intended for browser experiments that need multiple
connections to the same OPFS file and currently requires Chromium support for
OPFS `readwrite-unsafe` access handles.

## Playwright

`pnpm --filter @treecrdt/wa-sqlite test:e2e` runs Vite dev + Playwright and asserts the demo can append/fetch ops via the extension.
