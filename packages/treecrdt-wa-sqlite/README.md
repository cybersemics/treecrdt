# @treecrdt/wa-sqlite

Browser TreeCRDT SQLite client built on wa-sqlite, the TreeCRDT SQLite extension, and optional
worker/OPFS helpers.

Use this package for browser apps. Node apps should use `@treecrdt/sqlite-node` instead; there is
intentionally no shared `@treecrdt/sqlite` app import today.

## Usage

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite/client';

const client = await createTreecrdtClient({ docId });
```

`createTreecrdtClient` returns the common `TreecrdtEngine` surface from
`@treecrdt/interface/engine`, plus browser-specific runtime helpers.

## Vite Assets

Vite apps can use the package plugin to copy wa-sqlite assets from `@treecrdt/wa-sqlite-vendor` into
the app public directory.

```ts
// vite.config.ts
import { treecrdt } from '@treecrdt/wa-sqlite/vite-plugin';

export default {
  plugins: [treecrdt()],
};
```

## Low-Level Adapter

The package root exports `createWaSqliteApi` for lower-level adapter integrations that already own a
wa-sqlite database handle. App code should prefer `@treecrdt/wa-sqlite/client`.

## Import Decision

The Node and browser SQLite packages stay separate to keep native `better-sqlite3` dependencies out
of browser bundles and wa-sqlite/WASM assets out of Node apps. See
[`docs/SQLITE_CLIENTS.md`](../../docs/SQLITE_CLIENTS.md) for the full import-surface decision.
