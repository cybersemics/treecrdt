# @treecrdt/sqlite-node

Node.js TreeCRDT SQLite client built on `better-sqlite3` and the native TreeCRDT SQLite extension.

Use this package for Node processes. Browser apps should use `@treecrdt/wa-sqlite/client` instead;
there is intentionally no shared `@treecrdt/sqlite` app import today.

## Usage

```ts
import Database from 'better-sqlite3';
import { createTreecrdtClient, loadTreecrdtExtension } from '@treecrdt/sqlite-node';

const db = new Database('treecrdt.db');
loadTreecrdtExtension(db);

const client = await createTreecrdtClient(db, { docId });
```

`createTreecrdtClient` returns the common `TreecrdtEngine` surface from
`@treecrdt/interface/engine`, plus Node-specific `runner` and auth helpers.

## Import Decision

The Node and browser SQLite packages stay separate to keep native `better-sqlite3` dependencies out
of browser bundles and wa-sqlite/WASM assets out of Node apps. See
[`docs/SQLITE_CLIENTS.md`](../../docs/SQLITE_CLIENTS.md) for the full import-surface decision.
