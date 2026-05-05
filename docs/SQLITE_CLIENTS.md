# SQLite Client Import Surface

TreeCRDT intentionally keeps separate SQLite client packages for Node and browser runtimes:

- `@treecrdt/sqlite-node` for Node.js apps using `better-sqlite3` and the native SQLite extension.
- `@treecrdt/wa-sqlite` for browser apps using wa-sqlite, WASM assets, and optional worker/OPFS runtime support.

There is no `@treecrdt/sqlite` meta-package today. Keeping the runtime packages explicit avoids
accidentally pulling Node native dependencies into browser bundles or wa-sqlite/WASM assets into Node
apps.

## Shared Engine Surface

Both packages expose the common `TreecrdtEngine` shape from `@treecrdt/interface/engine`, so app code
can usually depend on the shared engine type after startup.

```ts
import type { TreecrdtEngine } from '@treecrdt/interface/engine';

async function readRootChildren(client: TreecrdtEngine) {
  return client.tree.children('0'.repeat(32));
}
```

## Node

Use `@treecrdt/sqlite-node` when the app owns a `better-sqlite3` database.

```ts
import Database from 'better-sqlite3';
import { createTreecrdtClient, loadTreecrdtExtension } from '@treecrdt/sqlite-node';

const db = new Database('treecrdt.db');
loadTreecrdtExtension(db);

const client = await createTreecrdtClient(db, { docId });
```

The Node package owns the native extension lookup/loading helpers and should not be imported from
browser code.

## Browser

Use `@treecrdt/wa-sqlite/client` for the high-level browser client and
`@treecrdt/wa-sqlite/vite-plugin` when a Vite app needs wa-sqlite assets copied into its public
directory.

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite/client';

const client = await createTreecrdtClient({ docId });
```

```ts
// vite.config.ts
import { treecrdt } from '@treecrdt/wa-sqlite/vite-plugin';

export default {
  plugins: [treecrdt()],
};
```

The browser package owns wa-sqlite asset loading, worker support, and OPFS helpers. Node apps should
use `@treecrdt/sqlite-node` instead.

## Lower-Level SQLite Helpers

Runtime-neutral SQLite material helpers live in `@treecrdt/interface/sqlite` and
`@treecrdt/sync-sqlite`. Those packages are for adapter/auth/sync plumbing, not the recommended app
startup import path.

## Decision

This repository is choosing explicit runtime packages over a unified conditional-export package for
now. Revisit a meta-package only if we have a concrete app integration that benefits from one import
enough to justify the bundler and dependency-separation risks.
