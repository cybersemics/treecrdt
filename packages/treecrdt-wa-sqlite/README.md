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
WASM and adapter together. Persistent OPFS databases are in WAL mode, which the
OPFS VFSs support only with exclusive locking: a connection that opens one must run
`PRAGMA locking_mode=EXCLUSIVE` before its first statement.

## Browser usage

Use `createTreecrdtClient()` with a required `docId` and optional `persistent`, `filename`, and `crossTab` options. Browser apps should use `@treecrdt/wa-sqlite/vite-plugin` to copy the JS assets into `public/wa-sqlite/` (asset URLs resolve from Vite's `BASE_URL`). Vite includes the imported WASM in its asset graph and emits it with a content hash.

```ts
import { createTreecrdtClient } from '@treecrdt/wa-sqlite';

// In-memory (default): direct in-process runtime.
const memoryClient = await createTreecrdtClient({ docId: 'my-doc' });

// Durable: OPFS storage in a dedicated worker. Throws when OPFS is unavailable
// (e.g. on an insecure origin) — there is no silent memory fallback. Cross-origin
// isolation (COOP/COEP headers) is not required.
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

Persistent databases use SQLite's WAL journal with exclusive locking, which roughly
halves the cost of a single-operation commit. A database is held by one client until
it closes: without `crossTab`, another client opening the same database, for example
in a second tab, waits until the first client closes or its tab goes away. Use
`crossTab: true` to share a persistent database between tabs. Tabs share a cross-tab
worker only when they load the same worker script URL. Bundlers usually hash that URL
by content, so after a deploy that changes the worker, a tab on the new build waits
the same way until the old build's tabs close their clients or go away.

Callers only see `TreecrdtClient` (`ops` / `tree` / `local` / `onMaterialized` / `onMaterializedDurable` / `close` / `drop`).

### Keeping derived data in step

`onMaterializedDurable(name, listener)` is `onMaterialized` that remembers how far the listener got. Use it for anything derived from the tree that has to survive restarts, such as an index table:

```ts
const subscription = await client.onMaterializedDurable(
  'search-index',
  async (event) => {
    for (const change of event.changes) await updateIndex(change);
  },
  { version: 1, reset: () => clearIndex() },
);

await client.local.insert(replica, parent, node, { type: 'last' }, payload);
await subscription.settled(); // the index now includes the insert
await subscription.close();
```

- The call resolves once the listener has caught up with every change since it last ran. With no stored position for `version`, `reset` runs first and every node is delivered.
- Events arrive in order, one at a time and at least once. If the listener rejects, its position stays where it was, and the same changes are delivered again with the next event or the next `settled()`.
- After a gap, a change reports a node's current state instead of how it got there: a live node as `insert` (a root as `payload`), a removed one as `delete`. Apply each change as "make it so", because a node reported as `insert` may already be in your index.
- Events are forwarded in the order the client emits them. One that arrives after a later one is not applied late; the subscription reads the affected nodes' current state instead.
- The position is stored in the client's database, so dropping the database drops it. It is saved after a catch-up, at most every two seconds while events flow, and on `close()`. `client.close()` closes open subscriptions first.
- A name can be open once per client.

`tree.changedSince(since)` is the read underneath, for callers that keep their own cursor. It returns nodes whose visible state changed after a `headSeq`, once each, with their current state. Changes are recorded in the same commit as the state. Save the cursor only after applying every returned page successfully, so a failed update can be retried after a crash or reload. `since = 0` reads every node.

```ts
let page = await client.tree.changedSince(loadCursor() ?? 0, { payloads: true, limit: 500 });
for (;;) {
  for (const node of page.nodes) updateIndex(node);
  if (!page.next) break;
  page = await client.tree.changedSince(page.next, { payloads: true, limit: 500 });
}
saveCursor(page.headSeq);
```

Each node comes with its current state, so applying it twice is harmless: if the app stops before `saveCursor`, the next catch-up repeats those updates. Don't hold a transaction open across the loop on the client's own database. Other writes on that connection would run inside it, and a rollback would discard ops whose events were already delivered. A cursor ahead of the database's `headSeq`, such as one saved before the database was dropped, is rejected; read from 0 to rebuild.

Pages are not a frozen snapshot: a node changed again before its page is read is deferred to the next catch-up. Retain notifications received during paging and catch up again afterward; completing a pass does not guarantee the index is current with concurrent writes.

`onMaterialized` remains the low-latency signal while the client is open; `changedSince` is what to rely on across restarts. Reading from an older `headSeq` scans the change table, which keeps recording cheap, so call it to catch up after a gap rather than after every write.

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
