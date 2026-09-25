# @treecrdt/wasm (experimental)

An explicitly initialized, synchronous Rust/WASM memory client. It has no SQLite storage and does not persist writes by itself.

```ts
import { createMemoryClient } from '@treecrdt/wasm';

const client = await createMemoryClient();
const result = client.transact((document) => {
  document.local.insert('0'.repeat(32), '1'.repeat(32), null, new TextEncoder().encode('hello'));
  return document.getSnapshot(); // read your own writes synchronously
});
// Persist or transmit these exact typed operations, without reauthoring them.
await durableReplica.ops.appendMany(result.operations);
client.close();
```

Only initialization is asynchronous. `local.insert(parent, node, afterId, payload)`, `move(node, parent, afterId)`,
`payload(node, payload)` and `delete(node)` return typed `Operation` objects synchronously. `null`/omitted `afterId`
means first. Omitted/null payload inserts a payload-less node or clears its payload. Calls outside `transact` are
standalone atomic transactions. Nested or asynchronous transaction callbacks are rejected.

`getSnapshot()` is a read-only map of all live nodes, including the reserved root. Rows contain `id`, `parentId`,
binary `payload` (or `null`), and ordered `children`. Unchanged snapshots and rows
retain identity. Held snapshots cannot be mutated through map, row, child-list or payload references. Reads within a
transaction are provisional; rollback restores the exact previous snapshot identity and the native operation log.
`subscribe(listener)` publishes once after a successful visible snapshot change, never midway through a transaction;
subscriber errors are reported without rolling back the committed edit. It returns an unsubscribe function.

`tree.children`, `exists`, `parent`, and `payload` query native state synchronously. `tree.get` and `tree.snapshot`
read the cached snapshot. The committed log is available through `operationCount()`, `operationsFrom(cursor)` and
`operationsAt(indices)`. Cursors are arrival-order indexes, not Lamport timestamps. `appendOperations` applies typed
remote operations atomically and ignores duplicate delivery. Operation-log reads are rejected during transactions.

## Loading and builds

`pnpm run build` generates one web WASM artifact and compiles both loaders. The default export condition chooses the
browser or Node loader; explicit `@treecrdt/wasm/browser` and `@treecrdt/wasm/node` entries are also available. Importing
does not initialize WASM. Browsers load a relative asset URL; Node reads the same artifact from disk. An explicit
`wasm` option accepts bytes, a compiled module, response, or URL. Bundlers can resolve the exported
`@treecrdt/wasm/treecrdt_wasm_bg.wasm` asset and pass its URL when necessary. There is no top-level await.
For Vite development, exclude `@treecrdt/wasm` from `optimizeDeps` to preserve its relative WASM URL, or pass an
explicit asset URL using the `wasm` option.

For local consumption, build then `pnpm pack --pack-destination /absolute/path/to/artifacts`; install the generated
tarball in the consumer. This captures resolved workspace dependency versions and includes the generated WASM asset.

Pushes to `prototype/synchronous-wasm-view` build and test this package, then attach `treecrdt-wasm.tgz` to a
`wasm-preview-<commit>` GitHub prerelease. Consumers can pin that asset URL without installing Rust or building
TreeCRDT locally. Preview releases do not publish to npm or change the stable release.

## Synchronization

```ts
import { createMemorySyncBackend } from '@treecrdt/wasm/sync';
const backend = createMemorySyncBackend(client, { docId: 'stable-document-id' });
```

This separate entry implements `SyncBackend<Operation>` with canonical v0 operation references and only a
reference-to-native-log-index lookup, not a duplicate operation store. It supports full-document filters only;
children filters are explicitly rejected. It does not configure transports, authorization, or persistence.

## Checks

After building workspace dependencies, `pnpm test` runs native-WASM transactions, immutable snapshots, rollback,
subscriptions, typed synchronization, and the applicable shared operation-conformance scenarios. `pnpm test:browser`
builds a real Vite browser fixture at a non-root base URL and checks it in Chromium. SQLite persistence/auth and
pagination conformance are not claimed. `@treecrdt/wasm/adapter` retains the async benchmark adapter.
