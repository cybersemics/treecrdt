# @treecrdt/sync

High-level **client** library for TreeCRDT sync v0. It combines **`@treecrdt/discovery`** ([`packages/discovery`](../discovery), resolve a `docId` to a websocket URL), **`@treecrdt/sync-protocol`** ([`packages/sync-protocol/protocol`](../sync-protocol/protocol), v0 `SyncPeer`, codecs, transports), and **`@treecrdt/sync-sqlite`** ([`packages/sync-protocol/material/sqlite`](../sync-protocol/material/sqlite), material layer → `SyncBackend`).

`connectTreecrdtWebSocketSync` needs a spec `WebSocket` client: the global in browsers, or **`webSocketCtor`** (e.g. `import { WebSocket } from "undici"`) in Node. For a fully custom path, use **`createTreecrdtWebSocketSyncFromTransport`**, which only needs a `DuplexTransport` (no `WebSocket`).

## When to use this package

- You want a single dependency to **open a websocket** to a sync server and run reconciliation against a SQLite-backed client store.
- You are fine with the built-in discovery + WebSocket + protobuf wiring.

## Recommended app path

Use `connectSyncController` when app writes may happen before sync startup has fully
settled. It buffers local ops through `start()`, keeps failed pushes queued for retry, and
reports lifecycle status.

```ts
import { connectSyncController } from '@treecrdt/sync';

const sync = await connectSyncController(client, {
  baseUrl,
  auth: authSession?.syncAuth,
  controller: {
    onStatus: (status) => console.log(status.state, status.pendingOps),
    onError: console.error,
  },
});

await sync.start();

const op = await client.local.insert(replica, parent, node, { type: 'last' }, payload);
await sync.pushLocalOps([op]);
```

`pushLocalOps` is safe before `start()` too:

```ts
const op = await client.local.payload(replica, node, payload);
await sync.pushLocalOps([op]); // queued if startup is not ready yet
await sync.start(); // queued ops flush as part of startup
```

For custom transports or tests, create a low-level sync handle with
`createTreecrdtWebSocketSyncFromTransport` and wrap it with `createSyncController`.

## Multi-peer apps

Use `createOutboundSync` with a `localPeer` when one `SyncPeer` owns several transports, such as
local-tab mesh peers plus a remote websocket server. The app still manages transport discovery, but
outbound sync owns local-op upload queues, dedupe, offline retry, and fallback reconciliation.

```ts
import { createOutboundSync } from '@treecrdt/sync';

const outbound = createOutboundSync({
  localPeer: peer,
  opKey: (op) => `${bytesToHex(op.meta.id.replica)}:${op.meta.id.counter}`,
  isOnline: () => navigator.onLine,
  shouldSyncPeer: (peerId) => peerId.startsWith('remote:'),
});

outbound.addPeer('remote:server', websocketTransport);

const op = await client.local.payload(replica, node, payload);
await peer.notifyLocalUpdate([op]); // local mesh fanout
outbound.queue([op]); // remote websocket upload/retry
```

## When not to

- You only need the protocol types and `SyncPeer` (use **`@treecrdt/sync-protocol`**).
- You use a custom transport, no discovery, or an in-memory backend (depend on the protocol and/or **`@treecrdt/discovery`** as needed).
- You want exact low-level control over each `syncOnce`, `startLive`, and direct push call; use
  `connectTreecrdtWebSocketSync` directly.

## Repo location

- Source: `packages/treecrdt-sync` in this monorepo.
- Spec and layout: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md), [docs/sync/v0.md](../../docs/sync/v0.md).

## Build

```bash
pnpm -C packages/treecrdt-sync run build
```

## Test

```bash
pnpm -C packages/treecrdt-sync run test
```

Builds and tests assume sibling packages under `packages/sync-protocol/` are available (workspace layout).
