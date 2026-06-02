# @treecrdt/sync

High-level **client** library for TreeCRDT sync v0. It combines **`@treecrdt/discovery`** ([`packages/discovery`](../discovery), resolve a `docId` to a websocket URL), **`@treecrdt/sync-protocol`** ([`packages/sync-protocol/protocol`](../sync-protocol/protocol), v0 `SyncPeer`, codecs, transports), and **`@treecrdt/sync-sqlite`** ([`packages/sync-protocol/material/sqlite`](../sync-protocol/material/sqlite), material layer → `SyncBackend`).

`connectTreecrdtWebSocketSync` needs a spec `WebSocket` client: the global in browsers, or **`webSocketCtor`** (e.g. `import { WebSocket } from "undici"`) in Node. For a fully custom path, use **`createTreecrdtWebSocketSyncFromTransport`**, which only needs a `DuplexTransport` (no `WebSocket`).

## When to use this package

- You want a single dependency to **open a websocket** to a sync server and run reconciliation against a SQLite-backed client store.
- You are fine with the built-in discovery + WebSocket + protobuf wiring.

## Multi-peer outbound upload

Use `createOutboundSync` with a `localPeer` when one `SyncPeer` owns several transports, such as
local-tab mesh peers plus a remote websocket server. The app still manages transport discovery, but
outbound sync owns the committed-local-op hook: it wakes live subscriptions on the `localPeer` and
queues exact local-op upload for registered outbound targets with dedupe and offline retry.

```ts
import { createOutboundSync } from '@treecrdt/sync';

const outbound = createOutboundSync({
  localPeer: peer,
  isOnline: () => navigator.onLine,
});

outbound.addPeer('remote:server', websocketTransport);

const op = await client.local.payload(replica, node, payload);
outbound.queueOps([op]); // live subscription wakeup + remote websocket upload/retry
```

`queueOps` dedupes standard TreeCRDT `Operation` values by `meta.id`. Pass `opKey` only for a
custom op shape or custom coalescing behavior.

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
