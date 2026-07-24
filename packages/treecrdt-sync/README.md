# @treecrdt/sync

High-level **client** library for TreeCRDT sync v0. It combines **`@treecrdt/discovery`** ([`packages/discovery`](../discovery), resolve a `docId` to a websocket URL), **`@treecrdt/sync-protocol`** ([`packages/sync-protocol/protocol`](../sync-protocol/protocol), v0 `SyncPeer`, codecs, transports), and **`@treecrdt/sync-sqlite`** ([`packages/sync-protocol/material/sqlite`](../sync-protocol/material/sqlite), material layer → `SyncBackend`).

`connectTreecrdtWebSocketSync` needs a spec `WebSocket` client: the global in browsers, or **`webSocketCtor`** (e.g. `import { WebSocket } from "undici"`) in Node. For a fully custom path, use **`createTreecrdtWebSocketSyncFromTransport`**, which only needs a `DuplexTransport` (no `WebSocket`).

## When to use this package

- You want a single dependency to **open a websocket** to a sync server and run reconciliation against a SQLite-backed client store.
- You are fine with the built-in discovery + WebSocket + protobuf wiring.

## Queued outbound upload

Use `createOutboundSync` to queue exact committed local ops for one replaceable remote destination.
The destination is only a push function, so the queue can sit above the high-level websocket handle
without taking ownership of its `SyncPeer` or transport.

```ts
import { createOutboundSync } from '@treecrdt/sync';

const outbound = createOutboundSync({
  isOnline: () => navigator.onLine,
});

const removeTarget = outbound.setTarget((ops, opts) => sync.pushLocalOps(ops, opts));

const op = await client.local.payload(replica, node, payload);
outbound.queueOps([op]);

const result = await outbound.flush();
if (result.status === 'failed') console.error(result.error);

window.addEventListener('online', () => void outbound.flush());
```

Installing a new target aborts an active push and replays its batch to the replacement. The cleanup
returned by `setTarget` is identity-safe: cleanup from an old socket cannot remove a newer target.
`queueOps` dedupes standard TreeCRDT `Operation` values by `meta.id`; pass `opKey` only for a custom
op shape or coalescing rule.

`flush()` reports `drained`, `deferred`, `failed`, or `closed`. Missing-target, offline, failed, and
timed-out pushes keep their operations queued. Setting a target triggers another attempt, but a
change in application-defined online state does not, so call `flush()` from the application's
online/reconnect event. `close()` is an async terminal barrier: it aborts and awaits the active
push, then discards queued work.

The queue is in memory, not durable across a reload or process exit; reconstruct or reconcile
outstanding work from durable CRDT state after startup.

Low-level `SyncPeer` users can pass `notifyLocalUpdate: ops => peer.notifyLocalUpdate(ops)` to wake
live mesh subscriptions while keeping those mesh transports separate from the single remote upload
target.

## Multi-peer inbound sync

Use `createInboundSync` to reconcile or subscribe the same filters across registered peer
transports. Despite the controller's inbound-oriented name, `syncOnce` performs bidirectional
reconciliation. It resolves only when every requested peer/filter target succeeds. Equivalent
filters are reconciled once. If any target fails, it finishes the remaining targets and rejects with
`InboundSyncAggregateError`; its `failures` entries identify the `peerId`, `filter`, and original
`error` for each failed target.

```ts
const inbound = createInboundSync({ localPeer: peer });
// The app has already attached websocketTransport to peer.
const unregisterRemote = inbound.addAttachedPeer('remote:server', websocketTransport);

await inbound.syncOnce({ all: {} }, { syncTimeoutMs: 10_000 });
inbound.subscribe([{ children: { parent: rootId } }]);

unregisterRemote();
await inbound.close();
```

`addAttachedPeer` only registers a transport; it deliberately does not attach it to or detach it from
the `SyncPeer`. This keeps ownership with the app when inbound and outbound controllers share a
transport. Its returned cleanup is bound to that exact registration and is safe to call after a
replacement has claimed the same peer id. Use `removePeer(peerId)` when intentionally removing the
current registration by id. Registering the same peer id and transport more than once creates
independent leases without restarting its subscription; the registration remains until its last
cleanup runs.

A `syncTimeoutMs` deadline aborts the underlying `SyncPeer.syncOnce` session, so reconciliation does
not keep sending after the caller receives the timeout failure. Replacing or removing a peer also
aborts reconciliation still using its stale registration. Concurrent `syncOnce` calls are
independent.

`close()` is terminal and idempotent. It aborts active reconciliation, stops subscriptions, and
resolves after their `syncOnce`, `ready`, and `done` promises settle. After closing, status remains
readable and repeated `close()` calls return the same promise; other operations throw or reject.

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
