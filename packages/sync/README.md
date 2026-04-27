# @treecrdt/sync

High-level **client** library for TreeCRDT sync v0 in browser or Node: combines **`@treecrdt/discovery`** ([`packages/discovery`](../discovery), resolve a `docId` to a websocket URL), **`@treecrdt/sync-protocol`** ([`packages/sync-protocol/protocol`](../sync-protocol/protocol), v0 `SyncPeer`, codecs, transports), and **`@treecrdt/sync-sqlite`** ([`packages/sync-protocol/material/sqlite`](../sync-protocol/material/sqlite), material layer → `SyncBackend`).

## When to use this package

- You want a single dependency to **open a websocket** to a sync server and run reconciliation against a SQLite-backed client store.
- You are fine with the built-in discovery + WebSocket + protobuf wiring.

## When not to

- You only need the protocol types and `SyncPeer` (use **`@treecrdt/sync-protocol`**).
- You use a custom transport, no discovery, or an in-memory backend (depend on the protocol and/or **`@treecrdt/discovery`** as needed).

## Repo location

- Source: `packages/sync` in this monorepo.
- Spec and layout: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md), [docs/sync/v0.md](../../docs/sync/v0.md).

## Build

```bash
pnpm -C packages/sync run build
```

## Test

```bash
pnpm -C packages/sync run test
```

Builds and tests assume sibling packages under `packages/sync-protocol/` are available (workspace layout).
