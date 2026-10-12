# treecrdt

TreeCRDT is a SQLite-backed tree CRDT workspace for browser clients, Node clients, sync protocol packages, and sync server packages.

## Quick Start

```sh
pnpm install
pnpm build
pnpm test
```

## Main Packages

- `@treecrdt/wa-sqlite`: browser SQLite client adapter (in-memory WASM on Node).
- `@treecrdt/sync`: client sync over discovery + WebSocket + SQLite backends.
- `@treecrdt/interface`: shared operation and engine types, adapter contracts, and SQLite bindings.
- `@treecrdt/sync-protocol`: transport-agnostic sync protocol runtime.
- `@treecrdt/discovery`: bootstrap contract for resolving docs to sync attachments.
- `@treecrdt/sync-server-postgres-node`: Postgres-backed WebSocket sync server.

See the package READMEs for package-specific setup and API details.

## Synchronous memory client

`@treecrdt/wasm/memory` loads WASM asynchronously, then exposes synchronous reads and writes:

```ts
import { createMemoryClient } from '@treecrdt/wasm/memory';

const memory = await createMemoryClient();
const operation = memory.local.insert('0', '1', { type: 'last' }, new TextEncoder().encode('Hello'));
const row = memory.get('1'); // Reads the preceding write immediately.
// Persist or send the exact operation; memory alone is not durable storage.
memory.close();
```

Reads return owned current values, not retained tree versions. Operation cursors count accepted
operations in arrival order, not Lamport time or cross-replica revisions.
`createMemorySyncBackend` from `@treecrdt/wasm/sync` connects the client to the existing sync protocol;
this initial backend supports full-document filters only.

## Playground

- Live demo: https://cybersemics.github.io/treecrdt/
- Local playground instructions: [examples/playground/README.md](examples/playground/README.md)
- Local Postgres sync server instructions: [packages/sync-protocol/server/postgres-node/README.md](packages/sync-protocol/server/postgres-node/README.md)

## Benchmarks

For benchmark commands, product-facing note/sync scenarios, and the sync target matrix, see [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Contributing

For implementation and review expectations, see [AGENTS.md](AGENTS.md).
