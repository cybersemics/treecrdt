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
- `@treecrdt/wasm/memory`: synchronous in-memory client after async WASM initialization.
- `@treecrdt/sync`: client sync over discovery + WebSocket + SQLite backends.
- `@treecrdt/interface`: shared operation and engine types, adapter contracts, and SQLite bindings.
- `@treecrdt/sync-protocol`: transport-agnostic sync protocol runtime.
- `@treecrdt/discovery`: bootstrap contract for resolving docs to sync attachments.
- `@treecrdt/sync-server-postgres-node`: Postgres-backed WebSocket sync server.

See the package READMEs for package-specific setup and API details.

## Synchronous memory client

```ts
import { createMemoryClient } from '@treecrdt/wasm/memory'

const memory = await createMemoryClient()
const root = '0'.repeat(32)
const node = '1'.padStart(32, '0')
const { operations } = memory.transact(transaction => {
  transaction.local.insert(root, node, undefined, new TextEncoder().encode('hello'))
  return transaction.get(node) // reads the preceding write
})
```

Transactions are synchronous: they either commit all operations or roll back, and their handles expire when the callback returns. Reads return owned rows from the current tree, not retained tree versions. `getChanges()` reports cumulative before/after rows within a transaction; `subscribe` publishes committed changes. Use the returned operations for persistence, including operations that leave the view unchanged. Memory commits alone are not durable.

`createMemorySyncBackend` from `@treecrdt/wasm/sync` connects the client to the existing sync protocol with full-document (`{ all: {} }`) filters. Partial hydration and eviction are not supported. Call `memory.close()` when finished.

## Playground

- Live demo: https://cybersemics.github.io/treecrdt/
- Local playground instructions: [examples/playground/README.md](examples/playground/README.md)
- Local Postgres sync server instructions: [packages/sync-protocol/server/postgres-node/README.md](packages/sync-protocol/server/postgres-node/README.md)

## Benchmarks

For benchmark commands, product-facing note/sync scenarios, and the sync target matrix, see [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Contributing

For implementation and review expectations, see [AGENTS.md](AGENTS.md).
