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
- `@treecrdt/interface`: shared TypeScript interfaces.
- `@treecrdt/content`: versioned app-layer codecs for payload text and inline images.
- `@treecrdt/sync-protocol`: transport-agnostic sync protocol runtime.
- `@treecrdt/discovery`: bootstrap contract for resolving docs to sync attachments.
- `@treecrdt/sync-server-postgres-node`: Postgres-backed WebSocket sync server.

See the package READMEs for package-specific setup and API details.

## Playground

- Live demo: https://cybersemics.github.io/treecrdt/
- Local playground instructions: [examples/playground/README.md](examples/playground/README.md)
- Local Postgres sync server instructions: [packages/sync-protocol/server/postgres-node/README.md](packages/sync-protocol/server/postgres-node/README.md)

## Benchmarks

For benchmark commands, product-facing note/sync scenarios, and the sync target matrix, see [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Contributing

For PR expectations, local checks, and changeset/release notes, see [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).
