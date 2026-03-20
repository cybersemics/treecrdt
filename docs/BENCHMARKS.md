# Benchmarks

The benchmark suite is most useful when you treat it as a set of product questions, not just a set of packages.

## Start Here

From the repo root:

```sh
pnpm benchmark
pnpm benchmark:sync:help
```

Useful top-level entrypoints:

```sh
pnpm benchmark
pnpm benchmark:sqlite-node
pnpm benchmark:sqlite-node:ops
pnpm benchmark:sqlite-node:note-paths
pnpm benchmark:sync
pnpm benchmark:sync:direct
pnpm benchmark:sync:local
pnpm benchmark:sync:remote
pnpm benchmark:web
pnpm benchmark:wasm
pnpm benchmark:postgres
```

`pnpm benchmark` writes JSON results under `benchmarks/`.

## Which Benchmark Answers What?

- First view on a new device, structure only: `benchmark:sync:*` with `sync-balanced-children-cold-start`
- First view on a new device, with payloads: `benchmark:sync:*` with `sync-balanced-children-payloads-cold-start`
- Local render cost after the data is already present: `benchmark:sqlite-node:note-paths -- --benches=read-children-payloads`
- Local mutation cost inside a large existing tree: `benchmark:sqlite-node:note-paths -- --benches=insert-into-large-tree`
- Protocol/storage baselines and worst-case stress: `sync-one-missing`, `sync-all`, `sync-children*`, `sync-root-children-fanout10`

That split is intentional:

- The sync benches answer "how long until the needed subtree data is in the local store?"
- The note-path benches answer "once the data is local, how quickly can the app render and mutate it?"

## Recommended Product-Facing Runs

### First View Sync

Balanced-tree cold-start sync is the closest current benchmark to "open a node on a fresh device and load the first visible page".

```sh
pnpm benchmark:sync:direct -- \
  --workloads=sync-balanced-children-cold-start,sync-balanced-children-payloads-cold-start \
  --counts=10000,50000,100000 \
  --fanout=10
```

```sh
pnpm sync-server:postgres:db:start
pnpm benchmark:sync:local -- \
  --workloads=sync-balanced-children-cold-start,sync-balanced-children-payloads-cold-start \
  --counts=10000,50000,100000 \
  --fanout=10
pnpm sync-server:postgres:db:stop
```

```sh
TREECRDT_SYNC_SERVER_URL=ws://host-or-elb/sync \
pnpm benchmark:sync:remote -- \
  --workloads=sync-balanced-children-cold-start,sync-balanced-children-payloads-cold-start \
  --counts=10000,50000,100000 \
  --fanout=10
```

Use `--fanout=20` when you want to model a broader notebook tree.

### Local First View Read Path

This measures the app-shaped local read immediately after sync: fetch the visible children page plus payloads for the parent and those children.

```sh
pnpm benchmark:sqlite-node:note-paths -- \
  --benches=read-children-payloads \
  --counts=10000,50000,100000 \
  --fanout=10 \
  --page-size=10 \
  --payload-bytes=512
```

### Local Mutation in a Large Tree

This measures inserting one node with a payload into an already-large balanced tree.

```sh
pnpm benchmark:sqlite-node:note-paths -- \
  --benches=insert-into-large-tree \
  --counts=10000,50000,100000 \
  --fanout=10 \
  --payload-bytes=512
```

## Sync Targets

The sync runner supports the same workload definitions across multiple environments:

- `direct`: in-memory connected peers, no sync server
- `local-postgres-sync-server`: local WebSocket sync server backed by Postgres
- `remote-sync-server`: remote WebSocket sync server

That keeps the workload constant while you compare transport and backend behavior.

### Local Postgres Defaults

```sh
postgres://postgres:postgres@127.0.0.1:5432/postgres
```

Override with `TREECRDT_POSTGRES_URL` or `--postgres-url=...`.

### Remote Sync Server URL

The remote URL is intentionally not hardcoded in this repo. Different deployments can have different latency, auth, retention, and scaling settings, so pass it at runtime through `TREECRDT_SYNC_SERVER_URL` or `--sync-server-url=...`.

For public HTTPS deployments, prefer `wss://.../sync`. Use `ws://.../sync` for local or other plain HTTP deployments.

## Current Sync Workloads

Current sync workload definitions live in `packages/treecrdt-benchmark/src/sync.ts`.

Product-facing defaults:

- `sync-one-missing`: narrow protocol baseline for a tiny delta
- `sync-balanced-children-cold-start`: new device already knows the scope root and pulls the immediate children of a node from a balanced tree
- `sync-balanced-children-payloads-cold-start`: same balanced-tree cold-start path, plus payloads

Specialized or synthetic workloads:

- `sync-all`: overlapping divergent peers reconcile all ops
- `sync-children`: scoped sync against a synthetic high-fanout parent
- `sync-children-cold-start`: same synthetic high-fanout shape in one-way mode
- `sync-children-payloads`: synthetic high-fanout subtree with payloads
- `sync-children-payloads-cold-start`: one-way version of that same synthetic high-fanout payload case
- `sync-root-children-fanout10`: balanced-tree root-children delta with a move boundary case

The `sync-children*` workloads are still worth keeping because they act as worst-case or stress-style scoped sync scenarios. They are not the best default proxy for normal note-taking, because they put a very large number of direct children under one parent.

## Useful Flags

All sync entrypoints forward arguments to `packages/treecrdt-sqlite-node/scripts/bench-sync.ts`.

Common sync flags:

- `--workloads=sync-balanced-children-cold-start,sync-balanced-children-payloads-cold-start`
- `--counts=100,1000,10000`
- `--count=1000`
- `--storages=memory,file`
- `--targets=direct,local-postgres-sync-server`
- `--fanout=10`
- `--sync-server-url=ws://host/sync`
- `--postgres-url=postgres://...`

Common note-path flags:

- `--benches=read-children-payloads,insert-into-large-tree`
- `--counts=10000,50000,100000`
- `--fanout=10`
- `--page-size=10`
- `--payload-bytes=512`

## What Is Still Missing?

The suite is much closer to real note-taking behavior now, but there is still one gap worth closing later:

- a single end-to-end benchmark that measures cold-start sync and the first local render in one number

Right now that path is covered in two pieces:

- sync time via `sync-balanced-children*-cold-start`
- local read time via `read-children-payloads`

That is already enough to identify where time is going, but an integrated "time to first visible page" benchmark would still be useful as a final product metric.
