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
pnpm benchmark:sync:prime
pnpm benchmark:sync:remote
pnpm benchmark:sync:bootstrap
pnpm benchmark:web
pnpm benchmark:wasm
pnpm benchmark:postgres
```

`pnpm benchmark` writes JSON results under `benchmarks/`.

## Which Benchmark Answers What?

- First view on a new device, structure only: `benchmark:sync:*` with `sync-balanced-children-cold-start`
- First view on a new device, with payloads: `benchmark:sync:*` with `sync-balanced-children-payloads-cold-start`
- Re-sync the same subtree on a restarted client that already has that scope locally: `benchmark:sync:*` with `sync-balanced-children-resync` or `sync-balanced-children-payloads-resync`
- Single end-to-end time-to-first-visible-page number: `benchmark:sync:*` with the same balanced workloads plus `--first-view`
- One-time bootstrap/discovery tax before opening the regional websocket: `benchmark:sync:bootstrap`
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

### Re-Sync The Same Subtree

Balanced-tree re-sync is the closest current benchmark to "restart a client that
already has this subtree locally, then reconcile that same scope again".

```sh
pnpm sync-server:postgres:db:start
pnpm benchmark:sync:local -- \
  --workloads=sync-balanced-children-resync,sync-balanced-children-payloads-resync \
  --counts=10000,100000 \
  --fanout=10
pnpm sync-server:postgres:db:stop
```

These workloads keep the same balanced immediate-subtree shape as the first-view
benchmarks, but the receiver already has the current scoped result. That means
they measure the normal non-empty scoped reconcile path instead of the
empty-receiver direct-send shortcut.

### Prime Sync Server Fixtures

Use this when you want to prebuild sync-server fixtures before running the actual sync benchmarks.

```sh
pnpm benchmark:sync:prime
```

By default this primes the read-only first-view workloads for `10k`, `50k`, and `100k` nodes and forces a rebuild. After that, matching local benchmark runs reuse those fixtures as cache hits instead of reimporting the same large server docs.

You can still override the forwarded args:

```sh
pnpm benchmark:sync:prime -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --counts=50000,100000 \
  --server-fixture-cache=rebuild
```

You can also prime the remote target explicitly:

```sh
TREECRDT_SYNC_SERVER_URL=ws://host-or-elb/sync \
pnpm benchmark:sync:remote prime -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --server-fixture-cache=rebuild
```

Then benchmark against that already-seeded remote doc without reseeding:

```sh
TREECRDT_SYNC_SERVER_URL=ws://host-or-elb/sync \
pnpm benchmark:sync:remote -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --first-view \
  --server-fixture-cache=reuse
```

For remote targets, `prime` now records the exact fixture doc ID locally under `tmp/sqlite-node-sync-bench/server-fixtures/`. That means a fresh endpoint can be primed once with `--server-fixture-cache=rebuild`, and later `--server-fixture-cache=reuse` runs on the same machine can reopen that exact remote fixture doc instead of relying on historical deterministic fixture residue.

By default, the local sync target runs the Postgres sync server in a spawned child process so local and remote measurements are closer to each other. When you add `--profile-backend`, the local target intentionally switches to the in-process server so per-backend timings are visible inside the benchmark process.

Local server benchmarks now seed the Postgres backend directly before the timer starts. That keeps the measured path honest, because the actual sync to the client still goes through the real websocket server, while avoiding huge protocol-seed setup costs that are not part of the benchmark question.

For read-only local server workloads, the harness now prepares that server fixture once per benchmark case and reuses it across warmup and measured samples. It also reuses the same seeded Postgres fixture across separate benchmark runs by default when the workload definition matches, so repeated `50k/100k` runs do not keep reimporting the same large server doc.

Use `--server-fixture-cache=rebuild` when you want to force a fresh fixture, or `--server-fixture-cache=off` when you want every run to seed an isolated throwaway fixture. For remote fixtures, `--server-fixture-cache=reuse` assumes the deterministic fixture doc already exists and skips reseeding.

### Time To First Visible Page

Add `--first-view` when you want one number that includes:

- scoped sync into the local store
- the immediate local `childrenPage(...)` read
- payload fetches for the parent and visible children when the workload carries payloads

```sh
pnpm benchmark:sync:local -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --counts=10000 \
  --fanout=10 \
  --first-view
```

For custom `--count` or `--counts` runs, the sync bench now defaults to multiple measured samples instead of silently falling back to one. Use `--iterations=N` and `--warmup=N` when you want explicit control over stability versus runtime.

Add `--post-seed-wait-ms=N` when you want to probe whether immediate post-upload backlog is skewing the measured first-view path. This is mainly a debugging aid for remote runs.

### Upload Benchmarks

Use prime/upload mode when you want an explicit benchmark for seeding a sync-server doc.

This measures the full server-fixture creation path and writes a result file under `benchmarks/sqlite-node-sync/server-fixture-*.json` with `durationMs`, `opsPerSec`, and the seeded `fixtureOpCount`.

```sh
TREECRDT_SYNC_SERVER_URL=ws://host/sync \
pnpm benchmark:sync:upload:remote -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --server-fixture-cache=rebuild
```

Use this to answer a different question than first-view:

- `benchmark:sync:remote ... --first-view` answers "how fast can a new device open an existing subtree?"
- `benchmark:sync:upload:remote ...` answers "how long does it take to upload and materialize a large tree on the sync server?"

### Small-Scope Direct Send

Add `--direct-send-threshold=N` when you want to experiment with a clean-slate shortcut for small scoped syncs.

When enabled, if the requesting peer has an empty local result for the requested filter and the responder has at most `N` matching ops, the protocol skips the RIBLT round and sends the scoped ops directly in `opsBatch`.

This is most relevant for first-view note loading where the client knows the scope root but has not synced its immediate children yet.

```sh
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
pnpm benchmark:sync:local -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --fanout=10 \
  --first-view \
  --direct-send-threshold=64
```

Add `--max-ops-per-batch=N` when you want to force smaller `opsBatch` messages. This is useful for stress-testing large upload paths and for debugging remote seed behavior where very large inbound batches may monopolize a server task.

```sh
TREECRDT_SYNC_SERVER_URL=ws://host/sync \
pnpm benchmark:sync:remote -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --first-view \
  --direct-send-threshold=64 \
  --max-ops-per-batch=500
```

### Bootstrap / Resolve Bench

Use `benchmark:sync:bootstrap` when you want to isolate the one-time discovery
layer from the steady-state sync path.

The benchmark target can be a standalone bootstrap server such as
`@justtemporary/discovery-server-node`, not just a colocated sync-server route.

It measures:

- `resolveSamplesMs`: `GET /resolve-doc?docId=...`
- `connectSamplesMs`: first websocket open after resolve
- `totalSamplesMs`: resolve + first websocket open
- `cachedConnectSamplesMs`: direct websocket reconnect using the already resolved attachment

```sh
TREECRDT_DISCOVERY_URL=https://bootstrap-host \
pnpm benchmark:sync:bootstrap -- \
  --iterations=5
```

This is the benchmark to use when you want to answer:

- how expensive the bootstrap lookup is on cold open
- how much faster cached reconnects are
- whether discovery is staying off the steady-state hot path

### Backend Call Profiling

Add `--profile-backend` when you want per-backend timings for:

- `listOpRefs`
- `getOpsByOpRefs`
- `applyOps`

This is especially useful on the local Postgres sync-server target because it shows whether the bottleneck is on the client SQLite side or the server Postgres side.

```sh
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
pnpm benchmark:sync:local -- \
  --workloads=sync-balanced-children-cold-start,sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --fanout=10 \
  --profile-backend
```

### Transport Profiling

Add `--profile-transport` when you want sync message counts, encoded byte counts, and a short event timeline showing where time is spent across the handshake, RIBLT exchange, and ops batches.

```sh
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
pnpm benchmark:sync:local -- \
  --workloads=sync-balanced-children-cold-start,sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --fanout=10 \
  --profile-transport
```

### Hello Stage Profiling

Add `--profile-hello` when you want the responder-side `hello -> helloAck` path broken into internal stages such as:

- `maxLamport`
- `listOpRefs`
- `filterOutgoingOps` when auth filtering is active
- decoder setup
- `helloAck` send

This is the right profiler when the coarse transport timeline says `hello -> helloAck` is expensive and you need to know whether that cost is database work, auth filtering, or protocol setup.

```sh
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
pnpm benchmark:sync:local -- \
  --workloads=sync-balanced-children-payloads-cold-start \
  --count=10000 \
  --fanout=10 \
  --first-view \
  --profile-hello
```

For `local-postgres-sync-server`, child-process runs capture hello traces by parsing the server process output. For `direct` and in-process debug runs, the benchmark collects the same trace in-process without writing debug noise into the result stream. Remote runs currently only have the coarse transport profile, not internal server hello stages.

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

The Docker helper is only a convenience. The local sync benchmark just needs a reachable Postgres URL, so a native local Postgres instance works too:

```sh
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
pnpm benchmark:sync:local -- --workloads=sync-balanced-children-payloads-cold-start --count=10000
```

### Remote Sync Server URL

The remote URL is intentionally not hardcoded in this repo. Different deployments can have different latency, auth, retention, and scaling settings, so pass it at runtime through `TREECRDT_SYNC_SERVER_URL` or `--sync-server-url=...`.

For public HTTPS deployments, prefer `wss://.../sync`. Use `ws://.../sync` for local or other plain HTTP deployments.

## Current Sync Workloads

Current sync workload definitions live in `packages/treecrdt-benchmark/src/sync.ts`.

Product-facing defaults:

- `sync-one-missing`: narrow protocol baseline for a tiny delta
- `sync-balanced-children-cold-start`: new device already knows the scope root and pulls the immediate children of a node from a balanced tree
- `sync-balanced-children-payloads-cold-start`: same balanced-tree cold-start path, plus payloads
- `sync-balanced-children-resync`: same balanced immediate-subtree shape, but the client already has that scoped result locally and re-runs scoped reconcile
- `sync-balanced-children-payloads-resync`: same balanced re-sync path, plus payloads for the scope root and those immediate children

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
- `--first-view`
- `--iterations=5`
- `--warmup=1`
- `--profile-backend`
- `--profile-transport`
- `--profile-hello`
- `--sync-server-url=ws://host/sync`
- `--postgres-url=postgres://...`

Common note-path flags:

- `--benches=read-children-payloads,insert-into-large-tree`
- `--counts=10000,50000,100000`
- `--fanout=10`
- `--page-size=10`
- `--payload-bytes=512`

## What Is Still Missing?

The remaining gaps are mostly infrastructure-related now:

- a healthy, repeatable local Postgres bootstrap path that does not depend on a stuck Docker daemon
- a working public websocket deployment path for the remote sync target
