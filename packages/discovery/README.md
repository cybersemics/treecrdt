# @treecrdt/discovery

Connect-time bootstrap contract for resolving a doc to an attachment plan.

This package is intentionally separate from `@treecrdt/sync` and the CRDT core.
It covers:

- `resolveDoc`: return an attachment plan for a known doc
- cache helpers for "resolve once, reconnect directly later"
- shared types used by standalone bootstrap servers such as `@treecrdt/discovery-server-node`

Typical flow:

1. client calls `resolveDoc`
2. client caches the returned attachment plan
3. client connects directly to the returned websocket endpoint

This keeps bootstrap out of the steady-state sync hot path.

Out of scope:

- sync protocol details
- storage or backend implementation
- regional routing policy
