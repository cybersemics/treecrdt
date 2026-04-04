# @treecrdt/discovery

Control-plane contract for document creation, bootstrap routing, and access listing.

This package is intentionally separate from `@treecrdt/sync` and the CRDT core.
It describes the metadata/bootstrap layer a product can use for:

- `createDoc`: create a doc and return an attachment plan
- `resolveDoc`: return an attachment plan for a known doc
- `listAccessibleDocs`: list docs a principal can discover in the product UI
- cached bootstrap helpers for "resolve once, reconnect directly later"

## Why this exists

Bootstrap routing and product discovery are not part of the steady-state sync hot
path. The intended flow is:

1. client calls `resolveDoc`
2. client caches the returned attachment plan
3. client opens a websocket directly to the chosen sync service
4. live sync stays entirely on the data plane

That keeps the discovery overhead mostly at connect time rather than per op.

## What this package is not

- not a sync protocol extension
- not a storage backend
- not an opinionated DynamoDB-only implementation

Concrete implementations can be layered later, for example:

- DynamoDB global tables for `docs_directory` and access indexes
- a Postgres-backed implementation
- an in-memory dev implementation

## Typical data split

- authoritative control plane:
  - `docId -> attachment plan`
  - principal access index
  - migration state
- data plane:
  - op log
  - materialized tables
  - live subscriptions
