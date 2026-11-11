# @treecrdt/sync-server-postgres-node

## 0.1.5

### Patch Changes

- bde0313: Add leveled sync-server logs. `TREECRDT_LOG_LEVEL` selects `debug`, `info`, `warn`, `error`, or `silent`, and defaults to `info` even when `NODE_ENV` is unset. JSON is the default format, and `TREECRDT_LOG_PRETTY=true` opts into pretty lines. Debug logs include each sync message the server sends or receives. A failed Postgres LISTEN connection exits the CLI or shuts down the library server by default. Clean WebSocket closes, including closes without a status code, log as disconnects.
- Updated dependencies [bde0313]
  - @treecrdt/sync-server-core@0.1.5

## 0.1.4

### Patch Changes

- Updated dependencies [fc2c6e5]
- Updated dependencies [a5d694f]
  - @treecrdt/interface@0.4.0
  - @treecrdt/sync-postgres@0.1.4
  - @treecrdt/sync-protocol@0.2.1
  - @treecrdt/sync-server-core@0.1.4

## 0.1.3

### Patch Changes

- d582500: Remove capability-CWT authentication because the server does not enforce token grants or signed operation authorship end to end.
- Updated dependencies [e1a6f4d]
- Updated dependencies [e69c29b]
- Updated dependencies [2594f3d]
- Updated dependencies [4686794]
- Updated dependencies [be09f09]
- Updated dependencies [e829a41]
  - @treecrdt/interface@0.3.0
  - @treecrdt/sync-protocol@0.2.0
  - @treecrdt/sync-server-core@0.1.3
  - @treecrdt/sync-postgres@0.1.3

## 0.1.2

### Patch Changes

- Updated dependencies [d859c20]
  - @treecrdt/sync-protocol@0.1.2
  - @treecrdt/sync-postgres@0.1.2
  - @treecrdt/sync-server-core@0.1.2
  - @treecrdt/auth@0.1.2

## 0.1.1

### Patch Changes

- Updated dependencies [2f864ec]
- Updated dependencies [60950b7]
  - @treecrdt/interface@0.2.0
  - @treecrdt/sync-postgres@0.1.1
  - @treecrdt/sync-protocol@0.1.1
  - @treecrdt/auth@0.1.1
  - @treecrdt/sync-server-core@0.1.1

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.

### Patch Changes

- Updated dependencies [ed5a001]
  - @treecrdt/auth@0.1.0
  - @treecrdt/interface@0.1.0
  - @treecrdt/sync-postgres@0.1.0
  - @treecrdt/sync-protocol@0.1.0
  - @treecrdt/sync-server-core@0.1.0
