# @treecrdt/interface

## 0.2.0

### Minor Changes

- 2f864ec: Move local materialization write ids from the event root to each materialized change's `source.writeIds`.
- 60950b7: Add optional per-change source metadata to materialization events so apps can derive local projections like update metadata from the operation that caused a visible tree change.

## 0.1.0

### Minor Changes

- ed5a001: Initial npm release for the public TreeCRDT runtime, browser storage, and sync packages.
