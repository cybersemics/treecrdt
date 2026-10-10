---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Replace the flat public `tree` surface with a lazy `get()` / `root` Node API, and split the wa-sqlite session into `operations` and `tree` sub-objects. Callers navigate with `tree.get(id)?.payload()` / `parent()` / `children()` instead of `exists` / `getPayload` / `parent` / `children`; Node handles expose canonical 32-character lowercase hex ids, and `parent()` returns `undefined` when either the node or its parent is not visible. Select browser runtime wiring via an exhaustive strategy map instead of ad hoc branching.
