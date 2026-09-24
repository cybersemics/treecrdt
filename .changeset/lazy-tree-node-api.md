---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Replace the flat public `tree` surface with a lazy `get()` / `root` Node API, and split the wa-sqlite session into `operations` and `tree` sub-objects. Callers navigate with `tree.get(id)?.payload()` / `parent()` / `children()` instead of `exists` / `getPayload` / `parent` / `children`. Select browser runtime wiring via an exhaustive strategy map instead of ad hoc branching.
