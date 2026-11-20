---
'@treecrdt/wa-sqlite': minor
---

Hide the nodes under a deleted node. A delete hid only the node itself, so its descendants could still be read by id, were counted by `nodeCount()` and stayed live in `changedSince`. Reads now treat them as deleted, and `onMaterialized` reports a `delete` for each, and a `restore` with its payload when the deleted node is restored, except for descendants that were deleted on their own. A local delete of a node that is already hidden reports nothing, as it does on peers. Existing materialized databases keep showing descendants deleted before this release until they are reset or replayed.
