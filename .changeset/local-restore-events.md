---
'@treecrdt/wa-sqlite': patch
---

Report a parent restored by a local write. A payload on a deleted node's child, or an insert under it, restores the node; `onMaterialized` now includes that restore, as it does on peers applying the same op.
