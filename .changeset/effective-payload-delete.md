---
"@treecrdt/wa-sqlite": minor
---

Base defensive deletion on structural subtree history and each node's current LWW payload writer, so superseded payload writes no longer restore deleted nodes. Existing materialized development databases must be reset or replayed once.
