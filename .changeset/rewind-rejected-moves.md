---
'@treecrdt/wa-sqlite': patch
---

Fix appends that never returned when a late remote operation arrived after a rejected self-move or cycle-closing move. Out-of-order appends now rewind in place only when the already-materialized suffix sets payloads, and otherwise run the full catch-up.

The catch-up now repairs every row its replayed operations touched. Before, a node the replay deleted and then restored, for example with a peer's payload the delete didn't know about, stayed deleted with its old payload, and neither `onMaterialized` nor `tree.changedSince` reported the restore.
