---
'@treecrdt/wa-sqlite': patch
---

Fix appends that never returned when a late remote operation arrived after a rejected self-move or cycle-closing move. Out-of-order appends now rewind in place only when the already-materialized suffix sets payloads, and otherwise run the full catch-up, which now repairs every row its replayed operations touched.
