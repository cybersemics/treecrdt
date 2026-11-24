---
'@treecrdt/wa-sqlite': patch
---

Report the payload a node stores after a batch. A delete and a concurrent edit that revived the node, appended in one batch, reported no change, and `changedSince` skipped the node. With the node's insert in the same batch, as on a new replica's first sync, the insert reported the payload from before the edit. An edit, a delete and a re-insert of one node in one batch, or concurrent inserts of one id, could also report an older payload than the stored one.
