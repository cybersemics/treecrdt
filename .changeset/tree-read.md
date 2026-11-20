---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add `tree.read(ids, { childPayloads })` to the SQLite-backed engines. It reads many nodes in one query, so all of them come from the same state, and returns `{ headSeq, nodes }`: each node's live parent, its position among its siblings, its payload and its live children in order, keyed by each id as given, or null when the node is not visible. With `childPayloads`, each child carries its payload. Use it where an application would otherwise read node by node: reading 2,000 siblings and their children takes one worker call instead of five per node.
