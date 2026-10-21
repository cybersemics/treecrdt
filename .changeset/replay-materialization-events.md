---
'@treecrdt/wa-sqlite': patch
---

Correct replayed move notifications to use actual before/after parents, and prevent duplicate replay/repair deletion notifications from cancelling each other out.
