---
'@treecrdt/auth': minor
---

Sign a BLAKE3 commitment to each operation payload instead of the payload bytes, so payload bodies
can later be detached without invalidating signatures. Recreate development documents and stored op
auth signed with the earlier format.
