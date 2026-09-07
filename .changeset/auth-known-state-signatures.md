---
'@treecrdt/auth': minor
---

Bind canonical defensive-delete `knownState` (or its explicit absence) into operation signatures.
Require it only on deletes, enforce the auth profile, use strict RFC 8032 identity verification,
and reject unsafe JavaScript operation counters and Lamport timestamps.

Recreate signatures from earlier drafts. Replace `encodeTreecrdtOpSigInputV1`, `signTreecrdtOpV1`,
and `verifyTreecrdtOpV1` with their unsuffixed names; `encodeTreecrdtOpSigInput` now returns
`Promise<Uint8Array>`. Signing and verification remain asynchronous. WASM loads automatically when
auth validates `knownState`.
