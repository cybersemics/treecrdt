# TreeCRDT Sync v0: Auth Extension (COSE + CWT) (Draft)

This document defines an optional auth extension for Sync v0, focused on:

- Integrity: prevent forged operations.
- Authorization: subtree-scoped write permissions (and a path to read gating).

It is intentionally **ACL-agnostic at the TreeCRDT core layer**: the CRDT operation
types and merge semantics do not change. Authorization is enforced by the sync layer
and by any server that chooses to validate inbound ops.

Status: draft. Backwards compatibility is not guaranteed.

## Threat model (baseline)

- The sync server is untrusted: it may reorder, drop, replay, and inject messages.
- Peers defend by verifying per-op signatures and per-op authorization proofs.
- This does **not** hide access patterns from the server. Payload confidentiality is
  a separate design (see confidentiality issue/design).

## Identity model

- Each replica uses a **doc-scoped Ed25519 keypair** to sign operations.
- In Sync v0, `replica_id` is the **32-byte Ed25519 public key** (verifying key) for that doc.
- Optionally, a stable cross-doc “global identity” key can exist, but should be exchanged
  end-to-end (not shown to the sync server) to avoid cross-doc correlation.

### Derived `key_id` (optional)

A short doc-scoped key identifier can be derived from the public key for local caches.

Reference derivation (v1):

- `key_id = blake3("treecrdt/keyid/v1" || ed25519_pubkey)[0..16]` (16 bytes)

This is **not transmitted per-op** in v0: verifiers derive it from `replica_id` when needed.

## Wire format: `OpAuth` in `OpsBatch`

Sync v0 already has a `Hello.capabilities` mechanism. This extension uses it to exchange
authorization tokens, and it adds optional per-op auth metadata aligned with each op.

### `Hello.capabilities`

Peers exchange capability tokens at session start:

- `Capability.name = "auth.capability"`
- `Capability.value = base64url(COSE_Sign1_bytes)`

### `OpsBatch.auth[]`

`OpsBatch` gains an optional `auth` field aligned with `ops` by index.

If present, `auth` MUST be either empty (no auth) or exactly the same length as `ops`.

### `OpAuth`

Each op may carry:

- `sig`: Ed25519 signature bytes (64 bytes)
- `proof_ref` (optional but RECOMMENDED): 16-byte reference to the capability token used
  to authorize this op

Rationale:

- `sig` provides integrity and non-repudiation for the op contents.
- `proof_ref` makes authorization deterministic in the presence of rotation/expiry: the
  verifier knows which token (permissions snapshot) the signer intended.

## Capability tokens: COSE_Sign1 + CWT

Authorization proofs are **COSE_Sign1** messages containing a **CWT** (RFC 8392) payload.

### `cnf` binding (proof of possession)

Tokens MUST be bound to the op signing key using the CWT `cnf` claim.

Reference encoding:

- `cnf` is a CBOR map with:
  - `pub`: raw Ed25519 public key bytes (32 bytes)
  - `kid` (optional): derived `key_id` bytes; if present it MUST match `pub`

### Private claim: `caps`

`caps` is a list of capability objects. The reference implementation treats each element as:

```
cap = {
  res: {
    doc_id: "...",
    // `root` is required; doc-wide access is represented by ROOT + no extra scope restrictions.
    root: <NodeId bytes>,        // required (ROOT means doc-wide)
    max_depth: <u32>,            // optional
    exclude: [<NodeId bytes>],   // optional
  },
  actions: ["write_structure", "write_payload", "delete", "tombstone", "grant", ...]
}
```

### Delegation / resharing (optional)

This extension supports delegated (“reshared”) capability tokens.

High-level idea:

- An **issuer** can mint a token for a subject key.
- If that token includes the `grant` action, the subject key can mint a **delegated token** for another subject key.
- The delegated token MUST be a subset of the proof token (same `doc_id`, narrower-or-equal scope, subset of actions, and time bounds within the proof).

Wire format (reference implementation):

- Delegated tokens are still COSE_Sign1 + CWT, but they are signed by the **subject key** of a *proof token*.
- The delegated token carries exactly one proof token in the COSE unprotected header:
  - key: `"treecrdt.delegation_proof_v1"`
  - value: a single `bstr` (or a 1-element array of `bstr`) containing the proof token bytes
- Proof tokens can themselves be delegated (chained), but they must ultimately verify against an issuer key.
  Implementations SHOULD enforce a maximum chain depth and reject cycles.

### Token id (`proof_ref`)

`proof_ref` is a short hash of the COSE token bytes.

Reference derivation (v1):

- `token_id = blake3("treecrdt/tokenid/v1" || cose_sign1_bytes)[0..16]` (16 bytes)

Verifiers SHOULD cache `token_id -> parsed claims` for efficiency.

### Revocation

Implementations SHOULD support token-id-based revocation (denylist) in addition to `exp`/`nbf`.
Revoking a proof token must also invalidate delegated tokens that depend on that proof chain.

Recommended policy modes:

- `hard`: token is invalid for all checks (retroactive if re-verified).
- `write_cutover`: token remains valid for ops strictly before `effective_from`, and is rejected at/after `effective_from`.

`write_cutover` can be anchored by:

- `(replica_id, counter)` only (deterministic and clock-free).
- Lamport-based cutover is intentionally not part of v1.

For convergence across peers, revocations SHOULD be exchanged as signed records (not only local denylist state).

Record shape (reference v1):

- capability: `name="auth.revocation"`, `value=base64url(COSE_Sign1(CWT))`
- claims:
  - `doc_id`
  - `token_id`
  - `mode` (`hard` or `write_cutover`)
  - `effective_from_counter` + optional `effective_from_replica`
  - `iat`
  - monotonic `rev_seq` (per issuer/doc), to avoid ambiguity on conflicting records

Deterministic verifier rule:

- verify record signature against issuer trust roots
- pick highest `rev_seq` for `(doc_id, token_id)`
- enforce by mode:
  - `hard`: reject always
  - `write_cutover`: reject only ops at/after cutover boundary

Reference implementation status:

- supports hard revocation now (`revokedCapabilityTokenIds`)
- supports custom runtime cutover logic via `isCapabilityTokenRevoked` callback with op context (`op`, `purpose`)
- supports `auth.revocation` wire records in `Hello.capabilities` / `HelloAck.capabilities`
- merges by highest `rev_seq` per `(doc_id, token_id)` (deterministic tie-break by lexical record bytes)

## Signed operations

Ops are signed with the doc-scoped Ed25519 key. The signature covers:

- `doc_id`
- `op_id` (`replica_id` + `counter`)
- `lamport`
- op kind + fields
- payload bytes (ciphertext bytes if payloads are encrypted)

### Canonical signing bytes

All integers are big-endian. Strings are UTF-8 with length prefixes.

```
sig_input = concat(
  "treecrdt/op-sig/v1", 0x00,
  u32_be(len(doc_id)), doc_id_utf8,
  u32_be(len(replica_id)), replica_id_bytes,
  u64_be(counter),
  u64_be(lamport),
  u8(kind_tag),
  kind_fields
)
```

Kind tags and fields:

- Insert: tag=1
  - parent(16) || node(16) || u32_be(len(order_key)) || order_key_bytes
  - payload_flag(u8): 0 or 1
  - if payload_flag=1: u32_be(len(payload)) || payload_bytes
- Move: tag=2
  - node(16) || new_parent(16) || u32_be(len(order_key)) || order_key_bytes
- Delete: tag=3
  - node(16)
- Tombstone: tag=4
  - node(16)
- Payload: tag=5
  - node(16)
  - value_tag(u8): 0=clear, 1=payload
  - if value_tag=1: u32_be(len(payload)) || payload_bytes

## Subtree scope enforcement and `pending_context`

Subtree ACLs require the verifier to answer: “is the node touched by this op within the granted subtree?”

That check depends on **tree context** (ancestry). Under partial replication and/or out-of-order delivery, a verifier may
receive an op before it has enough structure to decide.

This extension uses a **tri-state** outcome for scope checks:

- `allow`: op is within scope
- `deny`: op is outside scope (reject immediately)
- `unknown`: verifier lacks enough local ancestry context to decide

### `pending_context` disposition (fail-closed, replayable)

When the scope check is `unknown`, verifiers MUST NOT apply the op to CRDT state.

Instead, the sync layer can return a `pending_context` disposition and store the op in a sidecar pending table. Once more
structure arrives (e.g. an insert/move that establishes the parent chain), the verifier retries authorization and either:

- applies the op if it becomes `allow`, or
- drops it if it becomes provably `deny`

This gives **fail-closed behavior** without permanently losing valid ops.

### Practical guidance

To avoid pending ops that never resolve, deployments SHOULD ensure that any peer expected to *verify* subtree-scoped ops
syncs enough **clear structural metadata** for the affected subtree (and potentially its ancestor chain).

“Server” and “client” are not fundamentally different here: both are just peers with different local views. A sync server
often stores more structure (full tree index), so it can resolve `allow/deny` more often and avoid pending.

## Sidecar storage (same SQLite database)

To avoid changing core op-log storage, implementations SHOULD store auth data in sidecar tables in the same SQLite
database as the CRDT state (not separate files).

Two useful sidecar concepts:

1) **Verified proof cache** (optional): re-verify later without re-downloading proofs.
2) **Pending ops** (recommended): store ops that are validly signed and well-formed but not yet authorizable.
3) **Op auth cache** (recommended for forwarders): persist per-op auth (`sig` + `proof_ref`) for already-applied ops so a peer/server can re-serve ops it did not author after restart.

### Suggested SQLite schema (illustrative)

Implementations may choose different schemas; the important properties are:

- key by `op_ref` (or by `(replica_id, counter)`)
- store the per-op `OpAuth`
- store a reason/message for debugging

Example:

```sql
CREATE TABLE IF NOT EXISTS treecrdt_sync_pending_ops (
  doc_id TEXT NOT NULL,
  op_ref BLOB NOT NULL,              -- 16 bytes
  op BLOB NOT NULL,                  -- encoded sync/v0 Operation protobuf bytes (not a full SyncMessage)
  sig BLOB NOT NULL,
  proof_ref BLOB,
  reason TEXT NOT NULL,              -- e.g. "missing_context"
  message TEXT,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
```

If a peer/server forwards ops (or needs to re-serve verified ops after restart), it SHOULD also store the `OpAuth`
metadata for already-applied ops:

```sql
CREATE TABLE IF NOT EXISTS treecrdt_sync_op_auth (
  doc_id TEXT NOT NULL,
  op_ref BLOB NOT NULL,              -- 16 bytes
  sig BLOB NOT NULL,                 -- 64 bytes (Ed25519)
  proof_ref BLOB,                    -- 16 bytes (token id), nullable
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
```

### SQLite runner compatibility notes

TreeCRDT’s JS SQLite adapters intentionally use a minimal `SqliteRunner` API (`exec` + `getText`). This has two practical
implications for sidecar tables:

1) **DML via `getText`**: some runners implement `getText` using a “query” primitive (for example `better-sqlite3`’s
   `.get()`), which fails for statements that return no rows. A portable pattern is to use `RETURNING` so the statement
   yields a row:

   - `INSERT ... RETURNING 1`
   - `DELETE ... RETURNING 1`

2) **BLOB reads via text**: if the bridge only returns strings, read `BLOB` columns with `hex(blob_col)` and decode on
   the JS side. For bulk reads, SQLite’s JSON1 functions are convenient (`json_object`, `json_group_array`).

## Subscription note: push vs catch-up

Sync v0 push subscriptions (`Subscribe`) are meant for **new** ops. They do not guarantee delivery of historical ops.

Implementations SHOULD run an initial catch-up (`syncOnce`) and/or periodic catch-up to avoid missing updates due to
transient failures.
