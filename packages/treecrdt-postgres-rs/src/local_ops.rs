use std::cell::RefCell;
use std::rc::Rc;

use postgres::Client;

use treecrdt_core::{
    Error, LamportClock, LocalFinalizePlan, LocalPlacement, MaterializationCursor,
    MaterializationOutcome, MaterializationState, NodeId, Operation,
    PreparedLocalOp as CorePreparedLocalOp, ReplicaId, Result, TreeCrdt,
};

use crate::opref::derive_op_ref_v0;
use crate::store::{
    ensure_doc_meta, ensure_materialized_in_tx, load_tree_meta_for_update,
    set_tree_meta_replay_frontier, try_load_tree_meta_for_update, update_tree_meta_head, PgCtx,
    PgNodeStore, PgOpStorage, PgParentOpIndex, PgPayloadStore, TreeMeta,
};

type LocalCrdt = TreeCrdt<PgOpStorage, LamportClock, PgNodeStore, PgPayloadStore>;

struct LocalOpSession {
    ctx: PgCtx,
    meta: TreeMeta,
    nodes: PgNodeStore,
    crdt: LocalCrdt,
}

#[derive(Clone, Debug)]
pub struct LocalOpResult {
    pub op: Operation,
    pub outcome: MaterializationOutcome,
}

impl std::ops::Deref for LocalOpResult {
    type Target = Operation;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

/// Standard operation proof persisted atomically with an authenticated local operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalOpAuthProof {
    pub sig: Vec<u8>,
    pub proof_ref: Vec<u8>,
}

#[derive(Clone, Debug)]
enum LocalOpRequest {
    Insert {
        parent: NodeId,
        node: NodeId,
        placement: LocalPlacement,
        payload: Option<Vec<u8>>,
    },
    Move {
        node: NodeId,
        new_parent: NodeId,
        placement: LocalPlacement,
    },
    Delete {
        node: NodeId,
    },
    Payload {
        node: NodeId,
        payload: Option<Vec<u8>>,
    },
}

impl LocalOpRequest {
    fn prepare(&self, crdt: &mut LocalCrdt) -> Result<CorePreparedLocalOp> {
        match self {
            Self::Insert {
                parent,
                node,
                placement,
                payload,
            } => crdt.prepare_local_insert(*parent, *node, *placement, payload.clone()),
            Self::Move {
                node,
                new_parent,
                placement,
            } => crdt.prepare_local_move(*node, *new_parent, *placement),
            Self::Delete { node } => crdt.prepare_local_delete(*node),
            Self::Payload { node, payload } => crdt.prepare_local_payload(*node, payload.clone()),
        }
    }
}

/// An exact local-operation proposal detached from its preparation transaction.
///
/// The proposal owns only values. It holds no PostgreSQL connection, row lock, or transaction, so
/// callers may safely await authorization. Commit re-prepares the request under the same locked
/// revision and accepts only the exact authorized operation.
#[derive(Clone, Debug)]
pub struct PreparedLocalOpProposal {
    doc_id: String,
    replica: ReplicaId,
    request: LocalOpRequest,
    op: Operation,
    revision: MaterializationState,
    recovery_outcome: MaterializationOutcome,
}

impl PreparedLocalOpProposal {
    pub fn op(&self) -> &Operation {
        &self.op
    }

    pub fn recovery_outcome(&self) -> &MaterializationOutcome {
        &self.recovery_outcome
    }
}

fn begin_tx(client: &Rc<RefCell<Client>>) -> Result<()> {
    let mut c = client.borrow_mut();
    c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))
}

fn commit_tx(client: &Rc<RefCell<Client>>) -> Result<()> {
    let mut c = client.borrow_mut();
    c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))
}

fn rollback_tx(client: &Rc<RefCell<Client>>) -> Result<()> {
    let mut c = client.borrow_mut();
    c.batch_execute("ROLLBACK").map_err(|e| Error::Storage(e.to_string()))
}

fn finish_tx<T>(client: &Rc<RefCell<Client>>, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => {
            if let Err(error) = commit_tx(client) {
                let _ = rollback_tx(client);
                return Err(error);
            }
            Ok(value)
        }
        Err(error) => {
            let _ = rollback_tx(client);
            Err(error)
        }
    }
}

fn finish_optimistic_tx<T>(
    client: &Rc<RefCell<Client>>,
    result: Result<Option<T>>,
) -> Result<Option<T>> {
    match result {
        Ok(Some(value)) => finish_tx(client, Ok(value)).map(Some),
        Ok(None) => {
            rollback_tx(client)?;
            Ok(None)
        }
        Err(error) => {
            let _ = rollback_tx(client);
            Err(error)
        }
    }
}

fn ensure_doc_meta_if_missing(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let exists = {
        let mut c = client.borrow_mut();
        c.query_opt("SELECT 1 FROM treecrdt_meta WHERE doc_id = $1", &[&doc_id])
            .map_err(|error| Error::Storage(error.to_string()))?
            .is_some()
    };
    if exists {
        Ok(())
    } else {
        ensure_doc_meta(client, doc_id)
    }
}

fn local_op_session(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    meta: TreeMeta,
) -> Result<LocalOpSession> {
    let ctx = PgCtx::new(client.clone(), doc_id)?;
    let storage = PgOpStorage::new(ctx.clone());
    let nodes = PgNodeStore::new(ctx.clone());
    let payloads = PgPayloadStore::new(ctx.clone());
    let crdt = TreeCrdt::with_stores(
        replica.clone(),
        storage,
        LamportClock::default(),
        nodes.clone(),
        payloads,
    )?;

    Ok(LocalOpSession {
        ctx,
        meta,
        nodes,
        crdt,
    })
}

fn begin_local_core_op(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
) -> Result<LocalOpSession> {
    // Immediate local writes retain their original single-transaction behavior.
    ensure_materialized_in_tx(client, doc_id)?;
    let meta = load_tree_meta_for_update(client, doc_id)?;
    local_op_session(client, doc_id, replica, meta)
}

fn finish_local_core_op(
    session: &mut LocalOpSession,
    op: &Operation,
    plan: LocalFinalizePlan,
) -> Result<MaterializationOutcome> {
    let mut post_materialization_ok = true;
    let mut outcome = MaterializationOutcome::empty(session.meta.state().head_seq());

    let mut op_index = PgParentOpIndex::new(session.ctx.clone());
    // commit_prepared_local() already persisted the op and updated node/payload state. The finalize
    // step refreshes adapter-owned derived state that lives outside TreeCrdt itself.
    match session.crdt.finalize_local_with_outcome(
        op,
        &mut op_index,
        session.meta.state().head_seq(),
        &plan,
    ) {
        Ok(v) => {
            outcome = v;
            if session.nodes.flush_last_change().is_err() || op_index.flush().is_err() {
                post_materialization_ok = false;
            }
        }
        Err(_) => post_materialization_ok = false,
    }

    let head = treecrdt_core::MaterializationHead {
        at: treecrdt_core::MaterializationKey {
            lamport: op.meta.lamport,
            replica: op.meta.id.replica.as_bytes(),
            counter: op.meta.id.counter,
        },
        seq: outcome.head_seq,
    };
    if post_materialization_ok
        && update_tree_meta_head(&session.ctx.client, &session.ctx.doc_id, Some(&head)).is_err()
    {
        post_materialization_ok = false;
    }

    if !post_materialization_ok {
        set_tree_meta_replay_frontier(
            &session.ctx.client,
            &session.ctx.doc_id,
            &treecrdt_core::MaterializationFrontier {
                lamport: 0,
                replica: Vec::new(),
                counter: 0,
            },
        )?;
    }

    Ok(outcome)
}

fn commit_immediate_local<F>(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    build: F,
) -> Result<LocalOpResult>
where
    F: FnOnce(&mut LocalCrdt) -> Result<CorePreparedLocalOp>,
{
    begin_tx(client)?;
    let result = (|| {
        // A local write may be the first operation for a document. Create its revision row inside
        // this same transaction before materialization takes the row lock.
        ensure_doc_meta(client, doc_id)?;
        let mut session = begin_local_core_op(client, doc_id, replica)?;
        let prepared = build(&mut session.crdt)?;
        let (op, plan) = session.crdt.commit_prepared_local(prepared)?;
        let outcome = finish_local_core_op(&mut session, &op, plan)?;
        Ok(LocalOpResult { op, outcome })
    })();

    finish_tx(client, result)
}

fn validate_local_auth_proof(proof: &LocalOpAuthProof) -> Result<()> {
    if proof.sig.len() != 64 {
        return Err(Error::InvalidOperation(
            "local operation auth signature must be 64 bytes".into(),
        ));
    }
    if proof.proof_ref.len() != 16 {
        return Err(Error::InvalidOperation(
            "local operation auth proof reference must be 16 bytes".into(),
        ));
    }
    Ok(())
}

fn persist_local_auth_proof(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    op: &Operation,
    proof: &LocalOpAuthProof,
) -> Result<()> {
    let op_ref = derive_op_ref_v0(doc_id, op.meta.id.replica.as_bytes(), op.meta.id.counter);
    let mut c = client.borrow_mut();
    c.execute(
        "INSERT INTO treecrdt_sync_op_auth \
           (doc_id, op_ref, sig, proof_ref, created_at_ms) \
         VALUES ($1, $2, $3, $4, \
           (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint) \
         ON CONFLICT (doc_id, op_ref) DO UPDATE SET \
           sig = EXCLUDED.sig, \
           proof_ref = EXCLUDED.proof_ref, \
           created_at_ms = EXCLUDED.created_at_ms",
        &[&doc_id, &op_ref.as_slice(), &proof.sig, &proof.proof_ref],
    )
    .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}

fn try_prepare_local_request(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    request: LocalOpRequest,
) -> Result<Option<PreparedLocalOpProposal>> {
    // Create the revision row before the short proposal transaction. The returned proposal itself
    // retains neither this client nor any transaction state.
    ensure_doc_meta_if_missing(client, doc_id)?;
    begin_tx(client)?;
    let result = (|| {
        let Some(mut meta) = try_load_tree_meta_for_update(client, doc_id)? else {
            return Ok(None);
        };
        let recovery_outcome = if meta.0.replay_from.is_some() {
            let outcome = ensure_materialized_in_tx(client, doc_id)?;
            meta = load_tree_meta_for_update(client, doc_id)?;
            outcome
        } else {
            MaterializationOutcome::empty(meta.0.head_seq())
        };

        let revision = meta.0.clone();
        let mut session = local_op_session(client, doc_id, replica, meta)?;
        let prepared = request.prepare(&mut session.crdt)?;
        Ok(Some(PreparedLocalOpProposal {
            doc_id: doc_id.to_string(),
            replica: replica.clone(),
            request,
            op: prepared.op,
            revision,
            recovery_outcome,
        }))
    })();

    finish_optimistic_tx(client, result)
}

pub fn try_commit_prepared_local(
    client: &Rc<RefCell<Client>>,
    proposal: PreparedLocalOpProposal,
    proof: LocalOpAuthProof,
) -> Result<Option<LocalOpResult>> {
    // Keep user-controlled validation work outside the short row-locked transaction.
    validate_local_auth_proof(&proof)?;
    let doc_id = &proposal.doc_id;
    begin_tx(client)?;
    let result = (|| {
        let Some(meta) = try_load_tree_meta_for_update(client, doc_id)? else {
            return Ok(None);
        };
        if meta.0.replay_from.is_some() || meta.0 != proposal.revision {
            return Ok(None);
        }

        let mut session = local_op_session(client, doc_id, &proposal.replica, meta)?;
        let prepared = proposal.request.prepare(&mut session.crdt)?;
        if prepared.op != proposal.op {
            return Ok(None);
        }

        let (op, plan) = session.crdt.commit_prepared_local(prepared)?;
        let outcome = finish_local_core_op(&mut session, &op, plan)?;
        persist_local_auth_proof(client, doc_id, &op, &proof)?;
        Ok(Some(LocalOpResult { op, outcome }))
    })();

    finish_optimistic_tx(client, result)
}

#[allow(clippy::too_many_arguments)]
pub fn local_insert(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    parent: NodeId,
    node: NodeId,
    placement: &str,
    after: Option<NodeId>,
    payload: Option<Vec<u8>>,
) -> Result<LocalOpResult> {
    commit_immediate_local(client, doc_id, replica, |crdt| {
        let placement = LocalPlacement::from_parts(placement, after)?;
        crdt.prepare_local_insert(parent, node, placement, payload)
    })
}

#[allow(clippy::too_many_arguments)]
pub fn try_prepare_local_insert(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    parent: NodeId,
    node: NodeId,
    placement: &str,
    after: Option<NodeId>,
    payload: Option<Vec<u8>>,
) -> Result<Option<PreparedLocalOpProposal>> {
    try_prepare_local_request(
        client,
        doc_id,
        replica,
        LocalOpRequest::Insert {
            parent,
            node,
            placement: LocalPlacement::from_parts(placement, after)?,
            payload,
        },
    )
}

pub fn local_move(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    new_parent: NodeId,
    placement: &str,
    after: Option<NodeId>,
) -> Result<LocalOpResult> {
    commit_immediate_local(client, doc_id, replica, |crdt| {
        let placement = LocalPlacement::from_parts(placement, after)?;
        crdt.prepare_local_move(node, new_parent, placement)
    })
}

pub fn try_prepare_local_move(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    new_parent: NodeId,
    placement: &str,
    after: Option<NodeId>,
) -> Result<Option<PreparedLocalOpProposal>> {
    try_prepare_local_request(
        client,
        doc_id,
        replica,
        LocalOpRequest::Move {
            node,
            new_parent,
            placement: LocalPlacement::from_parts(placement, after)?,
        },
    )
}

pub fn local_delete(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
) -> Result<LocalOpResult> {
    commit_immediate_local(client, doc_id, replica, |crdt| {
        crdt.prepare_local_delete(node)
    })
}

pub fn try_prepare_local_delete(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
) -> Result<Option<PreparedLocalOpProposal>> {
    try_prepare_local_request(client, doc_id, replica, LocalOpRequest::Delete { node })
}

pub fn local_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    payload: Option<Vec<u8>>,
) -> Result<LocalOpResult> {
    commit_immediate_local(client, doc_id, replica, |crdt| {
        crdt.prepare_local_payload(node, payload)
    })
}

pub fn try_prepare_local_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    payload: Option<Vec<u8>>,
) -> Result<Option<PreparedLocalOpProposal>> {
    try_prepare_local_request(
        client,
        doc_id,
        replica,
        LocalOpRequest::Payload { node, payload },
    )
}
