use std::cell::RefCell;
use std::rc::Rc;

use postgres::Client;

use treecrdt_core::{
    Error, LamportClock, LocalFinalizePlan, LocalPlacement, MaterializationCursor,
    MaterializationOutcome, NodeId, Operation, PreparedLocalOp, ReplicaId, Result, TreeCrdt,
};

use crate::store::{
    ensure_materialized_in_tx, load_tree_meta_for_update, set_tree_meta_replay_frontier,
    update_tree_meta_head, PgCtx, PgNodeStore, PgOpStorage, PgParentOpIndex, PgPayloadStore,
    TreeMeta,
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

fn begin_local_core_op(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
) -> Result<LocalOpSession> {
    // Local ops take the opposite route from append_ops_in_tx: start from a clean materialized
    // snapshot, then let TreeCrdt mint/store/apply the local op directly against Postgres stores.
    ensure_materialized_in_tx(client, doc_id)?;
    let meta = load_tree_meta_for_update(client, doc_id)?;
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

pub struct PreparedLocalOpTx {
    session: Option<LocalOpSession>,
    prepared: Option<PreparedLocalOp>,
}

impl PreparedLocalOpTx {
    pub fn op(&self) -> &Operation {
        &self.prepared.as_ref().expect("prepared local op already closed").op
    }

    pub fn commit(mut self) -> Result<LocalOpResult> {
        let mut session = self.session.take().expect("prepared local op already closed");
        let prepared = self.prepared.take().expect("prepared local op already closed");
        let res = (|| {
            let (op, plan) = session.crdt.commit_prepared_local(prepared)?;
            let outcome = finish_local_core_op(&mut session, &op, plan)?;
            Ok(LocalOpResult { op, outcome })
        })();

        match res {
            Ok(v) => {
                if let Err(e) = commit_tx(&session.ctx.client) {
                    let _ = rollback_tx(&session.ctx.client);
                    return Err(e);
                }
                Ok(v)
            }
            Err(e) => {
                let _ = rollback_tx(&session.ctx.client);
                Err(e)
            }
        }
    }

    pub fn rollback(mut self) -> Result<()> {
        let Some(session) = self.session.take() else {
            return Ok(());
        };
        self.prepared.take();
        rollback_tx(&session.ctx.client)
    }
}

impl Drop for PreparedLocalOpTx {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = rollback_tx(&session.ctx.client);
        }
    }
}

fn prepare_local_core_op<F>(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    build: F,
) -> Result<PreparedLocalOpTx>
where
    F: FnOnce(&mut LocalCrdt) -> Result<PreparedLocalOp>,
{
    begin_tx(client)?;
    let res = (|| {
        let mut session = begin_local_core_op(client, doc_id, replica)?;
        let prepared = build(&mut session.crdt)?;
        Ok(PreparedLocalOpTx {
            session: Some(session),
            prepared: Some(prepared),
        })
    })();
    if res.is_err() {
        let _ = rollback_tx(client);
    }
    res
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
    prepare_local_insert_tx(
        client, doc_id, replica, parent, node, placement, after, payload,
    )?
    .commit()
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_local_insert_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    parent: NodeId,
    node: NodeId,
    placement: &str,
    after: Option<NodeId>,
    payload: Option<Vec<u8>>,
) -> Result<PreparedLocalOpTx> {
    prepare_local_core_op(client, doc_id, replica, |crdt| {
        let placement = LocalPlacement::from_parts(placement, after)?;
        crdt.prepare_local_insert(parent, node, placement, payload)
    })
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
    prepare_local_move_tx(client, doc_id, replica, node, new_parent, placement, after)?.commit()
}

pub fn prepare_local_move_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    new_parent: NodeId,
    placement: &str,
    after: Option<NodeId>,
) -> Result<PreparedLocalOpTx> {
    prepare_local_core_op(client, doc_id, replica, |crdt| {
        let placement = LocalPlacement::from_parts(placement, after)?;
        crdt.prepare_local_move(node, new_parent, placement)
    })
}

pub fn local_delete(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
) -> Result<LocalOpResult> {
    prepare_local_delete_tx(client, doc_id, replica, node)?.commit()
}

pub fn prepare_local_delete_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
) -> Result<PreparedLocalOpTx> {
    prepare_local_core_op(client, doc_id, replica, |crdt| {
        crdt.prepare_local_delete(node)
    })
}

pub fn local_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    payload: Option<Vec<u8>>,
) -> Result<LocalOpResult> {
    prepare_local_payload_tx(client, doc_id, replica, node, payload)?.commit()
}

pub fn prepare_local_payload_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    payload: Option<Vec<u8>>,
) -> Result<PreparedLocalOpTx> {
    prepare_local_core_op(client, doc_id, replica, |crdt| {
        crdt.prepare_local_payload(node, payload)
    })
}
