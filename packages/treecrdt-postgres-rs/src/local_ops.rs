use std::cell::RefCell;
use std::rc::Rc;

use postgres::Client;

use treecrdt_core::{
    Error, LamportClock, LocalFinalizePlan, LocalPlacement, MaterializationCursor, NodeId,
    Operation, ReplicaId, Result, TreeCrdt,
};

use crate::store::{
    ensure_materialized_in_tx, load_tree_meta_for_update, persist_materialized_head,
    set_tree_meta_replay_frontier, PgCtx, PgNodeStore, PgOpStorage, PgParentOpIndex,
    PgPayloadStore, TreeMeta,
};

type LocalCrdt = TreeCrdt<PgOpStorage, LamportClock, PgNodeStore, PgPayloadStore>;

struct LocalOpSession {
    ctx: PgCtx,
    meta: TreeMeta,
    nodes: PgNodeStore,
    crdt: LocalCrdt,
}

fn run_in_tx<T>(client: &Rc<RefCell<Client>>, f: impl FnOnce() -> Result<T>) -> Result<T> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = f();

    match res {
        Ok(v) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(v)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
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
) -> Result<()> {
    let mut post_materialization_ok = true;
    let mut seq = 0u64;

    let mut op_index = PgParentOpIndex::new(session.ctx.clone());
    // commit_local() already persisted the op and updated node/payload state. The finalize step
    // refreshes adapter-owned derived state that lives outside TreeCrdt itself.
    match session.crdt.finalize_local_with_plan(
        op,
        &mut op_index,
        session.meta.state().head_seq(),
        &plan,
    ) {
        Ok(v) => {
            seq = v;
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
        seq,
    };
    if post_materialization_ok
        && persist_materialized_head(&session.ctx.client, &session.ctx.doc_id, Some(&head)).is_err()
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

    Ok(())
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
) -> Result<Operation> {
    run_in_tx(client, || {
        let mut session = begin_local_core_op(client, doc_id, replica)?;
        let placement = LocalPlacement::from_parts(placement, after)?;
        let (op, plan) = session.crdt.local_insert_with_plan(parent, node, placement, payload)?;
        finish_local_core_op(&mut session, &op, plan)?;
        Ok(op)
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
) -> Result<Operation> {
    run_in_tx(client, || {
        let mut session = begin_local_core_op(client, doc_id, replica)?;
        let placement = LocalPlacement::from_parts(placement, after)?;
        let (op, plan) = session.crdt.local_move_with_plan(node, new_parent, placement)?;
        finish_local_core_op(&mut session, &op, plan)?;
        Ok(op)
    })
}

pub fn local_delete(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
) -> Result<Operation> {
    run_in_tx(client, || {
        let mut session = begin_local_core_op(client, doc_id, replica)?;
        let (op, plan) = session.crdt.local_delete_with_plan(node)?;
        finish_local_core_op(&mut session, &op, plan)?;
        Ok(op)
    })
}

pub fn local_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    payload: Option<Vec<u8>>,
) -> Result<Operation> {
    run_in_tx(client, || {
        let mut session = begin_local_core_op(client, doc_id, replica)?;
        let (op, plan) = session.crdt.local_payload_with_plan(node, payload)?;
        finish_local_core_op(&mut session, &op, plan)?;
        Ok(op)
    })
}
