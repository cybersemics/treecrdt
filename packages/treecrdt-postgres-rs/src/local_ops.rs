use std::cell::RefCell;
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::Instant;

use postgres::Client;

use treecrdt_core::{
    Error, LamportClock, LocalFinalizePlan, LocalPlacement, MaterializationCursor, NodeId,
    Operation, ReplicaId, Result, TreeCrdt,
};

use crate::store::{
    ensure_materialized_and_load_meta_for_update_in_tx, replica_max_counter_in_tx,
    set_tree_meta_dirty, update_tree_meta_head, PgCtx, PgNodeStore, PgOpStorage, PgParentOpIndex,
    PgPayloadStore, TreeMeta,
};

fn local_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("TREECRDT_PG_PROFILE_LOCAL").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
        )
    })
}

#[derive(Clone, Debug, Default)]
struct PgLocalOpProfile {
    kind: &'static str,
    ensure_materialized_ms: f64,
    load_meta_ms: f64,
    replica_counter_ms: f64,
    ctx_init_ms: f64,
    crdt_init_ms: f64,
    plan_ms: f64,
    flush_last_change_ms: f64,
    index_flush_ms: f64,
    update_head_ms: f64,
    fallback_mark_dirty: bool,
    total_ms: f64,
}

impl PgLocalOpProfile {
    fn log(&self, doc_id: &str) {
        eprintln!(
            "treecrdt_pg_local_profile kind={} doc_id={} ensure_materialized_ms={:.3} load_meta_ms={:.3} replica_counter_ms={:.3} ctx_init_ms={:.3} crdt_init_ms={:.3} plan_ms={:.3} flush_last_change_ms={:.3} index_flush_ms={:.3} update_head_ms={:.3} fallback_mark_dirty={} total_ms={:.3}",
            self.kind,
            doc_id,
            self.ensure_materialized_ms,
            self.load_meta_ms,
            self.replica_counter_ms,
            self.ctx_init_ms,
            self.crdt_init_ms,
            self.plan_ms,
            self.flush_last_change_ms,
            self.index_flush_ms,
            self.update_head_ms,
            self.fallback_mark_dirty,
            self.total_ms,
        );
    }
}

type LocalCrdt = TreeCrdt<PgOpStorage, LamportClock, PgNodeStore, PgPayloadStore>;

struct LocalOpSession {
    ctx: PgCtx,
    meta: TreeMeta,
    nodes: PgNodeStore,
    crdt: LocalCrdt,
    profile: Option<PgLocalOpProfile>,
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
    kind: &'static str,
) -> Result<LocalOpSession> {
    // Local ops take the opposite route from append_ops_in_tx: start from a clean materialized
    // snapshot, then let TreeCrdt mint/store/apply the local op directly against Postgres stores.
    // `kind` is only a human-readable label for profiling/debug output ("insert", "move", etc.),
    // so later timing logs can tell which local-op path they came from.
    let mut profile = local_profile_enabled().then(|| PgLocalOpProfile {
        kind,
        ..PgLocalOpProfile::default()
    });

    let ensure_started_at = Instant::now();
    let meta = ensure_materialized_and_load_meta_for_update_in_tx(client, doc_id)?;
    if let Some(profile) = &mut profile {
        profile.ensure_materialized_ms = ensure_started_at.elapsed().as_secs_f64() * 1000.0;
        profile.load_meta_ms = 0.0;
    }

    let replica_started_at = Instant::now();
    let replica_counter = replica_max_counter_in_tx(client, doc_id, replica.as_bytes())?;
    if let Some(profile) = &mut profile {
        profile.replica_counter_ms = replica_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    let ctx_started_at = Instant::now();
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    if let Some(profile) = &mut profile {
        profile.ctx_init_ms = ctx_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    let storage = PgOpStorage::new(ctx.clone());
    let nodes = PgNodeStore::new(ctx.clone());
    let payloads = PgPayloadStore::new(ctx.clone());
    let crdt_started_at = Instant::now();
    let crdt = TreeCrdt::with_stores_seeded(
        replica.clone(),
        storage,
        LamportClock::default(),
        nodes.clone(),
        payloads,
        replica_counter,
        meta.head_lamport(),
    )?;
    if let Some(profile) = &mut profile {
        profile.crdt_init_ms = crdt_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    Ok(LocalOpSession {
        ctx,
        meta,
        nodes,
        crdt,
        profile,
    })
}

fn finish_local_core_op(session: &mut LocalOpSession, op: &Operation, plan: LocalFinalizePlan) {
    let mut post_materialization_ok = true;
    let mut seq = 0u64;

    let mut op_index = PgParentOpIndex::new(session.ctx.clone());
    // commit_local() already persisted the op and updated node/payload state. The finalize step
    // refreshes adapter-owned derived state that lives outside TreeCrdt itself.
    match session
        .crdt
        .finalize_local_with_plan(op, &mut op_index, session.meta.head_seq(), &plan)
    {
        Ok(v) => {
            seq = v;
            let flush_last_change_started_at = Instant::now();
            let flush_last_change_ok = session.nodes.flush_last_change().is_ok();
            if let Some(profile) = &mut session.profile {
                profile.flush_last_change_ms =
                    flush_last_change_started_at.elapsed().as_secs_f64() * 1000.0;
            }

            let index_flush_started_at = Instant::now();
            let index_flush_ok = op_index.flush().is_ok();
            if let Some(profile) = &mut session.profile {
                profile.index_flush_ms = index_flush_started_at.elapsed().as_secs_f64() * 1000.0;
            }

            if !flush_last_change_ok || !index_flush_ok {
                post_materialization_ok = false;
            }
        }
        Err(_) => post_materialization_ok = false,
    }

    if post_materialization_ok {
        let update_head_started_at = Instant::now();
        if update_tree_meta_head(
            &session.ctx.client,
            &session.ctx.doc_id,
            op.meta.lamport,
            op.meta.id.replica.as_bytes(),
            op.meta.id.counter,
            seq,
        )
        .is_err()
        {
            post_materialization_ok = false;
        }
        if let Some(profile) = &mut session.profile {
            profile.update_head_ms = update_head_started_at.elapsed().as_secs_f64() * 1000.0;
        }
    }

    if !post_materialization_ok {
        let _ = set_tree_meta_dirty(&session.ctx.client, &session.ctx.doc_id, true);
        if let Some(profile) = &mut session.profile {
            profile.fallback_mark_dirty = true;
        }
    }
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
        let total_started_at = Instant::now();
        let mut session = begin_local_core_op(client, doc_id, replica, "insert")?;
        let placement = LocalPlacement::from_parts(placement, after)?;
        let plan_started_at = Instant::now();
        let (op, plan) = session.crdt.local_insert_with_plan(parent, node, placement, payload)?;
        if let Some(profile) = &mut session.profile {
            profile.plan_ms = plan_started_at.elapsed().as_secs_f64() * 1000.0;
        }
        finish_local_core_op(&mut session, &op, plan);
        if let Some(profile) = &mut session.profile {
            profile.total_ms = total_started_at.elapsed().as_secs_f64() * 1000.0;
            profile.log(doc_id);
        }
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
        let total_started_at = Instant::now();
        let mut session = begin_local_core_op(client, doc_id, replica, "move")?;
        let placement = LocalPlacement::from_parts(placement, after)?;
        let plan_started_at = Instant::now();
        let (op, plan) = session.crdt.local_move_with_plan(node, new_parent, placement)?;
        if let Some(profile) = &mut session.profile {
            profile.plan_ms = plan_started_at.elapsed().as_secs_f64() * 1000.0;
        }
        finish_local_core_op(&mut session, &op, plan);
        if let Some(profile) = &mut session.profile {
            profile.total_ms = total_started_at.elapsed().as_secs_f64() * 1000.0;
            profile.log(doc_id);
        }
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
        let total_started_at = Instant::now();
        let mut session = begin_local_core_op(client, doc_id, replica, "delete")?;
        let plan_started_at = Instant::now();
        let (op, plan) = session.crdt.local_delete_with_plan(node)?;
        if let Some(profile) = &mut session.profile {
            profile.plan_ms = plan_started_at.elapsed().as_secs_f64() * 1000.0;
        }
        finish_local_core_op(&mut session, &op, plan);
        if let Some(profile) = &mut session.profile {
            profile.total_ms = total_started_at.elapsed().as_secs_f64() * 1000.0;
            profile.log(doc_id);
        }
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
        let total_started_at = Instant::now();
        let mut session = begin_local_core_op(client, doc_id, replica, "payload")?;
        let plan_started_at = Instant::now();
        let (op, plan) = session.crdt.local_payload_with_plan(node, payload)?;
        if let Some(profile) = &mut session.profile {
            profile.plan_ms = plan_started_at.elapsed().as_secs_f64() * 1000.0;
        }
        finish_local_core_op(&mut session, &op, plan);
        if let Some(profile) = &mut session.profile {
            profile.total_ms = total_started_at.elapsed().as_secs_f64() * 1000.0;
            profile.log(doc_id);
        }
        Ok(op)
    })
}
