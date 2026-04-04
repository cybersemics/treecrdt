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
    ensure_materialized_in_tx, load_tree_meta_for_update, replica_max_counter_in_tx,
    set_tree_meta_replay_frontier, update_tree_meta_head, PgCtx, PgNodeStore, PgOpStorage,
    PgParentOpIndex, PgPayloadStore, TreeMeta,
};

type LocalCrdt = TreeCrdt<PgOpStorage, LamportClock, PgNodeStore, PgPayloadStore>;

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
    replica_counter_ms: f64,
    ctx_init_ms: f64,
    crdt_init_ms: f64,
    plan_ms: f64,
    flush_last_change_ms: f64,
    index_flush_ms: f64,
    update_head_ms: f64,
    fallback_replay_frontier: bool,
    total_ms: f64,
}

impl PgLocalOpProfile {
    fn log(&self, doc_id: &str) {
        eprintln!(
            "treecrdt_pg_local_profile kind={} doc_id={} ensure_materialized_ms={:.3} replica_counter_ms={:.3} ctx_init_ms={:.3} crdt_init_ms={:.3} plan_ms={:.3} flush_last_change_ms={:.3} index_flush_ms={:.3} update_head_ms={:.3} fallback_replay_frontier={} total_ms={:.3}",
            self.kind,
            doc_id,
            self.ensure_materialized_ms,
            self.replica_counter_ms,
            self.ctx_init_ms,
            self.crdt_init_ms,
            self.plan_ms,
            self.flush_last_change_ms,
            self.index_flush_ms,
            self.update_head_ms,
            self.fallback_replay_frontier,
            self.total_ms,
        );
    }
}

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
        c.batch_execute("BEGIN")
            .map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = f();

    match res {
        Ok(v) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT")
                .map_err(|e| Error::Storage(e.to_string()))?;
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
    let mut profile = local_profile_enabled().then(|| PgLocalOpProfile {
        kind,
        ..PgLocalOpProfile::default()
    });

    let ensure_started_at = Instant::now();
    ensure_materialized_in_tx(client, doc_id)?;
    let meta = load_tree_meta_for_update(client, doc_id)?;
    if let Some(profile) = &mut profile {
        profile.ensure_materialized_ms = ensure_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    let replica_started_at = Instant::now();
    let replica_counter = replica_max_counter_in_tx(client, doc_id, replica.as_bytes())?;
    if let Some(profile) = &mut profile {
        profile.replica_counter_ms = replica_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    let ctx_started_at = Instant::now();
    let ctx = PgCtx::new(client.clone(), doc_id)?;
    if let Some(profile) = &mut profile {
        profile.ctx_init_ms = ctx_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    let storage = PgOpStorage::new(ctx.clone());
    let nodes = PgNodeStore::new(ctx.clone());
    let payloads = PgPayloadStore::new(ctx.clone());
    let crdt_started_at = Instant::now();
    let latest_lamport = meta
        .state()
        .head
        .as_ref()
        .map(|head| head.at.lamport)
        .unwrap_or(0);
    let crdt = TreeCrdt::with_stores_seeded(
        replica.clone(),
        storage,
        LamportClock::default(),
        nodes.clone(),
        payloads,
        replica_counter,
        latest_lamport,
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

fn finish_local_core_op(
    session: &mut LocalOpSession,
    op: &Operation,
    plan: LocalFinalizePlan,
) -> Result<()> {
    let mut post_materialization_ok = true;
    let mut seq = 0u64;

    let mut op_index = PgParentOpIndex::new(session.ctx.clone());
    match session
        .crdt
        .finalize_local(op, &mut op_index, session.meta.state().head_seq(), &plan)
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

    let head = treecrdt_core::MaterializationHead {
        at: treecrdt_core::MaterializationKey {
            lamport: op.meta.lamport,
            replica: op.meta.id.replica.as_bytes(),
            counter: op.meta.id.counter,
        },
        seq,
    };
    if post_materialization_ok {
        let update_head_started_at = Instant::now();
        if update_tree_meta_head(&session.ctx.client, &session.ctx.doc_id, Some(&head)).is_err() {
            post_materialization_ok = false;
        }
        if let Some(profile) = &mut session.profile {
            profile.update_head_ms = update_head_started_at.elapsed().as_secs_f64() * 1000.0;
        }
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
        if let Some(profile) = &mut session.profile {
            profile.fallback_replay_frontier = true;
        }
    }

    Ok(())
}

fn run_profiled_local_core_op<F>(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    kind: &'static str,
    build: F,
) -> Result<Operation>
where
    F: FnOnce(&mut LocalCrdt) -> Result<(Operation, LocalFinalizePlan)>,
{
    run_in_tx(client, || {
        let total_started_at = Instant::now();
        let mut session = begin_local_core_op(client, doc_id, replica, kind)?;

        let plan_started_at = Instant::now();
        let (op, plan) = build(&mut session.crdt)?;
        if let Some(profile) = &mut session.profile {
            profile.plan_ms = plan_started_at.elapsed().as_secs_f64() * 1000.0;
        }

        finish_local_core_op(&mut session, &op, plan)?;

        if let Some(profile) = &mut session.profile {
            profile.total_ms = total_started_at.elapsed().as_secs_f64() * 1000.0;
            profile.log(doc_id);
        }

        Ok(op)
    })
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
    let placement = LocalPlacement::from_parts(placement, after)?;
    run_profiled_local_core_op(client, doc_id, replica, "insert", move |crdt| {
        crdt.local_insert(parent, node, placement, payload)
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
    let placement = LocalPlacement::from_parts(placement, after)?;
    run_profiled_local_core_op(client, doc_id, replica, "move", move |crdt| {
        crdt.local_move(node, new_parent, placement)
    })
}

pub fn local_delete(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
) -> Result<Operation> {
    run_profiled_local_core_op(client, doc_id, replica, "delete", move |crdt| {
        crdt.local_delete(node)
    })
}

pub fn local_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    payload: Option<Vec<u8>>,
) -> Result<Operation> {
    run_profiled_local_core_op(client, doc_id, replica, "payload", move |crdt| {
        crdt.local_payload(node, payload)
    })
}
