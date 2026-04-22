use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use postgres::Client;

use treecrdt_core::{
    catch_up_materialized_state, materialize_persisted_remote_ops_with_delta,
    orchestrate_persisted_remote_append, try_direct_rewind_catch_up_materialized_state, Error,
    LamportClock, MaterializationCursor, MaterializationHead, NodeId, Operation, OperationKind,
    PersistedRemoteStores, ReplicaId, Result,
};

use crate::profile::{append_profile_enabled, PgAppendProfile};
use crate::schema::reset_doc_for_tests;

use super::meta::load_tree_meta;
use super::*;

fn balanced_parent_and_position(node_index: usize, fanout: usize) -> (NodeId, u16) {
    if node_index <= fanout {
        return (NodeId::ROOT, ((node_index - 1) % fanout) as u16);
    }
    (
        NodeId((((node_index - (fanout + 1)) / fanout) + 1) as u128),
        ((node_index - 1) % fanout) as u16,
    )
}

fn order_key_from_position(position: u16) -> Vec<u8> {
    position.wrapping_add(1).to_be_bytes().to_vec()
}

fn materialize_inserted_ops(
    ctx: PgCtx,
    meta: &dyn MaterializationCursor,
    ops: Vec<Operation>,
) -> Result<treecrdt_core::IncrementalApplyResult> {
    // At this point treecrdt_ops already contains the inserted operations. This temporary
    // TreeCrdt exists only to replay those ops through core semantics and update derived tables.
    materialize_persisted_remote_ops_with_delta(
        PersistedRemoteStores {
            // Scratch identity for the temporary TreeCrdt; replayed ops keep their own ids.
            replica_id: ReplicaId::new(b"postgres"),
            clock: LamportClock::default(),
            nodes: PgNodeStore::new(ctx.clone()),
            payloads: PgPayloadStore::new(ctx.clone()),
            index: PgParentOpIndex::new(ctx.clone()),
        },
        &meta,
        ops,
        |nodes, ops| {
            if ops.iter().any(|op| matches!(op.kind, OperationKind::Payload { .. })) {
                // Payload ops can depend on the current node row, so front-load the reads here.
                nodes.preload_for_ops(ops)?;
            }
            Ok(())
        },
        |nodes| nodes.flush_last_change(),
        |index| index.flush(),
    )
}

pub fn append_ops(client: &Rc<RefCell<Client>>, doc_id: &str, ops: &[Operation]) -> Result<u64> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = append_ops_in_tx(client, doc_id, ops);

    match res {
        Ok(v) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(v.inserted_count)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

pub fn append_ops_with_affected_nodes(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    ops: &[Operation],
) -> Result<Vec<NodeId>> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = append_ops_in_tx(client, doc_id, ops);

    match res {
        Ok(v) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(v.affected_nodes)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

#[derive(Default)]
struct AppendOpsResult {
    inserted_count: u64,
    affected_nodes: Vec<NodeId>,
}

fn append_ops_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    ops: &[Operation],
) -> Result<AppendOpsResult> {
    // Serialize per-doc writers across all server instances (incremental materialization updates
    // derived tables + head_seq and is not safe to run concurrently for the same doc_id).
    let meta = load_tree_meta_for_update(client, doc_id)?;
    // This profiler is only for large-upload benchmark/debug runs. The normal
    // append path keeps the hook disabled and pays only the `OnceLock` check.
    let append_profile = append_profile_enabled().then(|| {
        Rc::new(RefCell::new(PgAppendProfile::new(
            ops.len(),
            meta.state().replay_from.is_some(),
            meta.state().head_seq(),
        )))
    });
    let ctx = PgCtx::new_with_profile(client.clone(), doc_id, append_profile.clone())?;

    let bulk_insert_started_at = Instant::now();
    let inserted_op_refs = {
        let mut c = client.borrow_mut();
        bulk_insert_ops_in_tx(&ctx, &mut c, ops)?
    };
    if let Some(profile) = &append_profile {
        let mut profile = profile.borrow_mut();
        profile.bulk_insert_ms += bulk_insert_started_at.elapsed().as_secs_f64() * 1000.0;
        profile.bulk_inserted_ops += inserted_op_refs.len();
    }

    let dedupe_filter_started_at = Instant::now();
    // Only materialize the ops Postgres actually inserted. This keeps duplicate opRefs in the
    // input batch from being replayed twice through core materialization.
    let inserted_ops = select_inserted_ops(&ctx, ops, inserted_op_refs);
    if let Some(profile) = &append_profile {
        profile.borrow_mut().dedupe_filter_ms +=
            dedupe_filter_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    let materialize_started_at = Instant::now();
    let mut update_head_ms = 0.0;
    let mut update_head = |head: &MaterializationHead| {
        let started_at = Instant::now();
        let result = update_tree_meta_head(&ctx.client, &ctx.doc_id, Some(head));
        update_head_ms += started_at.elapsed().as_secs_f64() * 1000.0;
        result
    };
    let apply_result = orchestrate_persisted_remote_append(
        &meta,
        inserted_ops,
        {
            let payloads = PgPayloadStore::new(ctx.clone());
            move |node| payloads.last_writer(node)
        },
        |meta, inserted| materialize_inserted_ops(ctx.clone(), meta, inserted),
        &mut update_head,
        |frontier| set_tree_meta_replay_frontier(client, doc_id, frontier),
        || Ok(load_tree_meta_for_update(client, doc_id)?.0),
        |meta, inserted_op_ids| {
            try_direct_rewind_catch_up_materialized_state(
                &PgOpStorage::new(ctx.clone()),
                inserted_op_ids,
                PersistedRemoteStores {
                    replica_id: ReplicaId::new(b"postgres"),
                    clock: LamportClock::default(),
                    nodes: PgNodeStore::new(ctx.clone()),
                    payloads: PgPayloadStore::new(ctx.clone()),
                    index: PgParentOpIndex::new(ctx.clone()),
                },
                &meta,
                |nodes| nodes.flush_last_change(),
                |index| index.flush(),
            )
        },
        |meta| {
            catch_up_materialized_state(
                PgOpStorage::new(ctx.clone()),
                PersistedRemoteStores {
                    replica_id: ReplicaId::new(b"postgres"),
                    clock: LamportClock::default(),
                    nodes: PgNodeStore::new(ctx.clone()),
                    payloads: PgPayloadStore::new(ctx.clone()),
                    index: PgParentOpIndex::new(ctx.clone()),
                },
                &meta,
                |nodes| nodes.flush_last_change(),
                |index| index.flush(),
            )
        },
        |message| Error::Storage(message.into()),
    )?;
    let catch_up_performed = apply_result.catch_up_needed;
    if let Some(profile) = &append_profile {
        profile.borrow_mut().materialize_ms +=
            materialize_started_at.elapsed().as_secs_f64() * 1000.0;
    }

    if let Some(profile) = &append_profile {
        profile.borrow_mut().update_head_ms += update_head_ms;
        if catch_up_performed {
            profile.borrow_mut().catch_up_performed = true;
        }
        profile.borrow().log(doc_id, apply_result.inserted_count as usize);
    }

    Ok(AppendOpsResult {
        inserted_count: apply_result.inserted_count,
        affected_nodes: apply_result.affected_nodes,
    })
}

pub fn ensure_materialized(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = ensure_materialized_in_tx(client, doc_id);

    match res {
        Ok(()) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(())
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

pub(crate) fn ensure_materialized_in_tx(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let meta = load_tree_meta(client, doc_id)?;
    if meta.state().replay_from.is_none() {
        return Ok(());
    }

    // Take a per-doc lock so catch-up can't race with concurrent append/materialization.
    let meta = load_tree_meta_for_update(client, doc_id)?;
    if meta.state().replay_from.is_none() {
        return Ok(());
    }

    let ctx = PgCtx::new(client.clone(), doc_id)?;
    let storage = PgOpStorage::new(ctx.clone());
    let catch_up = catch_up_materialized_state(
        storage,
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"postgres"),
            clock: LamportClock::default(),
            nodes: PgNodeStore::new(ctx.clone()),
            payloads: PgPayloadStore::new(ctx.clone()),
            index: PgParentOpIndex::new(ctx.clone()),
        },
        &meta,
        |nodes| nodes.flush_last_change(),
        |index| index.flush(),
    )?;

    update_tree_meta_head(client, doc_id, catch_up.head.as_ref())?;

    Ok(())
}

pub fn prime_doc_for_tests(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    ops: &[Operation],
) -> Result<()> {
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, doc_id)?;
    }

    if ops.is_empty() {
        return Ok(());
    }

    append_ops(client, doc_id, ops)?;
    Ok(())
}

pub fn prime_balanced_fanout_doc_for_tests(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    size: usize,
    fanout: usize,
    payload_bytes: usize,
    replica_label: &str,
) -> Result<()> {
    if size == 0 {
        return Err(Error::Storage("fixture size must be positive".into()));
    }
    if fanout == 0 {
        return Err(Error::Storage("fixture fanout must be positive".into()));
    }
    if payload_bytes > 0 && size <= fanout {
        return Err(Error::Storage(format!(
            "payload fixture requires size > fanout ({fanout})"
        )));
    }

    let replica = ReplicaId::new(replica_label.as_bytes());
    let mut ops = Vec::with_capacity(size + usize::from(payload_bytes > 0));
    for node_index in 1..=size {
        let (parent, position) = balanced_parent_and_position(node_index, fanout);
        ops.push(Operation::insert(
            &replica,
            node_index as u64,
            node_index as u64,
            parent,
            NodeId(node_index as u128),
            order_key_from_position(position),
        ));
    }
    if payload_bytes > 0 {
        let payload_counter = (size + 1) as u64;
        ops.push(Operation::set_payload(
            &replica,
            payload_counter,
            payload_counter,
            NodeId((fanout + 1) as u128),
            vec![0xAB; payload_bytes],
        ));
    }

    prime_doc_for_tests(client, doc_id, &ops)
}
