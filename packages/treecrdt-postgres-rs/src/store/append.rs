use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Instant;

use postgres::Client;

use treecrdt_core::{
    apply_persisted_remote_ops_with_delta, catch_up_materialized_state,
    materialize_persisted_remote_ops_with_delta, try_direct_rewind_catch_up_materialized_state,
    try_shortcut_out_of_order_payload_noops, Error, LamportClock, MaterializationCursor,
    MaterializationHead, MaterializationState, NodeId, Operation, OperationId, OperationKind,
    PersistedRemoteStores, ReplicaId, Result,
};

use crate::profile::{append_profile_enabled, PgAppendProfile};

use super::*;
use super::meta::load_tree_meta;

fn materialize_inserted_ops(
    ctx: PgCtx,
    meta: &TreeMeta,
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
        meta,
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

fn merge_affected_nodes(mut left: Vec<NodeId>, right: Vec<NodeId>) -> Vec<NodeId> {
    left.extend(right);
    left.sort();
    left.dedup();
    left
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
    let inserted_op_ids: HashSet<OperationId> =
        inserted_ops.iter().map(|op| op.meta.id.clone()).collect();
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
    let apply_result = if let Some(shortcut) = {
        let payloads = PgPayloadStore::new(ctx.clone());
        try_shortcut_out_of_order_payload_noops(&meta, inserted_ops.clone(), |node| {
            payloads.last_writer(node)
        })?
    } {
        if shortcut.remaining_ops.is_empty() {
            update_head(&shortcut.resumed_head)?;
            treecrdt_core::PersistedRemoteApplyResult {
                inserted_count: inserted_ops.len().min(u64::MAX as usize) as u64,
                affected_nodes: shortcut.affected_nodes,
                catch_up_needed: false,
            }
        } else {
            let shortcut_meta = TreeMeta(MaterializationState {
                head: Some(shortcut.resumed_head.clone()),
                replay_from: None,
            });
            let result =
                materialize_inserted_ops(ctx.clone(), &shortcut_meta, shortcut.remaining_ops)?;
            let head = result.head.ok_or_else(|| {
                Error::Storage("expected head after payload noop shortcut".into())
            })?;
            update_head(&head)?;
            treecrdt_core::PersistedRemoteApplyResult {
                inserted_count: inserted_ops.len().min(u64::MAX as usize) as u64,
                affected_nodes: merge_affected_nodes(
                    shortcut.affected_nodes,
                    result.affected_nodes,
                ),
                catch_up_needed: false,
            }
        }
    } else {
        apply_persisted_remote_ops_with_delta(
            &meta,
            inserted_ops,
            |inserted| materialize_inserted_ops(ctx.clone(), &meta, inserted),
            &mut update_head,
            |frontier| set_tree_meta_replay_frontier(client, doc_id, frontier),
        )?
    };
    let catch_up_performed = apply_result.catch_up_needed;
    let apply_result = if catch_up_performed {
        let refreshed_meta = load_tree_meta_for_update(client, doc_id)?;
        let catch_up = if meta.state().replay_from.is_none() {
            // No replay frontier existed before this append, so if one was just scheduled it came
            // from the current batch. That is the narrow case where the direct rewind fast path is
            // safe to try before falling back to conservative replay-from-frontier catch-up.
            try_direct_rewind_catch_up_materialized_state(
                &PgOpStorage::new(ctx.clone()),
                &inserted_op_ids,
                PersistedRemoteStores {
                    replica_id: ReplicaId::new(b"postgres"),
                    clock: LamportClock::default(),
                    nodes: PgNodeStore::new(ctx.clone()),
                    payloads: PgPayloadStore::new(ctx.clone()),
                    index: PgParentOpIndex::new(ctx.clone()),
                },
                &refreshed_meta,
                |nodes| nodes.flush_last_change(),
                |index| index.flush(),
            )?
            .unwrap_or(catch_up_materialized_state(
                PgOpStorage::new(ctx.clone()),
                PersistedRemoteStores {
                    replica_id: ReplicaId::new(b"postgres"),
                    clock: LamportClock::default(),
                    nodes: PgNodeStore::new(ctx.clone()),
                    payloads: PgPayloadStore::new(ctx.clone()),
                    index: PgParentOpIndex::new(ctx.clone()),
                },
                &refreshed_meta,
                |nodes| nodes.flush_last_change(),
                |index| index.flush(),
            )?)
        } else {
            catch_up_materialized_state(
                PgOpStorage::new(ctx.clone()),
                PersistedRemoteStores {
                    replica_id: ReplicaId::new(b"postgres"),
                    clock: LamportClock::default(),
                    nodes: PgNodeStore::new(ctx.clone()),
                    payloads: PgPayloadStore::new(ctx.clone()),
                    index: PgParentOpIndex::new(ctx.clone()),
                },
                &refreshed_meta,
                |nodes| nodes.flush_last_change(),
                |index| index.flush(),
            )?
        };
        update_head(
            catch_up
                .head
                .as_ref()
                .ok_or_else(|| Error::Storage("expected head after immediate catch-up".into()))?,
        )?;
        treecrdt_core::PersistedRemoteApplyResult {
            inserted_count: apply_result.inserted_count,
            affected_nodes: catch_up.affected_nodes,
            catch_up_needed: false,
        }
    } else {
        apply_result
    };
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
