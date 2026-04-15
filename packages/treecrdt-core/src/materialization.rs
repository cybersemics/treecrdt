use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{Clock, NodeStore, NoopStorage, ParentOpIndex, PayloadStore, Storage};
use crate::tree::TreeCrdt;
use crate::{Error, Lamport, NodeId, ReplicaId, Result};

/// Snapshot of adapter-maintained materialization metadata.
pub trait MaterializationCursor {
    fn dirty(&self) -> bool;
    fn head_lamport(&self) -> Lamport;
    fn head_replica(&self) -> &[u8];
    fn head_counter(&self) -> u64;
    fn head_seq(&self) -> u64;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationHead {
    pub lamport: Lamport,
    pub replica: Vec<u8>,
    pub counter: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalApplyResult {
    pub head: Option<MaterializationHead>,
    pub affected_nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRemoteApplyResult {
    /// Number of ops from the input batch that were actually inserted by adapter-side dedupe.
    pub inserted_count: u64,
    /// Nodes changed by core materialization when incremental replay succeeded.
    ///
    /// This is empty when nothing was inserted or when the helper had to fall back to marking the
    /// document dirty instead of trusting incremental materialization.
    pub affected_nodes: Vec<NodeId>,
    /// True when adapters should rely on rebuild-on-read instead of the incremental replay result.
    pub dirty_fallback: bool,
}

/// Backend-owned stores used to replay already-persisted remote ops through core semantics.
pub struct PersistedRemoteStores<C, N, P, I> {
    /// Scratch replica id for the temporary `TreeCrdt` used during replay.
    ///
    /// The replayed operations keep their original replica ids; this is only the identity of the
    /// in-memory materializer instance.
    pub replica_id: ReplicaId,
    pub clock: C,
    pub nodes: N,
    pub payloads: P,
    pub index: I,
}

/// Apply an incremental batch and return both head metadata and full affected-node delta.
///
/// `affected_nodes` is deduplicated and sorted (`NodeId` ascending) for stable consumers.
pub fn apply_incremental_ops_with_delta<S, C, N, P, I, M>(
    crdt: &mut TreeCrdt<S, C, N, P>,
    index: &mut I,
    meta: &M,
    mut ops: Vec<Operation>,
) -> Result<IncrementalApplyResult>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
    I: ParentOpIndex,
    M: MaterializationCursor,
{
    if ops.is_empty() {
        return Ok(IncrementalApplyResult {
            head: None,
            affected_nodes: Vec::new(),
        });
    }
    if meta.dirty() {
        return Err(Error::Storage("materialize called while dirty".into()));
    }

    ops.sort_by(cmp_ops);

    if let Some(first) = ops.first() {
        if cmp_op_key(
            first.meta.lamport,
            first.meta.id.replica.as_bytes(),
            first.meta.id.counter,
            meta.head_lamport(),
            meta.head_replica(),
            meta.head_counter(),
        ) == std::cmp::Ordering::Less
        {
            return Err(Error::Storage(
                "out-of-order op before materialized head".into(),
            ));
        }
    }

    let mut seq = meta.head_seq();
    let mut affected = std::collections::HashSet::new();
    for op in ops {
        if let Some(delta) = crdt.apply_remote_with_materialization_seq(op, index, &mut seq)? {
            affected.extend(delta.affected_nodes);
        }
    }

    let last = crdt
        .head_op()
        .ok_or_else(|| Error::Storage("expected head op after materialization".into()))?;

    let mut affected_nodes: Vec<NodeId> = affected.into_iter().collect();
    affected_nodes.sort();

    Ok(IncrementalApplyResult {
        head: Some(MaterializationHead {
            lamport: last.meta.lamport,
            replica: last.meta.id.replica.as_bytes().to_vec(),
            counter: last.meta.id.counter,
            seq,
        }),
        affected_nodes,
    })
}

/// Materialize an already-persisted remote-op batch through a temporary [`TreeCrdt`].
///
/// Adapters provide backend-specific stores plus lightweight prepare/flush hooks, while core owns
/// the canonical op ordering, replay semantics, and affected-node accumulation.
pub fn materialize_persisted_remote_ops_with_delta<C, N, P, I, M, Prepare, FlushNodes, FlushIndex>(
    stores: PersistedRemoteStores<C, N, P, I>,
    meta: &M,
    ops: Vec<Operation>,
    mut prepare_nodes: Prepare,
    mut flush_nodes: FlushNodes,
    mut flush_index: FlushIndex,
) -> Result<IncrementalApplyResult>
where
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
    I: ParentOpIndex,
    M: MaterializationCursor,
    Prepare: FnMut(&mut N, &[Operation]) -> Result<()>,
    FlushNodes: FnMut(&mut N) -> Result<()>,
    FlushIndex: FnMut(&mut I) -> Result<()>,
{
    if ops.is_empty() {
        return Ok(IncrementalApplyResult {
            head: None,
            affected_nodes: Vec::new(),
        });
    }

    let PersistedRemoteStores {
        replica_id,
        clock,
        mut nodes,
        payloads,
        mut index,
    } = stores;

    prepare_nodes(&mut nodes, &ops)?;

    // This temporary TreeCrdt replays ops that the adapter already persisted and filtered to the
    // inserted subset, so it needs core apply semantics but not a live op-log backend.
    let mut crdt = TreeCrdt::with_stores(replica_id, NoopStorage, clock, nodes, payloads)?;
    let result = apply_incremental_ops_with_delta(&mut crdt, &mut index, meta, ops)?;
    flush_nodes(crdt.node_store_mut())?;
    flush_index(&mut index)?;
    Ok(result)
}

/// Apply already-persisted inserted remote ops and commit adapter-owned metadata writes.
///
/// Adapters own persistence + dedupe and pass only the inserted subset here. If the materialized
/// doc is already dirty, or if incremental materialization / head update fails, this marks the
/// document dirty so rebuild-on-read can replay the full log later.
pub fn apply_persisted_remote_ops_with_delta<M, E>(
    meta: &M,
    inserted_ops: Vec<Operation>,
    materialize_inserted: impl FnOnce(Vec<Operation>) -> std::result::Result<IncrementalApplyResult, E>,
    update_head: impl FnOnce(&MaterializationHead) -> std::result::Result<(), E>,
    mut mark_dirty: impl FnMut() -> std::result::Result<(), E>,
) -> PersistedRemoteApplyResult
where
    M: MaterializationCursor,
{
    let inserted_count = inserted_ops.len().min(u64::MAX as usize) as u64;

    if inserted_count == 0 {
        return PersistedRemoteApplyResult {
            inserted_count: 0,
            affected_nodes: Vec::new(),
            dirty_fallback: false,
        };
    }

    if meta.dirty() {
        let _ = mark_dirty();
        return PersistedRemoteApplyResult {
            inserted_count,
            affected_nodes: Vec::new(),
            dirty_fallback: true,
        };
    }

    match materialize_inserted(inserted_ops) {
        Ok(result) => {
            let Some(head) = result.head else {
                let _ = mark_dirty();
                return PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    dirty_fallback: true,
                };
            };

            if update_head(&head).is_ok() {
                PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: result.affected_nodes,
                    dirty_fallback: false,
                }
            } else {
                let _ = mark_dirty();
                PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    dirty_fallback: true,
                }
            }
        }
        Err(_) => {
            let _ = mark_dirty();
            PersistedRemoteApplyResult {
                inserted_count,
                affected_nodes: Vec::new(),
                dirty_fallback: true,
            }
        }
    }
}
