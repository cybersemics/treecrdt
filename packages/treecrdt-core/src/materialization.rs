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

impl IncrementalApplyResult {
    pub fn head(self) -> Option<MaterializationHead> {
        self.head
    }
}

#[derive(Clone, Debug)]
pub struct PersistedRemoteOp {
    pub op: Operation,
    pub inserted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistedRemoteCommitPlan {
    NoChange,
    MarkDirty,
    UpdateHead(MaterializationHead),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRemoteApplyResult {
    pub inserted_count: u64,
    pub affected_nodes: Vec<NodeId>,
    pub commit: PersistedRemoteCommitPlan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistedRemoteCommitStatus {
    NoChange,
    DirtyFallback,
    Incremental,
}

/// Apply an incremental batch through core materialization semantics.
///
/// The batch is sorted in canonical op-key order, validated against the materialized head,
/// and applied with parent-op index + tombstone maintenance.
pub fn apply_incremental_ops<S, C, N, P, I, M>(
    crdt: &mut TreeCrdt<S, C, N, P>,
    index: &mut I,
    meta: &M,
    ops: Vec<Operation>,
) -> Result<Option<MaterializationHead>>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
    I: ParentOpIndex,
    M: MaterializationCursor,
{
    Ok(apply_incremental_ops_with_delta(crdt, index, meta, ops)?.head())
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
    replica_id: ReplicaId,
    clock: C,
    mut nodes: N,
    payloads: P,
    mut index: I,
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

    prepare_nodes(&mut nodes, &ops)?;

    let mut crdt = TreeCrdt::with_stores(replica_id, NoopStorage, clock, nodes, payloads)?;
    let result = apply_incremental_ops_with_delta(&mut crdt, &mut index, meta, ops)?;
    flush_nodes(crdt.node_store_mut())?;
    flush_index(&mut index)?;
    Ok(result)
}

/// Turn an adapter-persisted remote batch into a materialization commit plan.
///
/// Only entries marked `inserted` are replayed. If the materialized doc is already dirty, or if
/// incremental materialization fails, the result instructs adapters to keep the append and mark
/// the document dirty for rebuild-on-read.
pub fn apply_persisted_remote_ops_with_delta<M, E>(
    meta: &M,
    ops: Vec<PersistedRemoteOp>,
    materialize_inserted: impl FnOnce(Vec<Operation>) -> std::result::Result<IncrementalApplyResult, E>,
) -> PersistedRemoteApplyResult
where
    M: MaterializationCursor,
{
    let inserted_ops: Vec<Operation> =
        ops.into_iter().filter(|entry| entry.inserted).map(|entry| entry.op).collect();
    let inserted_count = inserted_ops.len().min(u64::MAX as usize) as u64;

    if inserted_count == 0 {
        return PersistedRemoteApplyResult {
            inserted_count: 0,
            affected_nodes: Vec::new(),
            commit: PersistedRemoteCommitPlan::NoChange,
        };
    }

    if meta.dirty() {
        return PersistedRemoteApplyResult {
            inserted_count,
            affected_nodes: Vec::new(),
            commit: PersistedRemoteCommitPlan::MarkDirty,
        };
    }

    match materialize_inserted(inserted_ops) {
        Ok(result) => {
            let Some(head) = result.head else {
                return PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    commit: PersistedRemoteCommitPlan::MarkDirty,
                };
            };

            PersistedRemoteApplyResult {
                inserted_count,
                affected_nodes: result.affected_nodes,
                commit: PersistedRemoteCommitPlan::UpdateHead(head),
            }
        }
        Err(_) => PersistedRemoteApplyResult {
            inserted_count,
            affected_nodes: Vec::new(),
            commit: PersistedRemoteCommitPlan::MarkDirty,
        },
    }
}

/// Commit a persisted-remote materialization plan using adapter-owned metadata writes.
///
/// If updating the head fails, this falls back to `mark_dirty` and clears the exact
/// `affected_nodes` delta because incremental state can no longer be trusted.
pub fn commit_persisted_remote_result<E>(
    result: &mut PersistedRemoteApplyResult,
    update_head: impl FnOnce(&MaterializationHead) -> std::result::Result<(), E>,
    mut mark_dirty: impl FnMut() -> std::result::Result<(), E>,
) -> PersistedRemoteCommitStatus {
    match &result.commit {
        PersistedRemoteCommitPlan::NoChange => PersistedRemoteCommitStatus::NoChange,
        PersistedRemoteCommitPlan::MarkDirty => {
            let _ = mark_dirty();
            result.affected_nodes.clear();
            PersistedRemoteCommitStatus::DirtyFallback
        }
        PersistedRemoteCommitPlan::UpdateHead(head) => {
            if update_head(head).is_ok() {
                PersistedRemoteCommitStatus::Incremental
            } else {
                let _ = mark_dirty();
                result.affected_nodes.clear();
                PersistedRemoteCommitStatus::DirtyFallback
            }
        }
    }
}

/// Run incremental materialization when possible; otherwise mark the document as dirty.
///
/// Returns `true` when incremental materialization succeeded, `false` when the caller
/// should rely on a full rebuild path later.
pub fn try_incremental_materialization<E>(
    already_dirty: bool,
    incremental: impl FnOnce() -> std::result::Result<(), E>,
    mut mark_dirty: impl FnMut(),
) -> bool {
    if already_dirty {
        mark_dirty();
        return false;
    }

    if incremental().is_err() {
        mark_dirty();
        return false;
    }

    true
}
