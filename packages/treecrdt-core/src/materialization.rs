use std::cmp::Ordering;

use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{Clock, NodeStore, NoopStorage, ParentOpIndex, PayloadStore, Storage};
use crate::tree::TreeCrdt;
use crate::{Error, Lamport, NodeId, ReplicaId, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationKey<R = Vec<u8>> {
    pub lamport: Lamport,
    pub replica: R,
    pub counter: u64,
}

impl<R: AsRef<[u8]>> MaterializationKey<R> {
    pub fn as_borrowed(&self) -> MaterializationKey<&[u8]> {
        MaterializationKey {
            lamport: self.lamport,
            replica: self.replica.as_ref(),
            counter: self.counter,
        }
    }

    fn to_owned(&self) -> MaterializationKey {
        MaterializationKey {
            lamport: self.lamport,
            replica: self.replica.as_ref().to_vec(),
            counter: self.counter,
        }
    }
}

pub type MaterializationFrontier = MaterializationKey<Vec<u8>>;
pub type MaterializationFrontierRef<'a> = MaterializationKey<&'a [u8]>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationHead<R = Vec<u8>> {
    pub at: MaterializationKey<R>,
    pub seq: u64,
}

impl<R: AsRef<[u8]>> MaterializationHead<R> {
    pub fn as_borrowed(&self) -> MaterializationHead<&[u8]> {
        MaterializationHead {
            at: self.at.as_borrowed(),
            seq: self.seq,
        }
    }

    fn to_owned(&self) -> MaterializationHead {
        MaterializationHead {
            at: self.at.to_owned(),
            seq: self.seq,
        }
    }
}

pub type MaterializationHeadRef<'a> = MaterializationHead<&'a [u8]>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationState<R = Vec<u8>> {
    pub head: Option<MaterializationHead<R>>,
    pub replay_from: Option<MaterializationKey<R>>,
}

pub type MaterializationStateRef<'a> = MaterializationState<&'a [u8]>;

pub const MATERIALIZATION_CHECKPOINT_INTERVAL: u64 = 64;

impl<R> MaterializationState<R> {
    pub fn head_seq(&self) -> u64 {
        self.head.as_ref().map_or(0, |head| head.seq)
    }
}

impl<R: AsRef<[u8]>> MaterializationState<R> {
    pub fn as_borrowed(&self) -> MaterializationState<&[u8]> {
        MaterializationState {
            head: self.head.as_ref().map(MaterializationHead::as_borrowed),
            replay_from: self.replay_from.as_ref().map(MaterializationKey::as_borrowed),
        }
    }
}

/// Snapshot of adapter-maintained materialization metadata.
pub trait MaterializationCursor {
    fn state(&self) -> MaterializationStateRef<'_>;
}

pub fn should_checkpoint_materialization<R>(head: &MaterializationHead<R>) -> bool {
    head.seq == 1 || head.seq.is_multiple_of(MATERIALIZATION_CHECKPOINT_INTERVAL)
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
    /// This is empty when nothing was inserted or when the helper had to defer catch-up by
    /// recording a replay frontier instead of trusting incremental materialization.
    pub affected_nodes: Vec<NodeId>,
    /// True when the helper recorded a replay frontier instead of advancing materialization head
    /// immediately.
    pub frontier_recorded: bool,
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

fn frontier_from_op(op: &Operation) -> MaterializationFrontier {
    MaterializationFrontier {
        lamport: op.meta.lamport,
        replica: op.meta.id.replica.as_bytes().to_vec(),
        counter: op.meta.id.counter,
    }
}

fn cmp_frontiers<R1: AsRef<[u8]>, R2: AsRef<[u8]>>(
    a: &MaterializationKey<R1>,
    b: &MaterializationKey<R2>,
) -> Ordering {
    cmp_op_key(
        a.lamport,
        a.replica.as_ref(),
        a.counter,
        b.lamport,
        b.replica.as_ref(),
        b.counter,
    )
}

fn earlier_frontier(
    left: MaterializationFrontier,
    right: MaterializationFrontier,
) -> MaterializationFrontier {
    if cmp_frontiers(&left, &right) == Ordering::Greater {
        right
    } else {
        left
    }
}

fn start_replay_frontier() -> MaterializationFrontier {
    MaterializationFrontier {
        lamport: 0,
        replica: Vec::new(),
        counter: 0,
    }
}

fn next_replay_frontier<M: MaterializationCursor>(
    meta: &M,
    inserted_ops: &[Operation],
) -> Option<MaterializationFrontier> {
    let earliest_inserted = inserted_ops.iter().map(frontier_from_op).min_by(cmp_frontiers)?;
    let state = meta.state();

    if let Some(existing) = state.replay_from.as_ref() {
        return Some(earlier_frontier(existing.to_owned(), earliest_inserted));
    }

    let head = state.head.as_ref()?;
    if cmp_frontiers(&earliest_inserted, &head.at) == Ordering::Less {
        Some(earliest_inserted)
    } else {
        None
    }
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
    let state = meta.state();

    if ops.is_empty() {
        return Ok(IncrementalApplyResult {
            head: None,
            affected_nodes: Vec::new(),
        });
    }
    if state.replay_from.is_some() {
        return Err(Error::Storage(
            "materialize called while replay frontier pending".into(),
        ));
    }

    ops.sort_by(cmp_ops);

    if let Some(first) = ops.first() {
        if let Some(head) = state.head.as_ref() {
            if cmp_op_key(
                first.meta.lamport,
                first.meta.id.replica.as_bytes(),
                first.meta.id.counter,
                head.at.lamport,
                head.at.replica,
                head.at.counter,
            ) == std::cmp::Ordering::Less
            {
                return Err(Error::Storage(
                    "out-of-order op before materialized head".into(),
                ));
            }
        }
    }

    let mut seq = state.head_seq();
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
            at: MaterializationKey {
                lamport: last.meta.lamport,
                replica: last.meta.id.replica.as_bytes().to_vec(),
                counter: last.meta.id.counter,
            },
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

/// Catch backend materialized state up to the persisted op log using the replay frontier when
/// available.
pub fn catch_up_materialized_state<
    S,
    C,
    N,
    P,
    I,
    M,
    LoadCheckpoint,
    RestoreCheckpoint,
    FlushNodes,
    FlushIndex,
>(
    storage: S,
    stores: PersistedRemoteStores<C, N, P, I>,
    meta: &M,
    mut load_checkpoint: LoadCheckpoint,
    mut restore_checkpoint: RestoreCheckpoint,
    mut flush_nodes: FlushNodes,
    mut flush_index: FlushIndex,
) -> Result<Option<MaterializationHead>>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
    I: ParentOpIndex,
    M: MaterializationCursor,
    LoadCheckpoint: FnMut(&MaterializationFrontier) -> Result<Option<MaterializationHead>>,
    RestoreCheckpoint: FnMut(Option<&MaterializationHead>, &mut N, &mut P, &mut I) -> Result<()>,
    FlushNodes: FnMut(&mut N) -> Result<()>,
    FlushIndex: FnMut(&mut I) -> Result<()>,
{
    let (current_head, replay_frontier) = {
        let state = meta.state();
        (
            state.head.as_ref().map(MaterializationHead::to_owned),
            state.replay_from.as_ref().map(MaterializationKey::to_owned),
        )
    };
    let Some(replay_frontier) = replay_frontier else {
        return Ok(current_head);
    };

    let PersistedRemoteStores {
        replica_id,
        clock,
        mut nodes,
        mut payloads,
        mut index,
    } = stores;

    let checkpoint = load_checkpoint(&replay_frontier)?;
    restore_checkpoint(checkpoint.as_ref(), &mut nodes, &mut payloads, &mut index)?;

    let mut seq = checkpoint.as_ref().map_or(0, |head| head.seq);
    let mut result_head = checkpoint.clone();

    let mut crdt = TreeCrdt::with_stores(replica_id, NoopStorage, clock, nodes, payloads)?;
    storage.scan_after(
        checkpoint
            .as_ref()
            .map(|head| (head.at.lamport, head.at.replica.as_slice(), head.at.counter)),
        &mut |op| {
            let next_frontier = frontier_from_op(&op);
            match crdt.apply_remote_with_materialization_seq(op, &mut index, &mut seq)? {
                Some(_) => {
                    result_head = Some(MaterializationHead {
                        at: next_frontier,
                        seq,
                    });
                    Ok(())
                }
                None => Err(Error::Storage(
                    "catch-up replay unexpectedly required nested catch-up".into(),
                )),
            }
        },
    )?;

    flush_nodes(crdt.node_store_mut())?;
    flush_index(&mut index)?;

    Ok(result_head)
}

/// Apply already-persisted inserted remote ops and commit adapter-owned metadata writes.
///
/// Adapters own persistence + dedupe and pass only the inserted subset here. If the materialized
/// doc is already behind a replay frontier, or if incremental materialization / metadata updates
/// fail, this records a replay frontier so catch-up can repair materialized state later.
pub fn apply_persisted_remote_ops_with_delta<M, E>(
    meta: &M,
    inserted_ops: Vec<Operation>,
    materialize_inserted: impl FnOnce(Vec<Operation>) -> std::result::Result<IncrementalApplyResult, E>,
    update_head: impl FnOnce(&MaterializationHead) -> std::result::Result<(), E>,
    mut schedule_replay: impl FnMut(&MaterializationFrontier) -> std::result::Result<(), E>,
) -> std::result::Result<PersistedRemoteApplyResult, E>
where
    M: MaterializationCursor,
{
    let inserted_count = inserted_ops.len().min(u64::MAX as usize) as u64;

    if inserted_count == 0 {
        return Ok(PersistedRemoteApplyResult {
            inserted_count: 0,
            affected_nodes: Vec::new(),
            frontier_recorded: false,
        });
    }

    if let Some(frontier) = next_replay_frontier(meta, &inserted_ops) {
        schedule_replay(&frontier)?;
        return Ok(PersistedRemoteApplyResult {
            inserted_count,
            affected_nodes: Vec::new(),
            frontier_recorded: true,
        });
    }

    match materialize_inserted(inserted_ops) {
        Ok(result) => {
            let Some(head) = result.head else {
                schedule_replay(&start_replay_frontier())?;
                return Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    frontier_recorded: true,
                });
            };

            if update_head(&head).is_ok() {
                Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: result.affected_nodes,
                    frontier_recorded: false,
                })
            } else {
                schedule_replay(&start_replay_frontier())?;
                Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    frontier_recorded: true,
                })
            }
        }
        Err(_) => {
            schedule_replay(&start_replay_frontier())?;
            Ok(PersistedRemoteApplyResult {
                inserted_count,
                affected_nodes: Vec::new(),
                frontier_recorded: true,
            })
        }
    }
}
