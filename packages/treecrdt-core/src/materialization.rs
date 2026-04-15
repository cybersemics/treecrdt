use std::cmp::Ordering;

use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{
    Clock, LamportClock, MemoryNodeStore, MemoryPayloadStore, NodeStore, NoopStorage,
    ParentOpIndex, PayloadStore, Storage,
};
use crate::tree::TreeCrdt;
use crate::{Error, Lamport, NodeId, OperationId, ReplicaId, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationFrontier {
    pub lamport: Lamport,
    pub replica: Vec<u8>,
    pub counter: u64,
}

/// Snapshot of adapter-maintained materialization metadata.
pub trait MaterializationCursor {
    fn head_lamport(&self) -> Lamport;
    fn head_replica(&self) -> &[u8];
    fn head_counter(&self) -> u64;
    fn head_seq(&self) -> u64;
    fn replay_lamport(&self) -> Option<Lamport> {
        None
    }
    fn replay_replica(&self) -> Option<&[u8]> {
        None
    }
    fn replay_counter(&self) -> Option<u64> {
        None
    }
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
    /// This is empty when nothing was inserted or when the helper had to defer catch-up by
    /// recording a replay frontier instead of trusting incremental materialization.
    pub affected_nodes: Vec<NodeId>,
    /// True when the helper recorded a replay frontier instead of advancing materialization head
    /// immediately.
    pub replay_deferred: bool,
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

#[derive(Default)]
struct RecordingIndex {
    records: Vec<(NodeId, OperationId, u64)>,
}

impl ParentOpIndex for RecordingIndex {
    fn reset(&mut self) -> Result<()> {
        self.records.clear();
        Ok(())
    }

    fn record(&mut self, parent: NodeId, op_id: &OperationId, seq: u64) -> Result<()> {
        self.records.push((parent, op_id.clone(), seq));
        Ok(())
    }
}

struct PrefixSnapshot {
    crdt: TreeCrdt<NoopStorage, LamportClock, MemoryNodeStore, MemoryPayloadStore>,
    index: RecordingIndex,
    head: Option<Operation>,
    seq: u64,
}

fn frontier_from_op(op: &Operation) -> MaterializationFrontier {
    MaterializationFrontier {
        lamport: op.meta.lamport,
        replica: op.meta.id.replica.as_bytes().to_vec(),
        counter: op.meta.id.counter,
    }
}

fn cmp_frontiers(a: &MaterializationFrontier, b: &MaterializationFrontier) -> Ordering {
    cmp_op_key(
        a.lamport,
        a.replica.as_slice(),
        a.counter,
        b.lamport,
        b.replica.as_slice(),
        b.counter,
    )
}

fn cursor_head<M: MaterializationCursor>(meta: &M) -> Option<MaterializationFrontier> {
    if meta.head_seq() == 0
        && meta.head_lamport() == 0
        && meta.head_replica().is_empty()
        && meta.head_counter() == 0
    {
        return None;
    }

    Some(MaterializationFrontier {
        lamport: meta.head_lamport(),
        replica: meta.head_replica().to_vec(),
        counter: meta.head_counter(),
    })
}

fn cursor_replay_frontier<M: MaterializationCursor>(meta: &M) -> Option<MaterializationFrontier> {
    Some(MaterializationFrontier {
        lamport: meta.replay_lamport()?,
        replica: meta.replay_replica()?.to_vec(),
        counter: meta.replay_counter()?,
    })
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
    let existing = cursor_replay_frontier(meta);

    if let Some(existing) = existing {
        return Some(earlier_frontier(existing, earliest_inserted));
    }

    let head = cursor_head(meta)?;
    if cmp_frontiers(&earliest_inserted, &head) == Ordering::Less {
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
    if ops.is_empty() {
        return Ok(IncrementalApplyResult {
            head: None,
            affected_nodes: Vec::new(),
        });
    }
    if cursor_replay_frontier(meta).is_some() {
        return Err(Error::Storage(
            "materialize called while replay frontier pending".into(),
        ));
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

fn build_prefix_snapshot<S: Storage>(
    storage: &S,
    frontier: &MaterializationFrontier,
    replica_id: &ReplicaId,
) -> Result<PrefixSnapshot> {
    let mut crdt = TreeCrdt::with_stores(
        replica_id.clone(),
        NoopStorage,
        LamportClock::default(),
        MemoryNodeStore::default(),
        MemoryPayloadStore::default(),
    )?;
    let mut index = RecordingIndex::default();
    let mut seq = 0u64;
    let mut head: Option<Operation> = None;

    storage.scan_since(0, &mut |op| {
        if cmp_frontiers(&frontier_from_op(&op), frontier) != Ordering::Less {
            return Ok(());
        }

        match crdt.apply_remote_with_materialization_seq(op.clone(), &mut index, &mut seq)? {
            Some(_) => {
                head = Some(op);
                Ok(())
            }
            None => Err(Error::Storage(
                "prefix replay unexpectedly required nested rebuild".into(),
            )),
        }
    })?;

    Ok(PrefixSnapshot {
        crdt,
        index,
        head,
        seq,
    })
}

fn restore_prefix_snapshot<N: NodeStore, P: PayloadStore, I: ParentOpIndex>(
    prefix: &mut PrefixSnapshot,
    nodes: &mut N,
    payloads: &mut P,
    index: &mut I,
) -> Result<()> {
    let mut all_nodes = prefix.crdt.node_store_mut().all_nodes()?;
    all_nodes.sort();

    for node in &all_nodes {
        nodes.ensure_node(*node)?;
    }

    for node in &all_nodes {
        if *node == NodeId::ROOT {
            continue;
        }
        let parent = prefix.crdt.node_store_mut().parent(*node)?;
        let order_key = prefix.crdt.node_store_mut().order_key(*node)?;
        if let Some(parent) = parent {
            nodes.attach(*node, parent, order_key.unwrap_or_default())?;
        } else {
            nodes.detach(*node)?;
        }
    }

    for node in &all_nodes {
        nodes.set_tombstone(*node, prefix.crdt.node_store_mut().tombstone(*node)?)?;

        let last_change = prefix.crdt.node_store_mut().last_change(*node)?;
        if !last_change.is_empty() {
            nodes.merge_last_change(*node, &last_change)?;
        }

        if let Some(deleted_at) = prefix.crdt.node_store_mut().deleted_at(*node)? {
            nodes.merge_deleted_at(*node, &deleted_at)?;
        }

        if let Some(writer) = prefix.crdt.payload_last_writer(*node)? {
            payloads.set_payload(*node, prefix.crdt.payload(*node)?, writer)?;
        }
    }

    let mut records = prefix.index.records.clone();
    records.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)).then_with(|| a.1.cmp(&b.1)));
    for (parent, op_id, seq) in records {
        index.record(parent, &op_id, seq)?;
    }

    Ok(())
}

/// Catch backend materialized state up to the persisted op log using the replay frontier when
/// available.
pub fn catch_up_materialized_state<S, C, N, P, I, M, FlushNodes, FlushIndex>(
    storage: S,
    stores: PersistedRemoteStores<C, N, P, I>,
    meta: &M,
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
    FlushNodes: FnMut(&mut N) -> Result<()>,
    FlushIndex: FnMut(&mut I) -> Result<()>,
{
    let replay_frontier = cursor_replay_frontier(meta);

    let PersistedRemoteStores {
        replica_id,
        clock,
        mut nodes,
        mut payloads,
        mut index,
    } = stores;

    nodes.reset()?;
    payloads.reset()?;
    index.reset()?;

    let mut head: Option<Operation> = None;
    let mut seq = 0u64;

    if let Some(frontier) = replay_frontier.as_ref() {
        let mut prefix = build_prefix_snapshot(&storage, frontier, &replica_id)?;
        restore_prefix_snapshot(&mut prefix, &mut nodes, &mut payloads, &mut index)?;
        head = prefix.head;
        seq = prefix.seq;
    }

    let mut crdt = TreeCrdt::with_stores(replica_id, NoopStorage, clock, nodes, payloads)?;
    storage.scan_since(0, &mut |op| {
        if let Some(frontier) = replay_frontier.as_ref() {
            if cmp_frontiers(&frontier_from_op(&op), frontier) == Ordering::Less {
                return Ok(());
            }
        }

        match crdt.apply_remote_with_materialization_seq(op.clone(), &mut index, &mut seq)? {
            Some(_) => {
                head = Some(op);
                Ok(())
            }
            None => Err(Error::Storage(
                "catch-up replay unexpectedly required nested rebuild".into(),
            )),
        }
    })?;

    flush_nodes(crdt.node_store_mut())?;
    flush_index(&mut index)?;

    Ok(head.map(|head| MaterializationHead {
        lamport: head.meta.lamport,
        replica: head.meta.id.replica.as_bytes().to_vec(),
        counter: head.meta.id.counter,
        seq,
    }))
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
            replay_deferred: false,
        });
    }

    if let Some(frontier) = next_replay_frontier(meta, &inserted_ops) {
        schedule_replay(&frontier)?;
        return Ok(PersistedRemoteApplyResult {
            inserted_count,
            affected_nodes: Vec::new(),
            replay_deferred: true,
        });
    }

    match materialize_inserted(inserted_ops) {
        Ok(result) => {
            let Some(head) = result.head else {
                schedule_replay(&start_replay_frontier())?;
                return Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    replay_deferred: true,
                });
            };

            if update_head(&head).is_ok() {
                Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: result.affected_nodes,
                    replay_deferred: false,
                })
            } else {
                schedule_replay(&start_replay_frontier())?;
                Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    replay_deferred: true,
                })
            }
        }
        Err(_) => {
            schedule_replay(&start_replay_frontier())?;
            Ok(PersistedRemoteApplyResult {
                inserted_count,
                affected_nodes: Vec::new(),
                replay_deferred: true,
            })
        }
    }
}
