use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{
    Clock, ExactNodeStore, ExactPayloadStore, LamportClock, MemoryNodeStore, MemoryPayloadStore,
    MemoryStorage, NodeStore, NoopStorage, ParentOpIndex, PayloadStore, Storage,
    TruncatingParentOpIndex,
};
use crate::tree::TreeCrdt;
use crate::{Error, Lamport, NodeId, OperationId, ReplicaId, Result};

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
}

pub type MaterializationHeadRef<'a> = MaterializationHead<&'a [u8]>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationState<R = Vec<u8>> {
    pub head: Option<MaterializationHead<R>>,
    pub replay_from: Option<MaterializationKey<R>>,
}

pub type MaterializationStateRef<'a> = MaterializationState<&'a [u8]>;

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

/// Optional storage hooks for direct rewind/replay of a frontier-invalidated suffix.
///
/// The default implementations are intentionally naive and scan the full op log in memory. Real
/// storage backends should override these with ordered SQL lookups so the direct rewind fast path
/// only touches the invalidated suffix plus node-scoped predecessor queries.
pub trait FrontierRewindStorage: Storage {
    fn scan_frontier_range(
        &self,
        start: &MaterializationFrontierRef<'_>,
        end: Option<&MaterializationKey<&[u8]>>,
        visit: &mut dyn FnMut(Operation) -> Result<()>,
    ) -> Result<()> {
        let mut ops = self.load_since(0)?;
        ops.sort_by(cmp_ops);
        for op in ops {
            let frontier = frontier_from_op(&op);
            if cmp_frontiers(&frontier, start) == Ordering::Less {
                continue;
            }
            if let Some(end) = end {
                if cmp_frontiers(&frontier, end) == Ordering::Greater {
                    continue;
                }
            }
            visit(op)?;
        }
        Ok(())
    }

    fn latest_structural_before(
        &self,
        node: NodeId,
        before: &MaterializationFrontierRef<'_>,
    ) -> Result<Option<Operation>> {
        let mut ops = self.load_since(0)?;
        ops.sort_by(cmp_ops);
        Ok(ops
            .into_iter()
            .filter(|op| {
                let frontier = frontier_from_op(op);
                cmp_frontiers(&frontier, before) == Ordering::Less
                    && matches!(
                        op.kind,
                        crate::ops::OperationKind::Insert { node: n, .. }
                            | crate::ops::OperationKind::Move { node: n, .. }
                            if n == node
                    )
            })
            .next_back())
    }

    fn latest_payload_before(
        &self,
        node: NodeId,
        before: &MaterializationFrontierRef<'_>,
    ) -> Result<Option<Operation>> {
        let mut ops = self.load_since(0)?;
        ops.sort_by(cmp_ops);
        Ok(ops
            .into_iter()
            .filter(|op| {
                let frontier = frontier_from_op(op);
                cmp_frontiers(&frontier, before) == Ordering::Less
                    && match &op.kind {
                        crate::ops::OperationKind::Insert {
                            node: n,
                            payload,
                            ..
                        } => *n == node && payload.is_some(),
                        crate::ops::OperationKind::Payload { node: n, .. } => *n == node,
                        _ => false,
                    }
            })
            .next_back())
    }
}

impl FrontierRewindStorage for MemoryStorage {}
impl FrontierRewindStorage for NoopStorage {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalApplyResult {
    pub head: Option<MaterializationHead>,
    pub affected_nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchUpResult {
    pub head: Option<MaterializationHead>,
    pub affected_nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRemoteApplyResult {
    /// Number of ops from the input batch that were actually inserted by adapter-side dedupe.
    pub inserted_count: u64,
    /// Nodes changed by core materialization when incremental replay succeeded.
    ///
    /// This is empty when nothing was inserted or when the helper could not advance
    /// materialization immediately and had to hand catch-up work back to the caller.
    pub affected_nodes: Vec<NodeId>,
    /// True when the helper recorded/kept a replay frontier and expects the caller to perform
    /// catch-up.
    pub catch_up_needed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadNoopShortcut {
    pub resumed_head: MaterializationHead,
    pub remaining_ops: Vec<Operation>,
    pub affected_nodes: Vec<NodeId>,
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

impl TruncatingParentOpIndex for RecordingIndex {
    fn truncate_from(&mut self, seq: u64) -> Result<()> {
        self.records.retain(|(_, _, existing_seq)| *existing_seq < seq);
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

fn frontier_from_writer(lamport: Lamport, id: &OperationId) -> MaterializationFrontier {
    MaterializationFrontier {
        lamport,
        replica: id.replica.as_bytes().to_vec(),
        counter: id.counter,
    }
}

fn owned_frontier<R: AsRef<[u8]>>(frontier: &MaterializationKey<R>) -> MaterializationFrontier {
    MaterializationFrontier {
        lamport: frontier.lamport,
        replica: frontier.replica.as_ref().to_vec(),
        counter: frontier.counter,
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

fn op_requires_full_replay(op: &Operation) -> bool {
    matches!(
        op.kind,
        crate::ops::OperationKind::Delete { .. } | crate::ops::OperationKind::Tombstone { .. }
    )
}

fn op_sets_payload(op: &Operation) -> bool {
    match &op.kind {
        crate::ops::OperationKind::Insert { payload, .. } => payload.is_some(),
        crate::ops::OperationKind::Payload { .. } => true,
        _ => false,
    }
}

fn payload_from_op(op: &Operation) -> Option<Option<Vec<u8>>> {
    match &op.kind {
        crate::ops::OperationKind::Insert { payload, .. } => payload.clone().map(Some),
        crate::ops::OperationKind::Payload { payload, .. } => Some(payload.clone()),
        _ => None,
    }
}

fn rewind_structure_op_in_place<S: FrontierRewindStorage, N: NodeStore>(
    nodes: &mut N,
    storage: &S,
    op: &Operation,
) -> Result<()> {
    let node = op.kind.node();
    let previous = storage.latest_structural_before(node, &frontier_from_op(op).as_borrowed())?;
    nodes.ensure_node(node)?;
    nodes.detach(node)?;

    match previous.as_ref().map(|prev| &prev.kind) {
        Some(crate::ops::OperationKind::Insert {
            parent,
            order_key,
            ..
        }) => nodes.attach(node, *parent, order_key.clone())?,
        Some(crate::ops::OperationKind::Move {
            new_parent,
            order_key,
            ..
        }) => nodes.attach(node, *new_parent, order_key.clone())?,
        Some(_) | None => {}
    }

    Ok(())
}

fn rewind_payload_op_in_place<S: FrontierRewindStorage, P: ExactPayloadStore>(
    payloads: &mut P,
    storage: &S,
    op: &Operation,
) -> Result<()> {
    let node = op.kind.node();
    let previous = storage.latest_payload_before(node, &frontier_from_op(op).as_borrowed())?;

    if let Some(previous) = previous {
        let payload = payload_from_op(&previous)
            .ok_or_else(|| Error::Storage("payload rewind expected payload-bearing op".into()))?;
        payloads.set_payload(
            node,
            payload,
            (previous.meta.lamport, previous.meta.id.clone()),
        )?;
    } else {
        payloads.clear_payload(node)?;
    }

    Ok(())
}

fn rewind_existing_suffix_in_place<S, N, P>(
    nodes: &mut N,
    payloads: &mut P,
    storage: &S,
    existing_suffix_ops: &[Operation],
) -> Result<()>
where
    S: FrontierRewindStorage,
    N: NodeStore,
    P: ExactPayloadStore,
{
    for op in existing_suffix_ops.iter().rev() {
        match &op.kind {
            crate::ops::OperationKind::Insert { .. } => {
                if op_sets_payload(op) {
                    rewind_payload_op_in_place(payloads, storage, op)?;
                }
                rewind_structure_op_in_place(nodes, storage, op)?;
            }
            crate::ops::OperationKind::Move { .. } => {
                rewind_structure_op_in_place(nodes, storage, op)?
            }
            crate::ops::OperationKind::Payload { .. } => {
                rewind_payload_op_in_place(payloads, storage, op)?
            }
            crate::ops::OperationKind::Delete { .. }
            | crate::ops::OperationKind::Tombstone { .. } => {
                return Err(Error::Storage(
                    "delete/tombstone ops are not supported by direct rewind".into(),
                ));
            }
        }
    }

    Ok(())
}

fn next_replay_frontier<M: MaterializationCursor>(
    meta: &M,
    inserted_ops: &[Operation],
) -> Option<MaterializationFrontier> {
    let earliest_inserted = inserted_ops.iter().map(frontier_from_op).min_by(cmp_frontiers)?;
    let state = meta.state();

    if let Some(existing) = state.replay_from.as_ref() {
        return Some(earlier_frontier(
            owned_frontier(existing),
            earliest_inserted,
        ));
    }

    let head = state.head.as_ref()?;
    if cmp_frontiers(&earliest_inserted, &head.at) == Ordering::Less {
        Some(earliest_inserted)
    } else {
        None
    }
}

/// Try to skip out-of-order payload ops that are already dominated by a later payload winner.
///
/// This allows adapters to avoid recording a replay frontier for a narrow but common case:
/// older payload ops that do not change materialized payload state even after being inserted
/// earlier in the canonical log order.
pub fn try_shortcut_out_of_order_payload_noops<M, LoadWriter, E>(
    meta: &M,
    inserted_ops: Vec<Operation>,
    mut load_last_writer: LoadWriter,
) -> std::result::Result<Option<PayloadNoopShortcut>, E>
where
    M: MaterializationCursor,
    LoadWriter: FnMut(NodeId) -> std::result::Result<Option<(Lamport, OperationId)>, E>,
{
    let state = meta.state();
    if state.replay_from.is_some() || inserted_ops.is_empty() {
        return Ok(None);
    }

    let Some(head) = state.head.as_ref() else {
        return Ok(None);
    };

    let mut ops = inserted_ops;
    ops.sort_by(cmp_ops);

    let mut candidate_nodes = HashSet::new();
    let mut has_out_of_order = false;
    for op in &ops {
        if cmp_frontiers(&frontier_from_op(op), &head.at) != Ordering::Less {
            continue;
        }
        has_out_of_order = true;
        match &op.kind {
            crate::ops::OperationKind::Payload { node, .. } => {
                candidate_nodes.insert(*node);
            }
            _ => return Ok(None),
        }
    }

    if !has_out_of_order {
        return Ok(None);
    }

    let mut final_writers: HashMap<NodeId, MaterializationFrontier> = HashMap::new();
    for node in &candidate_nodes {
        if let Some((lamport, id)) = load_last_writer(*node)? {
            final_writers.insert(*node, frontier_from_writer(lamport, &id));
        }
    }

    for op in &ops {
        let crate::ops::OperationKind::Payload { node, .. } = &op.kind else {
            continue;
        };
        if !candidate_nodes.contains(node) {
            continue;
        }
        let op_frontier = frontier_from_op(op);
        match final_writers.get(node) {
            Some(existing) if cmp_frontiers(&op_frontier, existing) != Ordering::Greater => {}
            _ => {
                final_writers.insert(*node, op_frontier);
            }
        }
    }

    let mut skipped = 0u64;
    let mut affected = HashSet::new();
    let mut remaining_ops = Vec::new();

    for op in ops {
        let op_frontier = frontier_from_op(&op);
        if cmp_frontiers(&op_frontier, &head.at) != Ordering::Less {
            remaining_ops.push(op);
            continue;
        }

        let node = match &op.kind {
            crate::ops::OperationKind::Payload { node, .. } => *node,
            _ => return Ok(None),
        };

        let Some(final_writer) = final_writers.get(&node) else {
            return Ok(None);
        };
        if cmp_frontiers(&op_frontier, final_writer) != Ordering::Less {
            return Ok(None);
        }

        skipped = skipped.saturating_add(1);
        affected.insert(node);
    }

    if skipped == 0 {
        return Ok(None);
    }

    let mut affected_nodes: Vec<NodeId> = affected.into_iter().collect();
    affected_nodes.sort();

    Ok(Some(PayloadNoopShortcut {
        resumed_head: MaterializationHead {
            at: MaterializationKey {
                lamport: head.at.lamport,
                replica: head.at.replica.to_vec(),
                counter: head.at.counter,
            },
            seq: head.seq.saturating_add(skipped),
        },
        remaining_ops,
        affected_nodes,
    }))
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

fn replay_frontier_in_memory<S: Storage>(
    storage: &S,
    frontier: &MaterializationFrontier,
    replica_id: &ReplicaId,
) -> Result<(PrefixSnapshot, u64, Vec<NodeId>)> {
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
    let mut affected = HashSet::new();
    let mut prefix_seq = None;

    storage.scan_since(0, &mut |op| {
        let in_suffix = cmp_frontiers(&frontier_from_op(&op), frontier) != Ordering::Less;
        if in_suffix && prefix_seq.is_none() {
            prefix_seq = Some(seq);
        }

        match crdt.apply_remote_with_materialization_seq(op.clone(), &mut index, &mut seq)? {
            Some(delta) => {
                head = Some(op);
                if in_suffix {
                    affected.extend(delta.affected_nodes);
                }
                Ok(())
            }
            None => Err(Error::Storage(
                "frontier replay unexpectedly required nested catch-up".into(),
            )),
        }
    })?;

    let mut affected_nodes: Vec<NodeId> = affected.into_iter().collect();
    affected_nodes.sort();
    Ok((
        PrefixSnapshot {
            crdt,
            index,
            head,
            seq,
        },
        prefix_seq.unwrap_or(seq),
        affected_nodes,
    ))
}

/// Try a direct rewind/replay catch-up for append-time out-of-order suffixes.
///
/// This rewinds the already-materialized suffix directly on the backend stores, truncates suffix
/// oprefs, and then replays the full invalidated suffix in canonical order. It deliberately bails
/// out for delete/tombstone suffixes and for broader recovery cases.
pub fn try_direct_rewind_catch_up_materialized_state<S, C, N, P, I, M, FlushNodes, FlushIndex>(
    storage: &S,
    inserted_op_ids: &HashSet<OperationId>,
    stores: PersistedRemoteStores<C, N, P, I>,
    meta: &M,
    mut flush_nodes: FlushNodes,
    mut flush_index: FlushIndex,
) -> Result<Option<CatchUpResult>>
where
    S: FrontierRewindStorage,
    C: Clock,
    N: ExactNodeStore,
    P: ExactPayloadStore,
    I: TruncatingParentOpIndex,
    M: MaterializationCursor,
    FlushNodes: FnMut(&mut N) -> Result<()>,
    FlushIndex: FnMut(&mut I) -> Result<()>,
{
    let state = meta.state();
    let Some(head) = state.head.as_ref() else {
        return Ok(None);
    };
    let Some(frontier) = state.replay_from.as_ref() else {
        return Ok(None);
    };

    if frontier.lamport == 0 && frontier.replica.is_empty() && frontier.counter == 0 {
        return Ok(None);
    }
    if cmp_frontiers(frontier, &head.at) != Ordering::Less {
        return Ok(None);
    }

    let mut existing_suffix_ops = Vec::new();
    storage.scan_frontier_range(frontier, Some(&head.at), &mut |op| {
        if !inserted_op_ids.contains(&op.meta.id) {
            existing_suffix_ops.push(op);
        }
        Ok(())
    })?;
    if existing_suffix_ops.is_empty() {
        return Ok(None);
    }

    let mut full_suffix_ops = Vec::new();
    storage.scan_frontier_range(frontier, None, &mut |op| {
        full_suffix_ops.push(op);
        Ok(())
    })?;
    if full_suffix_ops.is_empty() || full_suffix_ops.iter().any(op_requires_full_replay) {
        return Ok(None);
    }

    let prefix_seq = head
        .seq
        .saturating_sub(existing_suffix_ops.len().min(u64::MAX as usize) as u64);
    let truncate_from = prefix_seq.saturating_add(1);

    let PersistedRemoteStores {
        replica_id,
        clock,
        mut nodes,
        mut payloads,
        mut index,
    } = stores;

    index.truncate_from(truncate_from)?;
    rewind_existing_suffix_in_place(&mut nodes, &mut payloads, storage, &existing_suffix_ops)?;

    let mut crdt = TreeCrdt::with_stores(replica_id, NoopStorage, clock, nodes, payloads)?;

    let mut affected = HashSet::new();
    let mut seq = prefix_seq;
    let mut replay_head: Option<Operation> = None;
    for op in full_suffix_ops {
        seq = seq.saturating_add(1);
        let delta = crdt.apply_sorted_remote_with_materialization(op.clone(), &mut index, seq)?;
        affected.extend(delta.affected_nodes);
        replay_head = Some(op);
    }

    let mut affected_nodes: Vec<NodeId> = affected.into_iter().collect();
    affected_nodes.sort();

    flush_nodes(crdt.node_store_mut())?;
    flush_index(&mut index)?;

    Ok(Some(CatchUpResult {
        head: replay_head.map(|head| MaterializationHead {
            at: MaterializationKey {
                lamport: head.meta.lamport,
                replica: head.meta.id.replica.as_bytes().to_vec(),
                counter: head.meta.id.counter,
            },
            seq,
        }),
        affected_nodes,
    }))
}

fn patch_final_state_in_place<N, P, I>(
    prefix: &mut PrefixSnapshot,
    prefix_seq: u64,
    affected_nodes: &[NodeId],
    nodes: &mut N,
    payloads: &mut P,
    index: &mut I,
) -> Result<()>
where
    N: ExactNodeStore,
    P: ExactPayloadStore,
    I: TruncatingParentOpIndex,
{
    let truncate_from = prefix_seq.saturating_add(1);
    index.truncate_from(truncate_from)?;

    for node in affected_nodes {
        nodes.ensure_node(*node)?;
        nodes.detach(*node)?;

        if let Some(parent) = prefix.crdt.node_store_mut().parent(*node)? {
            let order_key = prefix.crdt.node_store_mut().order_key(*node)?.unwrap_or_default();
            nodes.attach(*node, parent, order_key)?;
        }

        nodes.set_tombstone(*node, prefix.crdt.node_store_mut().tombstone(*node)?)?;

        let last_change = prefix.crdt.node_store_mut().last_change(*node)?;
        nodes.set_last_change_exact(*node, &last_change)?;

        let deleted_at = prefix.crdt.node_store_mut().deleted_at(*node)?;
        nodes.set_deleted_at_exact(*node, deleted_at.as_ref())?;

        if let Some(writer) = prefix.crdt.payload_last_writer(*node)? {
            payloads.set_payload(*node, prefix.crdt.payload(*node)?, writer)?;
        } else {
            payloads.clear_payload(*node)?;
        }
    }

    let mut records: Vec<_> = prefix
        .index
        .records
        .iter()
        .filter(|(_, _, seq)| *seq >= truncate_from)
        .cloned()
        .collect();
    records.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)).then_with(|| a.1.cmp(&b.1)));
    for (parent, op_id, seq) in records {
        index.record(parent, &op_id, seq)?;
    }

    Ok(())
}

/// Catch backend materialized state up from a replay frontier by patching affected backend rows
/// and suffix index entries in place.
pub fn catch_up_materialized_state<S, C, N, P, I, M, FlushNodes, FlushIndex>(
    storage: S,
    stores: PersistedRemoteStores<C, N, P, I>,
    meta: &M,
    mut flush_nodes: FlushNodes,
    mut flush_index: FlushIndex,
) -> Result<CatchUpResult>
where
    S: Storage,
    C: Clock,
    N: ExactNodeStore,
    P: ExactPayloadStore,
    I: TruncatingParentOpIndex,
    M: MaterializationCursor,
    FlushNodes: FnMut(&mut N) -> Result<()>,
    FlushIndex: FnMut(&mut I) -> Result<()>,
{
    let replay_frontier = {
        let state = meta.state();
        state.replay_from.as_ref().map(owned_frontier)
    };

    let Some(frontier) = replay_frontier.as_ref() else {
        return Ok(CatchUpResult {
            head: meta.state().head.as_ref().map(|head| MaterializationHead {
                at: MaterializationKey {
                    lamport: head.at.lamport,
                    replica: head.at.replica.to_vec(),
                    counter: head.at.counter,
                },
                seq: head.seq,
            }),
            affected_nodes: Vec::new(),
        });
    };

    let PersistedRemoteStores {
        replica_id,
        clock: _clock,
        mut nodes,
        mut payloads,
        mut index,
    } = stores;

    let (mut prefix, prefix_seq, affected_nodes) =
        replay_frontier_in_memory(&storage, frontier, &replica_id)?;
    patch_final_state_in_place(
        &mut prefix,
        prefix_seq,
        &affected_nodes,
        &mut nodes,
        &mut payloads,
        &mut index,
    )?;

    flush_nodes(&mut nodes)?;
    flush_index(&mut index)?;

    Ok(CatchUpResult {
        head: prefix.head.map(|head| MaterializationHead {
            at: MaterializationKey {
                lamport: head.meta.lamport,
                replica: head.meta.id.replica.as_bytes().to_vec(),
                counter: head.meta.id.counter,
            },
            seq: prefix.seq,
        }),
        affected_nodes,
    })
}

/// Apply already-persisted inserted remote ops and commit adapter-owned metadata writes.
///
/// Adapters own persistence + dedupe and pass only the inserted subset here. If the materialized
/// doc is already behind a replay frontier, or if incremental materialization / metadata updates
/// fail, this records a replay frontier and returns control to the caller. Callers can then either
/// catch up immediately in the same append flow or defer catch-up to a later read/recovery path.
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
            catch_up_needed: false,
        });
    }

    if let Some(frontier) = next_replay_frontier(meta, &inserted_ops) {
        schedule_replay(&frontier)?;
        return Ok(PersistedRemoteApplyResult {
            inserted_count,
            affected_nodes: Vec::new(),
            catch_up_needed: true,
        });
    }

    match materialize_inserted(inserted_ops) {
        Ok(result) => {
            let Some(head) = result.head else {
                schedule_replay(&start_replay_frontier())?;
                return Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    catch_up_needed: true,
                });
            };

            if update_head(&head).is_ok() {
                Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: result.affected_nodes,
                    catch_up_needed: false,
                })
            } else {
                schedule_replay(&start_replay_frontier())?;
                Ok(PersistedRemoteApplyResult {
                    inserted_count,
                    affected_nodes: Vec::new(),
                    catch_up_needed: true,
                })
            }
        }
        Err(_) => {
            schedule_replay(&start_replay_frontier())?;
            Ok(PersistedRemoteApplyResult {
                inserted_count,
                affected_nodes: Vec::new(),
                catch_up_needed: true,
            })
        }
    }
}
