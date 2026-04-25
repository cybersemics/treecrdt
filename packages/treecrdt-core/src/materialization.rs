use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::affected::coalesce_materialization_changes;
use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{
    Clock, ExactNodeStore, ExactPayloadStore, LamportClock, MemoryNodeStore, MemoryPayloadStore,
    MemoryStorage, NodeStore, NoopStorage, ParentOpIndex, PayloadStore, Storage,
    TruncatingParentOpIndex,
};
use crate::tree::TreeCrdt;
use crate::{
    Error, Lamport, MaterializationChange, MaterializationOutcome, NodeId, OperationId, ReplicaId,
    Result,
};

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

    fn owned(&self) -> MaterializationHead {
        MaterializationHead {
            at: MaterializationKey {
                lamport: self.at.lamport,
                replica: self.at.replica.as_ref().to_vec(),
                counter: self.at.counter,
            },
            seq: self.seq,
        }
    }

    fn with_seq(&self, seq: u64) -> MaterializationHead {
        let mut head = self.owned();
        head.seq = seq;
        head
    }
}

impl MaterializationHead {
    fn from_op(op: &Operation, seq: u64) -> Self {
        Self {
            at: MaterializationKey {
                lamport: op.meta.lamport,
                replica: op.meta.id.replica.as_bytes().to_vec(),
                counter: op.meta.id.counter,
            },
            seq,
        }
    }
}

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

impl<R: AsRef<[u8]>> MaterializationCursor for MaterializationState<R> {
    fn state(&self) -> MaterializationStateRef<'_> {
        self.as_borrowed()
    }
}

impl<T: MaterializationCursor + ?Sized> MaterializationCursor for &T {
    fn state(&self) -> MaterializationStateRef<'_> {
        (**self).state()
    }
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
        visit: &mut dyn FnMut(Operation) -> Result<()>,
    ) -> Result<()> {
        let mut ops = self.load_since(0)?;
        ops.sort_by(cmp_ops);
        for op in ops {
            let frontier = frontier_from_op(&op);
            if cmp_frontiers(&frontier, start) == Ordering::Less {
                continue;
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
        Ok(ops.into_iter().rfind(|op| {
            let frontier = frontier_from_op(op);
            cmp_frontiers(&frontier, before) == Ordering::Less
                && matches!(
                    op.kind,
                    crate::ops::OperationKind::Insert { node: n, .. }
                        | crate::ops::OperationKind::Move { node: n, .. }
                        if n == node
                )
        }))
    }

    fn latest_payload_before(
        &self,
        node: NodeId,
        before: &MaterializationFrontierRef<'_>,
    ) -> Result<Option<Operation>> {
        let mut ops = self.load_since(0)?;
        ops.sort_by(cmp_ops);
        Ok(ops.into_iter().rfind(|op| {
            let frontier = frontier_from_op(op);
            cmp_frontiers(&frontier, before) == Ordering::Less
                && match &op.kind {
                    crate::ops::OperationKind::Insert {
                        node: n, payload, ..
                    } => *n == node && payload.is_some(),
                    crate::ops::OperationKind::Payload { node: n, .. } => *n == node,
                    _ => false,
                }
        }))
    }
}

impl FrontierRewindStorage for MemoryStorage {}
impl FrontierRewindStorage for NoopStorage {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalApplyResult {
    pub head: Option<MaterializationHead>,
    pub outcome: MaterializationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchUpResult {
    pub head: Option<MaterializationHead>,
    pub outcome: MaterializationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRemoteApplyResult {
    /// Number of ops from the input batch that were actually inserted by adapter-side dedupe.
    pub inserted_count: u64,
    /// Structured changes produced by core materialization when replay succeeded.
    ///
    /// This is empty when nothing was inserted or when the helper could not advance
    /// materialization immediately and had to hand catch-up work back to the caller.
    pub outcome: MaterializationOutcome,
    /// True when the helper recorded/kept a replay frontier and expects the caller to perform
    /// catch-up.
    pub catch_up_needed: bool,
}

impl PersistedRemoteApplyResult {
    fn empty(inserted_count: u64, head_seq: u64) -> Self {
        Self {
            inserted_count,
            outcome: MaterializationOutcome::empty(head_seq),
            catch_up_needed: false,
        }
    }

    fn applied(inserted_count: u64, outcome: MaterializationOutcome) -> Self {
        Self {
            inserted_count,
            outcome,
            catch_up_needed: false,
        }
    }

    fn needs_catch_up(inserted_count: u64, head_seq: u64) -> Self {
        Self {
            inserted_count,
            outcome: MaterializationOutcome::empty(head_seq),
            catch_up_needed: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadNoopShortcut {
    pub resumed_head: MaterializationHead,
    pub remaining_ops: Vec<Operation>,
    pub outcome: MaterializationOutcome,
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

struct RebuiltMaterialization {
    crdt: TreeCrdt<NoopStorage, LamportClock, MemoryNodeStore, MemoryPayloadStore>,
    index: RecordingIndex,
    head: Option<Operation>,
    seq: u64,
}

enum ReplayChangeScope<'a> {
    All,
    FromFrontier(&'a MaterializationFrontier),
}

impl ReplayChangeScope<'_> {
    fn includes(&self, op: &Operation) -> bool {
        match self {
            Self::All => true,
            Self::FromFrontier(frontier) => {
                cmp_frontiers(&frontier_from_op(op), frontier) != Ordering::Less
            }
        }
    }
}

struct ReplayRun {
    head: Option<Operation>,
    seq: u64,
    prefix_seq: u64,
    outcome: MaterializationOutcome,
}

struct ReplayAccumulator<'a> {
    seq: u64,
    head: Option<Operation>,
    changes: Vec<MaterializationChange>,
    prefix_seq: Option<u64>,
    change_scope: ReplayChangeScope<'a>,
}

impl<'a> ReplayAccumulator<'a> {
    fn new(seq: u64, change_scope: ReplayChangeScope<'a>) -> Self {
        Self {
            seq,
            head: None,
            changes: Vec::new(),
            prefix_seq: None,
            change_scope,
        }
    }

    fn before_op(&mut self, op: &Operation) -> bool {
        let collect = self.change_scope.includes(op);
        if collect && self.prefix_seq.is_none() {
            self.prefix_seq = Some(self.seq);
        }
        collect
    }

    fn record_applied(
        &mut self,
        op: Operation,
        collect: bool,
        changes: Vec<MaterializationChange>,
    ) {
        self.head = Some(op);
        if collect {
            self.changes.extend(changes);
        }
    }

    fn apply_remote(
        &mut self,
        crdt: &mut TreeCrdt<impl Storage, impl Clock, impl NodeStore, impl PayloadStore>,
        index: &mut impl ParentOpIndex,
        op: Operation,
        rejected_op_error: Option<&'static str>,
    ) -> Result<()> {
        let collect = self.before_op(&op);
        match crdt.apply_remote_with_materialization_seq(op.clone(), index, &mut self.seq)? {
            Some(delta) => self.record_applied(op, collect, delta.changes),
            None => {
                if let Some(message) = rejected_op_error {
                    return Err(Error::Storage(message.into()));
                }
            }
        }
        Ok(())
    }

    fn apply_sorted(
        &mut self,
        crdt: &mut TreeCrdt<impl Storage, impl Clock, impl NodeStore, impl PayloadStore>,
        index: &mut impl ParentOpIndex,
        op: Operation,
    ) -> Result<()> {
        let collect = self.before_op(&op);
        self.seq = self.seq.saturating_add(1);
        let delta = crdt.apply_sorted_remote_with_materialization(op.clone(), index, self.seq)?;
        self.record_applied(op, collect, delta.changes);
        Ok(())
    }

    fn finish(self) -> ReplayRun {
        ReplayRun {
            head: self.head,
            seq: self.seq,
            prefix_seq: self.prefix_seq.unwrap_or(self.seq),
            outcome: MaterializationOutcome::from_changes(self.seq, self.changes),
        }
    }
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
    // Direct rewind is deliberately local: ask storage for the previous structural winner for this
    // node, clear the currently materialized attachment, then restore that predecessor if one
    // exists. Anything more complicated (delete/tombstone/revival) stays on the conservative
    // replay-from-frontier path.
    let previous = storage.latest_structural_before(node, &frontier_from_op(op).as_borrowed())?;
    nodes.ensure_node(node)?;
    nodes.detach(node)?;

    match previous.as_ref().map(|prev| &prev.kind) {
        Some(crate::ops::OperationKind::Insert {
            parent, order_key, ..
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
    // Payload rewind needs the previous winning payload-bearing op, not just the current bytes.
    // If no predecessor exists, direct rewind must clear the payload row entirely.
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
    }

    if skipped == 0 {
        return Ok(None);
    }

    let resumed_seq = head.seq.saturating_add(skipped);
    Ok(Some(PayloadNoopShortcut {
        resumed_head: head.with_seq(resumed_seq),
        remaining_ops,
        outcome: MaterializationOutcome::empty(resumed_seq),
    }))
}

impl MaterializationOutcome {
    fn from_changes(head_seq: u64, changes: Vec<MaterializationChange>) -> Self {
        Self {
            head_seq,
            changes: coalesce_materialization_changes(changes),
        }
    }

    fn merge(head_seq: u64, outcomes: impl IntoIterator<Item = MaterializationOutcome>) -> Self {
        Self::from_changes(
            head_seq,
            outcomes.into_iter().flat_map(|outcome| outcome.changes).collect(),
        )
    }
}

/// Apply an incremental batch and return both head metadata and the structured materialization delta.
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
            outcome: MaterializationOutcome::empty(state.head_seq()),
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

    let mut replay = ReplayAccumulator::new(state.head_seq(), ReplayChangeScope::All);
    for op in ops {
        replay.apply_remote(crdt, index, op, None)?;
    }
    let replay = replay.finish();

    let last = replay
        .head
        .as_ref()
        .or_else(|| crdt.head_op())
        .ok_or_else(|| Error::Storage("expected head op after materialization".into()))?;

    Ok(IncrementalApplyResult {
        head: Some(MaterializationHead::from_op(last, replay.seq)),
        outcome: replay.outcome,
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
            outcome: MaterializationOutcome::empty(meta.state().head_seq()),
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
) -> Result<(RebuiltMaterialization, u64, MaterializationOutcome)> {
    let mut crdt = TreeCrdt::with_stores(
        replica_id.clone(),
        NoopStorage,
        LamportClock::default(),
        MemoryNodeStore::default(),
        MemoryPayloadStore::default(),
    )?;
    let mut index = RecordingIndex::default();
    let mut replay = ReplayAccumulator::new(0, ReplayChangeScope::FromFrontier(frontier));

    storage.scan_since(0, &mut |op| {
        replay.apply_remote(
            &mut crdt,
            &mut index,
            op,
            Some("frontier replay unexpectedly required nested catch-up"),
        )
    })?;

    let replay = replay.finish();
    let prefix_seq = replay.prefix_seq;
    let outcome = replay.outcome;
    Ok((
        RebuiltMaterialization {
            crdt,
            index,
            head: replay.head,
            seq: replay.seq,
        },
        prefix_seq,
        outcome,
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

    let mut full_suffix_ops = Vec::new();
    let mut existing_suffix_ops = Vec::new();
    let mut requires_full_replay = false;
    storage.scan_frontier_range(frontier, &mut |op| {
        // One pass does double duty:
        // - `full_suffix_ops` is the corrected suffix we will replay forward.
        // - `existing_suffix_ops` is the subset that was already materialized before this append,
        //   which is exactly what must be unwound first.
        let op_frontier = frontier_from_op(&op);
        if cmp_frontiers(&op_frontier, &head.at) != Ordering::Greater
            && !inserted_op_ids.contains(&op.meta.id)
        {
            existing_suffix_ops.push(op.clone());
        }
        requires_full_replay |= op_requires_full_replay(&op);
        full_suffix_ops.push(op);
        Ok(())
    })?;
    if full_suffix_ops.is_empty() || existing_suffix_ops.is_empty() || requires_full_replay {
        return Ok(None);
    }

    // `head.seq` reflects the fully materialized suffix. Removing the already-materialized suffix
    // yields the trusted prefix length and the first seq that must be rewritten in the index.
    let prefix_seq =
        head.seq.saturating_sub(existing_suffix_ops.len().min(u64::MAX as usize) as u64);
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

    // After rewinding, the backend stores now represent the prefix immediately before the
    // invalidated suffix. Replaying `full_suffix_ops` forward rebuilds only the corrected suffix.
    let mut crdt = TreeCrdt::with_stores(replica_id, NoopStorage, clock, nodes, payloads)?;

    let mut replay = ReplayAccumulator::new(prefix_seq, ReplayChangeScope::All);
    for op in full_suffix_ops {
        replay.apply_sorted(&mut crdt, &mut index, op)?;
    }
    let replay = replay.finish();

    flush_nodes(crdt.node_store_mut())?;
    flush_index(&mut index)?;

    Ok(Some(CatchUpResult {
        head: replay.head.as_ref().map(|head| MaterializationHead::from_op(head, replay.seq)),
        outcome: replay.outcome,
    }))
}

fn patch_final_state_in_place<N, P, I>(
    replay: &mut RebuiltMaterialization,
    prefix_seq: u64,
    affected_nodes: &[NodeId],
    nodes: &mut N,
    payloads: &mut P,
    index: &mut I,
) -> Result<Vec<MaterializationChange>>
where
    N: ExactNodeStore,
    P: ExactPayloadStore,
    I: TruncatingParentOpIndex,
{
    let truncate_from = prefix_seq.saturating_add(1);
    // The in-memory fallback rebuild computes a fresh suffix index. Drop the stale persisted
    // suffix first, then repopulate only the rebuilt suffix records below.
    index.truncate_from(truncate_from)?;

    let mut patch_changes = Vec::new();

    for node in affected_nodes {
        let existed_before = nodes.exists(*node)?;
        let previous_parent = if existed_before {
            nodes.parent(*node)?
        } else {
            None
        };
        let previous_tombstone = if existed_before {
            Some(nodes.tombstone(*node)?)
        } else {
            None
        };
        let final_parent = replay.crdt.node_store_mut().parent(*node)?;
        let final_tombstone = replay.crdt.node_store_mut().tombstone(*node)?;

        nodes.ensure_node(*node)?;
        nodes.detach(*node)?;

        if let Some(parent) = final_parent {
            let order_key = replay.crdt.node_store_mut().order_key(*node)?.unwrap_or_default();
            nodes.attach(*node, parent, order_key)?;
        }

        nodes.set_tombstone(*node, final_tombstone)?;

        if let Some(previous_tombstone) = previous_tombstone {
            match (previous_tombstone, final_tombstone) {
                (true, false) => patch_changes.push(MaterializationChange::Restore {
                    node: *node,
                    parent_after: final_parent.filter(|parent| *parent != NodeId::TRASH),
                    payload: replay.crdt.payload(*node)?,
                }),
                (false, true) => patch_changes.push(MaterializationChange::Delete {
                    node: *node,
                    parent_before: previous_parent.filter(|parent| *parent != NodeId::TRASH),
                }),
                _ => {}
            }
        }

        // These are exact setters on purpose: the backend may already contain newer-looking merged
        // values from the stale suffix, so fallback catch-up must overwrite them with the rebuilt
        // post-replay state rather than merge again.
        let last_change = replay.crdt.node_store_mut().last_change(*node)?;
        nodes.set_last_change_exact(*node, &last_change)?;

        let deleted_at = replay.crdt.node_store_mut().deleted_at(*node)?;
        nodes.set_deleted_at_exact(*node, deleted_at.as_ref())?;

        if let Some(writer) = replay.crdt.payload_last_writer(*node)? {
            payloads.set_payload(*node, replay.crdt.payload(*node)?, writer)?;
        } else {
            payloads.clear_payload(*node)?;
        }
    }

    let mut records: Vec<_> = replay
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

    Ok(MaterializationOutcome::from_changes(replay.seq, patch_changes).changes)
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
        let head_seq = meta.state().head_seq();
        return Ok(CatchUpResult {
            head: meta.state().head.as_ref().map(MaterializationHead::owned),
            outcome: MaterializationOutcome::empty(head_seq),
        });
    };

    let PersistedRemoteStores {
        replica_id,
        clock: _clock,
        mut nodes,
        mut payloads,
        mut index,
    } = stores;

    let (mut replay, prefix_seq, replay_outcome) =
        replay_frontier_in_memory(&storage, frontier, &replica_id)?;
    let mut affected_nodes = replay_outcome.affected_nodes();
    let mut seen_nodes: HashSet<NodeId> = affected_nodes.iter().copied().collect();
    let mut idx = 0usize;
    while idx < affected_nodes.len() {
        if let Some(parent) = replay.crdt.node_store_mut().parent(affected_nodes[idx])? {
            if parent != NodeId::ROOT && parent != NodeId::TRASH && seen_nodes.insert(parent) {
                affected_nodes.push(parent);
            }
        }
        idx += 1;
    }
    affected_nodes.sort();
    affected_nodes.dedup();
    let patch_changes = patch_final_state_in_place(
        &mut replay,
        prefix_seq,
        &affected_nodes,
        &mut nodes,
        &mut payloads,
        &mut index,
    )?;

    flush_nodes(&mut nodes)?;
    flush_index(&mut index)?;

    Ok(CatchUpResult {
        head: replay.head.as_ref().map(|head| MaterializationHead::from_op(head, replay.seq)),
        outcome: MaterializationOutcome::merge(
            replay.seq,
            [
                replay_outcome,
                MaterializationOutcome::from_changes(replay.seq, patch_changes),
            ],
        ),
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
    let head_seq = meta.state().head_seq();

    if inserted_count == 0 {
        return Ok(PersistedRemoteApplyResult::empty(0, head_seq));
    }

    if let Some(frontier) = next_replay_frontier(meta, &inserted_ops) {
        schedule_replay(&frontier)?;
        return Ok(PersistedRemoteApplyResult::needs_catch_up(
            inserted_count,
            head_seq,
        ));
    }

    match materialize_inserted(inserted_ops) {
        Ok(result) => {
            let Some(head) = result.head else {
                schedule_replay(&start_replay_frontier())?;
                return Ok(PersistedRemoteApplyResult::needs_catch_up(
                    inserted_count,
                    head_seq,
                ));
            };

            if update_head(&head).is_ok() {
                Ok(PersistedRemoteApplyResult::applied(
                    inserted_count,
                    result.outcome,
                ))
            } else {
                schedule_replay(&start_replay_frontier())?;
                Ok(PersistedRemoteApplyResult::needs_catch_up(
                    inserted_count,
                    head_seq,
                ))
            }
        }
        Err(_) => {
            schedule_replay(&start_replay_frontier())?;
            Ok(PersistedRemoteApplyResult::needs_catch_up(
                inserted_count,
                head_seq,
            ))
        }
    }
}

/// Shared adapter append orchestration for already-persisted remote ops.
///
/// Adapters still own transactions, dedupe, and concrete backend stores. This helper just
/// centralizes the repeated control flow around:
/// - payload noop shortcut
/// - incremental materialization vs replay frontier scheduling
/// - direct rewind fast path when the current batch introduced the frontier
/// - conservative catch-up fallback
#[allow(clippy::too_many_arguments)]
pub fn orchestrate_persisted_remote_append<
    M,
    LoadWriter,
    MaterializeInserted,
    UpdateHead,
    ScheduleReplay,
    LoadCatchUpMeta,
    TryDirectRewind,
    CatchUp,
    MissingHead,
    E,
>(
    meta: &M,
    inserted_ops: Vec<Operation>,
    mut load_last_writer: LoadWriter,
    mut materialize_inserted: MaterializeInserted,
    mut update_head: UpdateHead,
    mut schedule_replay: ScheduleReplay,
    mut load_catch_up_meta: LoadCatchUpMeta,
    mut try_direct_rewind: TryDirectRewind,
    mut catch_up: CatchUp,
    mut missing_head_error: MissingHead,
) -> std::result::Result<PersistedRemoteApplyResult, E>
where
    M: MaterializationCursor,
    LoadWriter: FnMut(NodeId) -> std::result::Result<Option<(Lamport, OperationId)>, E>,
    MaterializeInserted: FnMut(
        &dyn MaterializationCursor,
        Vec<Operation>,
    ) -> std::result::Result<IncrementalApplyResult, E>,
    UpdateHead: FnMut(&MaterializationHead) -> std::result::Result<(), E>,
    ScheduleReplay: FnMut(&MaterializationFrontier) -> std::result::Result<(), E>,
    LoadCatchUpMeta: FnMut() -> std::result::Result<MaterializationState, E>,
    TryDirectRewind: FnMut(
        &dyn MaterializationCursor,
        &HashSet<OperationId>,
    ) -> std::result::Result<Option<CatchUpResult>, E>,
    CatchUp: FnMut(&dyn MaterializationCursor) -> std::result::Result<CatchUpResult, E>,
    MissingHead: FnMut(&'static str) -> E,
{
    let inserted_count = inserted_ops.len().min(u64::MAX as usize) as u64;
    let head_seq = meta.state().head_seq();
    if inserted_count == 0 {
        return Ok(PersistedRemoteApplyResult::empty(0, head_seq));
    }

    let inserted_op_ids: HashSet<OperationId> =
        inserted_ops.iter().map(|op| op.meta.id.clone()).collect();
    let had_pending_frontier = meta.state().replay_from.is_some();

    let apply_result = if let Some(shortcut) = {
        try_shortcut_out_of_order_payload_noops(meta, inserted_ops.clone(), |node| {
            load_last_writer(node)
        })?
    } {
        if shortcut.remaining_ops.is_empty() {
            update_head(&shortcut.resumed_head)?;
            PersistedRemoteApplyResult::applied(inserted_count, shortcut.outcome)
        } else {
            let shortcut_meta = MaterializationState {
                head: Some(shortcut.resumed_head.clone()),
                replay_from: None,
            };
            let result = materialize_inserted(&shortcut_meta, shortcut.remaining_ops)?;
            let Some(head) = result.head else {
                schedule_replay(&start_replay_frontier())?;
                return Ok(PersistedRemoteApplyResult::needs_catch_up(
                    inserted_count,
                    shortcut.resumed_head.seq,
                ));
            };
            update_head(&head)?;

            PersistedRemoteApplyResult::applied(
                inserted_count,
                MaterializationOutcome::merge(head.seq, [shortcut.outcome, result.outcome]),
            )
        }
    } else {
        apply_persisted_remote_ops_with_delta(
            meta,
            inserted_ops,
            |inserted| materialize_inserted(meta, inserted),
            &mut update_head,
            &mut schedule_replay,
        )?
    };

    if !apply_result.catch_up_needed {
        return Ok(apply_result);
    }

    let refreshed_meta = load_catch_up_meta()?;
    let catch_up_result = if !had_pending_frontier {
        try_direct_rewind(&refreshed_meta, &inserted_op_ids)?.unwrap_or(catch_up(&refreshed_meta)?)
    } else {
        catch_up(&refreshed_meta)?
    };

    let head = catch_up_result
        .head
        .as_ref()
        .ok_or_else(|| missing_head_error("expected head after immediate catch-up"))?;
    update_head(head)?;

    Ok(PersistedRemoteApplyResult::applied(
        apply_result.inserted_count,
        catch_up_result.outcome,
    ))
}
