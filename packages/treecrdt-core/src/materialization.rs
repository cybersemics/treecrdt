use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::affected::coalesce_materialization_changes;
use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{
    Clock, ExactPayloadStore, LamportClock, MemoryNodeStore, MemoryPayloadStore, MemoryStorage,
    NodeStore, NoopStorage, ParentOpIndex, PayloadStore, Storage, TruncatingParentOpIndex,
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
            if cmp_op_to_frontier(&op, start) == Ordering::Less {
                continue;
            }
            visit(op)?;
        }
        Ok(())
    }

    fn latest_payload_before(
        &self,
        node: NodeId,
        before: &MaterializationFrontierRef<'_>,
    ) -> Result<Option<Operation>> {
        let mut ops = self.load_since(0)?;
        ops.sort_by(cmp_ops);
        Ok(ops.into_iter().rfind(|op| {
            cmp_op_to_frontier(op, before) == Ordering::Less
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
    /// Structured changes produced when materialization advanced.
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
            Self::FromFrontier(frontier) => cmp_op_to_frontier(op, frontier) != Ordering::Less,
        }
    }
}

struct ReplayRun {
    head: Option<Operation>,
    seq: u64,
    outcome: MaterializationOutcome,
}

struct ReplayAccumulator<'a> {
    seq: u64,
    head: Option<Operation>,
    changes: Vec<MaterializationChange>,
    change_scope: ReplayChangeScope<'a>,
}

impl<'a> ReplayAccumulator<'a> {
    fn new(seq: u64, change_scope: ReplayChangeScope<'a>) -> Self {
        Self {
            seq,
            head: None,
            changes: Vec::new(),
            change_scope,
        }
    }

    fn before_op(&mut self, op: &Operation) -> bool {
        self.change_scope.includes(op)
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
    ) -> Result<()> {
        let collect = self.before_op(&op);
        if let Some(delta) =
            crdt.apply_remote_with_materialization_seq(op.clone(), index, &mut self.seq)?
        {
            self.record_applied(op, collect, delta.changes);
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

fn cmp_op_to_frontier<R: AsRef<[u8]>>(
    op: &Operation,
    frontier: &MaterializationKey<R>,
) -> Ordering {
    cmp_op_key(
        op.meta.lamport,
        op.meta.id.replica.as_bytes(),
        op.meta.id.counter,
        frontier.lamport,
        frontier.replica.as_ref(),
        frontier.counter,
    )
}

fn start_replay_frontier() -> MaterializationFrontier {
    MaterializationFrontier {
        lamport: 0,
        replica: Vec::new(),
        counter: 0,
    }
}

fn is_start_replay_frontier<R: AsRef<[u8]>>(frontier: &MaterializationKey<R>) -> bool {
    frontier.lamport == 0 && frontier.replica.as_ref().is_empty() && frontier.counter == 0
}

fn op_requires_full_replay(op: &Operation) -> bool {
    matches!(
        op.kind,
        crate::ops::OperationKind::Delete { .. } | crate::ops::OperationKind::Tombstone { .. }
    )
}

fn payload_from_op(op: &Operation) -> Option<Option<Vec<u8>>> {
    match &op.kind {
        crate::ops::OperationKind::Insert { payload, .. } => payload.clone().map(Some),
        crate::ops::OperationKind::Payload { payload, .. } => Some(payload.clone()),
        _ => None,
    }
}

fn restore_payload_before<S: FrontierRewindStorage, P: ExactPayloadStore>(
    payloads: &mut P,
    storage: &S,
    node: NodeId,
    before: &MaterializationFrontierRef<'_>,
) -> Result<()> {
    let previous = storage.latest_payload_before(node, before)?;

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

fn rewind_existing_payload_suffix_in_place<S, P>(
    payloads: &mut P,
    storage: &S,
    affected_nodes: &HashSet<NodeId>,
    frontier: &MaterializationFrontierRef<'_>,
) -> Result<()>
where
    S: FrontierRewindStorage,
    P: ExactPayloadStore,
{
    for node in affected_nodes {
        restore_payload_before(payloads, storage, *node, frontier)?;
    }

    Ok(())
}

fn next_replay_frontier<M: MaterializationCursor>(
    meta: &M,
    inserted_ops: &[Operation],
) -> Option<MaterializationFrontier> {
    let earliest_inserted = inserted_ops.iter().min_by(|left, right| cmp_ops(left, right))?;
    let state = meta.state();

    if let Some(existing) = state.replay_from.as_ref() {
        return Some(
            if cmp_op_to_frontier(earliest_inserted, existing) == Ordering::Less {
                frontier_from_op(earliest_inserted)
            } else {
                owned_frontier(existing)
            },
        );
    }

    let head = state.head.as_ref()?;
    if cmp_op_to_frontier(earliest_inserted, &head.at) == Ordering::Less {
        Some(frontier_from_op(earliest_inserted))
    } else {
        None
    }
}

impl MaterializationOutcome {
    fn from_changes(head_seq: u64, changes: Vec<MaterializationChange>) -> Self {
        Self {
            head_seq,
            changes: coalesce_materialization_changes(changes),
        }
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
    for op in &ops {
        op.validate()?;
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
        replay.apply_remote(crdt, index, op)?;
    }
    let run = replay.finish();

    let last = run
        .head
        .as_ref()
        .or_else(|| crdt.head_op())
        .ok_or_else(|| Error::Storage("expected head op after materialization".into()))?;

    Ok(IncrementalApplyResult {
        head: Some(MaterializationHead::from_op(last, run.seq)),
        outcome: run.outcome,
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

fn replay_canonical_log_in_memory<S: Storage>(
    storage: &S,
    frontier: &MaterializationFrontier,
    replica_id: &ReplicaId,
) -> Result<(RebuiltMaterialization, MaterializationOutcome)> {
    let mut crdt = TreeCrdt::with_stores(
        replica_id.clone(),
        NoopStorage,
        LamportClock::default(),
        MemoryNodeStore::default(),
        MemoryPayloadStore::default(),
    )?;
    let mut index = RecordingIndex::default();
    let mut replay = ReplayAccumulator::new(0, ReplayChangeScope::FromFrontier(frontier));

    storage.scan_since(0, &mut |op| replay.apply_sorted(&mut crdt, &mut index, op))?;

    let run = replay.finish();
    let outcome = run.outcome;
    Ok((
        RebuiltMaterialization {
            crdt,
            index,
            head: run.head,
            seq: run.seq,
        },
        outcome,
    ))
}

/// Try a direct rewind/replay catch-up for append-time out-of-order suffixes.
///
/// Direct rewind is intentionally limited to an already-materialized payload-only suffix. Payload
/// winners are reversible from the operation log, while structural operations may have been
/// rejected based on the tree at their original apply point and therefore cannot be safely
/// reconstructed from their syntax alone.
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
    N: NodeStore,
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

    if is_start_replay_frontier(frontier) {
        return Ok(None);
    }
    if cmp_frontiers(frontier, &head.at) != Ordering::Less {
        return Ok(None);
    }

    let mut full_suffix_ops = Vec::new();
    let mut existing_suffix_count = 0_u64;
    let mut existing_suffix_payload_nodes = HashSet::new();
    let mut existing_suffix_is_payload_only = true;
    let mut requires_full_replay = false;
    storage.scan_frontier_range(frontier, &mut |op| {
        if cmp_op_to_frontier(&op, &head.at) != Ordering::Greater
            && !inserted_op_ids.contains(&op.meta.id)
        {
            existing_suffix_count = existing_suffix_count.saturating_add(1);
            if let crate::ops::OperationKind::Payload { node, .. } = &op.kind {
                existing_suffix_payload_nodes.insert(*node);
            } else {
                existing_suffix_is_payload_only = false;
            }
        }
        requires_full_replay |= op_requires_full_replay(&op);
        full_suffix_ops.push(op);
        Ok(())
    })?;

    if full_suffix_ops.is_empty()
        || existing_suffix_count == 0
        || requires_full_replay
        || !existing_suffix_is_payload_only
    {
        return Ok(None);
    }

    let prefix_seq = head.seq.saturating_sub(existing_suffix_count);
    let truncate_from = prefix_seq.saturating_add(1);

    let PersistedRemoteStores {
        replica_id,
        clock,
        nodes,
        mut payloads,
        mut index,
    } = stores;

    let before = snapshot_visible_state(&nodes, |node| payloads.payload(node))?;
    index.truncate_from(truncate_from)?;
    rewind_existing_payload_suffix_in_place(
        &mut payloads,
        storage,
        &existing_suffix_payload_nodes,
        frontier,
    )?;

    let mut crdt = TreeCrdt::with_stores(replica_id, NoopStorage, clock, nodes, payloads)?;
    let mut replay = ReplayAccumulator::new(prefix_seq, ReplayChangeScope::All);
    for op in full_suffix_ops {
        replay.apply_sorted(&mut crdt, &mut index, op)?;
    }
    let run = replay.finish();
    let after = snapshot_visible_state(crdt.node_store(), |node| crdt.payload(node))?;
    let visible_changes = visible_changes_between(&before, &after);

    flush_nodes(crdt.node_store_mut())?;
    flush_index(&mut index)?;

    Ok(Some(CatchUpResult {
        head: run.head.as_ref().map(|head| MaterializationHead::from_op(head, run.seq)),
        outcome: MaterializationOutcome::from_changes(run.seq, visible_changes),
    }))
}

fn validate_rebuilt_state<M: MaterializationCursor>(
    meta: &M,
    frontier: &MaterializationFrontier,
    rebuilt: &RebuiltMaterialization,
) -> Result<()> {
    let state = meta.state();
    if rebuilt.seq < state.head_seq() {
        return Err(Error::InconsistentState(format!(
            "canonical replay regressed from {} to {} operations",
            state.head_seq(),
            rebuilt.seq
        )));
    }

    if let Some(previous_head) = state.head.as_ref() {
        let Some(rebuilt_head) = rebuilt.head.as_ref() else {
            return Err(Error::InconsistentState(
                "canonical replay lost the materialized head".into(),
            ));
        };
        if cmp_op_to_frontier(rebuilt_head, &previous_head.at) == Ordering::Less {
            return Err(Error::InconsistentState(
                "canonical replay moved the materialized head backwards".into(),
            ));
        }
    }

    if !is_start_replay_frontier(frontier) {
        let Some(rebuilt_head) = rebuilt.head.as_ref() else {
            return Err(Error::InconsistentState(
                "replay frontier exists beyond an empty operation log".into(),
            ));
        };
        if cmp_op_to_frontier(rebuilt_head, frontier) == Ordering::Less {
            return Err(Error::InconsistentState(
                "replay frontier is beyond the canonical operation-log head".into(),
            ));
        }
    }

    Ok(())
}

struct VisibleNodeState {
    parent: Option<NodeId>,
    order_key: Option<Vec<u8>>,
    tombstone: bool,
    payload: Option<Vec<u8>>,
}

fn snapshot_visible_state<N: NodeStore>(
    nodes: &N,
    mut payload: impl FnMut(NodeId) -> Result<Option<Vec<u8>>>,
) -> Result<HashMap<NodeId, VisibleNodeState>> {
    let mut snapshot = HashMap::new();
    for node in nodes.all_nodes()? {
        snapshot.insert(
            node,
            VisibleNodeState {
                parent: nodes.parent(node)?,
                order_key: nodes.order_key(node)?,
                tombstone: nodes.tombstone(node)?,
                payload: payload(node)?,
            },
        );
    }
    Ok(snapshot)
}

fn visible_parent(state: &VisibleNodeState) -> Option<NodeId> {
    state.parent.filter(|parent| *parent != NodeId::TRASH)
}

fn payload_change_between(
    node: NodeId,
    before: Option<&VisibleNodeState>,
    after: Option<&VisibleNodeState>,
) -> Option<MaterializationChange> {
    let previous_payload = before.and_then(|state| state.payload.as_ref());
    let final_payload = after.and_then(|state| state.payload.as_ref());
    (previous_payload != final_payload).then(|| MaterializationChange::Payload {
        node,
        payload: final_payload.cloned(),
        source: None,
    })
}

fn node_change_between(
    node: NodeId,
    before: Option<&VisibleNodeState>,
    after: Option<&VisibleNodeState>,
) -> Option<MaterializationChange> {
    match (before, after) {
        (Some(previous), Some(final_state)) if previous.tombstone && !final_state.tombstone => {
            Some(MaterializationChange::Restore {
                node,
                parent_after: visible_parent(final_state),
                payload: final_state.payload.clone(),
                source: None,
            })
        }
        (Some(previous), Some(final_state)) if !previous.tombstone && final_state.tombstone => {
            Some(MaterializationChange::Delete {
                node,
                parent_before: visible_parent(previous),
                source: None,
            })
        }
        (None, Some(final_state)) if final_state.tombstone => {
            Some(MaterializationChange::Payload {
                node,
                payload: final_state.payload.clone(),
                source: None,
            })
        }
        (None, Some(final_state)) => Some(match final_state.parent {
            Some(parent_after) => MaterializationChange::Insert {
                node,
                parent_after,
                payload: final_state.payload.clone(),
                source: None,
            },
            None => MaterializationChange::Payload {
                node,
                payload: final_state.payload.clone(),
                source: None,
            },
        }),
        (Some(previous), None) if previous.tombstone => Some(MaterializationChange::Payload {
            node,
            payload: None,
            source: None,
        }),
        (Some(previous), None) => Some(MaterializationChange::Delete {
            node,
            parent_before: visible_parent(previous),
            source: None,
        }),
        (Some(previous), Some(final_state)) if !previous.tombstone && !final_state.tombstone => {
            match (previous.parent, final_state.parent) {
                (None, Some(parent_after)) => Some(MaterializationChange::Insert {
                    node,
                    parent_after,
                    payload: final_state.payload.clone(),
                    source: None,
                }),
                (Some(parent_before), None) => Some(MaterializationChange::Delete {
                    node,
                    parent_before: (parent_before != NodeId::TRASH).then_some(parent_before),
                    source: None,
                }),
                (Some(parent_before), Some(parent_after))
                    if parent_before != parent_after
                        || previous.order_key != final_state.order_key =>
                {
                    Some(MaterializationChange::Move {
                        node,
                        parent_before: (parent_before != NodeId::TRASH).then_some(parent_before),
                        parent_after,
                        source: None,
                    })
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn visible_changes_between(
    before: &HashMap<NodeId, VisibleNodeState>,
    after: &HashMap<NodeId, VisibleNodeState>,
) -> Vec<MaterializationChange> {
    let mut all_nodes: Vec<_> = before.keys().chain(after.keys()).copied().collect();
    all_nodes.sort();
    all_nodes.dedup();

    let mut changes = Vec::new();
    for node in all_nodes {
        if node == NodeId::ROOT || node == NodeId::TRASH {
            changes.extend(payload_change_between(
                node,
                before.get(&node),
                after.get(&node),
            ));
            continue;
        }

        let before_state = before.get(&node);
        let after_state = after.get(&node);
        changes.extend(node_change_between(node, before_state, after_state));
        changes.extend(payload_change_between(node, before_state, after_state));
    }
    changes
}

fn rebuild_derived_state<N, P, I>(
    rebuilt: &mut RebuiltMaterialization,
    nodes: &mut N,
    payloads: &mut P,
    index: &mut I,
) -> Result<()>
where
    N: NodeStore,
    P: PayloadStore,
    I: ParentOpIndex,
{
    // The oplog is authoritative. Resetting every derived store removes orphan rows and stale
    // payload/index entries without requiring a second family of exact-overwrite adapter APIs.
    nodes.reset()?;
    payloads.reset()?;
    index.reset()?;

    let mut rebuilt_nodes = rebuilt.crdt.node_store_mut().all_nodes()?;
    rebuilt_nodes.sort();
    for node in rebuilt_nodes {
        nodes.ensure_node(node)?;

        if let Some(parent) = rebuilt.crdt.node_store_mut().parent(node)? {
            let order_key = rebuilt.crdt.node_store_mut().order_key(node)?.unwrap_or_default();
            nodes.attach(node, parent, order_key)?;
        }

        nodes.set_tombstone(node, rebuilt.crdt.node_store_mut().tombstone(node)?)?;

        let last_change = rebuilt.crdt.node_store_mut().last_change(node)?;
        nodes.merge_last_change(node, &last_change)?;

        if let Some(deleted_at) = rebuilt.crdt.node_store_mut().deleted_at(node)? {
            nodes.merge_deleted_at(node, &deleted_at)?;
        }

        if let Some(writer) = rebuilt.crdt.payload_last_writer(node)? {
            payloads.set_payload(node, rebuilt.crdt.payload(node)?, writer)?;
        }
    }

    let mut records = std::mem::take(&mut rebuilt.index.records);
    records.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0)).then_with(|| a.1.cmp(&b.1)));
    for (parent, op_id, seq) in records {
        index.record(parent, &op_id, seq)?;
    }

    Ok(())
}

/// Catch backend materialized state up from a replay frontier by rebuilding derived stores from
/// the canonical operation log.
///
/// The returned outcome is the exact node-backed application-visible delta between persisted state
/// before the rebuild and canonical final state. Transient replay changes that cancel are omitted.
/// Payload-only orphan rows are still repaired, but cannot be reported because [`PayloadStore`]
/// does not expose key enumeration.
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
    N: NodeStore,
    P: PayloadStore,
    I: ParentOpIndex,
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

    let (mut rebuilt, _replay_outcome) =
        replay_canonical_log_in_memory(&storage, frontier, &replica_id)?;
    validate_rebuilt_state(meta, frontier, &rebuilt)?;

    // Replay operations can contain transient moves/restores that leave the externally visible
    // state unchanged. Snapshot both sides so the public outcome describes only the committed net
    // delta, while the rebuild below still repairs every piece of derived metadata.
    let before = snapshot_visible_state(&nodes, |node| payloads.payload(node))?;
    let after =
        snapshot_visible_state(rebuilt.crdt.node_store(), |node| rebuilt.crdt.payload(node))?;
    let visible_changes = visible_changes_between(&before, &after);

    rebuild_derived_state(&mut rebuilt, &mut nodes, &mut payloads, &mut index)?;

    flush_nodes(&mut nodes)?;
    flush_index(&mut index)?;

    Ok(CatchUpResult {
        head: rebuilt
            .head
            .as_ref()
            .map(|head| MaterializationHead::from_op(head, rebuilt.seq)),
        outcome: MaterializationOutcome::from_changes(rebuilt.seq, visible_changes),
    })
}

/// Apply already-persisted inserted remote ops and commit adapter-owned metadata writes.
///
/// Adapters own persistence + dedupe and pass only the inserted subset here. If the materialized
/// doc is already behind a replay frontier, or if incremental materialization fails, this records
/// a frontier and returns control to the caller. Adapters must make failed incremental writes
/// atomic so catch-up observes the state from before the attempt. Metadata-write failures are
/// returned so the adapter's enclosing transaction can roll back.
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

            update_head(&head)?;
            Ok(PersistedRemoteApplyResult::applied(
                inserted_count,
                result.outcome,
            ))
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
/// - incremental materialization vs replay frontier scheduling
/// - narrow direct rewind for an append-time payload-only materialized suffix
/// - canonical reset/rebuild fallback
#[allow(clippy::too_many_arguments)]
pub fn orchestrate_persisted_remote_append<
    M,
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
    let state = meta.state();
    let head_seq = state.head_seq();
    if inserted_count == 0 {
        return Ok(PersistedRemoteApplyResult::empty(0, head_seq));
    }

    let can_attempt_direct_rewind = if state.replay_from.is_none() {
        match (
            state.head.as_ref(),
            inserted_ops.iter().min_by(|left, right| cmp_ops(left, right)),
        ) {
            (Some(head), Some(earliest)) => {
                cmp_op_to_frontier(earliest, &head.at) == Ordering::Less
            }
            _ => false,
        }
    } else {
        false
    };
    let direct_rewind_op_ids = if can_attempt_direct_rewind {
        Some(inserted_ops.iter().map(|op| op.meta.id.clone()).collect::<HashSet<_>>())
    } else {
        None
    };

    let apply_result = apply_persisted_remote_ops_with_delta(
        meta,
        inserted_ops,
        |inserted| materialize_inserted(meta, inserted),
        &mut update_head,
        &mut schedule_replay,
    )?;

    if !apply_result.catch_up_needed {
        return Ok(apply_result);
    }

    // Scheduling replay mutates adapter-owned cursor state, so catch-up must observe a fresh
    // snapshot rather than the metadata passed to the initial incremental attempt.
    let refreshed_meta = load_catch_up_meta()?;
    let catch_up_result = if let Some(inserted_op_ids) = direct_rewind_op_ids.as_ref() {
        match try_direct_rewind(&refreshed_meta, inserted_op_ids)? {
            Some(result) => result,
            None => catch_up(&refreshed_meta)?,
        }
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::ops::Operation;
    use crate::traits::NoopParentOpIndex;

    #[derive(Clone, Default)]
    struct SharedPayloadStore(Rc<RefCell<MemoryPayloadStore>>);

    impl PayloadStore for SharedPayloadStore {
        fn reset(&mut self) -> Result<()> {
            self.0.borrow_mut().reset()
        }

        fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
            self.0.borrow().payload(node)
        }

        fn last_writer(&self, node: NodeId) -> Result<Option<(Lamport, OperationId)>> {
            self.0.borrow().last_writer(node)
        }

        fn set_payload(
            &mut self,
            node: NodeId,
            payload: Option<Vec<u8>>,
            writer: (Lamport, OperationId),
        ) -> Result<()> {
            self.0.borrow_mut().set_payload(node, payload, writer)
        }
    }

    impl ExactPayloadStore for SharedPayloadStore {
        fn clear_payload(&mut self, node: NodeId) -> Result<()> {
            self.0.borrow_mut().clear_payload(node)
        }
    }

    #[test]
    fn direct_rewind_replays_payload_only_existing_suffix() {
        let replica = ReplicaId::new(b"payload-only-direct-rewind");
        let node = NodeId(1);
        let insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, node, vec![0x10]);
        let late_payload = Operation::set_payload(&replica, 2, 2, node, vec![4]);
        let winning_payload = Operation::set_payload(&replica, 3, 3, node, vec![9]);

        let mut storage = MemoryStorage::default();
        storage.apply(insert.clone()).unwrap();
        storage.apply(winning_payload.clone()).unwrap();
        storage.apply(late_payload.clone()).unwrap();

        let mut nodes = MemoryNodeStore::default();
        nodes.ensure_node(node).unwrap();
        nodes.attach(node, NodeId::ROOT, vec![0x10]).unwrap();

        let payloads = SharedPayloadStore::default();
        payloads
            .0
            .borrow_mut()
            .set_payload(
                node,
                Some(vec![9]),
                (
                    winning_payload.meta.lamport,
                    winning_payload.meta.id.clone(),
                ),
            )
            .unwrap();

        let mut index = RecordingIndex::default();
        index.record(NodeId::ROOT, &insert.meta.id, 1).unwrap();
        index.record(NodeId::ROOT, &winning_payload.meta.id, 2).unwrap();

        let meta = MaterializationState {
            head: Some(MaterializationHead::from_op(&winning_payload, 2)),
            replay_from: Some(frontier_from_op(&late_payload)),
        };
        let inserted_ids = HashSet::from([late_payload.meta.id.clone()]);
        let flushed_records = Rc::new(RefCell::new(Vec::new()));
        let flushed_records_out = flushed_records.clone();

        let result = try_direct_rewind_catch_up_materialized_state(
            &storage,
            &inserted_ids,
            PersistedRemoteStores {
                replica_id: ReplicaId::new(b"adapter"),
                clock: LamportClock::default(),
                nodes,
                payloads: payloads.clone(),
                index,
            },
            &meta,
            |_| Ok(()),
            move |index| {
                *flushed_records_out.borrow_mut() = index.records.clone();
                Ok(())
            },
        )
        .unwrap()
        .expect("payload-only suffix should use direct rewind");

        assert_eq!(result.head.as_ref().map(|head| head.seq), Some(3));
        assert_eq!(payloads.payload(node).unwrap(), Some(vec![9]));
        assert_eq!(
            payloads.last_writer(node).unwrap(),
            Some((
                winning_payload.meta.lamport,
                winning_payload.meta.id.clone()
            ))
        );
        assert_eq!(
            flushed_records
                .borrow()
                .iter()
                .map(|(_, op_id, seq)| (op_id.counter, *seq))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 2), (3, 3)]
        );
    }

    #[test]
    fn direct_rewind_can_replay_a_new_move_before_an_existing_payload_suffix() {
        let replica = ReplicaId::new(b"move-before-payload");
        let parent_a = NodeId(1);
        let parent_b = NodeId(2);
        let child = NodeId(3);
        let insert_a = Operation::insert(&replica, 1, 1, NodeId::ROOT, parent_a, vec![0x10]);
        let insert_b = Operation::insert(&replica, 2, 2, NodeId::ROOT, parent_b, vec![0x20]);
        let insert_child = Operation::insert(&replica, 3, 3, parent_a, child, vec![0x10]);
        let late_move = Operation::move_node(&replica, 4, 4, child, parent_b, vec![0x10]);
        let winning_payload = Operation::set_payload(&replica, 5, 5, child, vec![9]);

        let mut storage = MemoryStorage::default();
        for op in [
            &insert_a,
            &insert_b,
            &insert_child,
            &winning_payload,
            &late_move,
        ] {
            storage.apply(op.clone()).unwrap();
        }

        let mut nodes = MemoryNodeStore::default();
        nodes.ensure_node(parent_a).unwrap();
        nodes.attach(parent_a, NodeId::ROOT, vec![0x10]).unwrap();
        nodes.ensure_node(parent_b).unwrap();
        nodes.attach(parent_b, NodeId::ROOT, vec![0x20]).unwrap();
        nodes.ensure_node(child).unwrap();
        nodes.attach(child, parent_a, vec![0x10]).unwrap();

        let payloads = SharedPayloadStore::default();
        payloads
            .0
            .borrow_mut()
            .set_payload(
                child,
                Some(vec![9]),
                (
                    winning_payload.meta.lamport,
                    winning_payload.meta.id.clone(),
                ),
            )
            .unwrap();

        let mut index = RecordingIndex::default();
        index.record(NodeId::ROOT, &insert_a.meta.id, 1).unwrap();
        index.record(NodeId::ROOT, &insert_b.meta.id, 2).unwrap();
        index.record(parent_a, &insert_child.meta.id, 3).unwrap();
        index.record(parent_a, &winning_payload.meta.id, 4).unwrap();

        let meta = MaterializationState {
            head: Some(MaterializationHead::from_op(&winning_payload, 4)),
            replay_from: Some(frontier_from_op(&late_move)),
        };
        let inserted_ids = HashSet::from([late_move.meta.id.clone()]);
        let final_parent = Rc::new(RefCell::new(None));
        let final_parent_out = final_parent.clone();
        let flushed_records = Rc::new(RefCell::new(Vec::new()));
        let flushed_records_out = flushed_records.clone();

        let result = try_direct_rewind_catch_up_materialized_state(
            &storage,
            &inserted_ids,
            PersistedRemoteStores {
                replica_id: ReplicaId::new(b"adapter"),
                clock: LamportClock::default(),
                nodes,
                payloads,
                index,
            },
            &meta,
            move |nodes| {
                *final_parent_out.borrow_mut() = nodes.parent(child)?;
                Ok(())
            },
            move |index| {
                *flushed_records_out.borrow_mut() = index.records.clone();
                Ok(())
            },
        )
        .unwrap()
        .expect("new structural op with a payload-only existing suffix should use direct rewind");

        assert_eq!(result.head.as_ref().map(|head| head.seq), Some(5));
        assert_eq!(*final_parent.borrow(), Some(parent_b));
        assert!(flushed_records.borrow().contains(&(parent_b, late_move.meta.id.clone(), 4)));
        assert!(flushed_records
            .borrow()
            .contains(&(parent_b, winning_payload.meta.id.clone(), 5)));
    }

    #[test]
    fn direct_rewind_declines_an_existing_structural_suffix() {
        let replica = ReplicaId::new(b"structural-fallback");
        let node = NodeId(1);
        let insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, node, vec![0x10]);
        let late_payload = Operation::set_payload(&replica, 2, 2, node, vec![4]);
        let existing_move = Operation::move_node(&replica, 3, 3, node, NodeId::ROOT, vec![0x20]);

        let mut storage = MemoryStorage::default();
        storage.apply(insert).unwrap();
        storage.apply(existing_move.clone()).unwrap();
        storage.apply(late_payload.clone()).unwrap();

        let meta = MaterializationState {
            head: Some(MaterializationHead::from_op(&existing_move, 2)),
            replay_from: Some(frontier_from_op(&late_payload)),
        };
        let inserted_ids = HashSet::from([late_payload.meta.id.clone()]);

        let result = try_direct_rewind_catch_up_materialized_state(
            &storage,
            &inserted_ids,
            PersistedRemoteStores {
                replica_id: ReplicaId::new(b"adapter"),
                clock: LamportClock::default(),
                nodes: MemoryNodeStore::default(),
                payloads: MemoryPayloadStore::default(),
                index: NoopParentOpIndex,
            },
            &meta,
            |_| Ok(()),
            |_| Ok(()),
        )
        .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn tombstoned_row_appearance_and_removal_preserve_payload_changes() {
        let node = NodeId(9);
        let mut state = VisibleNodeState {
            parent: Some(NodeId::TRASH),
            order_key: Some(Vec::new()),
            tombstone: true,
            payload: Some(vec![1]),
        };
        let payload = |payload| MaterializationChange::Payload {
            node,
            payload,
            source: None,
        };
        assert_eq!(
            node_change_between(node, None, Some(&state)),
            Some(payload(Some(vec![1])))
        );
        assert_eq!(
            node_change_between(node, Some(&state), None),
            Some(payload(None))
        );
        state.payload = None;
        assert_eq!(
            node_change_between(node, None, Some(&state)),
            Some(payload(None))
        );
    }
}
