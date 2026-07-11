use std::cmp::Ordering;
use std::collections::HashSet;

use crate::affected::coalesce_materialization_changes;
use crate::ops::{cmp_op_key, cmp_ops, Operation};
use crate::traits::{
    Clock, LamportClock, MemoryNodeStore, MemoryPayloadStore, NodeStore, NoopStorage,
    ParentOpIndex, PayloadStore, Storage,
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
        replay.apply_remote(crdt, index, op, None)?;
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

fn replay_frontier_in_memory<S: Storage>(
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

    storage.scan_since(0, &mut |op| {
        replay.apply_remote(
            &mut crdt,
            &mut index,
            op,
            Some("frontier replay unexpectedly required nested catch-up"),
        )
    })?;

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

    let rebuilt_frontier = rebuilt.head.as_ref().map(frontier_from_op);
    if let Some(previous_head) = state.head.as_ref() {
        let Some(rebuilt_head) = rebuilt_frontier.as_ref() else {
            return Err(Error::InconsistentState(
                "canonical replay lost the materialized head".into(),
            ));
        };
        if cmp_frontiers(rebuilt_head, &previous_head.at) == Ordering::Less {
            return Err(Error::InconsistentState(
                "canonical replay moved the materialized head backwards".into(),
            ));
        }
    }

    if frontier != &start_replay_frontier() {
        let Some(rebuilt_head) = rebuilt_frontier.as_ref() else {
            return Err(Error::InconsistentState(
                "replay frontier exists beyond an empty operation log".into(),
            ));
        };
        if cmp_frontiers(frontier, rebuilt_head) == Ordering::Greater {
            return Err(Error::InconsistentState(
                "replay frontier is beyond the canonical operation-log head".into(),
            ));
        }
    }

    Ok(())
}

fn repair_visibility_changes<N: NodeStore>(
    rebuilt: &mut RebuiltMaterialization,
    persisted_nodes: &[NodeId],
    nodes: &N,
) -> Result<Vec<MaterializationChange>> {
    let mut changes = Vec::new();
    for node in persisted_nodes {
        if *node == NodeId::ROOT || *node == NodeId::TRASH {
            continue;
        }

        let previous_parent = nodes.parent(*node)?;
        let previous_tombstone = nodes.tombstone(*node)?;
        if rebuilt.crdt.node_store_mut().exists(*node)? {
            let final_parent = rebuilt.crdt.node_store_mut().parent(*node)?;
            let final_tombstone = rebuilt.crdt.node_store_mut().tombstone(*node)?;
            match (previous_tombstone, final_tombstone) {
                (true, false) => changes.push(MaterializationChange::Restore {
                    node: *node,
                    parent_after: final_parent.filter(|parent| *parent != NodeId::TRASH),
                    payload: rebuilt.crdt.payload(*node)?,
                    source: None,
                }),
                (false, true) => changes.push(MaterializationChange::Delete {
                    node: *node,
                    parent_before: previous_parent.filter(|parent| *parent != NodeId::TRASH),
                    source: None,
                }),
                _ => {}
            }
        } else if !previous_tombstone {
            changes.push(MaterializationChange::Delete {
                node: *node,
                parent_before: previous_parent.filter(|parent| *parent != NodeId::TRASH),
                source: None,
            });
        }
    }
    Ok(changes)
}

fn visibility_transition_key(change: &MaterializationChange) -> Option<(NodeId, bool)> {
    match change {
        MaterializationChange::Delete { node, .. } => Some((*node, false)),
        MaterializationChange::Restore { node, .. } => Some((*node, true)),
        _ => None,
    }
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

    let (mut rebuilt, replay_outcome) = replay_frontier_in_memory(&storage, frontier, &replica_id)?;
    validate_rebuilt_state(meta, frontier, &rebuilt)?;

    // Public materialization changes intentionally describe only visible application state. They
    // are not a safe proxy for rows whose internal CRDT metadata changed: a non-dominating delete,
    // for example, can update `deleted_at` without emitting a visible delete event. Rebuild every
    // derived store from the authoritative oplog so metadata and orphan rows converge too.
    let persisted_nodes = nodes.all_nodes()?;
    let mut repair_changes = repair_visibility_changes(&mut rebuilt, &persisted_nodes, &nodes)?;
    // Replay describes the invalidated suffix, while repair changes describe persisted-before to
    // rebuilt-final visibility. The same transition can appear in both views; feeding duplicates
    // to the parity-based coalescer would incorrectly cancel a real Restore/Delete.
    let replayed_visibility: HashSet<_> =
        replay_outcome.changes.iter().filter_map(visibility_transition_key).collect();
    repair_changes.retain(|repair| match visibility_transition_key(repair) {
        Some(transition) => !replayed_visibility.contains(&transition),
        None => true,
    });

    rebuild_derived_state(&mut rebuilt, &mut nodes, &mut payloads, &mut index)?;

    flush_nodes(&mut nodes)?;
    flush_index(&mut index)?;

    Ok(CatchUpResult {
        head: rebuilt
            .head
            .as_ref()
            .map(|head| MaterializationHead::from_op(head, rebuilt.seq)),
        outcome: MaterializationOutcome::merge(
            rebuilt.seq,
            [
                replay_outcome,
                MaterializationOutcome::from_changes(rebuilt.seq, repair_changes),
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
/// - incremental materialization vs replay frontier scheduling
/// - conservative catch-up after any out-of-order append
#[allow(clippy::too_many_arguments)]
pub fn orchestrate_persisted_remote_append<
    M,
    MaterializeInserted,
    UpdateHead,
    ScheduleReplay,
    LoadCatchUpMeta,
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
    CatchUp: FnMut(&dyn MaterializationCursor) -> std::result::Result<CatchUpResult, E>,
    MissingHead: FnMut(&'static str) -> E,
{
    let inserted_count = inserted_ops.len().min(u64::MAX as usize) as u64;
    let head_seq = meta.state().head_seq();
    if inserted_count == 0 {
        return Ok(PersistedRemoteApplyResult::empty(0, head_seq));
    }

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
    let catch_up_result = catch_up(&refreshed_meta)?;

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
