use std::collections::HashSet;

use crate::affected::{
    coalesce_materialization_changes, direct_materialization_changes,
    materialization_change_from_tombstone_delta, parent_hints_from, TombstoneDelta,
};
use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::ops::{cmp_op_key, Operation, OperationKind};
use crate::traits::{
    Clock, MemoryNodeStore, MemoryPayloadStore, NodeStore, ParentOpIndex, PayloadStore, Storage,
};
use crate::types::{
    ApplyDelta, LocalFinalizePlan, LocalPlacement, MaterializationChange, MaterializationOutcome,
    MaterializationSource, NodeExport, NodeSnapshotExport, PreparedLocalOp,
};
use crate::version_vector::VersionVector;

#[derive(Clone)]
struct NodeSnapshot {
    parent: Option<NodeId>,
    order_key: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ApplyEffects {
    structure: bool,
    structure_changed: bool,
    payload: bool,
    delete: bool,
}

impl ApplyEffects {
    fn any(self) -> bool {
        self.structure || self.payload || self.delete
    }
}

struct ForwardApply {
    snapshot: NodeSnapshot,
    effects: ApplyEffects,
}

fn attach_source_if_missing(
    change: &mut MaterializationChange,
    next_source: Option<MaterializationSource>,
) {
    match change {
        MaterializationChange::Insert { source, .. }
        | MaterializationChange::Move { source, .. }
        | MaterializationChange::Delete { source, .. }
        | MaterializationChange::Restore { source, .. }
        | MaterializationChange::Payload { source, .. } => {
            if source.is_none() {
                *source = next_source;
            }
        }
    }
}

fn rejected_structural_fallback_change(
    op: &Operation,
    source: Option<MaterializationSource>,
) -> Vec<MaterializationChange> {
    match &op.kind {
        // Insert payloads retain their independent LWW semantics even when the requested tree
        // attachment is rejected. Report the payload change without claiming the insert occurred.
        OperationKind::Insert {
            node,
            payload: Some(payload),
            ..
        } => vec![MaterializationChange::Payload {
            node: *node,
            payload: Some(payload.clone()),
            source,
        }],
        OperationKind::Insert { .. } | OperationKind::Move { .. } => Vec::new(),
        _ => direct_materialization_changes(None, op),
    }
}

/// Generic Tree CRDT facade that wires clock and storage together.
pub struct TreeCrdt<S, C, N = MemoryNodeStore, P = MemoryPayloadStore>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    replica_id: ReplicaId,
    storage: S,
    clock: C,
    counter: u64,
    nodes: N,
    version_vector: VersionVector,
    payloads: P,
    head: Option<Operation>,
    op_count: u64,
}

impl<S, C> TreeCrdt<S, C, MemoryNodeStore>
where
    S: Storage,
    C: Clock,
{
    pub fn new(replica_id: ReplicaId, storage: S, clock: C) -> Result<Self> {
        Self::with_stores(
            replica_id,
            storage,
            clock,
            MemoryNodeStore::default(),
            MemoryPayloadStore::default(),
        )
    }
}

impl<S, C, N, P> TreeCrdt<S, C, N, P>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    pub fn with_stores(
        replica_id: ReplicaId,
        storage: S,
        clock: C,
        nodes: N,
        payloads: P,
    ) -> Result<Self> {
        let counter = storage.latest_counter(&replica_id)?;
        let mut clock = clock;
        clock.observe(storage.latest_lamport());
        Ok(Self {
            replica_id,
            storage,
            clock,
            counter,
            nodes,
            version_vector: VersionVector::new(),
            payloads,
            head: None,
            op_count: 0,
        })
    }

    fn is_in_order(&self, op: &Operation) -> bool {
        let Some(head) = self.head.as_ref() else {
            return true;
        };

        cmp_op_key(
            op.meta.lamport,
            op.meta.id.replica.as_bytes(),
            op.meta.id.counter,
            head.meta.lamport,
            head.meta.id.replica.as_bytes(),
            head.meta.id.counter,
        ) == std::cmp::Ordering::Greater
    }

    fn next_op_meta(&mut self) -> (ReplicaId, u64, Lamport, Vec<u8>) {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let seed = Self::seed(&replica, counter);
        (replica, counter, lamport, seed)
    }

    pub fn resolve_after_for_placement(
        &self,
        parent: NodeId,
        placement: LocalPlacement,
        exclude: Option<NodeId>,
    ) -> Result<Option<NodeId>> {
        match placement {
            LocalPlacement::First => Ok(None),
            LocalPlacement::After(after_id) => {
                if exclude == Some(after_id) {
                    return Err(Error::InvalidOperation(
                        "after cannot be excluded node".into(),
                    ));
                }
                Ok(Some(after_id))
            }
            LocalPlacement::Last => {
                let mut children = self.children(parent)?;
                if let Some(excluded) = exclude {
                    children.retain(|child| *child != excluded);
                }
                Ok(children.last().copied())
            }
        }
    }

    pub fn local_insert(
        &mut self,
        parent: NodeId,
        node: NodeId,
        placement: LocalPlacement,
        payload: Option<Vec<u8>>,
    ) -> Result<(Operation, LocalFinalizePlan)> {
        let prepared = self.prepare_local_insert(parent, node, placement, payload)?;
        self.commit_prepared_local(prepared)
    }

    pub fn prepare_local_insert(
        &mut self,
        parent: NodeId,
        node: NodeId,
        placement: LocalPlacement,
        payload: Option<Vec<u8>>,
    ) -> Result<PreparedLocalOp> {
        let after = self.resolve_after_for_placement(parent, placement, None)?;
        let payload_after = payload.clone();
        let (replica, counter, lamport, seed) = self.next_op_meta();
        let order_key = self.allocate_child_key_after(parent, node, after, &seed)?;
        let op = Operation::insert_with_optional_payload(
            &replica, counter, lamport, parent, node, order_key, payload,
        );
        Ok(PreparedLocalOp {
            op,
            plan: LocalFinalizePlan {
                parent_hints: vec![parent],
                extra_index_records: Vec::new(),
                changes: vec![MaterializationChange::Insert {
                    node,
                    parent_after: parent,
                    payload: payload_after,
                    source: None,
                }],
            },
        })
    }

    pub fn local_move(
        &mut self,
        node: NodeId,
        new_parent: NodeId,
        placement: LocalPlacement,
    ) -> Result<(Operation, LocalFinalizePlan)> {
        let prepared = self.prepare_local_move(node, new_parent, placement)?;
        self.commit_prepared_local(prepared)
    }

    pub fn prepare_local_move(
        &mut self,
        node: NodeId,
        new_parent: NodeId,
        placement: LocalPlacement,
    ) -> Result<PreparedLocalOp> {
        let old_parent = self.parent(node)?;
        let after = self.resolve_after_for_placement(new_parent, placement, Some(node))?;
        let (replica, counter, lamport, seed) = self.next_op_meta();
        let order_key = self.allocate_child_key_after(new_parent, node, after, &seed)?;
        let op = Operation::move_node(&replica, counter, lamport, node, new_parent, order_key);

        let mut parent_hints = vec![new_parent];
        if let Some(parent) = old_parent {
            parent_hints.push(parent);
        }

        let mut extra_index_records: Vec<(NodeId, OperationId)> = Vec::new();
        if old_parent != Some(new_parent) && new_parent != NodeId::TRASH {
            if let Some((_lamport, payload_id)) = self.payload_last_writer(node)? {
                extra_index_records.push((new_parent, payload_id));
            }
        }

        Ok(PreparedLocalOp {
            op,
            plan: LocalFinalizePlan {
                parent_hints,
                extra_index_records,
                changes: vec![MaterializationChange::Move {
                    node,
                    parent_before: old_parent,
                    parent_after: new_parent,
                    source: None,
                }],
            },
        })
    }

    pub fn local_delete(&mut self, node: NodeId) -> Result<(Operation, LocalFinalizePlan)> {
        let prepared = self.prepare_local_delete(node)?;
        self.commit_prepared_local(prepared)
    }

    pub fn prepare_local_delete(&mut self, node: NodeId) -> Result<PreparedLocalOp> {
        let old_parent = self.parent(node)?;
        let (replica, counter, lamport, _seed) = self.next_op_meta();
        let known_state = Some(self.nodes.subtree_version_vector(node)?);
        let op = Operation::delete(&replica, counter, lamport, node, known_state);
        Ok(PreparedLocalOp {
            op,
            plan: LocalFinalizePlan {
                parent_hints: parent_hints_from(old_parent),
                extra_index_records: Vec::new(),
                changes: vec![MaterializationChange::Delete {
                    node,
                    parent_before: old_parent.filter(|parent| *parent != NodeId::TRASH),
                    source: None,
                }],
            },
        })
    }

    pub fn local_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
    ) -> Result<(Operation, LocalFinalizePlan)> {
        let prepared = self.prepare_local_payload(node, payload)?;
        self.commit_prepared_local(prepared)
    }

    pub fn prepare_local_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
    ) -> Result<PreparedLocalOp> {
        let parent = self.parent(node)?;
        let payload_after = payload.clone();
        let (replica, counter, lamport, _seed) = self.next_op_meta();
        let op = if let Some(payload) = payload {
            Operation::set_payload(&replica, counter, lamport, node, payload)
        } else {
            Operation::clear_payload(&replica, counter, lamport, node)
        };
        Ok(PreparedLocalOp {
            op,
            plan: LocalFinalizePlan {
                parent_hints: parent_hints_from(parent),
                extra_index_records: Vec::new(),
                changes: vec![MaterializationChange::Payload {
                    node,
                    payload: payload_after,
                    source: None,
                }],
            },
        })
    }

    pub fn commit_prepared_local(
        &mut self,
        mut prepared: PreparedLocalOp,
    ) -> Result<(Operation, LocalFinalizePlan)> {
        let (op, forward, tombstone_changed) = self.commit_local(prepared.op)?;
        prepared.plan.parent_hints =
            self.parents_for_forward_apply(&forward.snapshot, &op, forward.effects)?;
        if !forward.effects.structure_changed {
            prepared.plan.changes = rejected_structural_fallback_change(&op, None);
        }
        prepared.plan.changes.extend(
            tombstone_changed
                .into_iter()
                .filter_map(|delta| materialization_change_from_tombstone_delta(delta, None)),
        );
        Self::extend_parents_from_changes(&mut prepared.plan.parent_hints, &prepared.plan.changes);
        Self::dedupe_parents(&mut prepared.plan.parent_hints);

        if !forward.effects.structure {
            prepared.plan.extra_index_records.clear();
        } else if let Some(parent_after) = self.nodes.parent(op.kind.node())? {
            if parent_after != NodeId::TRASH && forward.snapshot.parent != Some(parent_after) {
                if let Some((_lamport, payload_id)) = self.payload_last_writer(op.kind.node())? {
                    let record = (parent_after, payload_id);
                    if !prepared.plan.extra_index_records.contains(&record) {
                        prepared.plan.extra_index_records.push(record);
                    }
                }
            }
        }

        Ok((op, prepared.plan))
    }

    pub fn apply_remote(&mut self, op: Operation) -> Result<()> {
        self.apply_remote_with_delta(op)?;
        Ok(())
    }

    /// Apply one remote operation and return exact incremental delta when available.
    ///
    /// Returns:
    /// - `Some(delta)` for in-order applies where an exact changed-node set is known,
    /// - `None` for duplicate/not-applied ops or paths that require replay.
    pub fn apply_remote_with_delta(&mut self, op: Operation) -> Result<Option<ApplyDelta>> {
        Ok(self.apply_remote_with_effects(op)?.map(|(delta, _)| delta))
    }

    fn apply_remote_with_effects(
        &mut self,
        op: Operation,
    ) -> Result<Option<(ApplyDelta, ApplyEffects)>> {
        op.validate()?;
        let inserted = self.storage.apply(op.clone())?;
        self.clock.observe(op.meta.lamport);
        self.version_vector.observe(&op.meta.id.replica, op.meta.id.counter);
        if op.meta.id.replica == self.replica_id {
            self.counter = self.counter.max(op.meta.id.counter);
        }
        if !inserted {
            return Ok(None);
        }

        if self.is_in_order(&op) {
            let (snapshot, emit_direct_change) =
                Self::apply_forward(&mut self.nodes, &mut self.payloads, &op)?;
            let effects = self.effects_after_forward(&op, emit_direct_change)?;
            self.op_count += 1;
            self.head = Some(op.clone());

            let changes = if emit_direct_change {
                direct_materialization_changes(snapshot.parent, &op)
            } else {
                rejected_structural_fallback_change(&op, Some(MaterializationSource::from_op(&op)))
            };
            return Ok(Some((
                ApplyDelta {
                    snapshot: NodeSnapshotExport {
                        parent: snapshot.parent,
                        order_key: snapshot.order_key,
                    },
                    changes,
                },
                effects,
            )));
        }

        // Out-of-order operation: catch derived state up from storage.
        self.replay_from_storage()?;
        Ok(None)
    }

    /// Apply a remote op with full materialization bookkeeping.
    ///
    /// This wires together core CRDT semantics (`apply_remote_with_delta`),
    /// a parent-op index (`ParentOpIndex`) for partial sync, and cached
    /// tombstone flags in the [`NodeStore`]. The materialization sequence
    /// is advanced only when the operation is actually accepted.
    pub fn apply_remote_with_materialization_seq<I: ParentOpIndex>(
        &mut self,
        op: Operation,
        index: &mut I,
        seq: &mut u64,
    ) -> Result<Option<ApplyDelta>> {
        *seq = (*seq).saturating_add(1);
        let applied = self.apply_remote_with_effects(op.clone())?;
        let Some((delta, effects)) = applied else {
            *seq = (*seq).saturating_sub(1);
            return Ok(None);
        };
        let snapshot = NodeSnapshot {
            parent: delta.snapshot.parent,
            order_key: delta.snapshot.order_key,
        };
        Ok(Some(self.finalize_materialized_apply(
            snapshot,
            &op,
            index,
            *seq,
            delta.changes,
            effects,
        )?))
    }

    /// Finalize adapter-owned local ops by refreshing tombstones and recording parent-op index rows.
    ///
    /// This is intended for adapters that execute local operations directly against core and then
    /// need to keep external materialized indexes/metadata in sync.
    fn finalize_materialized_apply<I: ParentOpIndex>(
        &mut self,
        snapshot: NodeSnapshot,
        op: &Operation,
        index: &mut I,
        seq: u64,
        mut changes: Vec<MaterializationChange>,
        effects: ApplyEffects,
    ) -> Result<ApplyDelta> {
        let op_node = op.kind.node();
        let mut parents = self.parents_for_forward_apply(&snapshot, op, effects)?;

        // Ensure the latest payload op for `op_node` is discoverable under its current parent.
        // This supports partial sync subscribers that only track `children(parent)` opRefs.
        if effects.structure {
            let parent_after = self.nodes.parent(op_node)?;
            if let Some(parent_after) = parent_after {
                if parent_after != NodeId::TRASH && snapshot.parent != Some(parent_after) {
                    if let Some((_lamport, payload_id)) = self.payload_last_writer(op_node)? {
                        index.record(parent_after, &payload_id, seq)?;
                    }
                }
            }
        }

        let tombstone_changed = if effects.any() {
            let mut starts = parents.clone();
            starts.push(op_node);
            self.refresh_tombstones_upward_with_delta(starts)?
        } else {
            Vec::new()
        };
        let source = Some(MaterializationSource::from_op(op));
        for change in &mut changes {
            attach_source_if_missing(change, source.clone());
        }

        changes.extend(tombstone_changed.into_iter().filter_map(|delta| {
            materialization_change_from_tombstone_delta(delta, source.clone())
        }));
        Self::extend_parents_from_changes(&mut parents, &changes);
        Self::record_op_for_parents(index, &mut parents, &op.meta.id, seq)?;

        Ok(ApplyDelta {
            snapshot: NodeSnapshotExport {
                parent: snapshot.parent,
                order_key: snapshot.order_key,
            },
            changes: coalesce_materialization_changes(changes),
        })
    }

    fn parents_for_forward_apply(
        &self,
        snapshot: &NodeSnapshot,
        op: &Operation,
        effects: ApplyEffects,
    ) -> Result<Vec<NodeId>> {
        let mut parents = Vec::new();
        if effects.structure {
            parents.extend(self.nodes.parent(op.kind.node())?);
        }
        if effects.any() {
            parents.extend(snapshot.parent);
        }
        Self::dedupe_parents(&mut parents);
        Ok(parents)
    }

    fn effects_after_forward(
        &self,
        op: &Operation,
        structure_changed: bool,
    ) -> Result<ApplyEffects> {
        let node = op.kind.node();
        let structure = match &op.kind {
            OperationKind::Insert {
                parent, order_key, ..
            }
            | OperationKind::Move {
                new_parent: parent,
                order_key,
                ..
            } => {
                node != NodeId::ROOT
                    && node != NodeId::TRASH
                    && self.nodes.parent(node)? == Some(*parent)
                    && self.nodes.order_key(node)?.as_deref() == Some(order_key.as_slice())
            }
            _ => false,
        };
        let payload = match &op.kind {
            OperationKind::Insert {
                payload: Some(_), ..
            }
            | OperationKind::Payload { .. } => self
                .payload_last_writer(node)?
                .is_some_and(|writer| writer == (op.meta.lamport, op.meta.id.clone())),
            _ => false,
        };
        let delete = matches!(
            op.kind,
            OperationKind::Delete { .. } | OperationKind::Tombstone { .. }
        ) && node != NodeId::ROOT
            && node != NodeId::TRASH;

        Ok(ApplyEffects {
            structure,
            structure_changed: structure && structure_changed,
            payload,
            delete,
        })
    }

    fn extend_parents_from_changes(parents: &mut Vec<NodeId>, changes: &[MaterializationChange]) {
        for change in changes {
            match change {
                MaterializationChange::Insert { parent_after, .. } => parents.push(*parent_after),
                MaterializationChange::Move {
                    parent_before,
                    parent_after,
                    ..
                } => {
                    parents.extend(*parent_before);
                    parents.push(*parent_after);
                }
                MaterializationChange::Delete { parent_before, .. } => {
                    parents.extend(*parent_before)
                }
                MaterializationChange::Restore { parent_after, .. } => {
                    parents.extend(*parent_after)
                }
                MaterializationChange::Payload { .. } => {}
            }
        }
    }

    fn record_op_for_parents<I: ParentOpIndex>(
        index: &mut I,
        parents: &mut Vec<NodeId>,
        op_id: &OperationId,
        seq: u64,
    ) -> Result<()> {
        parents.sort();
        parents.dedup();
        for parent in parents {
            if *parent != NodeId::TRASH {
                index.record(*parent, op_id, seq)?;
            }
        }
        Ok(())
    }

    fn dedupe_parents(parents: &mut Vec<NodeId>) {
        let mut seen = HashSet::new();
        parents.retain(|parent| seen.insert(*parent));
    }

    pub fn finalize_local_with_outcome<I: ParentOpIndex>(
        &mut self,
        op: &Operation,
        index: &mut I,
        head_seq: u64,
        plan: &LocalFinalizePlan,
    ) -> Result<MaterializationOutcome> {
        let seq = head_seq.saturating_add(1);

        let mut refresh_starts: Vec<NodeId> = plan.parent_hints.to_vec();
        refresh_starts.push(op.kind.node());
        let tombstone_changed = self.refresh_tombstones_upward_with_delta(refresh_starts)?;

        let source = Some(MaterializationSource::from_op(op));
        let mut changes = plan.changes.clone();
        for change in &mut changes {
            attach_source_if_missing(change, source.clone());
        }
        changes.extend(tombstone_changed.into_iter().filter_map(|delta| {
            materialization_change_from_tombstone_delta(delta, source.clone())
        }));

        let mut parents = plan.parent_hints.clone();
        Self::extend_parents_from_changes(&mut parents, &changes);
        Self::record_op_for_parents(index, &mut parents, &op.meta.id, seq)?;

        for (parent, op_id) in &plan.extra_index_records {
            if *parent == NodeId::TRASH {
                continue;
            }
            index.record(*parent, op_id, seq)?;
        }

        Ok(MaterializationOutcome {
            head_seq: seq,
            changes: coalesce_materialization_changes(changes),
        })
    }

    pub fn finalize_local<I: ParentOpIndex>(
        &mut self,
        op: &Operation,
        index: &mut I,
        head_seq: u64,
        plan: &LocalFinalizePlan,
    ) -> Result<u64> {
        Ok(self.finalize_local_with_outcome(op, index, head_seq, plan)?.head_seq)
    }

    /// Refresh tombstone cache for nodes on the upward closure of `starts`.
    ///
    /// Returns every node whose cached tombstone value actually changed.
    fn refresh_tombstones_upward_with_delta<I>(&mut self, starts: I) -> Result<Vec<TombstoneDelta>>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let mut stack: Vec<NodeId> = starts.into_iter().collect();
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut changed: Vec<TombstoneDelta> = Vec::new();

        while let Some(node) = stack.pop() {
            if node == NodeId::ROOT || node == NodeId::TRASH {
                continue;
            }
            if !visited.insert(node) {
                continue;
            }
            let Some((parent, has_deleted_at)) = self.nodes.parent_and_has_deleted_at(node)? else {
                continue;
            };

            if has_deleted_at {
                let previous = self.nodes.tombstone(node)?;
                let tombstoned = self.is_tombstoned(node)?;
                if previous != tombstoned {
                    self.nodes.set_tombstone(node, tombstoned)?;
                    changed.push(TombstoneDelta {
                        node,
                        parent,
                        previous,
                        tombstoned,
                        payload_after: if tombstoned {
                            None
                        } else {
                            self.payloads.payload(node)?
                        },
                    });
                }
            }

            if let Some(parent) = parent {
                stack.push(parent);
            }
        }

        changed.sort_by_key(|delta| delta.node);
        changed.dedup_by(|left, right| left.node == right.node);
        Ok(changed)
    }

    pub fn operations_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        self.storage.load_since(lamport)
    }

    /// Return the canonical operation-id closure needed to reproduce `children(parent)`.
    ///
    /// This query-time implementation replays the operation log through core's [`ParentOpIndex`]
    /// semantics. It is intended for in-memory adapters that do not persist that index; indexed
    /// storage backends should continue to serve their materialized parent-op rows directly.
    pub fn operation_ids_for_children_filter(&self, parent: NodeId) -> Result<Vec<OperationId>> {
        crate::materialization::operation_ids_for_children_filter(
            &self.storage,
            &self.replica_id,
            parent,
        )
    }

    pub fn replay_from_storage(&mut self) -> Result<()> {
        self.version_vector = VersionVector::new();
        self.nodes.reset()?;
        self.payloads.reset()?;
        self.head = None;
        self.op_count = 0;

        let storage = &self.storage;
        let nodes = &mut self.nodes;
        let payloads = &mut self.payloads;
        let clock = &mut self.clock;
        let version_vector = &mut self.version_vector;

        let mut seq: u64 = 0;
        let mut head: Option<Operation> = None;
        storage.scan_since(0, &mut |op| {
            op.validate()?;
            clock.observe(op.meta.lamport);
            version_vector.observe(&op.meta.id.replica, op.meta.id.counter);
            let _ = Self::apply_forward(nodes, payloads, &op)?;
            seq += 1;
            head = Some(op);
            Ok(())
        })?;

        self.head = head;
        self.op_count = seq;
        self.counter = self.counter.max(self.version_vector.get(&self.replica_id));
        Ok(())
    }

    pub fn children(&self, parent: NodeId) -> Result<Vec<NodeId>> {
        if !self.nodes.exists(parent)? {
            return Ok(Vec::new());
        }
        let children = self.nodes.children(parent)?;
        let mut filtered = Vec::with_capacity(children.len());
        for child_id in children {
            if !self.is_tombstoned(child_id)? {
                filtered.push(child_id);
            }
        }
        Ok(filtered)
    }

    pub fn parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        if !self.nodes.exists(node)? {
            return Ok(None);
        }
        if self.is_tombstoned(node)? {
            return Ok(Some(NodeId::TRASH));
        }
        Ok(self.nodes.parent(node)?.filter(|&p| p != NodeId::TRASH))
    }

    pub fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        self.payloads.payload(node)
    }

    pub fn payload_last_writer(&self, node: NodeId) -> Result<Option<(Lamport, OperationId)>> {
        self.payloads.last_writer(node)
    }

    pub fn is_tombstoned(&self, node: NodeId) -> Result<bool> {
        if !self.nodes.exists(node)? {
            return Ok(false);
        }
        let Some(deleted_vv) = self.nodes.deleted_at(node)? else {
            return Ok(false);
        };
        let subtree_vv = self.nodes.subtree_version_vector(node)?;
        Ok(deleted_vv.is_aware_of(&subtree_vv))
    }

    pub fn lamport(&self) -> Lamport {
        self.clock.now()
    }

    pub fn nodes(&self) -> Result<Vec<(NodeId, Option<NodeId>)>> {
        let mut pairs = Vec::new();
        for id in self.nodes.all_nodes()? {
            if id == NodeId::TRASH || id == NodeId::ROOT || self.is_tombstoned(id)? {
                continue;
            }
            pairs.push((id, self.nodes.parent(id)?));
        }
        pairs.sort_by_key(|(id, _)| id.0);
        Ok(pairs)
    }

    pub fn subtree_version_vector(&self, node: NodeId) -> Result<VersionVector> {
        self.nodes.subtree_version_vector(node)
    }

    pub fn export_nodes(&self) -> Result<Vec<NodeExport>> {
        let mut nodes = Vec::new();
        for node in self.nodes.all_nodes()? {
            nodes.push(NodeExport {
                node,
                parent: self.nodes.parent(node)?,
                children: self.nodes.children(node)?,
                last_change: self.nodes.last_change(node)?,
                deleted_at: self.nodes.deleted_at(node)?,
            });
        }
        Ok(nodes)
    }

    pub fn head_op(&self) -> Option<&Operation> {
        self.head.as_ref()
    }

    pub(crate) fn node_store(&self) -> &N {
        &self.nodes
    }

    pub(crate) fn node_store_mut(&mut self) -> &mut N {
        &mut self.nodes
    }

    fn commit_local(
        &mut self,
        op: Operation,
    ) -> Result<(Operation, ForwardApply, Vec<TombstoneDelta>)> {
        op.validate()?;
        let inserted = self.storage.apply(op.clone())?;
        self.version_vector.observe(&self.replica_id, op.meta.id.counter);
        if !inserted {
            // A duplicate local retry must not call `snapshot`: that helper intentionally ensures
            // the target row for a newly accepted op. Read an existing row only, so a storage-only
            // duplicate cannot mutate derived state while reporting an empty finalize plan.
            let node = op.kind.node();
            let snapshot = if self.nodes.exists(node)? {
                NodeSnapshot {
                    parent: self.nodes.parent(node)?,
                    order_key: self.nodes.order_key(node)?,
                }
            } else {
                NodeSnapshot {
                    parent: None,
                    order_key: None,
                }
            };
            return Ok((
                op,
                ForwardApply {
                    snapshot,
                    effects: ApplyEffects::default(),
                },
                Vec::new(),
            ));
        }
        let (snapshot, emit_direct_change) =
            Self::apply_forward(&mut self.nodes, &mut self.payloads, &op)?;
        let effects = self.effects_after_forward(&op, emit_direct_change)?;
        let forward = ForwardApply { snapshot, effects };
        let tombstone_changed = if forward.effects.any() {
            let mut starts =
                self.parents_for_forward_apply(&forward.snapshot, &op, forward.effects)?;
            starts.push(op.kind.node());
            self.refresh_tombstones_upward_with_delta(starts)?
        } else {
            Vec::new()
        };
        self.op_count += 1;
        self.head = Some(op.clone());
        Ok((op, forward, tombstone_changed))
    }

    fn seed(replica: &ReplicaId, counter: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(replica.as_bytes().len() + 8);
        out.extend_from_slice(replica.as_bytes());
        out.extend_from_slice(&counter.to_be_bytes());
        out
    }

    fn allocate_child_key_after(
        &self,
        parent: NodeId,
        node: NodeId,
        after: Option<NodeId>,
        seed: &[u8],
    ) -> Result<Vec<u8>> {
        if parent == NodeId::TRASH {
            return Ok(Vec::new());
        }

        let mut children = self.children(parent)?;
        children.retain(|child| *child != node);

        let (left, right) = if let Some(after) = after {
            let idx = children.iter().position(|c| *c == after).ok_or_else(|| {
                Error::InvalidOperation("after node is not a child of parent".into())
            })?;
            let left = self.nodes.order_key(after)?.ok_or_else(|| {
                Error::InvalidOperation("after node is missing its structural order_key".into())
            })?;
            let right = if idx + 1 < children.len() {
                let next = self.nodes.order_key(children[idx + 1])?.ok_or_else(|| {
                    Error::InvalidOperation(
                        "next sibling is missing its structural order_key".into(),
                    )
                })?;
                if next == left {
                    return Err(Error::InvalidOperation(
                        "cannot place directly after node: the next sibling has the same order_key"
                            .into(),
                    ));
                }
                Some(next)
            } else {
                None
            };
            (Some(left), right)
        } else {
            let right = if let Some(first) = children.first().copied() {
                Some(self.nodes.order_key(first)?.ok_or_else(|| {
                    Error::InvalidOperation(
                        "first sibling is missing its structural order_key".into(),
                    )
                })?)
            } else {
                None
            };
            (None, right)
        };

        crate::order_key::allocate_between(left.as_deref(), right.as_deref(), seed)
    }

    fn next_counter(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }
}

impl<S, C, N, P> TreeCrdt<S, C, N, P>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
    P: PayloadStore,
{
    pub fn is_known(&self, node: NodeId) -> Result<bool> {
        self.nodes.exists(node)
    }

    fn apply_forward(
        nodes: &mut N,
        payloads: &mut P,
        op: &Operation,
    ) -> Result<(NodeSnapshot, bool)> {
        let snapshot = Self::snapshot(nodes, op)?;
        let emit_direct_change = match &op.kind {
            OperationKind::Insert {
                parent,
                node,
                order_key,
                payload,
            } => {
                let applied = Self::apply_insert(nodes, op, *parent, *node, order_key.clone())?;
                if payload.is_some() {
                    Self::apply_payload(nodes, payloads, op, *node, payload.as_deref())?;
                }
                applied
            }
            OperationKind::Move {
                node,
                new_parent,
                order_key,
            } => {
                let changes_position = snapshot.parent != Some(*new_parent)
                    || snapshot.order_key.as_deref() != Some(order_key.as_slice());
                Self::apply_move(nodes, op, *node, *new_parent, order_key.clone())?
                    && changes_position
            }
            OperationKind::Delete { node } => {
                Self::apply_delete(nodes, op, *node)?;
                true
            }
            OperationKind::Tombstone { node } => {
                Self::apply_delete(nodes, op, *node)?;
                true
            }
            OperationKind::Payload { node, payload } => {
                Self::apply_payload(nodes, payloads, op, *node, payload.as_deref())?;
                true
            }
        };
        Ok((snapshot, emit_direct_change))
    }

    fn snapshot(nodes: &mut N, op: &Operation) -> Result<NodeSnapshot> {
        let node_id = match &op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node }
            | OperationKind::Payload { node, .. } => *node,
        };
        nodes.ensure_node(node_id)?;
        let parent = nodes.parent(node_id)?;
        let order_key = nodes.order_key(node_id)?;
        Ok(NodeSnapshot { parent, order_key })
    }

    fn apply_insert(
        nodes: &mut N,
        op: &Operation,
        parent: NodeId,
        node: NodeId,
        order_key: Vec<u8>,
    ) -> Result<bool> {
        if node == NodeId::ROOT
            || node == NodeId::TRASH
            || parent == node
            || Self::introduces_cycle(nodes, node, parent)?
        {
            return Ok(false);
        }
        nodes.ensure_node(parent)?;
        nodes.ensure_node(node)?;
        nodes.detach(node)?;
        nodes.attach(node, parent, order_key)?;
        Self::update_last_change(nodes, op, node)?;
        Self::update_last_change(nodes, op, parent)?;
        Ok(true)
    }

    fn apply_move(
        nodes: &mut N,
        op: &Operation,
        node: NodeId,
        new_parent: NodeId,
        order_key: Vec<u8>,
    ) -> Result<bool> {
        if node == NodeId::ROOT || node == NodeId::TRASH {
            return Ok(false);
        }
        nodes.ensure_node(node)?;
        nodes.ensure_node(new_parent)?;
        if new_parent != NodeId::TRASH
            && (Self::introduces_cycle(nodes, node, new_parent)? || node == new_parent)
        {
            return Ok(false);
        }

        let old_parent = nodes.parent(node)?;

        nodes.detach(node)?;
        nodes.attach(node, new_parent, order_key)?;

        Self::update_last_change(nodes, op, node)?;
        if let Some(old_p) = old_parent {
            if old_p != NodeId::TRASH {
                Self::update_last_change(nodes, op, old_p)?;
            }
        }
        if new_parent != NodeId::TRASH {
            Self::update_last_change(nodes, op, new_parent)?;
        }
        Ok(true)
    }

    fn apply_delete(nodes: &mut N, op: &Operation, node: NodeId) -> Result<()> {
        if node == NodeId::ROOT || node == NodeId::TRASH {
            return Ok(());
        }

        nodes.ensure_node(node)?;

        let mut delete_vv = Self::operation_version_vector(op);
        if let Some(known_state) = &op.meta.known_state {
            delete_vv.merge(known_state);
        }

        nodes.merge_deleted_at(node, &delete_vv)?;
        Ok(())
    }

    fn apply_payload(
        nodes: &mut N,
        payloads: &mut P,
        op: &Operation,
        node: NodeId,
        payload: Option<&[u8]>,
    ) -> Result<()> {
        nodes.ensure_node(node)?;

        if let Some((lamport, id)) = payloads.last_writer(node)? {
            if cmp_op_key(
                op.meta.lamport,
                op.meta.id.replica.as_bytes(),
                op.meta.id.counter,
                lamport,
                id.replica.as_bytes(),
                id.counter,
            ) != std::cmp::Ordering::Greater
            {
                return Ok(());
            }
        }

        payloads.set_payload(
            node,
            payload.map(|bytes| bytes.to_vec()),
            (op.meta.lamport, op.meta.id.clone()),
        )?;
        Self::update_last_change(nodes, op, node)?;
        Ok(())
    }

    fn operation_version_vector(op: &Operation) -> VersionVector {
        let mut vv = VersionVector::new();
        vv.observe(&op.meta.id.replica, op.meta.id.counter);
        vv
    }

    fn update_last_change(nodes: &mut N, op: &Operation, node: NodeId) -> Result<()> {
        nodes.merge_last_change(node, &Self::operation_version_vector(op))?;
        if let Some(known_state) = &op.meta.known_state {
            nodes.merge_last_change(node, known_state)?;
        }
        Ok(())
    }

    fn introduces_cycle(nodes: &N, node: NodeId, potential_parent: NodeId) -> Result<bool> {
        if potential_parent == NodeId::TRASH || potential_parent == NodeId::ROOT {
            return Ok(false);
        }
        let mut current = Some(potential_parent);
        let mut visited = HashSet::new();
        while let Some(n) = current {
            if n == node {
                return Ok(true);
            }
            if n == NodeId::TRASH || n == NodeId::ROOT {
                return Ok(false);
            }
            // Existing persisted state may already be malformed. Treat an ancestry loop as an
            // unsafe destination and reject the structural operation instead of traversing it
            // forever.
            if !visited.insert(n) {
                return Ok(true);
            }
            current = nodes.parent(n)?;
        }
        Ok(false)
    }
}
