use std::collections::HashSet;

use crate::affected::{
    affected_parents, coalesce_materialization_changes, direct_materialization_changes,
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
    NodeExport, NodeSnapshotExport,
};
use crate::version_vector::VersionVector;

#[derive(Clone)]
struct NodeSnapshot {
    parent: Option<NodeId>,
    order_key: Option<Vec<u8>>,
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
        let after = self.resolve_after_for_placement(parent, placement, None)?;
        let payload_after = payload.clone();
        let (replica, counter, lamport, seed) = self.next_op_meta();
        let order_key = self.allocate_child_key_after(parent, node, after, &seed)?;
        let op = Operation::insert_with_optional_payload(
            &replica, counter, lamport, parent, node, order_key, payload,
        );
        let op = self.commit_local(op)?;
        Ok((
            op,
            LocalFinalizePlan {
                parent_hints: vec![parent],
                extra_index_records: Vec::new(),
                changes: vec![MaterializationChange::Insert {
                    node,
                    parent_after: parent,
                    payload: payload_after,
                }],
            },
        ))
    }

    pub fn local_move(
        &mut self,
        node: NodeId,
        new_parent: NodeId,
        placement: LocalPlacement,
    ) -> Result<(Operation, LocalFinalizePlan)> {
        let old_parent = self.parent(node)?;
        let after = self.resolve_after_for_placement(new_parent, placement, Some(node))?;
        let (replica, counter, lamport, seed) = self.next_op_meta();
        let order_key = self.allocate_child_key_after(new_parent, node, after, &seed)?;
        let op = Operation::move_node(&replica, counter, lamport, node, new_parent, order_key);
        let op = self.commit_local(op)?;

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

        Ok((
            op,
            LocalFinalizePlan {
                parent_hints,
                extra_index_records,
                changes: vec![MaterializationChange::Move {
                    node,
                    parent_before: old_parent,
                    parent_after: new_parent,
                }],
            },
        ))
    }

    pub fn local_delete(&mut self, node: NodeId) -> Result<(Operation, LocalFinalizePlan)> {
        let old_parent = self.parent(node)?;
        let (replica, counter, lamport, _seed) = self.next_op_meta();
        let known_state = Some(self.nodes.subtree_version_vector(node)?);
        let op = Operation::delete(&replica, counter, lamport, node, known_state);
        let op = self.commit_local(op)?;
        Ok((
            op,
            LocalFinalizePlan {
                parent_hints: parent_hints_from(old_parent),
                extra_index_records: Vec::new(),
                changes: vec![MaterializationChange::Delete {
                    node,
                    parent_before: old_parent.filter(|parent| *parent != NodeId::TRASH),
                }],
            },
        ))
    }

    pub fn local_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
    ) -> Result<(Operation, LocalFinalizePlan)> {
        let parent = self.parent(node)?;
        let payload_after = payload.clone();
        let (replica, counter, lamport, _seed) = self.next_op_meta();
        let op = if let Some(payload) = payload {
            Operation::set_payload(&replica, counter, lamport, node, payload)
        } else {
            Operation::clear_payload(&replica, counter, lamport, node)
        };
        let op = self.commit_local(op)?;
        Ok((
            op,
            LocalFinalizePlan {
                parent_hints: parent_hints_from(parent),
                extra_index_records: Vec::new(),
                changes: vec![MaterializationChange::Payload {
                    node,
                    payload: payload_after,
                }],
            },
        ))
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
        self.clock.observe(op.meta.lamport);
        self.version_vector.observe(&op.meta.id.replica, op.meta.id.counter);
        if op.meta.id.replica == self.replica_id {
            self.counter = self.counter.max(op.meta.id.counter);
        }

        if !self.storage.apply(op.clone())? {
            return Ok(None);
        }

        if self.is_in_order(&op) {
            let snapshot = Self::apply_forward(&mut self.nodes, &mut self.payloads, &op)?;
            self.op_count += 1;
            self.head = Some(op.clone());

            let changes = direct_materialization_changes(snapshot.parent, &op.kind);
            return Ok(Some(ApplyDelta {
                snapshot: NodeSnapshotExport {
                    parent: snapshot.parent,
                    order_key: snapshot.order_key,
                },
                changes,
            }));
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
        let snapshot = self.apply_remote_with_delta(op.clone())?.map(|delta| NodeSnapshot {
            parent: delta.snapshot.parent,
            order_key: delta.snapshot.order_key,
        });
        let Some(snapshot) = snapshot else {
            *seq = (*seq).saturating_sub(1);
            return Ok(None);
        };
        let changes = direct_materialization_changes(snapshot.parent, &op.kind);
        Ok(Some(self.finalize_materialized_apply(
            snapshot, &op, index, *seq, changes,
        )?))
    }

    /// Apply a canonically sorted remote op directly against the current materialized state.
    ///
    /// This skips storage persistence and out-of-order detection, and is intended for callers
    /// that already reconstructed/rewound state to the correct prefix and now need to replay a
    /// suffix in canonical op-key order.
    pub fn apply_sorted_remote_with_materialization<I: ParentOpIndex>(
        &mut self,
        op: Operation,
        index: &mut I,
        seq: u64,
    ) -> Result<ApplyDelta> {
        self.clock.observe(op.meta.lamport);
        self.version_vector.observe(&op.meta.id.replica, op.meta.id.counter);
        if op.meta.id.replica == self.replica_id {
            self.counter = self.counter.max(op.meta.id.counter);
        }

        let snapshot = Self::apply_forward(&mut self.nodes, &mut self.payloads, &op)?;
        self.op_count = seq;
        self.head = Some(op.clone());

        let changes = direct_materialization_changes(snapshot.parent, &op.kind);
        self.finalize_materialized_apply(snapshot, &op, index, seq, changes)
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
    ) -> Result<ApplyDelta> {
        let op_node = op.kind.node();
        let parent_after = match &op.kind {
            OperationKind::Insert { parent, .. } => Some(*parent),
            OperationKind::Move { new_parent, .. } => Some(*new_parent),
            _ => None,
        };
        let parents = affected_parents(snapshot.parent, &op.kind);

        for parent in &parents {
            if *parent == NodeId::TRASH {
                continue;
            }
            index.record(*parent, &op.meta.id, seq)?;
        }

        // Ensure the latest payload op for `op_node` is discoverable under its current parent.
        // This supports partial sync subscribers that only track `children(parent)` opRefs.
        if let Some(parent_after) = parent_after {
            if parent_after != NodeId::TRASH && snapshot.parent != Some(parent_after) {
                if let Some((_lamport, payload_id)) = self.payload_last_writer(op_node)? {
                    index.record(parent_after, &payload_id, seq)?;
                }
            }
        }

        let mut starts = parents;
        starts.push(op_node);
        let tombstone_changed = self.refresh_tombstones_upward_with_delta(starts)?;
        changes.extend(
            tombstone_changed
                .into_iter()
                .filter_map(materialization_change_from_tombstone_delta),
        );

        Ok(ApplyDelta {
            snapshot: NodeSnapshotExport {
                parent: snapshot.parent,
                order_key: snapshot.order_key,
            },
            changes: coalesce_materialization_changes(changes),
        })
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

        let mut seen: HashSet<NodeId> = HashSet::new();
        for parent in &plan.parent_hints {
            if *parent == NodeId::TRASH || !seen.insert(*parent) {
                continue;
            }
            index.record(*parent, &op.meta.id, seq)?;
        }

        for (parent, op_id) in &plan.extra_index_records {
            if *parent == NodeId::TRASH {
                continue;
            }
            index.record(*parent, op_id, seq)?;
        }

        let mut changes = plan.changes.clone();
        changes.extend(
            tombstone_changed
                .into_iter()
                .filter_map(materialization_change_from_tombstone_delta),
        );

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

    fn refresh_tombstones_upward<I>(&mut self, starts: I) -> Result<()>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let _ = self.refresh_tombstones_upward_with_delta(starts)?;
        Ok(())
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

    fn commit_local(&mut self, op: Operation) -> Result<Operation> {
        self.version_vector.observe(&self.replica_id, op.meta.id.counter);
        if !self.storage.apply(op.clone())? {
            return Ok(op);
        }
        let snapshot = Self::apply_forward(&mut self.nodes, &mut self.payloads, &op)?;
        let mut starts = affected_parents(snapshot.parent, &op.kind);
        starts.push(op.kind.node());
        self.refresh_tombstones_upward(starts)?;
        self.op_count += 1;
        self.head = Some(op.clone());
        Ok(op)
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
            let left = self.nodes.order_key(after)?;
            let right = if idx + 1 < children.len() {
                self.nodes.order_key(children[idx + 1])?
            } else {
                None
            };
            (left, right)
        } else {
            let right = if let Some(first) = children.first().copied() {
                self.nodes.order_key(first)?
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

    fn apply_forward(nodes: &mut N, payloads: &mut P, op: &Operation) -> Result<NodeSnapshot> {
        let snapshot = Self::snapshot(nodes, op)?;
        match &op.kind {
            OperationKind::Insert {
                parent,
                node,
                order_key,
                payload,
            } => {
                Self::apply_insert(nodes, op, *parent, *node, order_key.clone())?;
                if payload.is_some() {
                    Self::apply_payload(nodes, payloads, op, *node, payload.as_deref())?;
                }
            }
            OperationKind::Move {
                node,
                new_parent,
                order_key,
            } => Self::apply_move(nodes, op, *node, *new_parent, order_key.clone())?,
            OperationKind::Delete { node } => Self::apply_delete(nodes, op, *node)?,
            OperationKind::Tombstone { node } => Self::apply_delete(nodes, op, *node)?,
            OperationKind::Payload { node, payload } => {
                Self::apply_payload(nodes, payloads, op, *node, payload.as_deref())?
            }
        }
        Ok(snapshot)
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
    ) -> Result<()> {
        if parent == node || Self::introduces_cycle(nodes, node, parent)? {
            return Ok(());
        }
        nodes.ensure_node(parent)?;
        nodes.ensure_node(node)?;
        nodes.detach(node)?;
        nodes.attach(node, parent, order_key)?;
        Self::update_last_change(nodes, op, node)?;
        Self::update_last_change(nodes, op, parent)?;
        Ok(())
    }

    fn apply_move(
        nodes: &mut N,
        op: &Operation,
        node: NodeId,
        new_parent: NodeId,
        order_key: Vec<u8>,
    ) -> Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        nodes.ensure_node(node)?;
        nodes.ensure_node(new_parent)?;
        if new_parent != NodeId::TRASH
            && (Self::introduces_cycle(nodes, node, new_parent)? || node == new_parent)
        {
            return Ok(());
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
        Ok(())
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
        while let Some(n) = current {
            if n == node {
                return Ok(true);
            }
            if n == NodeId::TRASH || n == NodeId::ROOT {
                return Ok(false);
            }
            current = nodes.parent(n)?;
        }
        Ok(false)
    }
}
