use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::ops::{cmp_op_key, Operation, OperationKind};
use crate::traits::{
    Clock, MemoryNodeStore, MemoryPayloadStore, NodeStore, ParentOpIndex, PayloadStore, Storage,
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

#[derive(Clone, Debug)]
pub struct NodeExport {
    pub node: NodeId,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub last_change: VersionVector,
    pub deleted_at: Option<VersionVector>,
}

#[derive(Clone, Debug)]
pub struct NodeSnapshotExport {
    pub parent: Option<NodeId>,
    pub order_key: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct ApplyDelta {
    pub snapshot: NodeSnapshotExport,
    pub affected_parents: Vec<NodeId>,
}

fn affected_parents(snapshot_parent: Option<NodeId>, kind: &OperationKind) -> Vec<NodeId> {
    let mut parents = Vec::new();
    if let Some(p) = snapshot_parent {
        parents.push(p);
    }
    match kind {
        OperationKind::Insert { parent, .. } => parents.push(*parent),
        OperationKind::Move { new_parent, .. } => parents.push(*new_parent),
        OperationKind::Delete { .. }
        | OperationKind::Tombstone { .. }
        | OperationKind::Payload { .. } => {}
    }
    parents.sort();
    parents.dedup();
    parents
}

impl<S, C> TreeCrdt<S, C, MemoryNodeStore>
where
    S: Storage,
    C: Clock,
{
    pub fn new(replica_id: ReplicaId, storage: S, clock: C) -> Result<Self> {
        let counter = storage.latest_counter(&replica_id)?;
        let mut clock = clock;
        clock.observe(storage.latest_lamport());
        Ok(Self {
            replica_id,
            storage,
            clock,
            counter,
            nodes: MemoryNodeStore::default(),
            version_vector: VersionVector::new(),
            payloads: MemoryPayloadStore::default(),
            head: None,
            op_count: 0,
        })
    }
}

impl<S, C, N> TreeCrdt<S, C, N, MemoryPayloadStore>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
{
    pub fn with_node_store(replica_id: ReplicaId, storage: S, clock: C, nodes: N) -> Result<Self> {
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
            payloads: MemoryPayloadStore::default(),
            head: None,
            op_count: 0,
        })
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

    pub fn local_insert_after(
        &mut self,
        parent: NodeId,
        node: NodeId,
        after: Option<NodeId>,
    ) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let seed = Self::seed(&replica, counter);
        let order_key = self.allocate_child_key_after(parent, node, after, &seed)?;
        let op = Operation::insert(&replica, counter, lamport, parent, node, order_key);
        self.commit_local(op)
    }

    pub fn local_insert_after_with_payload(
        &mut self,
        parent: NodeId,
        node: NodeId,
        after: Option<NodeId>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let seed = Self::seed(&replica, counter);
        let order_key = self.allocate_child_key_after(parent, node, after, &seed)?;
        let op = Operation::insert_with_payload(
            &replica, counter, lamport, parent, node, order_key, payload,
        );
        self.commit_local(op)
    }

    pub fn local_move_after(
        &mut self,
        node: NodeId,
        new_parent: NodeId,
        after: Option<NodeId>,
    ) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let seed = Self::seed(&replica, counter);
        let order_key = self.allocate_child_key_after(new_parent, node, after, &seed)?;
        let op = Operation::move_node(&replica, counter, lamport, node, new_parent, order_key);
        self.commit_local(op)
    }

    pub fn local_delete(&mut self, node: NodeId) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let known_state = Some(self.nodes.subtree_version_vector(node)?);
        let op = Operation::delete(&replica, counter, lamport, node, known_state);
        self.commit_local(op)
    }

    pub fn local_set_payload(
        &mut self,
        node: NodeId,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let op = Operation::set_payload(&replica, counter, lamport, node, payload);
        self.commit_local(op)
    }

    pub fn local_clear_payload(&mut self, node: NodeId) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let op = Operation::clear_payload(&replica, counter, lamport, node);
        self.commit_local(op)
    }

    pub fn apply_remote(&mut self, op: Operation) -> Result<()> {
        self.clock.observe(op.meta.lamport);
        self.version_vector.observe(&op.meta.id.replica, op.meta.id.counter);
        if !self.storage.apply(op.clone())? {
            return Ok(());
        }

        if self.is_in_order(&op) {
            let _ = Self::apply_forward(&mut self.nodes, &mut self.payloads, &op)?;
            self.op_count += 1;
            self.head = Some(op);
            return Ok(());
        }

        self.replay_from_storage()
    }

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

            let parents = affected_parents(snapshot.parent, &op.kind);
            return Ok(Some(ApplyDelta {
                snapshot: NodeSnapshotExport {
                    parent: snapshot.parent,
                    order_key: snapshot.order_key,
                },
                affected_parents: parents,
            }));
        }

        // Out-of-order operation: rebuild derived state from storage.
        self.replay_from_storage()?;
        Ok(None)
    }

    /// Apply a remote operation while maintaining adapter-provided derived state.
    ///
    /// This wires together:
    /// - core CRDT semantics (`apply_remote_with_delta`)
    /// - a parent→op index (`ParentOpIndex`) for partial sync
    /// - cached tombstone flags in the [`NodeStore`] (via `set_tombstone`)
    pub fn apply_remote_with_materialization<I: ParentOpIndex>(
        &mut self,
        op: Operation,
        index: &mut I,
        seq: u64,
    ) -> Result<Option<ApplyDelta>> {
        let op_node = op.kind.node();
        let parent_after = match &op.kind {
            OperationKind::Insert { parent, .. } => Some(*parent),
            OperationKind::Move { new_parent, .. } => Some(*new_parent),
            _ => None,
        };
        let op_id = op.meta.id.clone();

        let Some(delta) = self.apply_remote_with_delta(op)? else {
            return Ok(None);
        };

        for parent in &delta.affected_parents {
            if *parent == NodeId::TRASH {
                continue;
            }
            index.record(*parent, &op_id, seq)?;
        }

        // Ensure the latest payload op for `op_node` is discoverable under its current parent.
        // This supports partial sync subscribers that only track `children(parent)` opRefs.
        if let Some(parent_after) = parent_after {
            if parent_after != NodeId::TRASH && delta.snapshot.parent != Some(parent_after) {
                if let Some((_lamport, payload_id)) = self.payload_last_writer(op_node)? {
                    index.record(parent_after, &payload_id, seq)?;
                }
            }
        }

        let mut starts = delta.affected_parents.clone();
        starts.push(op_node);
        self.refresh_tombstones_upward(starts)?;

        Ok(Some(delta))
    }

    /// Apply a remote op and advance materialization sequence only when it is accepted.
    ///
    /// Adapters can hold `seq` in metadata and pass it by mutable reference across a batch.
    pub fn apply_remote_with_materialization_seq<I: ParentOpIndex>(
        &mut self,
        op: Operation,
        index: &mut I,
        seq: &mut u64,
    ) -> Result<Option<ApplyDelta>> {
        *seq = (*seq).saturating_add(1);
        let applied = self.apply_remote_with_materialization(op, index, *seq)?;
        if applied.is_none() {
            *seq = (*seq).saturating_sub(1);
        }
        Ok(applied)
    }

    pub fn refresh_tombstones_upward<I>(&mut self, starts: I) -> Result<()>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let mut stack: Vec<NodeId> = starts.into_iter().collect();
        let mut visited: HashSet<NodeId> = HashSet::new();

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
                let tombstoned = self.is_tombstoned(node)?;
                self.nodes.set_tombstone(node, tombstoned)?;
            }

            if let Some(parent) = parent {
                stack.push(parent);
            }
        }

        Ok(())
    }

    pub fn refresh_all_tombstones(&mut self) -> Result<()> {
        fn subtree_vv<N: NodeStore>(
            nodes: &N,
            node: NodeId,
            cache: &mut HashMap<NodeId, VersionVector>,
            visiting: &mut HashSet<NodeId>,
        ) -> Result<VersionVector> {
            if let Some(vv) = cache.get(&node) {
                return Ok(vv.clone());
            }
            if !visiting.insert(node) {
                return Err(Error::InconsistentState(
                    "cycle detected while computing subtree version vector".into(),
                ));
            }

            let mut vv = nodes.last_change(node)?;
            for child in nodes.children(node)? {
                let child_vv = subtree_vv(nodes, child, cache, visiting)?;
                vv.merge(&child_vv);
            }

            visiting.remove(&node);
            cache.insert(node, vv.clone());
            Ok(vv)
        }

        let nodes = self.nodes.all_nodes()?;
        let nodes_ro = &self.nodes;

        let mut cache: HashMap<NodeId, VersionVector> = HashMap::new();
        let mut visiting: HashSet<NodeId> = HashSet::new();
        let mut updates: Vec<(NodeId, bool)> = Vec::new();

        for node in nodes {
            if node == NodeId::ROOT || node == NodeId::TRASH {
                continue;
            }
            let Some(deleted_vv) = nodes_ro.deleted_at(node)? else {
                continue;
            };
            let subtree = subtree_vv(nodes_ro, node, &mut cache, &mut visiting)?;
            updates.push((node, deleted_vv.is_aware_of(&subtree)));
        }

        for (node, tombstoned) in updates {
            self.nodes.set_tombstone(node, tombstoned)?;
        }

        Ok(())
    }

    pub fn replay_from_storage_with_materialization<I: ParentOpIndex>(
        &mut self,
        index: &mut I,
    ) -> Result<()> {
        index.reset()?;

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

            let snapshot = Self::apply_forward(nodes, payloads, &op)?;
            seq += 1;

            let parents = affected_parents(snapshot.parent, &op.kind);
            for parent in &parents {
                if *parent == NodeId::TRASH {
                    continue;
                }
                index.record(*parent, &op.meta.id, seq)?;
            }

            head = Some(op);
            Ok(())
        })?;

        self.head = head;
        self.op_count = seq;
        self.counter = self.counter.max(self.version_vector.get(&self.replica_id));

        // Refresh cached tombstone flags and then ensure the latest payload op for each node is
        // discoverable under its current parent.
        self.refresh_all_tombstones()?;

        let payload_seq = seq.max(1);
        for node in self.nodes.all_nodes()? {
            if node == NodeId::ROOT || node == NodeId::TRASH {
                continue;
            }
            let Some(parent) = self.nodes.parent(node)? else {
                continue;
            };
            if parent == NodeId::TRASH {
                continue;
            }
            let Some((_lamport, payload_id)) = self.payload_last_writer(node)? else {
                continue;
            };
            index.record(parent, &payload_id, payload_seq)?;
        }

        Ok(())
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

    pub fn log_len(&self) -> usize {
        self.op_count.min(usize::MAX as u64) as usize
    }

    pub fn head_op(&self) -> Option<&Operation> {
        self.head.as_ref()
    }

    pub fn validate_invariants(&self) -> Result<()> {
        for pid in self.nodes.all_nodes()? {
            let pchildren = self.nodes.children(pid)?;
            let mut seen = HashSet::new();
            for child in pchildren {
                if !seen.insert(child) {
                    return Err(Error::InvalidOperation("duplicate child entry".into()));
                }
                if !self.nodes.exists(child)? {
                    return Err(Error::InvalidOperation("child not present in nodes".into()));
                }
                if self.nodes.parent(child)? != Some(pid) {
                    return Err(Error::InvalidOperation("child parent mismatch".into()));
                }
            }
        }

        for node in self.nodes.all_nodes()? {
            if self.has_cycle_from(node)? {
                return Err(Error::InvalidOperation("cycle detected".into()));
            }
        }
        Ok(())
    }

    fn has_cycle_from(&self, start: NodeId) -> Result<bool> {
        if start == NodeId::ROOT || start == NodeId::TRASH {
            return Ok(false);
        }
        let mut visited = HashSet::new();
        let mut current = Some(start);
        while let Some(n) = current {
            if !visited.insert(n) {
                return Ok(true);
            }
            if n == NodeId::ROOT || n == NodeId::TRASH {
                return Ok(false);
            }
            current = self.nodes.parent(n)?;
        }
        Ok(false)
    }

    fn commit_local(&mut self, op: Operation) -> Result<Operation> {
        self.version_vector.observe(&self.replica_id, op.meta.id.counter);
        if !self.storage.apply(op.clone())? {
            return Ok(op);
        }
        let _ = Self::apply_forward(&mut self.nodes, &mut self.payloads, &op)?;
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
