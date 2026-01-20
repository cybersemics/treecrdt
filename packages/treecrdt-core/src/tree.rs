use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::ops::{cmp_op_key, cmp_ops, Operation, OperationKind};
use crate::traits::{Clock, MemoryNodeStore, NodeStore, Storage};
use crate::version_vector::VersionVector;

#[derive(Clone)]
struct NodeSnapshot {
    parent: Option<NodeId>,
    position: Option<usize>,
}

#[derive(Clone)]
struct LogEntry {
    op: Operation,
    snapshot: NodeSnapshot,
}

/// Generic Tree CRDT facade that wires clock and storage together.
pub struct TreeCrdt<S, C, N = MemoryNodeStore>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
{
    replica_id: ReplicaId,
    storage: S,
    clock: C,
    counter: u64,
    applied: HashSet<OperationId>,
    log: Vec<LogEntry>,
    nodes: N,
    version_vector: VersionVector,
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
    pub position: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct ApplyDelta {
    pub snapshot: NodeSnapshotExport,
    pub affected_parents: Vec<NodeId>,
}

#[derive(Clone, Debug)]
pub struct LogEntryExport {
    pub op: Operation,
    pub snapshot: NodeSnapshotExport,
}

impl<S, C> TreeCrdt<S, C, MemoryNodeStore>
where
    S: Storage,
    C: Clock,
{
    pub fn new(replica_id: ReplicaId, storage: S, clock: C) -> Self {
        Self {
            replica_id,
            storage,
            clock,
            counter: 0,
            applied: HashSet::new(),
            log: Vec::new(),
            nodes: MemoryNodeStore::default(),
            version_vector: VersionVector::new(),
        }
    }
}

impl<S, C, N> TreeCrdt<S, C, N>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
{
    pub fn with_node_store(replica_id: ReplicaId, storage: S, clock: C, nodes: N) -> Self {
        Self {
            replica_id,
            storage,
            clock,
            counter: 0,
            applied: HashSet::new(),
            log: Vec::new(),
            nodes,
            version_vector: VersionVector::new(),
        }
    }

    pub fn local_insert(
        &mut self,
        parent: NodeId,
        node: NodeId,
        position: usize,
    ) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let op = Operation::insert(&replica, counter, lamport, parent, node, position);
        self.commit_local(op)
    }

    pub fn local_move(
        &mut self,
        node: NodeId,
        new_parent: NodeId,
        position: usize,
    ) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let op = Operation::move_node(&replica, counter, lamport, node, new_parent, position);
        self.commit_local(op)
    }

    pub fn local_delete(&mut self, node: NodeId) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let known_state = Some(Self::calculate_subtree_version_vector(&self.nodes, node)?);
        let op = Operation::delete(&replica, counter, lamport, node, known_state);
        self.commit_local(op)
    }

    pub fn apply_remote(&mut self, op: Operation) -> Result<()> {
        self.clock.observe(op.meta.lamport);
        self.version_vector.observe(&op.meta.id.replica, op.meta.id.counter);
        self.ingest(op)
    }

    pub fn apply_remote_with_delta(&mut self, op: Operation) -> Result<Option<ApplyDelta>> {
        let lamport = op.meta.lamport;
        let replica = op.meta.id.replica.clone();
        let counter = op.meta.id.counter;

        self.clock.observe(lamport);
        self.version_vector.observe(&replica, counter);

        let Some(snapshot) = self.ingest_with_snapshot(op.clone())? else {
            return Ok(None);
        };

        let mut parents = Vec::new();
        if let Some(p) = snapshot.parent {
            parents.push(p);
        }
        match op.kind {
            OperationKind::Insert { parent, .. } => parents.push(parent),
            OperationKind::Move { new_parent, .. } => parents.push(new_parent),
            OperationKind::Delete { .. } | OperationKind::Tombstone { .. } => {}
        }
        parents.sort();
        parents.dedup();

        Ok(Some(ApplyDelta {
            snapshot: NodeSnapshotExport {
                parent: snapshot.parent,
                position: snapshot.position,
            },
            affected_parents: parents,
        }))
    }

    pub fn operations_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        self.storage.load_since(lamport)
    }

    pub fn replay_from_storage(&mut self) -> Result<()> {
        let mut ops = self.storage.load_since(0)?;
        ops.sort_by(cmp_ops);
        self.applied.clear();
        self.log.clear();
        self.version_vector = VersionVector::new();
        self.nodes.reset()?;
        for op in ops {
            self.clock.observe(op.meta.lamport);
            self.version_vector.observe(&op.meta.id.replica, op.meta.id.counter);
            self.applied.insert(op.meta.id.clone());
            let snapshot = Self::apply_forward(&mut self.nodes, &op)?;
            self.log.push(LogEntry { op, snapshot });
        }
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

    pub fn is_tombstoned(&self, node: NodeId) -> Result<bool> {
        if !self.nodes.exists(node)? {
            return Ok(false);
        }
        let Some(deleted_vv) = self.nodes.deleted_at(node)? else {
            return Ok(false);
        };
        let subtree_vv = Self::calculate_subtree_version_vector(&self.nodes, node)?;
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
        Self::calculate_subtree_version_vector(&self.nodes, node)
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

    pub fn export_log(&self) -> Vec<LogEntryExport> {
        self.log
            .iter()
            .map(|entry| LogEntryExport {
                op: entry.op.clone(),
                snapshot: NodeSnapshotExport {
                    parent: entry.snapshot.parent,
                    position: entry.snapshot.position,
                },
            })
            .collect()
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
        self.ingest(op.clone())?;
        Ok(op)
    }

    fn next_counter(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }
}

impl<S, C, N> TreeCrdt<S, C, N>
where
    S: Storage,
    C: Clock,
    N: NodeStore,
{
    pub fn is_known(&self, node: NodeId) -> Result<bool> {
        self.nodes.exists(node)
    }

    fn ingest(&mut self, op: Operation) -> Result<()> {
        let _ = self.ingest_with_snapshot(op)?;
        Ok(())
    }

    fn ingest_with_snapshot(&mut self, op: Operation) -> Result<Option<NodeSnapshot>> {
        if self.applied.contains(&op.meta.id) {
            return Ok(None);
        }

        self.applied.insert(op.meta.id.clone());
        let idx = match self.log.binary_search_by(|existing| {
            cmp_op_key(
                existing.op.meta.lamport,
                existing.op.meta.id.replica.as_bytes(),
                existing.op.meta.id.counter,
                op.meta.lamport,
                op.meta.id.replica.as_bytes(),
                op.meta.id.counter,
            )
        }) {
            Ok(i) => i,
            Err(i) => i,
        };
        for entry in self.log.iter().rev().take(self.log.len() - idx) {
            Self::undo_entry(&mut self.nodes, entry)?;
        }

        let snapshot = Self::apply_forward(&mut self.nodes, &op)?;
        self.log.insert(
            idx,
            LogEntry {
                op: op.clone(),
                snapshot,
            },
        );

        for entry in self.log.iter_mut().skip(idx + 1) {
            entry.snapshot = Self::apply_forward(&mut self.nodes, &entry.op)?;
        }

        self.storage.apply(op)?;
        Ok(Some(self.log[idx].snapshot.clone()))
    }

    fn apply_forward(nodes: &mut N, op: &Operation) -> Result<NodeSnapshot> {
        let snapshot = Self::snapshot(nodes, op)?;
        match &op.kind {
            OperationKind::Insert {
                parent,
                node,
                position,
            } => Self::apply_insert(nodes, op, *parent, *node, *position)?,
            OperationKind::Move {
                node,
                new_parent,
                position,
            } => Self::apply_move(nodes, op, *node, *new_parent, *position)?,
            OperationKind::Delete { node } => Self::apply_delete(nodes, op, *node)?,
            OperationKind::Tombstone { node } => Self::apply_delete(nodes, op, *node)?,
        }
        Ok(snapshot)
    }

    fn snapshot(nodes: &mut N, op: &Operation) -> Result<NodeSnapshot> {
        let node_id = match &op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node } => *node,
        };
        nodes.ensure_node(node_id)?;
        let parent = nodes.parent(node_id)?;
        let position = match parent {
            Some(p) => nodes.children(p)?.iter().position(|c| c == &node_id),
            None => None,
        };
        Ok(NodeSnapshot { parent, position })
    }

    fn undo_entry(nodes: &mut N, entry: &LogEntry) -> Result<()> {
        let node_id = match &entry.op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node } => *node,
        };
        nodes.ensure_node(node_id)?;
        nodes.detach(node_id)?;
        if let Some(parent) = entry.snapshot.parent {
            nodes.ensure_node(parent)?;
            let pos = match entry.snapshot.position {
                Some(pos) => pos,
                None => nodes.children(parent)?.len(),
            };
            nodes.attach(node_id, parent, pos)?;
        }
        Ok(())
    }

    fn apply_insert(
        nodes: &mut N,
        op: &Operation,
        parent: NodeId,
        node: NodeId,
        position: usize,
    ) -> Result<()> {
        if parent == node || Self::introduces_cycle(nodes, node, parent)? {
            return Ok(());
        }
        nodes.ensure_node(parent)?;
        nodes.ensure_node(node)?;
        nodes.detach(node)?;
        nodes.attach(node, parent, position)?;
        Self::update_last_change(nodes, op, node)?;
        Self::update_last_change(nodes, op, parent)?;
        Ok(())
    }

    fn apply_move(
        nodes: &mut N,
        op: &Operation,
        node: NodeId,
        new_parent: NodeId,
        position: usize,
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
        nodes.attach(node, new_parent, position)?;

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

    fn calculate_subtree_version_vector(nodes: &N, node: NodeId) -> Result<VersionVector> {
        if !nodes.exists(node)? {
            return Ok(VersionVector::new());
        }

        let mut subtree_vv = nodes.last_change(node)?;
        for child_id in nodes.children(node)? {
            let child_vv = Self::calculate_subtree_version_vector(nodes, child_id)?;
            subtree_vv.merge(&child_vv);
        }

        Ok(subtree_vv)
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
