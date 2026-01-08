use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::ops::{Operation, OperationKind};
use crate::traits::{Clock, Storage};
use crate::version_vector::VersionVector;

#[derive(Clone, Debug)]
struct NodeState {
    parent: Option<NodeId>,
    children: Vec<NodeId>,
    last_change: VersionVector,
    deleted_at: Option<VersionVector>,
}

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

impl NodeState {
    fn new_root() -> Self {
        Self {
            parent: None,
            children: Vec::new(),
            last_change: VersionVector::new(),
            deleted_at: None,
        }
    }

    fn new() -> Self {
        Self {
            parent: None,
            children: Vec::new(),
            last_change: VersionVector::new(),
            deleted_at: None,
        }
    }
}

/// Generic Tree CRDT facade that wires clock and storage together.
pub struct TreeCrdt<S, C>
where
    S: Storage,
    C: Clock,
{
    replica_id: ReplicaId,
    storage: S,
    clock: C,
    counter: u64,
    applied: HashSet<OperationId>,
    log: Vec<LogEntry>,
    nodes: HashMap<NodeId, NodeState>,
    version_vector: VersionVector,
}

fn tie_breaker_id(op: &Operation) -> u128 {
    let mut bytes = [0u8; 16];
    let rep = &op.meta.id.replica.0;
    let len = rep.len().min(8);
    bytes[..len].copy_from_slice(&rep[..len]);
    bytes[8..].copy_from_slice(&op.meta.id.counter.to_be_bytes());
    u128::from_be_bytes(bytes)
}

impl<S, C> TreeCrdt<S, C>
where
    S: Storage,
    C: Clock,
{
    pub fn new(replica_id: ReplicaId, storage: S, clock: C) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(NodeId::ROOT, NodeState::new_root());
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
        let known_state = Some(self.version_vector.clone());
        let op = Operation::delete(&replica, counter, lamport, node, known_state);
        self.commit_local(op)
    }

    pub fn apply_remote(&mut self, op: Operation) -> Result<()> {
        self.clock.observe(op.meta.lamport);
        self.version_vector.observe(&op.meta.id.replica, op.meta.lamport);
        self.ingest(op)
    }

    pub fn operations_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        self.storage.load_since(lamport)
    }

    pub fn replay_from_storage(&mut self) -> Result<()> {
        let mut ops = self.storage.load_since(0)?;
        ops.sort_by(|a, b| (a.meta.lamport, &a.meta.id).cmp(&(b.meta.lamport, &b.meta.id)));
        self.applied.clear();
        self.log.clear();
        self.version_vector = VersionVector::new();
        for op in ops {
            self.clock.observe(op.meta.lamport);
            self.version_vector.observe(&op.meta.id.replica, op.meta.lamport);
            self.applied.insert(op.meta.id.clone());
            let snapshot = Self::snapshot(&mut self.nodes, &op);
            self.log.push(LogEntry { op, snapshot });
        }
        self.rebuild_materialized();
        Ok(())
    }

    pub fn children(&self, parent: NodeId) -> Option<Vec<NodeId>> {
        self.nodes.get(&parent).map(|n| {
            n.children
                .iter()
                .filter(|&&child_id| !self.is_tombstoned(child_id))
                .copied()
                .collect()
        })
    }

    pub fn children_slice(&self, parent: NodeId) -> Option<&[NodeId]> {
        self.nodes.get(&parent).map(|n| n.children.as_slice())
    }

    pub fn parent(&self, node: NodeId) -> Option<NodeId> {
        self.nodes.get(&node).and_then(|n| {
            if self.is_tombstoned(node) {
                n.parent
            } else {
                n.parent.filter(|&p| p != NodeId::TRASH)
            }
        })
    }

    pub fn is_tombstoned(&self, node: NodeId) -> bool {
        self.nodes
            .get(&node)
            .and_then(|state| state.deleted_at.as_ref())
            .map(|deleted_vv| {
                let subtree_vv = Self::calculate_subtree_version_vector(&self.nodes, node);
                deleted_vv.is_aware_of(&subtree_vv)
            })
            .unwrap_or(false)
    }

    pub fn lamport(&self) -> Lamport {
        self.clock.now()
    }

    pub fn nodes(&self) -> Vec<(NodeId, Option<NodeId>)> {
        let mut pairs: Vec<_> = self
            .nodes
            .iter()
            .filter(|(id, _)| {
                **id != NodeId::TRASH && **id != NodeId::ROOT && !self.is_tombstoned(**id)
            })
            .map(|(id, state)| (*id, state.parent))
            .collect();
        pairs.sort_by_key(|(id, _)| id.0);
        pairs
    }

    pub fn validate_invariants(&self) -> Result<()> {
        for (pid, pstate) in &self.nodes {
            let mut seen = HashSet::new();
            for child in &pstate.children {
                if !seen.insert(child) {
                    return Err(Error::InvalidOperation("duplicate child entry".into()));
                }
                if let Some(child_state) = self.nodes.get(child) {
                    if child_state.parent != Some(*pid) {
                        return Err(Error::InvalidOperation("child parent mismatch".into()));
                    }
                } else {
                    return Err(Error::InvalidOperation("child not present in nodes".into()));
                }
            }
        }

        for node in self.nodes.keys() {
            if self.has_cycle_from(*node) {
                return Err(Error::InvalidOperation("cycle detected".into()));
            }
        }
        Ok(())
    }

    fn has_cycle_from(&self, start: NodeId) -> bool {
        if start == NodeId::ROOT || start == NodeId::TRASH {
            return false;
        }
        let mut visited = HashSet::new();
        let mut current = Some(start);
        while let Some(n) = current {
            if !visited.insert(n) {
                return true;
            }
            if n == NodeId::ROOT || n == NodeId::TRASH {
                return false;
            }
            current = self.nodes.get(&n).and_then(|s| s.parent);
        }
        false
    }

    fn commit_local(&mut self, op: Operation) -> Result<Operation> {
        self.version_vector.observe(&self.replica_id, op.meta.lamport);
        self.ingest(op.clone())?;
        Ok(op)
    }

    fn next_counter(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }
}

impl<S, C> TreeCrdt<S, C>
where
    S: Storage,
    C: Clock,
{
    pub fn is_known(&self, node: NodeId) -> bool {
        self.nodes.contains_key(&node)
    }

    fn ingest(&mut self, op: Operation) -> Result<()> {
        if self.applied.contains(&op.meta.id) {
            return Ok(());
        }

        self.applied.insert(op.meta.id.clone());
        let idx = match self.log.binary_search_by(|existing| {
            (
                existing.op.meta.lamport,
                tie_breaker_id(&existing.op),
                &existing.op.meta.id,
            )
                .cmp(&(op.meta.lamport, tie_breaker_id(&op), &op.meta.id))
        }) {
            Ok(i) => i,
            Err(i) => i,
        };
        for entry in self.log.iter().rev().take(self.log.len() - idx) {
            Self::undo_entry(&mut self.nodes, entry);
        }

        let snapshot = Self::apply_forward(&mut self.nodes, &op);
        self.log.insert(
            idx,
            LogEntry {
                op: op.clone(),
                snapshot,
            },
        );

        for entry in self.log.iter().skip(idx + 1) {
            Self::apply_forward_with_snapshot(&mut self.nodes, entry);
        }

        self.storage.apply(op)?;
        Ok(())
    }

    fn rebuild_materialized(&mut self) {
        let mut nodes = HashMap::new();
        nodes.insert(NodeId::ROOT, NodeState::new_root());
        for entry in &self.log {
            Self::apply_forward_with_snapshot(&mut nodes, entry);
        }
        self.nodes = nodes;
    }

    fn ensure_node(nodes: &mut HashMap<NodeId, NodeState>, id: NodeId) {
        nodes.entry(id).or_insert_with(|| {
            if id == NodeId::ROOT {
                NodeState::new_root()
            } else {
                NodeState::new()
            }
        });
    }

    fn apply_forward(nodes: &mut HashMap<NodeId, NodeState>, op: &Operation) -> NodeSnapshot {
        let snapshot = Self::snapshot(nodes, op);
        match &op.kind {
            OperationKind::Insert {
                parent,
                node,
                position,
            } => Self::apply_insert(nodes, op, *parent, *node, *position),
            OperationKind::Move {
                node,
                new_parent,
                position,
            } => Self::apply_move(nodes, op, *node, *new_parent, *position),
            OperationKind::Delete { node } => Self::apply_delete(nodes, op, *node),
            OperationKind::Tombstone { node } => Self::apply_delete(nodes, op, *node),
        }
        snapshot
    }

    fn apply_forward_with_snapshot(nodes: &mut HashMap<NodeId, NodeState>, entry: &LogEntry) {
        Self::apply_forward(nodes, &entry.op);
    }

    fn snapshot(nodes: &mut HashMap<NodeId, NodeState>, op: &Operation) -> NodeSnapshot {
        let node_id = match &op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node } => *node,
        };
        Self::ensure_node(nodes, node_id);
        let parent = nodes.get(&node_id).and_then(|n| n.parent);
        let position =
            parent.and_then(|p| nodes.get(&p)?.children.iter().position(|c| c == &node_id));
        NodeSnapshot { parent, position }
    }

    fn undo_entry(nodes: &mut HashMap<NodeId, NodeState>, entry: &LogEntry) {
        let node_id = match &entry.op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node } => *node,
        };
        Self::ensure_node(nodes, node_id);
        Self::detach(nodes, node_id);
        if let Some(parent) = entry.snapshot.parent {
            Self::ensure_node(nodes, parent);
            let pos = entry
                .snapshot
                .position
                .unwrap_or_else(|| nodes.get(&parent).map(|p| p.children.len()).unwrap_or(0));
            Self::attach(nodes, node_id, parent, pos);
        } else if let Some(state) = nodes.get_mut(&node_id) {
            state.parent = None;
        }
    }

    fn apply_insert(
        nodes: &mut HashMap<NodeId, NodeState>,
        op: &Operation,
        parent: NodeId,
        node: NodeId,
        position: usize,
    ) {
        if parent == node || Self::introduces_cycle(nodes, node, parent) {
            return;
        }
        Self::ensure_node(nodes, parent);
        Self::ensure_node(nodes, node);
        Self::detach(nodes, node);
        Self::attach(nodes, node, parent, position);
        Self::update_last_change(nodes, op, node);
        Self::update_last_change(nodes, op, parent);
    }

    fn apply_move(
        nodes: &mut HashMap<NodeId, NodeState>,
        op: &Operation,
        node: NodeId,
        new_parent: NodeId,
        position: usize,
    ) {
        if node == NodeId::ROOT {
            return;
        }
        Self::ensure_node(nodes, node);
        Self::ensure_node(nodes, new_parent);
        if new_parent != NodeId::TRASH
            && (Self::introduces_cycle(nodes, node, new_parent) || node == new_parent)
        {
            return;
        }

        let old_parent = nodes.get(&node).and_then(|n| n.parent);

        Self::detach(nodes, node);
        Self::attach(nodes, node, new_parent, position);

        Self::update_last_change(nodes, op, node);
        if let Some(old_p) = old_parent {
            if old_p != NodeId::TRASH {
                Self::update_last_change(nodes, op, old_p);
            }
        }
        if new_parent != NodeId::TRASH {
            Self::update_last_change(nodes, op, new_parent);
        }
    }

    fn apply_delete(nodes: &mut HashMap<NodeId, NodeState>, op: &Operation, node: NodeId) {
        if node == NodeId::ROOT || node == NodeId::TRASH {
            return;
        }

        let Some(mut state) = nodes.get(&node).cloned() else {
            return;
        };

        let mut delete_vv = Self::operation_version_vector(op);
        if let Some(known_state) = &op.meta.known_state {
            delete_vv.merge(known_state);
        }

        if let Some(existing) = &mut state.deleted_at {
            existing.merge(&delete_vv);
        } else {
            state.deleted_at = Some(delete_vv);
        }

        Self::detach(nodes, node);
        state.parent = Some(NodeId::TRASH);
        nodes.insert(node, state);
    }

    fn operation_version_vector(op: &Operation) -> VersionVector {
        let mut vv = VersionVector::new();
        vv.observe(&op.meta.id.replica, op.meta.lamport);
        vv
    }

    fn update_last_change(nodes: &mut HashMap<NodeId, NodeState>, op: &Operation, node: NodeId) {
        let Some(state) = nodes.get_mut(&node) else {
            return;
        };
        state.last_change.merge(&Self::operation_version_vector(op));
        if let Some(known_state) = &op.meta.known_state {
            state.last_change.merge(known_state);
        }
    }

    fn calculate_subtree_version_vector(
        nodes: &HashMap<NodeId, NodeState>,
        node: NodeId,
    ) -> VersionVector {
        let Some(state) = nodes.get(&node) else {
            return VersionVector::new();
        };

        let mut subtree_vv = state.last_change.clone();
        for &child_id in &state.children {
            let child_vv = Self::calculate_subtree_version_vector(nodes, child_id);
            subtree_vv.merge(&child_vv);
        }

        subtree_vv
    }

    fn detach(nodes: &mut HashMap<NodeId, NodeState>, node: NodeId) {
        if let Some(Some(parent)) = nodes.get(&node).map(|n| n.parent) {
            if let Some(parent_state) = nodes.get_mut(&parent) {
                parent_state.children.retain(|c| c != &node);
            }
        }
    }

    fn attach(
        nodes: &mut HashMap<NodeId, NodeState>,
        node: NodeId,
        parent: NodeId,
        position: usize,
    ) {
        if parent == NodeId::TRASH {
            return;
        }
        if let Some(parent_state) = nodes.get_mut(&parent) {
            let idx = position.min(parent_state.children.len());
            parent_state.children.insert(idx, node);
        }
        if let Some(node_state) = nodes.get_mut(&node) {
            node_state.parent = Some(parent);
        }
    }

    fn introduces_cycle(
        nodes: &HashMap<NodeId, NodeState>,
        node: NodeId,
        potential_parent: NodeId,
    ) -> bool {
        if potential_parent == NodeId::TRASH || potential_parent == NodeId::ROOT {
            return false;
        }
        let mut current = Some(potential_parent);
        while let Some(n) = current {
            if n == node {
                return true;
            }
            if n == NodeId::TRASH || n == NodeId::ROOT {
                return false;
            }
            current = nodes.get(&n).and_then(|state| state.parent);
        }
        false
    }
}
