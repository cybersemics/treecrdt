use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::ops::{Operation, OperationKind};
use crate::traits::{Clock, Storage};

#[derive(Clone, Debug)]
struct NodeState {
    parent: Option<NodeId>,
    children: Vec<NodeId>,
    tombstone: bool,
}

#[derive(Clone)]
struct NodeSnapshot {
    parent: Option<NodeId>,
    position: Option<usize>,
    tombstone: bool,
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
            tombstone: false,
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
    log: Vec<LogEntry>, // ordered by (lamport, id)
    nodes: HashMap<NodeId, NodeState>,
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
        }
    }

    /// Create an insert operation and persist it to storage.
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

    /// Create a move operation and persist it to storage.
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

    /// Create a delete operation and persist it to storage.
    pub fn local_delete(&mut self, node: NodeId) -> Result<Operation> {
        let replica = self.replica_id.clone();
        let counter = self.next_counter();
        let lamport = self.clock.tick();
        let op = Operation::delete(&replica, counter, lamport, node);
        self.commit_local(op)
    }

    /// Apply an operation received from a remote peer.
    pub fn apply_remote(&mut self, op: Operation) -> Result<()> {
        self.clock.observe(op.meta.lamport);
        self.ingest(op)
    }

    /// Pull operations newer than the provided Lamport timestamp.
    pub fn operations_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        self.storage.load_since(lamport)
    }

    /// Replay the operation log from storage to rebuild materialized state.
    pub fn replay_from_storage(&mut self) -> Result<()> {
        let mut ops = self.storage.load_since(0)?;
        ops.sort_by(|a, b| (a.meta.lamport, &a.meta.id).cmp(&(b.meta.lamport, &b.meta.id)));
        self.applied.clear();
        self.log.clear();
        for op in ops {
            self.clock.observe(op.meta.lamport);
            self.applied.insert(op.meta.id.clone());
            let snapshot = Self::snapshot(&mut self.nodes, &op);
            self.log.push(LogEntry { op, snapshot });
        }
        self.rebuild_materialized();
        Ok(())
    }

    /// Returns children of a node in the current materialized state.
    pub fn children(&self, parent: NodeId) -> Option<&[NodeId]> {
        self.nodes.get(&parent).map(|n| n.children.as_slice())
    }

    /// Current parent of a node.
    pub fn parent(&self, node: NodeId) -> Option<NodeId> {
        self.nodes.get(&node).and_then(|n| n.parent)
    }

    /// Whether the node is tombstoned.
    pub fn is_tombstoned(&self, node: NodeId) -> bool {
        self.nodes.get(&node).map(|n| n.tombstone).unwrap_or(false)
    }

    /// Current Lamport time as observed by this replica.
    pub fn lamport(&self) -> Lamport {
        self.clock.now()
    }

    /// Returns all nodes in the tree with their parent relationships.
    /// Returns a vector of (node_id, parent_id) pairs, sorted by node_id.
    pub fn nodes(&self) -> Vec<(NodeId, Option<NodeId>)> {
        let mut pairs: Vec<_> = self.nodes.iter().map(|(id, state)| (*id, state.parent)).collect();
        pairs.sort_by_key(|(id, _)| id.0);
        pairs
    }

    /// Validate invariants: unique parent for each node, children parent pointers consistent,
    /// and no cycles. Intended for tests and debugging.
    pub fn validate_invariants(&self) -> Result<()> {
        // parent consistency / duplicate child detection
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

        // acyclic check
        for node in self.nodes.keys() {
            if self.has_cycle_from(*node) {
                return Err(Error::InvalidOperation("cycle detected".into()));
            }
        }
        Ok(())
    }

    fn has_cycle_from(&self, start: NodeId) -> bool {
        let mut visited = HashSet::new();
        let mut current = Some(start);
        while let Some(n) = current {
            if !visited.insert(n) {
                return true;
            }
            current = self.nodes.get(&n).and_then(|s| s.parent);
        }
        false
    }

    fn commit_local(&mut self, op: Operation) -> Result<Operation> {
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
    /// Helper primarily for tests to confirm whether a node exists according to storage.
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
        // undo suffix
        for entry in self.log.iter().rev().take(self.log.len() - idx) {
            Self::undo_entry(&mut self.nodes, entry);
        }

        // apply new op
        let snapshot = Self::apply_forward(&mut self.nodes, &op);
        let new_entry = LogEntry {
            op: op.clone(),
            snapshot,
        };
        self.log.insert(idx, new_entry);

        // redo suffix
        for entry in self.log.iter().skip(idx + 1) {
            Self::apply_forward_with_snapshot(&mut self.nodes, entry);
        }

        self.storage.apply(op)?;
        Ok(())
    }

    fn rebuild_materialized(&mut self) {
        let mut nodes: HashMap<NodeId, NodeState> = HashMap::new();
        nodes.insert(NodeId::ROOT, NodeState::new_root());
        for entry in &self.log {
            Self::apply_forward_with_snapshot(&mut nodes, entry);
        }
        self.nodes = nodes;
    }

    fn ensure_node(nodes: &mut HashMap<NodeId, NodeState>, id: NodeId) {
        nodes.entry(id).or_insert(NodeState {
            parent: None,
            children: Vec::new(),
            tombstone: false,
        });
    }

    fn apply_forward(nodes: &mut HashMap<NodeId, NodeState>, op: &Operation) -> NodeSnapshot {
        let snapshot = Self::snapshot(nodes, op);
        match &op.kind {
            OperationKind::Insert {
                parent,
                node,
                position,
            } => Self::apply_insert(nodes, *parent, *node, *position),
            OperationKind::Move {
                node,
                new_parent,
                position,
            } => Self::apply_move(nodes, *node, *new_parent, *position),
            OperationKind::Delete { node } | OperationKind::Tombstone { node } => {
                Self::apply_delete(nodes, *node)
            }
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
        let position = parent.and_then(|p| {
            nodes
                .get(&p)
                .and_then(|pnode| pnode.children.iter().position(|c| c == &node_id))
        });
        let tombstone = nodes.get(&node_id).map(|n| n.tombstone).unwrap_or(false);
        NodeSnapshot {
            parent,
            position,
            tombstone,
        }
    }

    fn undo_entry(nodes: &mut HashMap<NodeId, NodeState>, entry: &LogEntry) {
        let node_id = match &entry.op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node } => *node,
        };
        Self::ensure_node(nodes, node_id);
        // detach current
        Self::detach(nodes, node_id);
        if let Some(parent) = entry.snapshot.parent {
            Self::ensure_node(nodes, parent);
            let pos = entry
                .snapshot
                .position
                .unwrap_or_else(|| nodes.get(&parent).map(|p| p.children.len()).unwrap_or(0));
            Self::attach(nodes, node_id, parent, pos);
        } else if let Some(entry_state) = nodes.get_mut(&node_id) {
            entry_state.parent = None;
        }
        if let Some(node_state) = nodes.get_mut(&node_id) {
            node_state.tombstone = entry.snapshot.tombstone;
        }
    }

    fn apply_insert(
        nodes: &mut HashMap<NodeId, NodeState>,
        parent: NodeId,
        node: NodeId,
        position: usize,
    ) {
        if parent == node {
            return;
        }
        Self::ensure_node(nodes, parent);
        Self::ensure_node(nodes, node);
        if Self::introduces_cycle(nodes, node, parent) {
            return;
        }
        Self::detach(nodes, node);
        Self::attach(nodes, node, parent, position);
    }

    fn apply_move(
        nodes: &mut HashMap<NodeId, NodeState>,
        node: NodeId,
        new_parent: NodeId,
        position: usize,
    ) {
        if node == NodeId::ROOT {
            return;
        }
        Self::ensure_node(nodes, node);
        Self::ensure_node(nodes, new_parent);
        if Self::introduces_cycle(nodes, node, new_parent) || node == new_parent {
            return;
        }
        Self::detach(nodes, node);
        Self::attach(nodes, node, new_parent, position);
    }

    fn apply_delete(nodes: &mut HashMap<NodeId, NodeState>, node: NodeId) {
        if node == NodeId::ROOT {
            return;
        }
        if !nodes.contains_key(&node) {
            return;
        }
        Self::detach(nodes, node);
        if let Some(entry) = nodes.get_mut(&node) {
            entry.parent = None;
            entry.tombstone = true;
        }
    }

    fn detach(nodes: &mut HashMap<NodeId, NodeState>, node: NodeId) {
        if let Some(parent) = nodes.get(&node).and_then(|n| n.parent) {
            if let Some(p) = nodes.get_mut(&parent) {
                p.children.retain(|c| c != &node);
            }
        }
    }

    fn attach(
        nodes: &mut HashMap<NodeId, NodeState>,
        node: NodeId,
        parent: NodeId,
        position: usize,
    ) {
        if let Some(parent_entry) = nodes.get_mut(&parent) {
            let idx = position.min(parent_entry.children.len());
            parent_entry.children.insert(idx, node);
        }
        if let Some(entry) = nodes.get_mut(&node) {
            entry.parent = Some(parent);
            entry.tombstone = false;
        }
    }

    fn introduces_cycle(
        nodes: &HashMap<NodeId, NodeState>,
        node: NodeId,
        potential_parent: NodeId,
    ) -> bool {
        let mut current = Some(potential_parent);
        while let Some(n) = current {
            if n == node {
                return true;
            }
            current = nodes.get(&n).and_then(|state| state.parent);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{LamportClock, MemoryStorage};

    #[test]
    fn inserts_and_moves_nodes() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            LamportClock::default(),
        );

        let root = NodeId::ROOT;
        let a = NodeId(1);
        let b = NodeId(2);

        crdt.local_insert(root, a, 0).unwrap();
        crdt.local_insert(a, b, 0).unwrap();

        assert_eq!(crdt.parent(a), Some(root));
        assert_eq!(crdt.parent(b), Some(a));

        // move b under root
        crdt.local_move(b, root, 0).unwrap();
        assert_eq!(crdt.parent(b), Some(root));
        assert_eq!(crdt.children(root).unwrap(), &[b, a]);
    }

    #[test]
    fn duplicate_operations_are_ignored() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            LamportClock::default(),
        );

        let op = crdt.local_insert(NodeId::ROOT, NodeId(1), 0).unwrap();
        // applying again should be idempotent
        crdt.apply_remote(op.clone()).unwrap();
        crdt.apply_remote(op).unwrap();
        assert_eq!(crdt.children(NodeId::ROOT).unwrap(), &[NodeId(1)]);
    }

    #[test]
    fn delete_marks_tombstone_and_removes_from_parent() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            LamportClock::default(),
        );

        let child = NodeId(1);
        crdt.local_insert(NodeId::ROOT, child, 0).unwrap();
        crdt.local_delete(child).unwrap();

        assert!(crdt.is_tombstoned(child));
        assert!(crdt.children(NodeId::ROOT).unwrap().is_empty());
    }
}
