use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::ops::{Operation, OperationKind};
use crate::traits::{AccessControl, Clock, Storage};

#[derive(Clone, Debug)]
struct NodeState {
    parent: Option<NodeId>,
    children: Vec<NodeId>,
    tombstone: bool,
    last: Option<Stamp>,
}

#[derive(Clone, Debug)]
struct Stamp {
    lamport: Lamport,
    id: OperationId,
}

impl NodeState {
    fn new_root() -> Self {
        Self {
            parent: None,
            children: Vec::new(),
            tombstone: false,
            last: None,
        }
    }
}

/// Generic Tree CRDT facade that wires clock, access control, and storage together.
pub struct TreeCrdt<S, A, C>
where
    S: Storage,
    A: AccessControl,
    C: Clock,
{
    replica_id: ReplicaId,
    storage: S,
    access: A,
    clock: C,
    counter: u64,
    applied: HashSet<OperationId>,
    nodes: HashMap<NodeId, NodeState>,
    pending: Vec<Operation>,
    pending_ids: HashSet<OperationId>,
}

impl<S, A, C> TreeCrdt<S, A, C>
where
    S: Storage,
    A: AccessControl,
    C: Clock,
{
    pub fn new(replica_id: ReplicaId, storage: S, access: A, clock: C) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(NodeId::ROOT, NodeState::new_root());
        Self {
            replica_id,
            storage,
            access,
            clock,
            counter: 0,
            applied: HashSet::new(),
            nodes,
            pending: Vec::new(),
            pending_ids: HashSet::new(),
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
        self.access.can_apply(&op)?;
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
        for op in ops {
            self.clock.observe(op.meta.lamport);
            self.ingest_with_persist(op, false)?;
        }
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

    fn commit_local(&mut self, op: Operation) -> Result<Operation> {
        self.access.can_apply(&op)?;
        self.ingest(op.clone())?;
        Ok(op)
    }

    fn next_counter(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }
}

impl<S, A, C> TreeCrdt<S, A, C>
where
    S: Storage,
    A: AccessControl,
    C: Clock,
{
    /// Helper primarily for tests to confirm whether a node exists according to storage.
    pub fn is_known(&self, node: NodeId) -> bool {
        self.storage
            .load_since(0)
            .map(|ops| {
                ops.iter().any(|op| match op.kind {
                    OperationKind::Insert { node: n, .. } => n == node,
                    OperationKind::Move { node: n, .. } => n == node,
                    OperationKind::Delete { node: n } => n == node,
                    OperationKind::Tombstone { node: n } => n == node,
                })
            })
            .unwrap_or(false)
    }

    fn ingest(&mut self, op: Operation) -> Result<()> {
        self.ingest_with_persist(op, true)
    }

    fn ingest_with_persist(&mut self, op: Operation, persist: bool) -> Result<()> {
        if self.applied.contains(&op.meta.id) || self.pending_ids.contains(&op.meta.id) {
            return Ok(());
        }

        match self.apply_op(&op) {
            Ok(()) => {
                if persist {
                    self.storage.apply(op.clone())?;
                }
                self.process_pending();
                Ok(())
            }
            Err(Error::MissingDependency(_)) => {
                self.queue_pending(op.clone());
                if persist {
                    self.storage.apply(op)?;
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn apply_op(&mut self, op: &Operation) -> Result<()> {
        if self.applied.contains(&op.meta.id) {
            return Ok(());
        }

        match &op.kind {
            OperationKind::Insert {
                parent,
                node,
                position,
            } => self.apply_insert(op, *parent, *node, *position)?,
            OperationKind::Move {
                node,
                new_parent,
                position,
            } => self.apply_move(op, *node, *new_parent, *position)?,
            OperationKind::Delete { node } | OperationKind::Tombstone { node } => {
                self.apply_delete(op, *node)?
            }
        }

        self.applied.insert(op.meta.id.clone());
        Ok(())
    }

    fn apply_insert(
        &mut self,
        op: &Operation,
        parent: NodeId,
        node: NodeId,
        position: usize,
    ) -> Result<()> {
        if parent == node {
            return Err(Error::InvalidOperation("parent cannot equal node".into()));
        }

        if !self.nodes.contains_key(&parent) {
            return Err(Error::MissingDependency(format!(
                "missing parent {:?}",
                parent
            )));
        }

        {
            let entry = self.nodes.entry(node).or_insert(NodeState {
                parent: Some(parent),
                children: Vec::new(),
                tombstone: false,
                last: None,
            });
            if !is_newer(entry.last.as_ref(), op) {
                return Ok(());
            }
        }

        let old_parent = self.nodes.get(&node).and_then(|n| n.parent);
        if let Some(old_parent) = old_parent {
            if let Some(parent_entry) = self.nodes.get_mut(&old_parent) {
                parent_entry.children.retain(|c| c != &node);
            }
        }

        {
            let parent_entry = self
                .nodes
                .get_mut(&parent)
                .expect("validated parent existence");
            let idx = position.min(parent_entry.children.len());
            parent_entry.children.insert(idx, node);
        }

        if let Some(entry) = self.nodes.get_mut(&node) {
            entry.parent = Some(parent);
            entry.tombstone = false;
            entry.last = Some(Stamp {
                lamport: op.meta.lamport,
                id: op.meta.id.clone(),
            });
        }
        Ok(())
    }

    fn apply_move(
        &mut self,
        op: &Operation,
        node: NodeId,
        new_parent: NodeId,
        position: usize,
    ) -> Result<()> {
        if node == NodeId::ROOT {
            return Err(Error::InvalidOperation("cannot move root".into()));
        }

        if self.introduces_cycle(node, new_parent) {
            return Err(Error::InvalidOperation("move introduces cycle".into()));
        }

        if !self.nodes.contains_key(&new_parent) {
            return Err(Error::MissingDependency(format!(
                "missing parent {:?}",
                new_parent
            )));
        }

        if !self.nodes.contains_key(&node) {
            return Err(Error::MissingDependency(format!("missing node {:?}", node)));
        }

        let last_stamp = self.nodes.get(&node).and_then(|n| n.last.clone());
        if !is_newer(last_stamp.as_ref(), op) {
            return Ok(());
        }

        let old_parent = self.nodes.get(&node).and_then(|n| n.parent);
        if let Some(old_parent) = old_parent {
            if let Some(parent_entry) = self.nodes.get_mut(&old_parent) {
                parent_entry.children.retain(|c| c != &node);
            }
        }

        {
            let parent_entry = self
                .nodes
                .get_mut(&new_parent)
                .expect("validated parent existence");
            let idx = position.min(parent_entry.children.len());
            parent_entry.children.insert(idx, node);
        }

        if let Some(entry) = self.nodes.get_mut(&node) {
            entry.parent = Some(new_parent);
            entry.tombstone = false;
            entry.last = Some(Stamp {
                lamport: op.meta.lamport,
                id: op.meta.id.clone(),
            });
        }
        Ok(())
    }

    fn apply_delete(&mut self, op: &Operation, node: NodeId) -> Result<()> {
        if node == NodeId::ROOT {
            return Err(Error::InvalidOperation("cannot delete root".into()));
        }

        if !self.nodes.contains_key(&node) {
            return Err(Error::MissingDependency(format!("missing node {:?}", node)));
        }

        let last_stamp = self.nodes.get(&node).and_then(|n| n.last.clone());
        if !is_newer(last_stamp.as_ref(), op) {
            return Ok(());
        }

        let parent = self.nodes.get(&node).and_then(|n| n.parent);
        if let Some(parent) = parent {
            if let Some(parent_entry) = self.nodes.get_mut(&parent) {
                parent_entry.children.retain(|c| c != &node);
            }
        }

        if let Some(entry) = self.nodes.get_mut(&node) {
            entry.parent = None;
            entry.tombstone = true;
            entry.last = Some(Stamp {
                lamport: op.meta.lamport,
                id: op.meta.id.clone(),
            });
        }
        Ok(())
    }

    fn introduces_cycle(&self, node: NodeId, new_parent: NodeId) -> bool {
        let mut current = Some(new_parent);
        while let Some(n) = current {
            if n == node {
                return true;
            }
            current = self.nodes.get(&n).and_then(|state| state.parent);
        }
        false
    }

    fn queue_pending(&mut self, op: Operation) {
        if self.pending_ids.insert(op.meta.id.clone()) {
            self.pending.push(op);
        }
    }

    fn process_pending(&mut self) {
        let mut progressed = true;
        while progressed {
            progressed = false;
            let mut idx = 0;
            while idx < self.pending.len() {
                let op = self.pending[idx].clone();
                match self.apply_op(&op) {
                    Ok(()) => {
                        self.pending_ids.remove(&op.meta.id);
                        self.pending.swap_remove(idx);
                        progressed = true;
                    }
                    Err(Error::MissingDependency(_)) => {
                        idx += 1;
                    }
                    Err(_) => {
                        self.pending_ids.remove(&op.meta.id);
                        self.pending.swap_remove(idx);
                        progressed = true;
                    }
                }
            }
        }
    }
}

fn is_newer(current: Option<&Stamp>, incoming: &Operation) -> bool {
    match current {
        None => true,
        Some(stamp) => {
            incoming.meta.lamport > stamp.lamport
                || (incoming.meta.lamport == stamp.lamport && incoming.meta.id > stamp.id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{AllowAllAccess, LamportClock, MemoryStorage};

    #[test]
    fn inserts_and_moves_nodes() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            AllowAllAccess,
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
    fn prevents_cycle_on_move() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            AllowAllAccess,
            LamportClock::default(),
        );

        let root = NodeId::ROOT;
        let a = NodeId(1);
        let b = NodeId(2);

        crdt.local_insert(root, a, 0).unwrap();
        crdt.local_insert(a, b, 0).unwrap();

        let err = crdt.local_move(a, b, 0).unwrap_err();
        assert!(format!("{err}").contains("cycle"));
    }

    #[test]
    fn higher_lamport_wins_on_conflict() {
        let mut crdt_a = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            AllowAllAccess,
            LamportClock::default(),
        );

        let root = NodeId::ROOT;
        let x = NodeId(1);
        let left = NodeId(10);
        let right = NodeId(11);

        let insert_left = crdt_a.local_insert(root, left, 0).unwrap();
        let insert_right = crdt_a.local_insert(root, right, 1).unwrap();
        let insert_x = crdt_a.local_insert(root, x, 2).unwrap();

        // replica a moves x under left (lamport 4)
        let move_left = crdt_a.local_move(x, left, 0).unwrap();

        // replica b moves x under right with higher lamport
        let mut crdt_b = TreeCrdt::new(
            ReplicaId::new(b"b"),
            MemoryStorage::default(),
            AllowAllAccess,
            LamportClock::default(),
        );
        crdt_b.apply_remote(insert_left.clone()).unwrap();
        crdt_b.apply_remote(insert_right.clone()).unwrap();
        crdt_b.apply_remote(insert_x.clone()).unwrap();
        let move_right = Operation::move_node(
            &ReplicaId::new(b"b"),
            1,
            move_left.meta.lamport + 1,
            x,
            right,
            0,
        );
        crdt_b.apply_remote(move_right.clone()).unwrap();

        // apply both moves to a; higher lamport should win
        crdt_a.apply_remote(move_right).unwrap();
        crdt_a.apply_remote(move_left).unwrap();

        assert_eq!(crdt_a.parent(x), Some(right));
    }

    #[test]
    fn duplicate_operations_are_ignored() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            AllowAllAccess,
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
            AllowAllAccess,
            LamportClock::default(),
        );

        let child = NodeId(1);
        crdt.local_insert(NodeId::ROOT, child, 0).unwrap();
        crdt.local_delete(child).unwrap();

        assert!(crdt.is_tombstoned(child));
        assert!(crdt.children(NodeId::ROOT).unwrap().is_empty());
    }

    #[test]
    fn applies_insert_after_parent_arrives_out_of_order() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            AllowAllAccess,
            LamportClock::default(),
        );

        let parent = NodeId(1);
        let child = NodeId(2);
        let replica = ReplicaId::new(b"r1");

        let child_first = Operation::insert(&replica, 1, 1, parent, child, 0);
        crdt.apply_remote(child_first).unwrap();
        assert!(crdt.parent(child).is_none());

        let parent_op = Operation::insert(&replica, 2, 2, NodeId::ROOT, parent, 0);
        crdt.apply_remote(parent_op).unwrap();

        assert_eq!(crdt.parent(child), Some(parent));
        assert_eq!(crdt.children(parent).unwrap(), &[child]);
    }

    #[test]
    fn move_applied_after_insert_when_delivered_out_of_order() {
        let mut crdt = TreeCrdt::new(
            ReplicaId::new(b"a"),
            MemoryStorage::default(),
            AllowAllAccess,
            LamportClock::default(),
        );

        let parent = NodeId(1);
        let node = NodeId(2);
        let replica = ReplicaId::new(b"r1");

        // Move arrives first (references node + parent that do not yet exist)
        let move_op = Operation::move_node(&replica, 3, 3, node, parent, 0);
        crdt.apply_remote(move_op).unwrap();
        assert!(crdt.parent(node).is_none());

        // Later, parent and node inserts arrive
        let parent_insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, 0);
        let node_insert = Operation::insert(&replica, 2, 2, NodeId::ROOT, node, 0);
        crdt.apply_remote(parent_insert).unwrap();
        crdt.apply_remote(node_insert).unwrap();

        assert_eq!(crdt.parent(node), Some(parent));
        assert_eq!(crdt.children(parent).unwrap(), &[node]);
    }

    #[test]
    fn replay_rebuilds_state_and_advanced_clock() {
        let mut storage = MemoryStorage::default();
        let replica = ReplicaId::new(b"r1");
        let parent = NodeId(10);
        let node = NodeId(20);

        // out-of-order arrival persisted to storage
        let move_first = Operation::move_node(&replica, 3, 4, node, parent, 0);
        let node_insert = Operation::insert(&replica, 1, 2, NodeId::ROOT, node, 0);
        let parent_insert = Operation::insert(&replica, 2, 5, NodeId::ROOT, parent, 0);
        storage.apply(move_first.clone()).unwrap();
        storage.apply(node_insert.clone()).unwrap();
        storage.apply(parent_insert.clone()).unwrap();

        let mut crdt = TreeCrdt::new(
            replica.clone(),
            storage,
            AllowAllAccess,
            LamportClock::default(),
        );
        crdt.replay_from_storage().unwrap();

        assert_eq!(crdt.parent(node), Some(parent));
        assert_eq!(crdt.children(parent).unwrap(), &[node]);
        assert_eq!(crdt.lamport(), 5);

        // applying an already-seen op should be ignored
        crdt.apply_remote(move_first).unwrap();
        assert_eq!(crdt.children(parent).unwrap(), &[node]);
    }
}
