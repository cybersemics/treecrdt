use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::ops::{Operation, OperationKind};
use crate::traits::{Clock, MaterializedStorage, NodeState};
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

fn tie_breaker_id(op: &Operation) -> u128 {
    let mut bytes = [0u8; 16];
    let rep = &op.meta.id.replica.0;
    let len = rep.len().min(8);
    bytes[..len].copy_from_slice(&rep[..len]);
    bytes[8..].copy_from_slice(&op.meta.id.counter.to_be_bytes());
    u128::from_be_bytes(bytes)
}

/// Generic Tree CRDT facade that wires clock and storage together.
pub struct TreeCrdt<S, C>
where
    S: MaterializedStorage,
    C: Clock,
{
    replica_id: ReplicaId,
    storage: S,
    clock: C,
    counter: u64,
    applied: HashSet<OperationId>,
    log: Vec<LogEntry>,
    version_vector: VersionVector,
}

impl<S, C> TreeCrdt<S, C>
where
    S: MaterializedStorage,
    C: Clock,
{
    pub fn new(replica_id: ReplicaId, mut storage: S, clock: C) -> Self {
        let _ = storage.clear_materialized();
        Self::ensure_seed_nodes(&mut storage)
            .expect("failed to initialize materialized storage");
        Self {
            replica_id,
            storage,
            clock,
            counter: 0,
            applied: HashSet::new(),
            log: Vec::new(),
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
        let known_state = Some(Self::calculate_subtree_version_vector(&self.storage, node)?);
        let op = Operation::delete(&replica, counter, lamport, node, known_state);
        self.commit_local(op)
    }

    pub fn apply_remote(&mut self, op: Operation) -> Result<()> {
        self.clock.observe(op.meta.lamport);
        self.version_vector
            .observe(&op.meta.id.replica, op.meta.id.counter);
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
        self.storage.clear_materialized()?;
        Self::ensure_seed_nodes(&mut self.storage)?;

        for op in ops {
            self.clock.observe(op.meta.lamport);
            self.version_vector
                .observe(&op.meta.id.replica, op.meta.id.counter);
            self.applied.insert(op.meta.id.clone());
            let snapshot = Self::snapshot(&mut self.storage, &op)?;
            self.log.push(LogEntry { op, snapshot });
        }

        self.rebuild_materialized()?;
        Ok(())
    }

    pub fn children(&self, parent: NodeId) -> Option<Vec<NodeId>> {
        self.storage
            .get_node(parent)
            .ok()
            .flatten()
            .map(|n| {
                n.children
                    .into_iter()
                    .filter(|&child_id| !self.is_tombstoned(child_id))
                    .collect()
            })
    }

    pub fn children_slice(&self, parent: NodeId) -> Option<Vec<NodeId>> {
        self.storage
            .get_node(parent)
            .ok()
            .flatten()
            .map(|n| n.children)
    }

    pub fn parent(&self, node: NodeId) -> Option<NodeId> {
        self.storage
            .get_node(node)
            .ok()
            .flatten()
            .and_then(|n| {
                if self.is_tombstoned(node) {
                    Some(NodeId::TRASH)
                } else {
                    n.parent.filter(|&p| p != NodeId::TRASH)
                }
            })
    }

    pub fn is_tombstoned(&self, node: NodeId) -> bool {
        self.storage
            .get_node(node)
            .ok()
            .flatten()
            .and_then(|state| state.deleted_at)
            .map(|deleted_vv| {
                let subtree_vv = Self::calculate_subtree_version_vector(&self.storage, node)
                    .unwrap_or_default();
                deleted_vv.is_aware_of(&subtree_vv)
            })
            .unwrap_or(false)
    }

    pub fn lamport(&self) -> Lamport {
        self.clock.now()
    }

    pub fn nodes(&self) -> Vec<(NodeId, Option<NodeId>)> {
        let mut pairs: Vec<_> = self
            .storage
            .all_nodes()
            .unwrap_or_default()
            .into_iter()
            .filter(|(id, _)| *id != NodeId::TRASH && *id != NodeId::ROOT && !self.is_tombstoned(*id))
            .map(|(id, state)| (id, state.parent))
            .collect();
        pairs.sort_by_key(|(id, _)| id.0);
        pairs
    }

    pub fn validate_invariants(&self) -> Result<()> {
        let nodes = self.storage.all_nodes()?;
        for (pid, pstate) in &nodes {
            let mut seen = HashSet::new();
            for child in &pstate.children {
                if !seen.insert(child) {
                    return Err(Error::InvalidOperation("duplicate child entry".into()));
                }
                if let Some((_, child_state)) = nodes.iter().find(|(id, _)| id == child) {
                    if child_state.parent != Some(*pid) {
                        return Err(Error::InvalidOperation("child parent mismatch".into()));
                    }
                } else {
                    return Err(Error::InvalidOperation("child not present in nodes".into()));
                }
            }
        }

        for (node, _) in &nodes {
            if self.has_cycle_from(*node)? {
                return Err(Error::InvalidOperation("cycle detected".into()));
            }
        }
        Ok(())
    }

    pub fn is_known(&self, node: NodeId) -> bool {
        self.storage.get_node(node).ok().flatten().is_some()
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
            current = self.storage.get_node(n)?.and_then(|s| s.parent);
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

    fn ensure_seed_nodes(storage: &mut S) -> Result<()> {
        if storage.get_node(NodeId::ROOT)?.is_none() {
            storage.put_node(NodeId::ROOT, NodeState::default())?;
        }
        if storage.get_node(NodeId::TRASH)?.is_none() {
            storage.put_node(NodeId::TRASH, NodeState::default())?;
        }
        Ok(())
    }

    fn ensure_node(storage: &mut S, id: NodeId) -> Result<NodeState> {
        if let Some(state) = storage.get_node(id)? {
            return Ok(state);
        }
        let state = NodeState::default();
        storage.put_node(id, state.clone())?;
        Ok(state)
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
            Self::undo_entry(&mut self.storage, entry)?;
        }

        let snapshot = Self::apply_forward(&mut self.storage, &op)?;
        self.log.insert(
            idx,
            LogEntry {
                op: op.clone(),
                snapshot,
            },
        );

        for entry in self.log.iter().skip(idx + 1) {
            Self::apply_forward_with_snapshot(&mut self.storage, entry)?;
        }

        self.storage.apply(op)?;
        Ok(())
    }

    fn rebuild_materialized(&mut self) -> Result<()> {
        self.storage.clear_materialized()?;
        Self::ensure_seed_nodes(&mut self.storage)?;
        for entry in &self.log {
            Self::apply_forward_with_snapshot(&mut self.storage, entry)?;
        }
        Ok(())
    }

    fn snapshot(storage: &mut S, op: &Operation) -> Result<NodeSnapshot> {
        let node_id = match &op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node } => *node,
        };
        let state = Self::ensure_node(storage, node_id)?;
        let parent = state.parent;
        let position = if let Some(p) = parent {
            storage
                .get_node(p)?
                .and_then(|pstate| pstate.children.iter().position(|c| c == &node_id))
        } else {
            None
        };
        Ok(NodeSnapshot { parent, position })
    }

    fn undo_entry(storage: &mut S, entry: &LogEntry) -> Result<()> {
        let node_id = match &entry.op.kind {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node } => *node,
        };
        Self::ensure_node(storage, node_id)?;
        Self::detach(storage, node_id)?;
        if let Some(parent) = entry.snapshot.parent {
            Self::ensure_node(storage, parent)?;
            let pos = entry
                .snapshot
                .position
                .unwrap_or_else(|| {
                    storage
                        .get_node(parent)
                        .ok()
                        .flatten()
                        .map(|p| p.children.len())
                        .unwrap_or(0)
                });
            Self::attach(storage, node_id, parent, pos)?;
        } else if let Some(mut state) = storage.get_node(node_id)? {
            state.parent = None;
            storage.put_node(node_id, state)?;
        }
        Ok(())
    }

    fn apply_forward(storage: &mut S, op: &Operation) -> Result<NodeSnapshot> {
        let snapshot = Self::snapshot(storage, op)?;
        match &op.kind {
            OperationKind::Insert {
                parent,
                node,
                position,
            } => Self::apply_insert(storage, op, *parent, *node, *position)?,
            OperationKind::Move {
                node,
                new_parent,
                position,
            } => Self::apply_move(storage, op, *node, *new_parent, *position)?,
            OperationKind::Delete { node } => Self::apply_delete(storage, op, *node)?,
            OperationKind::Tombstone { node } => Self::apply_delete(storage, op, *node)?,
        }
        Ok(snapshot)
    }

    fn apply_forward_with_snapshot(storage: &mut S, entry: &LogEntry) -> Result<()> {
        Self::apply_forward(storage, &entry.op)?;
        Ok(())
    }

    fn apply_insert(
        storage: &mut S,
        op: &Operation,
        parent: NodeId,
        node: NodeId,
        position: usize,
    ) -> Result<()> {
        if parent == node || Self::introduces_cycle(storage, node, parent)? {
            return Ok(());
        }
        Self::ensure_node(storage, parent)?;
        Self::ensure_node(storage, node)?;
        Self::detach(storage, node)?;
        Self::attach(storage, node, parent, position)?;
        Self::update_last_change(storage, op, node)?;
        Self::update_last_change(storage, op, parent)?;
        Ok(())
    }

    fn apply_move(
        storage: &mut S,
        op: &Operation,
        node: NodeId,
        new_parent: NodeId,
        position: usize,
    ) -> Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        Self::ensure_node(storage, node)?;
        Self::ensure_node(storage, new_parent)?;
        if new_parent != NodeId::TRASH
            && (Self::introduces_cycle(storage, node, new_parent)? || node == new_parent)
        {
            return Ok(());
        }

        let old_parent = storage.get_node(node)?.and_then(|n| n.parent);

        Self::detach(storage, node)?;
        Self::attach(storage, node, new_parent, position)?;

        Self::update_last_change(storage, op, node)?;
        if let Some(old_p) = old_parent {
            if old_p != NodeId::TRASH {
                Self::update_last_change(storage, op, old_p)?;
            }
        }
        if new_parent != NodeId::TRASH {
            Self::update_last_change(storage, op, new_parent)?;
        }
        Ok(())
    }

    fn apply_delete(storage: &mut S, op: &Operation, node: NodeId) -> Result<()> {
        if node == NodeId::ROOT || node == NodeId::TRASH {
            return Ok(());
        }

        Self::ensure_node(storage, node)?;

        let mut delete_vv = Self::operation_version_vector(op);
        if let Some(known_state) = &op.meta.known_state {
            delete_vv.merge(known_state);
        }

        let Some(mut state) = storage.get_node(node)? else {
            return Ok(());
        };

        if let Some(existing) = &mut state.deleted_at {
            existing.merge(&delete_vv);
        } else {
            state.deleted_at = Some(delete_vv);
        }
        storage.put_node(node, state)?;
        Ok(())
    }

    fn operation_version_vector(op: &Operation) -> VersionVector {
        let mut vv = VersionVector::new();
        vv.observe(&op.meta.id.replica, op.meta.id.counter);
        vv
    }

    fn update_last_change(storage: &mut S, op: &Operation, node: NodeId) -> Result<()> {
        let mut state = Self::ensure_node(storage, node)?;
        state
            .last_change
            .merge(&Self::operation_version_vector(op));
        if let Some(known_state) = &op.meta.known_state {
            state.last_change.merge(known_state);
        }
        storage.put_node(node, state)?;
        Ok(())
    }

    fn calculate_subtree_version_vector(storage: &S, node: NodeId) -> Result<VersionVector> {
        let Some(state) = storage.get_node(node)? else {
            return Ok(VersionVector::new());
        };

        let mut subtree_vv = state.last_change.clone();
        for child_id in state.children {
            let child_vv = Self::calculate_subtree_version_vector(storage, child_id)?;
            subtree_vv.merge(&child_vv);
        }

        Ok(subtree_vv)
    }

    fn detach(storage: &mut S, node: NodeId) -> Result<()> {
        if let Some(mut node_state) = storage.get_node(node)? {
            if let Some(parent) = node_state.parent {
                if let Some(mut parent_state) = storage.get_node(parent)? {
                    parent_state.children.retain(|c| c != &node);
                    storage.put_node(parent, parent_state)?;
                }
            }
            node_state.parent = None;
            storage.put_node(node, node_state)?;
        }
        Ok(())
    }

    fn attach(storage: &mut S, node: NodeId, parent: NodeId, position: usize) -> Result<()> {
        let mut node_state = Self::ensure_node(storage, node)?;
        node_state.parent = Some(parent);
        storage.put_node(node, node_state)?;

        if parent == NodeId::TRASH {
            return Ok(());
        }

        let mut parent_state = Self::ensure_node(storage, parent)?;
        let idx = position.min(parent_state.children.len());
        parent_state.children.insert(idx, node);
        storage.put_node(parent, parent_state)?;
        Ok(())
    }

    fn introduces_cycle(storage: &S, node: NodeId, potential_parent: NodeId) -> Result<bool> {
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
            current = storage.get_node(n)?.and_then(|state| state.parent);
        }
        Ok(false)
    }
}
