use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId, OperationId};
use crate::ops::Operation;
use crate::version_vector::VersionVector;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Pluggable clock to allow Lamport, Hybrid Logical Clock, or custom time strategies.
pub trait Clock {
    fn tick(&mut self) -> Lamport;
    fn observe(&mut self, external: Lamport);
    fn now(&self) -> Lamport;
}

/// Access control hook that can deny operations or reads.
pub trait AccessControl {
    fn can_apply(&self, op: &Operation) -> Result<()>;
    fn can_read(&self, node: NodeId) -> Result<()>;
}

/// Persistent or in-memory operation log.
pub trait Storage {
    fn apply(&mut self, op: Operation) -> Result<()>;
    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>>;
    fn latest_lamport(&self) -> Lamport;
    fn snapshot(&self) -> Result<Snapshot>;
}

/// Index provider used to accelerate subtree queries when partial sync is requested.
pub trait IndexProvider {
    fn children_of(&self, node: NodeId) -> Result<Vec<NodeId>>;
    fn exists(&self, node: NodeId) -> bool;
}

/// Storage for materialized node state (parent/children ordering + causal metadata).
///
/// This is the seam used by SQLite/wa-sqlite adapters so the core CRDT owns all tree logic
/// while embedders can decide how to persist and index node state.
pub trait NodeStore {
    fn reset(&mut self) -> Result<()>;
    fn ensure_node(&mut self, node: NodeId) -> Result<()>;
    fn exists(&self, node: NodeId) -> Result<bool>;

    fn parent(&self, node: NodeId) -> Result<Option<NodeId>>;
    fn children(&self, parent: NodeId) -> Result<Vec<NodeId>>;

    fn detach(&mut self, node: NodeId) -> Result<()>;
    fn attach(&mut self, node: NodeId, parent: NodeId, position: usize) -> Result<()>;

    fn last_change(&self, node: NodeId) -> Result<VersionVector>;
    fn merge_last_change(&mut self, node: NodeId, delta: &VersionVector) -> Result<()>;

    fn deleted_at(&self, node: NodeId) -> Result<Option<VersionVector>>;
    fn merge_deleted_at(&mut self, node: NodeId, delta: &VersionVector) -> Result<()>;

    fn all_nodes(&self) -> Result<Vec<NodeId>>;
}

/// Storage for last-writer-wins node payloads.
///
/// Payloads are application-defined opaque bytes. Merge semantics are last-writer-wins per node,
/// ordered by `(lamport, replica, counter)`. This trait allows embedders (SQLite, wasm, etc) to
/// persist payload state without re-implementing CRDT ordering rules.
pub trait PayloadStore {
    fn reset(&mut self) -> Result<()>;
    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>>;
    fn last_writer(&self, node: NodeId) -> Result<Option<(Lamport, OperationId)>>;
    fn set_payload(&mut self, node: NodeId, payload: Option<Vec<u8>>, writer: (Lamport, OperationId)) -> Result<()>;
}

/// Lightweight snapshot to expose to storage adapters.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Snapshot {
    pub head: Lamport,
}

/// Basic Lamport clock implementation useful for tests and default flows.
#[derive(Clone, Debug, Default)]
pub struct LamportClock {
    counter: Lamport,
}

impl Clock for LamportClock {
    fn tick(&mut self) -> Lamport {
        self.counter += 1;
        self.counter
    }

    fn observe(&mut self, external: Lamport) {
        self.counter = self.counter.max(external);
    }

    fn now(&self) -> Lamport {
        self.counter
    }
}

/// Allows unrestricted access; helpful for early prototyping.
pub struct AllowAllAccess;

impl AccessControl for AllowAllAccess {
    fn can_apply(&self, _op: &Operation) -> Result<()> {
        Ok(())
    }

    fn can_read(&self, _node: NodeId) -> Result<()> {
        Ok(())
    }
}

/// In-memory vector-backed storage for early prototyping and tests.
#[derive(Default)]
pub struct MemoryStorage {
    ops: Vec<Operation>,
}

impl Storage for MemoryStorage {
    fn apply(&mut self, op: Operation) -> Result<()> {
        self.ops.push(op);
        Ok(())
    }

    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        Ok(self.ops.iter().filter(|&op| op.meta.lamport > lamport).cloned().collect())
    }

    fn latest_lamport(&self) -> Lamport {
        self.ops.iter().map(|op| op.meta.lamport).max().unwrap_or_default()
    }

    fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            head: self.latest_lamport(),
        })
    }
}

impl IndexProvider for MemoryStorage {
    fn children_of(&self, _node: NodeId) -> Result<Vec<NodeId>> {
        // The memory adapter does not maintain indexes; callers can layer custom indexes as needed.
        Err(Error::Storage(
            "MemoryStorage does not provide index lookups".into(),
        ))
    }

    fn exists(&self, node: NodeId) -> bool {
        self.ops.iter().any(|op| match op.kind {
            crate::ops::OperationKind::Insert { node: n, .. } => n == node,
            crate::ops::OperationKind::Move { node: n, .. } => n == node,
            crate::ops::OperationKind::Delete { node: n } => n == node,
            crate::ops::OperationKind::Tombstone { node: n } => n == node,
            crate::ops::OperationKind::Payload { node: n, .. } => n == node,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryPayloadStore {
    entries: HashMap<NodeId, MemoryPayloadEntry>,
}

#[derive(Clone, Debug)]
struct MemoryPayloadEntry {
    payload: Option<Vec<u8>>,
    last_writer: Option<(Lamport, OperationId)>,
}

impl Default for MemoryPayloadEntry {
    fn default() -> Self {
        Self {
            payload: None,
            last_writer: None,
        }
    }
}

impl PayloadStore for MemoryPayloadStore {
    fn reset(&mut self) -> Result<()> {
        self.entries.clear();
        Ok(())
    }

    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        Ok(self.entries.get(&node).and_then(|e| e.payload.clone()))
    }

    fn last_writer(&self, node: NodeId) -> Result<Option<(Lamport, OperationId)>> {
        Ok(self.entries.get(&node).and_then(|e| e.last_writer.clone()))
    }

    fn set_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
        writer: (Lamport, OperationId),
    ) -> Result<()> {
        let entry = self.entries.entry(node).or_default();
        entry.payload = payload;
        entry.last_writer = Some(writer);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct MemoryNodeState {
    parent: Option<NodeId>,
    children: Vec<NodeId>,
    last_change: VersionVector,
    deleted_at: Option<VersionVector>,
}

impl MemoryNodeState {
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

/// In-memory [`NodeStore`] implementation used by default.
#[derive(Clone, Debug)]
pub struct MemoryNodeStore {
    nodes: HashMap<NodeId, MemoryNodeState>,
}

impl Default for MemoryNodeStore {
    fn default() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(NodeId::ROOT, MemoryNodeState::new_root());
        Self { nodes }
    }
}

impl MemoryNodeStore {
    fn get_state(&self, node: NodeId) -> Result<&MemoryNodeState> {
        self.nodes
            .get(&node)
            .ok_or_else(|| Error::InconsistentState(format!("node {} missing from store", node.0)))
    }

    fn get_state_mut(&mut self, node: NodeId) -> Result<&mut MemoryNodeState> {
        self.nodes
            .get_mut(&node)
            .ok_or_else(|| Error::InconsistentState(format!("node {} missing from store", node.0)))
    }
}

impl NodeStore for MemoryNodeStore {
    fn reset(&mut self) -> Result<()> {
        self.nodes.clear();
        self.nodes.insert(NodeId::ROOT, MemoryNodeState::new_root());
        Ok(())
    }

    fn ensure_node(&mut self, node: NodeId) -> Result<()> {
        self.nodes.entry(node).or_insert_with(|| {
            if node == NodeId::ROOT {
                MemoryNodeState::new_root()
            } else {
                MemoryNodeState::new()
            }
        });
        Ok(())
    }

    fn exists(&self, node: NodeId) -> Result<bool> {
        Ok(self.nodes.contains_key(&node))
    }

    fn parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        Ok(self.nodes.get(&node).and_then(|s| s.parent))
    }

    fn children(&self, parent: NodeId) -> Result<Vec<NodeId>> {
        Ok(self.get_state(parent)?.children.clone())
    }

    fn detach(&mut self, node: NodeId) -> Result<()> {
        let Some(parent) = self.nodes.get(&node).and_then(|s| s.parent) else {
            return Ok(());
        };

        if parent != NodeId::TRASH {
            if let Some(parent_state) = self.nodes.get_mut(&parent) {
                parent_state.children.retain(|c| *c != node);
            }
        }

        self.get_state_mut(node)?.parent = None;
        Ok(())
    }

    fn attach(&mut self, node: NodeId, parent: NodeId, position: usize) -> Result<()> {
        self.ensure_node(parent)?;
        self.ensure_node(node)?;
        self.get_state_mut(node)?.parent = Some(parent);

        if parent == NodeId::TRASH {
            return Ok(());
        }

        let parent_state = self.get_state_mut(parent)?;
        let idx = position.min(parent_state.children.len());
        parent_state.children.insert(idx, node);
        Ok(())
    }

    fn last_change(&self, node: NodeId) -> Result<VersionVector> {
        Ok(self.get_state(node)?.last_change.clone())
    }

    fn merge_last_change(&mut self, node: NodeId, delta: &VersionVector) -> Result<()> {
        self.get_state_mut(node)?.last_change.merge(delta);
        Ok(())
    }

    fn deleted_at(&self, node: NodeId) -> Result<Option<VersionVector>> {
        Ok(self.get_state(node)?.deleted_at.clone())
    }

    fn merge_deleted_at(&mut self, node: NodeId, delta: &VersionVector) -> Result<()> {
        let state = self.get_state_mut(node)?;
        if let Some(existing) = &mut state.deleted_at {
            existing.merge(delta);
        } else {
            state.deleted_at = Some(delta.clone());
        }
        Ok(())
    }

    fn all_nodes(&self) -> Result<Vec<NodeId>> {
        Ok(self.nodes.keys().copied().collect())
    }
}
