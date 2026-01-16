use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId};
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

/// Materialized node representation persisted by a storage backend.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeState {
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
    pub last_change: VersionVector,
    pub deleted_at: Option<VersionVector>,
}

/// Storage that also owns materialized tree state (nodes and children ordering).
///
/// The Tree CRDT core drives all semantic logic; storage only persists and mutates
/// materialized state and the op log.
pub trait MaterializedStorage: Storage {
    /// Return the current state for a node, if present.
    fn get_node(&self, id: NodeId) -> Result<Option<NodeState>>;

    /// Persist the given node state (insert or overwrite).
    fn put_node(&mut self, id: NodeId, state: NodeState) -> Result<()>;

    /// Clear all materialized nodes (used during replay) and re-initialize any
    /// backend-specific invariants.
    fn clear_materialized(&mut self) -> Result<()>;

    /// Enumerate all known nodes and their materialized state.
    fn all_nodes(&self) -> Result<Vec<(NodeId, NodeState)>>;
}

/// Index provider used to accelerate subtree queries when partial sync is requested.
pub trait IndexProvider {
    fn children_of(&self, node: NodeId) -> Result<Vec<NodeId>>;
    fn exists(&self, node: NodeId) -> bool;
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
pub struct MemoryStorage {
    ops: Vec<Operation>,
    nodes: std::collections::HashMap<NodeId, NodeState>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        let mut this = Self {
            ops: Vec::new(),
            nodes: std::collections::HashMap::new(),
        };
        this.ensure_seed_nodes();
        this
    }
}

impl Storage for MemoryStorage {
    fn apply(&mut self, op: Operation) -> Result<()> {
        self.ops.push(op);
        Ok(())
    }

    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        Ok(self
            .ops
            .iter()
            .filter(|&op| op.meta.lamport > lamport)
            .cloned()
            .collect())
    }

    fn latest_lamport(&self) -> Lamport {
        self.ops
            .iter()
            .map(|op| op.meta.lamport)
            .max()
            .unwrap_or_default()
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
        })
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn ensure_seed_nodes(&mut self) {
        self.nodes.entry(NodeId::ROOT).or_insert_with(NodeState::default);
        self.nodes.entry(NodeId::TRASH).or_insert_with(NodeState::default);
    }
}

impl MaterializedStorage for MemoryStorage {
    fn get_node(&self, id: NodeId) -> Result<Option<NodeState>> {
        Ok(self.nodes.get(&id).cloned())
    }

    fn put_node(&mut self, id: NodeId, state: NodeState) -> Result<()> {
        self.nodes.insert(id, state);
        Ok(())
    }

    fn clear_materialized(&mut self) -> Result<()> {
        self.nodes.clear();
        self.ensure_seed_nodes();
        Ok(())
    }

    fn all_nodes(&self) -> Result<Vec<(NodeId, NodeState)>> {
        Ok(self.nodes.iter().map(|(k, v)| (*k, v.clone())).collect())
    }
}
