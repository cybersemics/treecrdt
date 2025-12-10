use crate::error::{Error, Result};
use crate::ids::{Lamport, NodeId};
use crate::ops::Operation;

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
        })
    }
}
