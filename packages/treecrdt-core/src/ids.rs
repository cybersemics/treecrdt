#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Lamport timestamp used for ordering operations.
pub type Lamport = u64;

/// Unique identifier for a replica. Backed by raw bytes to support arbitrary identity formats.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ReplicaId(pub Vec<u8>);

impl ReplicaId {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Unique identifier for a node in the tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NodeId(pub u128);

impl NodeId {
    pub const ROOT: NodeId = NodeId(0);
    pub const TRASH: NodeId = NodeId(u128::MAX);
}

/// Globally unique identifier for an operation.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OperationId {
    pub replica: ReplicaId,
    pub counter: u64,
}

impl OperationId {
    pub fn new(replica: &ReplicaId, counter: u64) -> Self {
        Self {
            replica: replica.clone(),
            counter,
        }
    }
}
