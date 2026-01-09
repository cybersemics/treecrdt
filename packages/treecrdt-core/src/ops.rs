use crate::ids::{Lamport, NodeId, OperationId, ReplicaId};
use crate::version_vector::VersionVector;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Metadata that accompanies every operation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OperationMetadata {
    pub id: OperationId,
    pub lamport: Lamport,
    #[cfg_attr(feature = "serde", serde(default))]
    pub known_state: Option<VersionVector>,
}

/// The CRDT tree mutations.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OperationKind {
    Insert {
        parent: NodeId,
        node: NodeId,
        position: usize,
    },
    Move {
        node: NodeId,
        new_parent: NodeId,
        position: usize,
    },
    Delete {
        node: NodeId,
    },
    Tombstone {
        node: NodeId,
    },
}

/// Full operation envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Operation {
    pub meta: OperationMetadata,
    pub kind: OperationKind,
}

impl Operation {
    pub fn insert(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        parent: NodeId,
        node: NodeId,
        position: usize,
    ) -> Self {
        Self {
            meta: OperationMetadata {
                id: OperationId::new(replica, counter),
                lamport,
                known_state: None,
            },
            kind: OperationKind::Insert {
                parent,
                node,
                position,
            },
        }
    }

    pub fn move_node(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        node: NodeId,
        new_parent: NodeId,
        position: usize,
    ) -> Self {
        Self {
            meta: OperationMetadata {
                id: OperationId::new(replica, counter),
                lamport,
                known_state: None,
            },
            kind: OperationKind::Move {
                node,
                new_parent,
                position,
            },
        }
    }

    pub fn delete(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        node: NodeId,
        known_state: Option<VersionVector>,
    ) -> Self {
        Self {
            meta: OperationMetadata {
                id: OperationId::new(replica, counter),
                lamport,
                known_state,
            },
            kind: OperationKind::Delete { node },
        }
    }

    pub fn tombstone(replica: &ReplicaId, counter: u64, lamport: Lamport, node: NodeId) -> Self {
        Self {
            meta: OperationMetadata {
                id: OperationId::new(replica, counter),
                lamport,
                known_state: None,
            },
            kind: OperationKind::Tombstone { node },
        }
    }
}
