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
        /// Optional application payload to initialize alongside insert.
        ///
        /// When present, this is treated like a `Payload` op at the same `(lamport, replica, counter)`,
        /// with last-writer-wins ordering per node.
        payload: Option<Vec<u8>>,
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
    /// Update the node payload (application data) as an opaque byte string.
    ///
    /// Merge semantics are last-writer-wins per node, ordered by
    /// `(lamport, replica, counter)` (see `OperationMetadata`).
    ///
    /// - `payload = Some(bytes)` sets the payload
    /// - `payload = None` clears the payload
    Payload {
        node: NodeId,
        payload: Option<Vec<u8>>,
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
        Self::insert_with_optional_payload(replica, counter, lamport, parent, node, position, None)
    }

    pub fn insert_with_payload(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        parent: NodeId,
        node: NodeId,
        position: usize,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self::insert_with_optional_payload(
            replica,
            counter,
            lamport,
            parent,
            node,
            position,
            Some(payload.into()),
        )
    }

    pub fn insert_with_optional_payload(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        parent: NodeId,
        node: NodeId,
        position: usize,
        payload: Option<Vec<u8>>,
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
                payload,
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

    pub fn payload(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        node: NodeId,
        payload: Option<Vec<u8>>,
    ) -> Self {
        Self {
            meta: OperationMetadata {
                id: OperationId::new(replica, counter),
                lamport,
                known_state: None,
            },
            kind: OperationKind::Payload { node, payload },
        }
    }

    pub fn set_payload(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        node: NodeId,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self::payload(replica, counter, lamport, node, Some(payload.into()))
    }

    pub fn clear_payload(
        replica: &ReplicaId,
        counter: u64,
        lamport: Lamport,
        node: NodeId,
    ) -> Self {
        Self::payload(replica, counter, lamport, node, None)
    }
}
