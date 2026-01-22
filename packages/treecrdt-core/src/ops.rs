use std::cmp::Ordering;

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

impl OperationKind {
    pub fn node(&self) -> NodeId {
        match self {
            OperationKind::Insert { node, .. }
            | OperationKind::Move { node, .. }
            | OperationKind::Delete { node }
            | OperationKind::Tombstone { node }
            | OperationKind::Payload { node, .. } => *node,
        }
    }
}

/// Deterministic tie-breaker used to order operations with equal Lamport timestamps.
///
/// This intentionally avoids comparing the full replica id in the hot path by using the first
/// 8 bytes (zero-padded) plus the counter, while still producing a total order when combined
/// with the full `(replica, counter)` id as a final tiebreak.
pub fn op_tie_breaker_id(replica: &[u8], counter: u64) -> u128 {
    let mut bytes = [0u8; 16];
    let len = replica.len().min(8);
    bytes[..len].copy_from_slice(&replica[..len]);
    bytes[8..].copy_from_slice(&counter.to_be_bytes());
    u128::from_be_bytes(bytes)
}

/// Canonical ordering for operation ids used throughout the core.
pub fn cmp_op_key(
    a_lamport: Lamport,
    a_replica: &[u8],
    a_counter: u64,
    b_lamport: Lamport,
    b_replica: &[u8],
    b_counter: u64,
) -> Ordering {
    (
        a_lamport,
        op_tie_breaker_id(a_replica, a_counter),
        a_replica,
        a_counter,
    )
        .cmp(&(
            b_lamport,
            op_tie_breaker_id(b_replica, b_counter),
            b_replica,
            b_counter,
        ))
}

/// Canonical ordering for full operations.
pub fn cmp_ops(a: &Operation, b: &Operation) -> Ordering {
    cmp_op_key(
        a.meta.lamport,
        a.meta.id.replica.as_bytes(),
        a.meta.id.counter,
        b.meta.lamport,
        b.meta.id.replica.as_bytes(),
        b.meta.id.counter,
    )
}
