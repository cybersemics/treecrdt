use serde::{Deserialize, Serialize, Serializer};
use treecrdt_core::{Operation, OperationId, OperationKind, ReplicaId};

use crate::{hex_to_node, node_to_hex};

/// Owned bytes cross the WASM boundary as Uint8Array, without a JSON or hexadecimal round trip.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Bytes(#[serde(with = "serde_bytes")] pub Vec<u8>);

pub(crate) fn serialize_optional_bytes<S: Serializer>(
    bytes: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match bytes {
        Some(bytes) => serializer.serialize_bytes(bytes),
        None => serializer.serialize_none(),
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct TypedOperation {
    meta: Metadata,
    kind: Kind,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    id: TypedOperationId,
    lamport: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    known_state: Option<Bytes>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct TypedOperationId {
    replica: Bytes,
    counter: u64,
}

impl TypedOperationId {
    pub(crate) fn into_id(self) -> Result<OperationId, String> {
        if self.counter > 9_007_199_254_740_991 {
            return Err("operation counter must be a safe JavaScript integer".into());
        }
        Ok(OperationId::new(
            &ReplicaId::new(self.replica.0),
            self.counter,
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Kind {
    Insert {
        parent: String,
        node: String,
        #[serde(rename = "orderKey")]
        order_key: Bytes,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Bytes>,
    },
    Move {
        node: String,
        #[serde(rename = "newParent")]
        new_parent: String,
        #[serde(rename = "orderKey")]
        order_key: Bytes,
    },
    Delete {
        node: String,
    },
    Tombstone {
        node: String,
    },
    Payload {
        node: String,
        payload: Option<Bytes>,
    },
}

impl TypedOperation {
    pub(crate) fn from_op(op: Operation) -> Result<Self, String> {
        let kind = match op.kind {
            OperationKind::Insert {
                parent,
                node,
                order_key,
                payload,
            } => Kind::Insert {
                parent: node_to_hex(parent),
                node: node_to_hex(node),
                order_key: Bytes(order_key),
                payload: payload.map(Bytes),
            },
            OperationKind::Move {
                node,
                new_parent,
                order_key,
            } => Kind::Move {
                node: node_to_hex(node),
                new_parent: node_to_hex(new_parent),
                order_key: Bytes(order_key),
            },
            OperationKind::Delete { node } => Kind::Delete {
                node: node_to_hex(node),
            },
            OperationKind::Tombstone { node } => Kind::Tombstone {
                node: node_to_hex(node),
            },
            OperationKind::Payload { node, payload } => Kind::Payload {
                node: node_to_hex(node),
                payload: payload.map(Bytes),
            },
        };
        Ok(Self {
            meta: Metadata {
                id: TypedOperationId {
                    replica: Bytes(op.meta.id.replica.0),
                    counter: op.meta.id.counter,
                },
                lamport: op.meta.lamport,
                known_state: op
                    .meta
                    .known_state
                    .map(|vv| serde_json::to_vec(&vv).map(Bytes))
                    .transpose()
                    .map_err(|error| error.to_string())?,
            },
            kind,
        })
    }

    pub(crate) fn into_op(self) -> Result<Operation, String> {
        let replica = ReplicaId::new(self.meta.id.replica.0);
        let counter = self.meta.id.counter;
        let lamport = self.meta.lamport;
        if counter > 9_007_199_254_740_991 || lamport > 9_007_199_254_740_991 {
            return Err("operation counter and Lamport must be safe JavaScript integers".into());
        }
        // Keep the existing protocol's opaque, gap-aware defensive-deletion awareness bytes.
        let known_state = self
            .meta
            .known_state
            .map(|bytes| serde_json::from_slice(&bytes.0))
            .transpose()
            .map_err(|error| error.to_string())?;
        let mut op = match self.kind {
            Kind::Insert {
                parent,
                node,
                order_key,
                payload,
            } => Operation::insert_with_optional_payload(
                &replica,
                counter,
                lamport,
                hex_to_node(&parent)?,
                hex_to_node(&node)?,
                order_key.0,
                payload.map(|bytes| bytes.0),
            ),
            Kind::Move {
                node,
                new_parent,
                order_key,
            } => Operation::move_node(
                &replica,
                counter,
                lamport,
                hex_to_node(&node)?,
                hex_to_node(&new_parent)?,
                order_key.0,
            ),
            Kind::Delete { node } => {
                if known_state.is_none() {
                    return Err("delete operation requires meta.knownState".into());
                }
                Operation::delete(
                    &replica,
                    counter,
                    lamport,
                    hex_to_node(&node)?,
                    known_state.clone(),
                )
            }
            Kind::Tombstone { node } => {
                Operation::tombstone(&replica, counter, lamport, hex_to_node(&node)?)
            }
            Kind::Payload { node, payload } => Operation::payload(
                &replica,
                counter,
                lamport,
                hex_to_node(&node)?,
                payload.map(|bytes| bytes.0),
            ),
        };
        op.meta.known_state = known_state;
        Ok(op)
    }
}
