#![forbid(unsafe_code)]
//! WASM-friendly bridge for TreeCRDT.
//! Exposes a small wasm-bindgen surface that matches the JS adapter needs.

mod version_vector;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_wasm_bindgen::to_value;
use treecrdt_core::{
    Lamport, LamportClock, LocalPlacement, MaterializationOutcome, MemoryCheckpoint, MemoryStorage,
    NodeId, Operation, OperationId, OperationKind, OperationMetadata, ReadNode, ReplicaId,
    TreeCrdt, VersionVector,
};
use wasm_bindgen::prelude::*;

#[derive(Serialize)]
struct JsOp {
    replica: String, // hex
    counter: u64,
    lamport: Lamport,
    kind: String,
    parent: Option<String>,
    node: String,
    new_parent: Option<String>,
    order_key: Option<String>, // hex
    known_state: Option<Vec<u8>>,
    payload: Option<String>, // hex
}

// Decode JS fields directly: internally tagged enums buffer through Serde Content,
// which would also accept strings as byte arrays and byte arrays as node IDs.
#[derive(Deserialize)]
struct OperationInput {
    meta: MetadataValue,
    kind: KindInput,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KindInput {
    #[serde(rename = "type")]
    kind: KindTag,
    node: String,
    parent: Option<String>,
    new_parent: Option<String>,
    order_key: Option<ByteBuf>,
    payload: Option<ByteBuf>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum KindTag {
    Insert,
    Move,
    Delete,
    Tombstone,
    Payload,
}

#[derive(Serialize)]
struct OperationValue {
    meta: MetadataValue,
    kind: KindValue,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataValue {
    id: OperationIdValue,
    lamport: Lamport,
    #[serde(skip_serializing_if = "Option::is_none")]
    known_state: Option<ByteBuf>,
}

#[derive(Deserialize, Serialize)]
struct OperationIdValue {
    replica: ByteBuf,
    counter: u64,
}

#[derive(Serialize)]
#[serde(
    tag = "type",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
enum KindValue {
    Insert {
        node: String,
        parent: String,
        order_key: ByteBuf,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<ByteBuf>,
    },
    Move {
        node: String,
        new_parent: String,
        order_key: ByteBuf,
    },
    Delete {
        node: String,
    },
    Tombstone {
        node: String,
    },
    Payload {
        node: String,
        payload: Option<ByteBuf>,
    },
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    let clean = hex.trim_start_matches("0x");
    if !clean.is_ascii() || !clean.len().is_multiple_of(2) {
        return Err("hex must contain an even number of ASCII characters".into());
    }
    (0..clean.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&clean[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_to_node(hex: &str) -> Result<NodeId, String> {
    let bytes = hex_to_bytes(hex)?;
    if bytes.len() > 16 {
        return Err("node id longer than 16 bytes".into());
    }
    let mut buf = [0u8; 16];
    let offset = 16 - bytes.len();
    buf[offset..].copy_from_slice(&bytes);
    Ok(NodeId(u128::from_be_bytes(buf)))
}

fn node_to_hex(id: NodeId) -> String {
    format!("{:032x}", id.0)
}

fn op_to_js(op: &Operation) -> Result<JsOp, String> {
    let (kind, parent, node, new_parent, order_key, payload) = match &op.kind {
        OperationKind::Insert {
            parent,
            node,
            order_key,
            payload,
        } => (
            "insert",
            Some(*parent),
            *node,
            None,
            Some(bytes_to_hex(order_key)),
            payload.as_deref().map(bytes_to_hex),
        ),
        OperationKind::Move {
            node,
            new_parent,
            order_key,
        } => (
            "move",
            None,
            *node,
            Some(*new_parent),
            Some(bytes_to_hex(order_key)),
            None,
        ),
        OperationKind::Delete { node } => ("delete", None, *node, None, None, None),
        OperationKind::Tombstone { node } => ("tombstone", None, *node, None, None, None),
        OperationKind::Payload { node, payload } => (
            "payload",
            None,
            *node,
            None,
            None,
            payload.as_deref().map(bytes_to_hex),
        ),
    };
    let known_state = op
        .meta
        .known_state
        .as_ref()
        .map(VersionVector::encode)
        .transpose()
        .map_err(|e| e.to_string())?;
    Ok(JsOp {
        replica: bytes_to_hex(&op.meta.id.replica.0),
        counter: op.meta.id.counter,
        lamport: op.meta.lamport,
        kind: kind.to_string(),
        parent: parent.map(node_to_hex),
        node: node_to_hex(node),
        new_parent: new_parent.map(node_to_hex),
        order_key,
        known_state,
        payload,
    })
}

fn js_to_op(js: OperationInput) -> Result<Operation, String> {
    if js.meta.id.counter > 9_007_199_254_740_991 || js.meta.lamport > 9_007_199_254_740_991 {
        return Err("operation counter and Lamport must be safe JavaScript integers".into());
    }
    if matches!(js.kind.kind, KindTag::Delete)
        && js.meta.known_state.as_ref().is_none_or(|bytes| bytes.is_empty())
    {
        return Err("treecrdt: delete operations require meta.knownState".into());
    }
    let known_state = js
        .meta
        .known_state
        .map(|bytes| VersionVector::decode(&bytes))
        .transpose()
        .map_err(|e| e.to_string())?;
    let input = js.kind;
    let node = hex_to_node(&input.node)?;
    let kind = match input.kind {
        KindTag::Insert => OperationKind::Insert {
            parent: hex_to_node(&input.parent.ok_or("insert requires parent")?)?,
            node,
            order_key: input.order_key.ok_or("insert requires orderKey")?.into_vec(),
            payload: input.payload.map(ByteBuf::into_vec),
        },
        KindTag::Move => OperationKind::Move {
            node,
            new_parent: hex_to_node(&input.new_parent.ok_or("move requires newParent")?)?,
            order_key: input.order_key.ok_or("move requires orderKey")?.into_vec(),
        },
        KindTag::Delete => OperationKind::Delete { node },
        KindTag::Tombstone => OperationKind::Tombstone { node },
        KindTag::Payload => OperationKind::Payload {
            node,
            payload: input.payload.map(ByteBuf::into_vec),
        },
    };
    Ok(Operation {
        meta: OperationMetadata {
            id: OperationId {
                replica: ReplicaId::new(js.meta.id.replica.into_vec()),
                counter: js.meta.id.counter,
            },
            lamport: js.meta.lamport,
            known_state,
        },
        kind,
    })
}

impl TryFrom<Operation> for OperationValue {
    type Error = String;

    fn try_from(op: Operation) -> Result<Self, String> {
        if op.meta.id.counter > 9_007_199_254_740_991 || op.meta.lamport > 9_007_199_254_740_991 {
            return Err("operation counter and Lamport must be safe JavaScript integers".into());
        }
        let known_state = op
            .meta
            .known_state
            .as_ref()
            .map(VersionVector::encode)
            .transpose()
            .map_err(|error| error.to_string())?
            .map(ByteBuf::from);
        let kind = match op.kind {
            OperationKind::Insert {
                parent,
                node,
                order_key,
                payload,
            } => KindValue::Insert {
                node: node_to_hex(node),
                parent: node_to_hex(parent),
                order_key: ByteBuf::from(order_key),
                payload: payload.map(ByteBuf::from),
            },
            OperationKind::Move {
                node,
                new_parent,
                order_key,
            } => KindValue::Move {
                node: node_to_hex(node),
                new_parent: node_to_hex(new_parent),
                order_key: ByteBuf::from(order_key),
            },
            OperationKind::Delete { node } => KindValue::Delete {
                node: node_to_hex(node),
            },
            OperationKind::Tombstone { node } => KindValue::Tombstone {
                node: node_to_hex(node),
            },
            OperationKind::Payload { node, payload } => KindValue::Payload {
                node: node_to_hex(node),
                payload: payload.map(ByteBuf::from),
            },
        };
        Ok(Self {
            meta: MetadataValue {
                id: OperationIdValue {
                    replica: ByteBuf::from(op.meta.id.replica.0),
                    counter: op.meta.id.counter,
                },
                lamport: op.meta.lamport,
                known_state,
            },
            kind,
        })
    }
}

fn serialize(value: &impl Serialize) -> Result<JsValue, String> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true))
        .map_err(|error| error.to_string())
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn placement(after: JsValue) -> Result<LocalPlacement, String> {
    if after.is_undefined() {
        return Ok(LocalPlacement::Last);
    }
    if after.is_null() {
        return Ok(LocalPlacement::First);
    }
    after
        .as_string()
        .ok_or_else(|| "after must be a node ID, null, or undefined".into())
        .and_then(|id| hex_to_node(&id))
        .map(LocalPlacement::After)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsReadRow {
    id: String,
    parent_id: Option<String>,
    payload: Option<ByteBuf>,
    children: Vec<String>,
}

impl From<ReadNode> for JsReadRow {
    fn from(row: ReadNode) -> Self {
        Self {
            id: node_to_hex(row.id),
            parent_id: row.parent.map(node_to_hex),
            payload: row.payload.map(ByteBuf::from),
            children: row.children.into_iter().map(node_to_hex).collect(),
        }
    }
}

#[derive(Serialize)]
struct JsReadChange {
    id: String,
    before: Option<JsReadRow>,
    after: Option<JsReadRow>,
}

#[derive(Serialize)]
struct JsReadChanges {
    reset: bool,
    changes: Vec<JsReadChange>,
}

#[wasm_bindgen(typescript_custom_section)]
const READ_TYPES: &str = r#"
export interface TreeReadRow {
    id: string;
    parentId: string | null;
    payload: Uint8Array | null;
    children: string[];
}
export interface TreeReadChanges {
    reset: boolean;
    changes: { id: string; before: TreeReadRow | null; after: TreeReadRow | null }[];
}
"#;

#[wasm_bindgen]
pub struct WasmTree {
    inner: TreeCrdt<MemoryStorage, LamportClock>,
    checkpoint: Option<MemoryCheckpoint>,
    transaction_failed: bool,
}

impl WasmTree {
    fn begin(&mut self) -> Result<(), String> {
        if self.checkpoint.is_some() {
            return Err("a memory transaction is already active".into());
        }
        self.checkpoint = Some(self.inner.memory_checkpoint());
        self.transaction_failed = false;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), String> {
        if self.transaction_failed {
            return Err("the memory transaction failed; roll it back before continuing".into());
        }
        self.checkpoint.take().ok_or("no memory transaction is active")?;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), String> {
        let checkpoint = self.checkpoint.take().ok_or("no memory transaction is active")?;
        self.inner.rollback_memory(checkpoint).map_err(|error| error.to_string())?;
        self.transaction_failed = false;
        Ok(())
    }

    /// Boundary failures poison explicit transactions. New local commands also roll back standalone failures;
    /// legacy ingestion retains its caller-managed transaction contract without checkpoint overhead.
    fn mutate<T>(
        &mut self,
        rollback_standalone: bool,
        work: impl FnOnce(&mut TreeCrdt<MemoryStorage, LamportClock>) -> Result<T, String>,
    ) -> Result<T, String> {
        if self.transaction_failed {
            return Err("the memory transaction failed; roll it back before continuing".into());
        }
        let standalone = (rollback_standalone && self.checkpoint.is_none())
            .then(|| self.inner.memory_checkpoint());
        match work(&mut self.inner) {
            Ok(value) => Ok(value),
            Err(error) => {
                if let Some(checkpoint) = standalone {
                    self.inner.rollback_memory(checkpoint).map_err(|rollback| {
                        format!("{error}; memory rollback failed: {rollback}")
                    })?;
                } else if self.checkpoint.is_some() {
                    self.transaction_failed = true;
                }
                Err(error)
            }
        }
    }
}

#[wasm_bindgen]
impl WasmTree {
    #[wasm_bindgen(constructor)]
    pub fn new(replica_hex: String) -> WasmTree {
        let replica_bytes = hex_to_bytes(&replica_hex).unwrap_or_else(|_| b"wasm".to_vec());
        let replica = ReplicaId::new(replica_bytes);
        WasmTree {
            inner: TreeCrdt::new(replica, MemoryStorage::default(), LamportClock::default())
                .unwrap(),
            checkpoint: None,
            transaction_failed: false,
        }
    }

    #[wasm_bindgen(js_name = enableReadTracking)]
    pub fn enable_read_tracking(&mut self) {
        self.inner.track_read_changes();
    }

    #[wasm_bindgen(js_name = readNode, unchecked_return_type = "TreeReadRow | null")]
    pub fn read_node(&self, node: String) -> Result<JsValue, JsValue> {
        let row = self
            .inner
            .read_node(hex_to_node(&node).map_err(js_error)?)
            .map_err(js_error)?
            .map(JsReadRow::from);
        serialize(&row).map_err(js_error)
    }

    #[wasm_bindgen(js_name = nodeIds, unchecked_return_type = "string[]")]
    pub fn node_ids(&self) -> Result<JsValue, JsValue> {
        let nodes: Vec<_> =
            self.inner.node_ids().map_err(js_error)?.into_iter().map(node_to_hex).collect();
        serialize(&nodes).map_err(js_error)
    }

    /// Owned deltas are cleared only after all rows serialize successfully.
    #[wasm_bindgen(js_name = drainReadChanges, unchecked_return_type = "TreeReadChanges")]
    pub fn drain_read_changes(&mut self) -> Result<JsValue, JsValue> {
        let changes = self.inner.pending_read_changes().map_err(js_error)?;
        let changes = JsReadChanges {
            reset: changes.reset,
            changes: changes
                .changes
                .into_iter()
                .map(|change| JsReadChange {
                    id: node_to_hex(change.id),
                    before: change.before.map(JsReadRow::from),
                    after: change.after.map(JsReadRow::from),
                })
                .collect(),
        };
        let value = serialize(&changes).map_err(js_error)?;
        self.inner.clear_read_changes();
        Ok(value)
    }

    #[wasm_bindgen(js_name = beginTransaction)]
    pub fn begin_transaction(&mut self) -> Result<(), JsValue> {
        self.begin().map_err(js_error)
    }

    #[wasm_bindgen(js_name = commitTransaction)]
    pub fn commit_transaction(&mut self) -> Result<(), JsValue> {
        self.commit().map_err(js_error)
    }

    #[wasm_bindgen(js_name = rollbackTransaction)]
    pub fn rollback_transaction(&mut self) -> Result<(), JsValue> {
        self.rollback().map_err(js_error)
    }

    #[wasm_bindgen(js_name = localInsert, unchecked_return_type = "import('@treecrdt/interface').Operation")]
    pub fn local_insert(
        &mut self,
        parent: String,
        node: String,
        #[wasm_bindgen(unchecked_param_type = "string | null | undefined")] after: JsValue,
        payload: Option<Vec<u8>>,
    ) -> Result<JsValue, JsValue> {
        self.mutate(true, |inner| {
            let (op, _) = inner
                .local_insert(
                    hex_to_node(&parent)?,
                    hex_to_node(&node)?,
                    placement(after)?,
                    payload,
                )
                .map_err(|error| error.to_string())?;
            serialize(&OperationValue::try_from(op)?)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = localMove, unchecked_return_type = "import('@treecrdt/interface').Operation")]
    pub fn local_move(
        &mut self,
        node: String,
        parent: String,
        #[wasm_bindgen(unchecked_param_type = "string | null | undefined")] after: JsValue,
    ) -> Result<JsValue, JsValue> {
        self.mutate(true, |inner| {
            let (op, _) = inner
                .local_move(
                    hex_to_node(&node)?,
                    hex_to_node(&parent)?,
                    placement(after)?,
                )
                .map_err(|error| error.to_string())?;
            serialize(&OperationValue::try_from(op)?)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = localPayload, unchecked_return_type = "import('@treecrdt/interface').Operation")]
    pub fn local_payload(
        &mut self,
        node: String,
        payload: Option<Vec<u8>>,
    ) -> Result<JsValue, JsValue> {
        self.mutate(true, |inner| {
            let (op, _) = inner
                .local_payload(hex_to_node(&node)?, payload)
                .map_err(|error| error.to_string())?;
            serialize(&OperationValue::try_from(op)?)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = localDelete, unchecked_return_type = "import('@treecrdt/interface').Operation")]
    pub fn local_delete(&mut self, node: String) -> Result<JsValue, JsValue> {
        self.mutate(true, |inner| {
            let (op, _) =
                inner.local_delete(hex_to_node(&node)?).map_err(|error| error.to_string())?;
            serialize(&OperationValue::try_from(op)?)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = operationCount)]
    pub fn operation_count(&self) -> usize {
        self.inner.operation_count()
    }

    #[wasm_bindgen(js_name = revertOperations, unchecked_return_type = "import('@treecrdt/interface').Operation[]")]
    pub fn revert_operations(
        &mut self,
        #[wasm_bindgen(
            unchecked_param_type = "readonly import('@treecrdt/interface').OperationId[]"
        )]
        ids: JsValue,
    ) -> Result<JsValue, JsValue> {
        self.mutate(true, |inner| {
            let values: Vec<OperationIdValue> =
                serde_wasm_bindgen::from_value(ids).map_err(|error| error.to_string())?;
            let ids = values
                .into_iter()
                .map(|value| {
                    if value.counter > 9_007_199_254_740_991 {
                        return Err(
                            "operation counter must be a safe JavaScript integer".to_string()
                        );
                    }
                    Ok(OperationId {
                        replica: ReplicaId::new(value.replica.into_vec()),
                        counter: value.counter,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let operations = inner.revert_operations(&ids).map_err(|error| error.to_string())?;
            let values = operations
                .into_iter()
                .map(OperationValue::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            serialize(&values)
        })
        .map_err(js_error)
    }

    /// Cursors count accepted operations, not Lamport time, so late historical arrivals are not skipped.
    #[wasm_bindgen(js_name = operationsFrom, unchecked_return_type = "import('@treecrdt/interface').Operation[]")]
    pub fn operations_from(
        &self,
        #[wasm_bindgen(unchecked_param_type = "number")] cursor: JsValue,
    ) -> Result<JsValue, JsValue> {
        let cursor: usize = serde_wasm_bindgen::from_value(cursor).map_err(js_error)?;
        let ops = self.inner.operations_from(cursor).map_err(js_error)?;
        let values = ops
            .into_iter()
            .map(OperationValue::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        serialize(&values).map_err(js_error)
    }

    #[wasm_bindgen(js_name = operationsAt, unchecked_return_type = "import('@treecrdt/interface').Operation[]")]
    pub fn operations_at(
        &self,
        #[wasm_bindgen(unchecked_param_type = "readonly number[]")] indices: JsValue,
    ) -> Result<JsValue, JsValue> {
        let indices: Vec<usize> = serde_wasm_bindgen::from_value(indices).map_err(js_error)?;
        let ops = self.inner.operations_at(&indices).map_err(js_error)?;
        let values = ops
            .into_iter()
            .map(OperationValue::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        serialize(&values).map_err(js_error)
    }

    #[wasm_bindgen(js_name = maxLamport, unchecked_return_type = "number")]
    pub fn max_lamport(&self) -> Result<JsValue, JsValue> {
        serialize(&self.inner.lamport()).map_err(js_error)
    }

    #[wasm_bindgen(js_name = appendOp)]
    pub fn append_op(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "import('@treecrdt/interface').Operation")]
        op: JsValue,
    ) -> Result<(), JsValue> {
        self.mutate(false, |inner| {
            let js_op = serde_wasm_bindgen::from_value(op).map_err(|error| error.to_string())?;
            inner.apply_remote(js_to_op(js_op)?).map_err(|error| error.to_string())
        })
        .map_err(js_error)
    }

    /// Decode the complete batch before ingestion. Atomic only inside an explicit transaction.
    #[wasm_bindgen(js_name = appendOps)]
    pub fn append_ops(
        &mut self,
        #[wasm_bindgen(
            unchecked_param_type = "readonly import('@treecrdt/interface').Operation[]"
        )]
        operations: JsValue,
    ) -> Result<(), JsValue> {
        self.mutate(false, |inner| {
            let js_ops: Vec<OperationInput> =
                serde_wasm_bindgen::from_value(operations).map_err(|error| error.to_string())?;
            let ops = js_ops.into_iter().map(js_to_op).collect::<Result<Vec<_>, _>>()?;
            inner.apply_remote_batch(ops).map_err(|error| error.to_string())
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = appendOpWithDelta)]
    pub fn append_op_with_delta(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "import('@treecrdt/interface').Operation")]
        op: JsValue,
    ) -> Result<JsValue, JsValue> {
        self.mutate(false, |inner| {
            let js_op = serde_wasm_bindgen::from_value(op).map_err(|error| error.to_string())?;
            let delta = inner
                .apply_remote_with_delta(js_to_op(js_op)?)
                .map_err(|error| error.to_string())?;
            let affected: Vec<String> = delta
                .map(|d| {
                    MaterializationOutcome {
                        head_seq: 0,
                        changes: d.changes,
                    }
                    .affected_nodes()
                    .into_iter()
                    .map(node_to_hex)
                    .collect()
                })
                .unwrap_or_default();
            serialize(&affected)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = opsSince)]
    pub fn ops_since(&self, lamport: u64) -> Result<JsValue, JsValue> {
        let ops = self
            .inner
            .operations_since(lamport)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        let mapped: Vec<JsOp> = ops
            .iter()
            .map(op_to_js)
            .collect::<Result<_, _>>()
            .map_err(|e| JsValue::from_str(&e))?;
        to_value(&mapped).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = subtreeKnownState)]
    pub fn subtree_known_state(&self, node_hex: String) -> Result<Vec<u8>, JsValue> {
        let node = hex_to_node(&node_hex).map_err(|e| JsValue::from_str(&e))?;
        let vv = self
            .inner
            .subtree_version_vector(node)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        vv.encode().map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = treeChildren)]
    pub fn tree_children(&self, parent_hex: String) -> Result<JsValue, JsValue> {
        let parent = hex_to_node(&parent_hex).map_err(|e| JsValue::from_str(&e))?;
        let children = self
            .inner
            .children(parent)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        let mapped: Vec<String> = children.into_iter().map(node_to_hex).collect();
        to_value(&mapped).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = treeNodeCount)]
    pub fn tree_node_count(&self) -> Result<u32, JsValue> {
        self.inner
            .nodes()
            .map(|pairs| pairs.len() as u32)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    #[wasm_bindgen(js_name = treeParent)]
    pub fn tree_parent(&self, node_hex: String) -> Result<JsValue, JsValue> {
        let node = hex_to_node(&node_hex).map_err(|e| JsValue::from_str(&e))?;
        let parent = self.inner.parent(node).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        match parent {
            None => Ok(JsValue::NULL),
            Some(p) => {
                let hex = node_to_hex(p);
                to_value(&hex).map_err(|e| JsValue::from_str(&e.to_string()))
            }
        }
    }

    #[wasm_bindgen(js_name = treeExists)]
    pub fn tree_exists(&self, node_hex: String) -> Result<bool, JsValue> {
        let node = hex_to_node(&node_hex).map_err(|e| JsValue::from_str(&e))?;
        let known =
            self.inner.is_known(node).map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        if !known {
            return Ok(false);
        }
        let tombstoned = self
            .inner
            .is_tombstoned(node)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        Ok(!tombstoned)
    }

    #[wasm_bindgen(js_name = treePayload)]
    pub fn tree_payload(&self, node_hex: String) -> Result<Option<Vec<u8>>, JsValue> {
        let node = hex_to_node(&node_hex).map_err(|e| JsValue::from_str(&e))?;
        self.inner.payload(node).map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    #[wasm_bindgen(js_name = treeDump)]
    pub fn tree_dump(&self) -> Result<JsValue, JsValue> {
        #[derive(Serialize)]
        struct DumpRow {
            node: Vec<u8>,
            parent: Option<Vec<u8>>,
            pos: Option<u64>,
            tombstone: bool,
        }

        let nodes =
            self.inner.export_nodes().map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        use std::collections::HashMap;
        let mut parent_pos: HashMap<NodeId, (NodeId, u64)> = HashMap::new();
        for n in &nodes {
            for (pos, child) in n.children.iter().enumerate() {
                parent_pos.insert(*child, (n.node, pos as u64));
            }
        }

        let mut rows: Vec<DumpRow> = Vec::with_capacity(nodes.len());
        for n in &nodes {
            let tombstone = self
                .inner
                .is_tombstoned(n.node)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
            let (parent, pos) = if n.node == NodeId::ROOT {
                (None, Some(0u64))
            } else if let Some((p, ppos)) = parent_pos.get(&n.node) {
                (Some(p.0.to_be_bytes().to_vec()), Some(*ppos))
            } else {
                (n.parent.map(|p| p.0.to_be_bytes().to_vec()), None)
            };

            rows.push(DumpRow {
                node: n.node.0.to_be_bytes().to_vec(),
                parent,
                pos,
                tombstone,
            });
        }

        to_value(&rows).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_failures_poison_explicit_transactions_and_restore_reads_on_rollback() {
        let mut tree = WasmTree::new("01".into());
        tree.enable_read_tracking();
        tree.inner.drain_read_changes().unwrap();
        tree.inner
            .local_insert(NodeId::ROOT, NodeId(1), LocalPlacement::Last, None)
            .unwrap();
        let before = tree.inner.read_node(NodeId(1)).unwrap();
        let pending = tree.inner.pending_read_changes().unwrap();
        tree.begin().unwrap();
        assert!(tree.begin().is_err());
        tree.inner
            .local_insert(NodeId(1), NodeId(2), LocalPlacement::Last, None)
            .unwrap();
        tree.inner.drain_read_changes().unwrap();
        assert!(tree
            .mutate(false, |inner| {
                inner
                    .local_move(NodeId(2), NodeId::ROOT, LocalPlacement::After(NodeId(99)))
                    .map_err(|error| error.to_string())
            })
            .is_err());
        assert!(tree.commit().is_err());
        assert!(tree.mutate(false, |_| Ok(())).is_err());
        tree.rollback().unwrap();
        assert_eq!(tree.inner.read_node(NodeId(1)).unwrap(), before);
        assert_eq!(tree.inner.read_node(NodeId(2)).unwrap(), None);
        assert_eq!(tree.inner.pending_read_changes().unwrap(), pending);
        assert_eq!(tree.operation_count(), 1);
        let failure: Result<(), String> = tree.mutate(true, |inner| {
            inner.local_delete(NodeId(1)).unwrap();
            Err("injected serialization failure after native mutation".into())
        });
        assert!(failure.is_err());
        assert_eq!(tree.inner.read_node(NodeId(1)).unwrap(), before);
        assert_eq!(tree.inner.pending_read_changes().unwrap(), pending);
        let (next, _) = tree
            .inner
            .local_insert(NodeId::ROOT, NodeId(2), LocalPlacement::Last, None)
            .unwrap();
        assert_eq!((next.meta.id.counter, next.meta.lamport), (2, 2));
        tree.begin().unwrap();
        tree.commit().unwrap();
    }
}
