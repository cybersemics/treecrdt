#![forbid(unsafe_code)]
//! WASM-friendly bridge for TreeCRDT.
//! Exposes a small wasm-bindgen surface that matches the JS adapter needs.

mod version_vector;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_wasm_bindgen::to_value;
use treecrdt_core::{
    Lamport, LamportClock, MaterializationOutcome, MemoryStorage, NodeId, Operation, OperationId,
    OperationKind, OperationMetadata, ReplicaId, TreeCrdt, VersionVector,
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

#[derive(Deserialize)]
struct OperationInput {
    meta: MetadataInput,
    kind: KindInput,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetadataInput {
    id: OperationIdInput,
    lamport: Lamport,
    known_state: Option<ByteBuf>,
}

#[derive(Deserialize)]
struct OperationIdInput {
    replica: ByteBuf,
    counter: u64,
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

#[wasm_bindgen]
pub struct WasmTree {
    inner: TreeCrdt<MemoryStorage, LamportClock>,
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
        }
    }

    #[wasm_bindgen(js_name = appendOp)]
    pub fn append_op(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "import('@treecrdt/interface').Operation")]
        op: JsValue,
    ) -> Result<(), JsValue> {
        let js_op =
            serde_wasm_bindgen::from_value(op).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let op = js_to_op(js_op).map_err(|e| JsValue::from_str(&e))?;
        self.inner.apply_remote(op).map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    /// Decode the complete batch before ingestion. Applying it is not an atomic transaction.
    #[wasm_bindgen(js_name = appendOps)]
    pub fn append_ops(
        &mut self,
        #[wasm_bindgen(
            unchecked_param_type = "readonly import('@treecrdt/interface').Operation[]"
        )]
        operations: JsValue,
    ) -> Result<(), JsValue> {
        let js_ops: Vec<OperationInput> = serde_wasm_bindgen::from_value(operations)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let ops = js_ops
            .into_iter()
            .map(js_to_op)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| JsValue::from_str(&e))?;
        self.inner
            .apply_remote_batch(ops)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    #[wasm_bindgen(js_name = appendOpWithDelta)]
    pub fn append_op_with_delta(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "import('@treecrdt/interface').Operation")]
        op: JsValue,
    ) -> Result<JsValue, JsValue> {
        let js_op =
            serde_wasm_bindgen::from_value(op).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let op = js_to_op(js_op).map_err(|e| JsValue::from_str(&e))?;
        let delta = self
            .inner
            .apply_remote_with_delta(op)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
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
        to_value(&affected).map_err(|e| JsValue::from_str(&e.to_string()))
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
