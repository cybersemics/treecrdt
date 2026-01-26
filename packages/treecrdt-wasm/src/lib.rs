#![forbid(unsafe_code)]
//! WASM-friendly bridge for TreeCRDT.
//! Exposes a small wasm-bindgen surface that matches the JS adapter needs.

use serde::{Deserialize, Serialize};
use serde_wasm_bindgen::to_value;
use treecrdt_core::{
    Lamport, LamportClock, MemoryStorage, NodeId, Operation, OperationKind, ReplicaId, TreeCrdt,
};
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize)]
struct JsOp {
    replica: String, // hex
    counter: u64,
    lamport: Lamport,
    kind: String,
    parent: Option<String>,
    node: String,
    new_parent: Option<String>,
    order_key: Option<String>, // hex
    #[serde(default)]
    known_state: Option<Vec<u8>>,
    payload: Option<String>, // hex
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    let clean = hex.trim_start_matches("0x");
    if !clean.len().is_multiple_of(2) {
        return Err("hex length must be even".into());
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

fn op_to_js(op: &Operation) -> JsOp {
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
    let known_state = op.meta.known_state.as_ref().and_then(|vv| serde_json::to_vec(vv).ok());
    JsOp {
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
    }
}

fn js_to_op(js: JsOp) -> Result<Operation, String> {
    let replica_bytes = hex_to_bytes(&js.replica)?;
    let replica = ReplicaId::new(replica_bytes);
    let counter = js.counter;
    let lamport = js.lamport;

    let op = match js.kind.as_str() {
        "insert" => {
            let parent = js.parent.as_deref().map(hex_to_node).transpose()?.unwrap_or(NodeId::ROOT);
            let node = hex_to_node(&js.node)?;
            let order_key =
                js.order_key.as_deref().map(hex_to_bytes).transpose()?.unwrap_or_default();
            if let Some(payload_hex) = js.payload.as_deref() {
                let payload = hex_to_bytes(payload_hex)?;
                Operation::insert_with_payload(
                    &replica, counter, lamport, parent, node, order_key, payload,
                )
            } else {
                Operation::insert(&replica, counter, lamport, parent, node, order_key)
            }
        }
        "move" => {
            let order_key =
                js.order_key.as_deref().map(hex_to_bytes).transpose()?.unwrap_or_default();
            Operation::move_node(
                &replica,
                counter,
                lamport,
                hex_to_node(&js.node)?,
                js.new_parent.as_deref().map(hex_to_node).transpose()?.unwrap_or(NodeId::ROOT),
                order_key,
            )
        }
        "delete" => {
            let Some(bytes) = js.known_state else {
                return Err("delete op missing known_state".into());
            };
            if bytes.is_empty() {
                return Err("delete known_state must not be empty".into());
            }
            let vv = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
            Operation::delete(&replica, counter, lamport, hex_to_node(&js.node)?, Some(vv))
        }
        "tombstone" => Operation::tombstone(&replica, counter, lamport, hex_to_node(&js.node)?),
        "payload" => Operation::payload(
            &replica,
            counter,
            lamport,
            hex_to_node(&js.node)?,
            js.payload.as_deref().map(hex_to_bytes).transpose()?,
        ),
        _ => return Err("unknown kind".into()),
    };
    Ok(op)
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
    pub fn append_op(&mut self, op_json: String) -> Result<(), JsValue> {
        let js_op: JsOp =
            serde_json::from_str(&op_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let op = js_to_op(js_op).map_err(|e| JsValue::from_str(&e))?;
        self.inner.apply_remote(op).map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    #[wasm_bindgen(js_name = opsSince)]
    pub fn ops_since(&self, lamport: u64) -> Result<JsValue, JsValue> {
        let ops = self
            .inner
            .operations_since(lamport)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        let mapped: Vec<JsOp> = ops.iter().map(op_to_js).collect();
        to_value(&mapped).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = subtreeKnownState)]
    pub fn subtree_known_state(&self, node_hex: String) -> Result<Vec<u8>, JsValue> {
        let node = hex_to_node(&node_hex).map_err(|e| JsValue::from_str(&e))?;
        let vv = self
            .inner
            .subtree_version_vector(node)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        serde_json::to_vec(&vv).map_err(|e| JsValue::from_str(&e.to_string()))
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
