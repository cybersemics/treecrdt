#![forbid(unsafe_code)]
//! WASM-friendly bridge for TreeCRDT.
//! Exposes a small wasm-bindgen surface that matches the JS adapter needs.

use serde::{Deserialize, Serialize};
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
    position: Option<usize>,
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    let clean = hex.trim_start_matches("0x");
    if clean.len() % 2 != 0 {
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
    let (kind, parent, node, new_parent, position) = match &op.kind {
        OperationKind::Insert { parent, node, position } => (
            "insert",
            Some(*parent),
            *node,
            None,
            Some(*position),
        ),
        OperationKind::Move {
            node,
            new_parent,
            position,
        } => ("move", None, *node, Some(*new_parent), Some(*position)),
        OperationKind::Delete { node } => ("delete", None, *node, None, None),
        OperationKind::Tombstone { node } => ("tombstone", None, *node, None, None),
    };
    JsOp {
        replica: bytes_to_hex(&op.meta.id.replica.0),
        counter: op.meta.id.counter,
        lamport: op.meta.lamport,
        kind: kind.to_string(),
        parent: parent.map(node_to_hex),
        node: node_to_hex(node),
        new_parent: new_parent.map(node_to_hex),
        position,
    }
}

fn js_to_op(js: JsOp) -> Result<Operation, String> {
    let replica_bytes = hex_to_bytes(&js.replica)?;
    let replica = ReplicaId::new(replica_bytes);
    let counter = js.counter;
    let lamport = js.lamport;

    let op = match js.kind.as_str() {
        "insert" => Operation::insert(
            &replica,
            counter,
            lamport,
            js.parent
                .as_deref()
                .map(hex_to_node)
                .transpose()?
                .unwrap_or(NodeId::ROOT),
            hex_to_node(&js.node)?,
            js.position.unwrap_or(0),
        ),
        "move" => Operation::move_node(
            &replica,
            counter,
            lamport,
            hex_to_node(&js.node)?,
            js.new_parent
                .as_deref()
                .map(hex_to_node)
                .transpose()?
                .unwrap_or(NodeId::ROOT),
            js.position.unwrap_or(0),
        ),
        "delete" => Operation::delete(&replica, counter, lamport, hex_to_node(&js.node)?),
        "tombstone" => Operation::tombstone(&replica, counter, lamport, hex_to_node(&js.node)?),
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
            inner: TreeCrdt::new(
                replica,
                MemoryStorage::default(),
                LamportClock::default(),
            ),
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
        JsValue::from_serde(&mapped).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}
