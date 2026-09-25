#![forbid(unsafe_code)]
//! WASM-friendly bridge for TreeCRDT.
//! Exposes a small wasm-bindgen surface that matches the JS adapter needs.

use serde::Serialize;
use serde_wasm_bindgen::to_value;
use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryCheckpoint, MemoryStorage, NodeId, Operation, ReplicaId,
    TreeCrdt,
};
use wasm_bindgen::prelude::*;

mod wire;
use wire::TypedOperation;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsSnapshotRow {
    id: String,
    parent_id: Option<String>,
    #[serde(serialize_with = "wire::serialize_optional_bytes")]
    payload: Option<Vec<u8>>,
    children: Vec<String>,
}

#[wasm_bindgen(typescript_custom_section)]
const SNAPSHOT_TYPES: &str = r#"
/** A live node, including payload-less structural nodes and the known root. Children retain canonical order. */
export interface TreeSnapshotRow {
    id: string;
    parentId: string | null;
    payload: Uint8Array | null;
    children: string[];
}
/** Conservative final-row invalidations; reset replaces the complete snapshot on initialization or historical replay. */
export interface TreeSnapshotChanges {
    reset: boolean;
    rows: TreeSnapshotRow[];
    removed: string[];
}
/** Matches @treecrdt/interface Operation without JSON or hexadecimal binary fields. */
export interface TreeOperation {
    meta: { id: { replica: Uint8Array; counter: number }; lamport: number; knownState?: Uint8Array };
    kind:
        | { type: 'insert'; parent: string; node: string; orderKey: Uint8Array; payload?: Uint8Array }
        | { type: 'move'; node: string; newParent: string; orderKey: Uint8Array }
        | { type: 'delete'; node: string }
        | { type: 'tombstone'; node: string }
        | { type: 'payload'; node: string; payload: Uint8Array | null };
}
"#;

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    let clean = hex.trim_start_matches("0x");
    if !clean.is_ascii() || !clean.len().is_multiple_of(2) {
        return Err("hex must be ASCII with an even length".into());
    }
    (0..clean.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&clean[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
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

#[wasm_bindgen]
pub struct WasmTree {
    inner: TreeCrdt<MemoryStorage, LamportClock>,
    checkpoint: Option<MemoryCheckpoint>,
    transaction_failed: bool,
}

#[derive(Debug, PartialEq, Serialize)]
struct JsSnapshotChanges {
    reset: bool,
    rows: Vec<JsSnapshotRow>,
    removed: Vec<String>,
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn serialize(value: &impl Serialize) -> Result<JsValue, String> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true))
        .map_err(|error| error.to_string())
}

fn serialize_operation(op: Operation) -> Result<JsValue, String> {
    serialize(&TypedOperation::from_op(op)?)
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

    /// Standalone mutations are atomic too. Explicit transactions remain poisoned until rollback.
    fn mutate<T>(
        &mut self,
        work: impl FnOnce(&mut TreeCrdt<MemoryStorage, LamportClock>) -> Result<T, String>,
    ) -> Result<T, String> {
        if self.transaction_failed {
            return Err("the memory transaction failed; roll it back before continuing".into());
        }
        let standalone = self.checkpoint.is_none().then(|| self.inner.memory_checkpoint());
        match work(&mut self.inner) {
            Ok(value) => Ok(value),
            Err(error) => {
                if let Some(checkpoint) = standalone {
                    self.inner.rollback_memory(checkpoint).map_err(|rollback| {
                        format!("{error}; memory rollback failed: {rollback}")
                    })?;
                } else {
                    self.transaction_failed = true;
                }
                Err(error)
            }
        }
    }

    fn snapshot_row(&self, node: NodeId) -> treecrdt_core::Result<Option<JsSnapshotRow>> {
        if !self.inner.is_known(node)? || self.inner.is_tombstoned(node)? {
            return Ok(None);
        }
        Ok(Some(JsSnapshotRow {
            id: node_to_hex(node),
            parent_id: self
                .inner
                .parent(node)?
                .filter(|parent| *parent != NodeId::TRASH)
                .map(node_to_hex),
            payload: self.inner.payload(node)?,
            children: self.inner.children(node)?.into_iter().map(node_to_hex).collect(),
        }))
    }

    fn snapshot_changes(&self) -> treecrdt_core::Result<JsSnapshotChanges> {
        let changes = self.inner.pending_snapshot_changes();
        let mut rows = Vec::new();
        let mut removed = Vec::new();
        if changes.reset {
            rows = self.snapshot_rows()?;
        } else {
            for node in changes.nodes {
                match self.snapshot_row(node)? {
                    Some(row) => rows.push(row),
                    None => removed.push(node_to_hex(node)),
                }
            }
        }
        Ok(JsSnapshotChanges {
            reset: changes.reset,
            rows,
            removed,
        })
    }

    fn snapshot_rows(&self) -> treecrdt_core::Result<Vec<JsSnapshotRow>> {
        let mut nodes = self.inner.nodes()?;
        // `nodes()` excludes the reserved IDs; include them when known, like treeExists.
        for node in [NodeId::ROOT, NodeId::TRASH] {
            if self.inner.is_known(node)? {
                nodes.push((node, self.inner.parent(node)?));
            }
        }
        nodes.sort_by_key(|(node, _)| *node);
        let mut rows = Vec::with_capacity(nodes.len());
        for (node, _) in nodes {
            if let Some(row) = self.snapshot_row(node)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }
}

#[wasm_bindgen]
impl WasmTree {
    #[wasm_bindgen(constructor)]
    pub fn new(replica_hex: String) -> WasmTree {
        let replica_bytes = hex_to_bytes(&replica_hex).unwrap_or_else(|_| b"wasm".to_vec());
        let replica = ReplicaId::new(replica_bytes);
        let mut inner =
            TreeCrdt::new(replica, MemoryStorage::default(), LamportClock::default()).unwrap();
        inner.track_snapshot_changes();
        WasmTree {
            inner,
            checkpoint: None,
            transaction_failed: false,
        }
    }

    #[wasm_bindgen(js_name = localInsert, unchecked_return_type = "TreeOperation")]
    pub fn local_insert(
        &mut self,
        parent: String,
        node: String,
        after: Option<String>,
        payload: Option<Vec<u8>>,
    ) -> Result<JsValue, JsValue> {
        self.mutate(|inner| {
            let placement = after
                .as_deref()
                .map(hex_to_node)
                .transpose()?
                .map(LocalPlacement::After)
                .unwrap_or(LocalPlacement::First);
            let (op, _) = inner
                .local_insert(
                    hex_to_node(&parent)?,
                    hex_to_node(&node)?,
                    placement,
                    payload,
                )
                .map_err(|error| error.to_string())?;
            serialize_operation(op)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = localMove, unchecked_return_type = "TreeOperation")]
    pub fn local_move(
        &mut self,
        node: String,
        parent: String,
        after: Option<String>,
    ) -> Result<JsValue, JsValue> {
        self.mutate(|inner| {
            let placement = after
                .as_deref()
                .map(hex_to_node)
                .transpose()?
                .map(LocalPlacement::After)
                .unwrap_or(LocalPlacement::First);
            let (op, _) = inner
                .local_move(hex_to_node(&node)?, hex_to_node(&parent)?, placement)
                .map_err(|error| error.to_string())?;
            serialize_operation(op)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = localPayload, unchecked_return_type = "TreeOperation")]
    pub fn local_payload(
        &mut self,
        node: String,
        payload: Option<Vec<u8>>,
    ) -> Result<JsValue, JsValue> {
        self.mutate(|inner| {
            let (op, _) = inner
                .local_payload(hex_to_node(&node)?, payload)
                .map_err(|error| error.to_string())?;
            serialize_operation(op)
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = localDelete, unchecked_return_type = "TreeOperation")]
    pub fn local_delete(&mut self, node: String) -> Result<JsValue, JsValue> {
        self.mutate(|inner| {
            let (op, _) =
                inner.local_delete(hex_to_node(&node)?).map_err(|error| error.to_string())?;
            serialize_operation(op)
        })
        .map_err(js_error)
    }

    /// Explicit transactions may span local writes and synchronous reads. Never hold a Rust borrow across JS callbacks.
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

    /// Decodes the complete batch before applying it. Standalone calls are atomic.
    #[wasm_bindgen(js_name = appendOperations)]
    pub fn append_operations(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "readonly TreeOperation[]")] operations: JsValue,
    ) -> Result<(), JsValue> {
        self.mutate(|inner| {
            let values: Vec<TypedOperation> =
                serde_wasm_bindgen::from_value(operations).map_err(|error| error.to_string())?;
            let ops =
                values.into_iter().map(TypedOperation::into_op).collect::<Result<Vec<_>, _>>()?;
            inner.apply_remote_batch(ops).map_err(|error| error.to_string())
        })
        .map_err(js_error)
    }

    #[wasm_bindgen(js_name = operationCount)]
    pub fn operation_count(&self) -> usize {
        self.inner.operation_count()
    }

    /// Cursor is an accepted-operation index, not a Lamport timestamp; historical arrivals are never skipped.
    #[wasm_bindgen(js_name = operationsFrom, unchecked_return_type = "TreeOperation[]")]
    pub fn operations_from(&self, cursor: usize) -> Result<JsValue, JsValue> {
        let ops = self.inner.operations_from(cursor).map_err(js_error)?;
        let values = ops
            .into_iter()
            .map(TypedOperation::from_op)
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        serialize(&values).map_err(js_error)
    }

    #[wasm_bindgen(js_name = operationsAt, unchecked_return_type = "TreeOperation[]")]
    pub fn operations_at(
        &self,
        #[wasm_bindgen(unchecked_param_type = "readonly number[]")] indices: JsValue,
    ) -> Result<JsValue, JsValue> {
        let indices: Vec<usize> = serde_wasm_bindgen::from_value(indices).map_err(js_error)?;
        let ops = self.inner.operations_at(&indices).map_err(js_error)?;
        let values = ops
            .into_iter()
            .map(TypedOperation::from_op)
            .collect::<Result<Vec<_>, _>>()
            .map_err(js_error)?;
        serialize(&values).map_err(js_error)
    }

    #[wasm_bindgen(js_name = maxLamport, unchecked_return_type = "number")]
    pub fn max_lamport(&self) -> Result<JsValue, JsValue> {
        serialize(&self.inner.lamport()).map_err(js_error)
    }

    /// Drains conservative final-row invalidations. Reads inside a transaction are provisional until commit.
    #[wasm_bindgen(js_name = drainSnapshotChanges, unchecked_return_type = "TreeSnapshotChanges")]
    pub fn drain_snapshot_changes(&mut self) -> Result<JsValue, JsValue> {
        let changes = self.snapshot_changes().map_err(js_error)?;
        let value = serialize(&changes).map_err(js_error)?;
        self.inner.drain_snapshot_changes();
        Ok(value)
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

    /// Return all live nodes in ID order, including the known root and payload-less structural nodes.
    /// Child IDs use exactly the same visibility and sibling order as `treeChildren`.
    #[wasm_bindgen(js_name = treeSnapshot, unchecked_return_type = "TreeSnapshotRow[]")]
    pub fn tree_snapshot(&self) -> Result<JsValue, JsValue> {
        let rows = self.snapshot_rows().map_err(|e| JsValue::from_str(&e.to_string()))?;
        rows.serialize(&serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true))
            .map_err(|e| JsValue::from_str(&e.to_string()))
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
    use std::collections::BTreeMap;

    fn insert(tree: &mut WasmTree, parent: NodeId, node: NodeId) -> Operation {
        tree.mutate(|inner| {
            inner
                .local_insert(
                    parent,
                    node,
                    LocalPlacement::First,
                    Some(b"payload".to_vec()),
                )
                .map(|(op, _)| op)
                .map_err(|error| error.to_string())
        })
        .unwrap()
    }

    #[test]
    fn local_mutations_author_queryable_canonical_operations() {
        let mut tree = WasmTree::new("01".into());
        let parent = NodeId(1);
        let child = NodeId(2);
        insert(&mut tree, NodeId::ROOT, parent);
        insert(&mut tree, parent, child);
        tree.inner.local_payload(child, Some(b"updated".to_vec())).unwrap();
        assert_eq!(tree.inner.parent(child).unwrap(), Some(parent));
        assert_eq!(
            tree.inner.payload(child).unwrap(),
            Some(b"updated".to_vec())
        );

        tree.inner.local_move(child, NodeId::ROOT, LocalPlacement::First).unwrap();
        assert_eq!(tree.inner.parent(child).unwrap(), Some(NodeId::ROOT));
        let (deleted, _) = tree.inner.local_delete(child).unwrap();
        assert!(deleted.meta.known_state.is_some());
        assert!(tree.inner.is_tombstoned(child).unwrap());
        assert_eq!(tree.inner.children(NodeId::ROOT).unwrap(), vec![parent]);
        assert_eq!(
            tree.inner
                .operations_since(0)
                .unwrap()
                .iter()
                .map(|op| op.meta.id.counter)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn bulk_historical_append_preserves_canonical_state() {
        let mut source = WasmTree::new("01".into());
        let mut receiver = WasmTree::new("02".into());
        let parent = NodeId(1);
        let child = NodeId(2);
        insert(&mut source, NodeId::ROOT, parent);
        insert(&mut source, parent, child);
        source.inner.local_payload(child, Some(b"updated".to_vec())).unwrap();
        source.inner.local_move(child, NodeId::ROOT, LocalPlacement::First).unwrap();
        source.inner.local_payload(NodeId::ROOT, Some(b"newest".to_vec())).unwrap();
        let ops = source.inner.operations_since(0).unwrap();
        receiver.inner.apply_remote(ops[4].clone()).unwrap();

        let batch: Vec<_> = [3, 1, 0, 2, 1].map(|index| ops[index].clone()).into();
        receiver.inner.apply_remote_batch(batch).unwrap();
        assert_eq!(receiver.inner.operations_since(0).unwrap().len(), ops.len());
        for node in [NodeId::ROOT, parent, child] {
            assert_eq!(
                receiver.inner.parent(node).unwrap(),
                source.inner.parent(node).unwrap()
            );
            assert_eq!(
                receiver.inner.children(node).unwrap(),
                source.inner.children(node).unwrap()
            );
            assert_eq!(
                receiver.inner.payload(node).unwrap(),
                source.inner.payload(node).unwrap()
            );
            assert_eq!(
                receiver.inner.subtree_version_vector(node).unwrap(),
                source.inner.subtree_version_vector(node).unwrap()
            );
        }
        let (local, _) = receiver.inner.local_payload(child, Some(b"local".to_vec())).unwrap();
        assert_eq!(local.meta.id.counter, 1);
        assert_eq!(local.meta.lamport, ops[4].meta.lamport + 1);
    }

    #[test]
    fn snapshot_preserves_root_payload_child_order_and_payloadless_rank_gaps() {
        let mut tree = WasmTree::new("01".into());
        tree.inner.local_payload(NodeId::ROOT, Some(b"root".to_vec())).unwrap();
        insert(&mut tree, NodeId::ROOT, NodeId(3));
        tree.inner
            .local_insert(
                NodeId::ROOT,
                NodeId(1),
                LocalPlacement::Last,
                Some(b"last".to_vec()),
            )
            .unwrap();
        tree.inner
            .local_insert(
                NodeId::ROOT,
                NodeId(2),
                LocalPlacement::After(NodeId(3)),
                None,
            )
            .unwrap();
        // A payload-only ghost remains readable despite having no parent, including empty payloads.
        tree.inner.local_payload(NodeId(4), Some(Vec::new())).unwrap();
        insert(&mut tree, NodeId(10), NodeId(11));

        let rows = tree.snapshot_rows().unwrap();
        assert_eq!(
            rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 10, 11].map(|id| node_to_hex(NodeId(id)))
        );
        assert_eq!(rows[0].parent_id, None);
        assert_eq!(rows[0].payload.as_deref(), Some(b"root".as_slice()));
        assert_eq!(
            rows[0].children,
            [3, 2, 1].map(|id| node_to_hex(NodeId(id)))
        );
        assert_eq!(rows[2].payload, None);
        assert_eq!(rows[4].parent_id, None);
        assert_eq!(rows[4].payload, Some(Vec::new()));
        assert_eq!(rows[5].children, vec![node_to_hex(NodeId(11))]);
        assert_eq!(rows[6].parent_id, Some(node_to_hex(NodeId(10))));
    }

    #[test]
    fn snapshot_uses_defensive_visibility_after_delete_restore_and_historical_replay() {
        let mut source = WasmTree::new("01".into());
        source.inner.local_payload(NodeId::ROOT, Some(b"root".to_vec())).unwrap();
        insert(&mut source, NodeId::ROOT, NodeId(1));
        insert(&mut source, NodeId(1), NodeId(2));
        source.inner.local_delete(NodeId(1)).unwrap();

        let mut receiver = WasmTree::new("02".into());
        receiver
            .inner
            .apply_remote_batch(source.inner.operations_since(0).unwrap())
            .unwrap();
        let deleted = receiver.snapshot_rows().unwrap();
        assert_eq!(deleted, source.snapshot_rows().unwrap());
        assert_eq!(deleted.len(), 2);
        assert!(deleted[0].children.is_empty());
        // Deleting a parent does not tombstone its descendants individually.
        assert_eq!(deleted[1].id, node_to_hex(NodeId(2)));
        assert_eq!(deleted[1].parent_id, Some(node_to_hex(NodeId(1))));

        let (restore, _) =
            source.inner.local_payload(NodeId(2), Some(b"restored".to_vec())).unwrap();
        receiver.inner.apply_remote(restore.clone()).unwrap();
        let restored = receiver.snapshot_rows().unwrap();
        assert_eq!(restored, source.snapshot_rows().unwrap());
        assert_eq!(restored[0].children, vec![node_to_hex(NodeId(1))]);
        assert_eq!(restored[1].children, vec![node_to_hex(NodeId(2))]);
        assert_eq!(restored[2].payload.as_deref(), Some(b"restored".as_slice()));

        let mut replayed = WasmTree::new("03".into());
        replayed.inner.apply_remote(restore).unwrap();
        replayed
            .inner
            .apply_remote_batch(source.inner.operations_since(0).unwrap())
            .unwrap();
        assert_eq!(replayed.snapshot_rows().unwrap(), restored);
    }

    fn apply_snapshot_changes(
        tree: &mut WasmTree,
        view: &mut BTreeMap<String, JsSnapshotRow>,
    ) -> bool {
        let changes = tree.snapshot_changes().unwrap();
        if changes.reset {
            view.clear();
        }
        for id in changes.removed {
            view.remove(&id);
        }
        for row in changes.rows {
            view.insert(row.id.clone(), row);
        }
        assert_eq!(
            *view,
            tree.snapshot_rows()
                .unwrap()
                .into_iter()
                .map(|row| (row.id.clone(), row))
                .collect(),
        );
        tree.inner.drain_snapshot_changes();
        changes.reset
    }

    #[test]
    fn incremental_snapshots_match_full_view_for_defensive_visibility_and_payloadless_nodes() {
        let mut tree = WasmTree::new("01".into());
        let mut view = BTreeMap::new();
        assert!(apply_snapshot_changes(&mut tree, &mut view));
        tree.inner.local_payload(NodeId::ROOT, Some(b"root".to_vec())).unwrap();
        insert(&mut tree, NodeId::ROOT, NodeId(1));
        insert(&mut tree, NodeId(1), NodeId(2));
        insert(&mut tree, NodeId(2), NodeId(3));
        assert!(!apply_snapshot_changes(&mut tree, &mut view));
        tree.inner.local_delete(NodeId(2)).unwrap();
        assert!(!apply_snapshot_changes(&mut tree, &mut view));
        // Descendants are still visible: deletion is defensive per node, not recursive eviction.
        assert!(view.contains_key(&node_to_hex(NodeId(3))));
        tree.inner.local_delete(NodeId(1)).unwrap();
        apply_snapshot_changes(&mut tree, &mut view);
        tree.inner
            .local_payload(NodeId(3), Some(b"restore ancestors".to_vec()))
            .unwrap();
        assert!(!apply_snapshot_changes(&mut tree, &mut view));
        assert!(view.contains_key(&node_to_hex(NodeId(1))));
        assert!(view.contains_key(&node_to_hex(NodeId(2))));
        tree.inner.local_payload(NodeId(2), None).unwrap();
        apply_snapshot_changes(&mut tree, &mut view);
        assert_eq!(view[&node_to_hex(NodeId(2))].payload, None);
        assert_eq!(
            view[&node_to_hex(NodeId(1))].children,
            vec![node_to_hex(NodeId(2))]
        );
        tree.inner.local_payload(NodeId(2), Some(Vec::new())).unwrap();
        apply_snapshot_changes(&mut tree, &mut view);
        tree.inner.local_move(NodeId(3), NodeId::ROOT, LocalPlacement::Last).unwrap();
        apply_snapshot_changes(&mut tree, &mut view);
        assert!(tree.snapshot_changes().unwrap().rows.is_empty());
    }

    #[test]
    fn snapshot_differential_covers_forward_batches_duplicates_ignored_cycles_and_replay() {
        let mut source = WasmTree::new("01".into());
        let mut forward = WasmTree::new("02".into());
        let mut source_view = BTreeMap::new();
        let mut forward_view = BTreeMap::new();
        assert!(apply_snapshot_changes(&mut source, &mut source_view));
        assert!(apply_snapshot_changes(&mut forward, &mut forward_view));
        let mut seed = 17u64;
        for i in 0..120 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let node = NodeId(1 + ((seed >> 32) % 12) as u128);
            let parent = NodeId(((seed >> 48) % 13) as u128);
            let (op, _) = match i % 5 {
                0 => source.inner.local_insert(parent, node, LocalPlacement::First, Some(vec![i])),
                1 => source.inner.local_move(node, parent, LocalPlacement::Last),
                2 => source.inner.local_payload(node, Some(vec![i, 0, 255])),
                3 => source.inner.local_delete(node),
                _ => source.inner.local_payload(node, None),
            }
            .unwrap();
            assert!(!apply_snapshot_changes(&mut source, &mut source_view));
            forward.inner.apply_remote_batch(vec![op.clone(), op]).unwrap();
            assert!(!apply_snapshot_changes(&mut forward, &mut forward_view));
            assert_eq!(source_view, forward_view);
        }

        let mut replayed = WasmTree::new("03".into());
        let mut replayed_view = BTreeMap::new();
        let mut resets = 0;
        let history = source.inner.operations_from(0).unwrap();
        for batch in history.chunks(9).rev() {
            replayed
                .inner
                .apply_remote_batch(batch.iter().rev().cloned().collect())
                .unwrap();
            resets += usize::from(apply_snapshot_changes(&mut replayed, &mut replayed_view));
        }
        assert!(resets > 0);
        assert_eq!(replayed_view, source_view);
    }

    #[test]
    fn explicit_and_standalone_failures_restore_drained_invalidations_and_poison_commit() {
        let mut tree = WasmTree::new("01".into());
        insert(&mut tree, NodeId::ROOT, NodeId(1));
        let before = tree.snapshot_rows().unwrap();
        let pending = tree.inner.pending_snapshot_changes();
        tree.begin().unwrap();
        assert!(tree.begin().is_err());
        insert(&mut tree, NodeId(1), NodeId(2));
        tree.inner.drain_snapshot_changes();
        let result = tree.mutate(|inner| {
            inner
                .local_move(NodeId(2), NodeId::ROOT, LocalPlacement::After(NodeId(99)))
                .map_err(|error| error.to_string())
        });
        assert!(result.is_err());
        assert!(tree.commit().is_err());
        assert!(tree.mutate(|_| Ok(())).is_err());
        tree.rollback().unwrap();
        assert_eq!(tree.snapshot_rows().unwrap(), before);
        assert_eq!(tree.inner.pending_snapshot_changes(), pending);
        assert_eq!(tree.operation_count(), 1);
        let result: Result<(), String> = tree.mutate(|inner| {
            inner.local_delete(NodeId(1)).unwrap();
            Err("injected failure after native mutation".into())
        });
        assert!(result.is_err());
        assert_eq!(tree.snapshot_rows().unwrap(), before);
        assert_eq!(tree.inner.pending_snapshot_changes(), pending);
        let next = insert(&mut tree, NodeId::ROOT, NodeId(2));
        assert_eq!((next.meta.id.counter, next.meta.lamport), (2, 2));
        tree.begin().unwrap();
        tree.commit().unwrap();
    }

    #[test]
    fn typed_wire_round_trips_payload_clear_and_gap_aware_delete_without_hex_binary_fields() {
        let mut tree = WasmTree::new("01".into());
        insert(&mut tree, NodeId::ROOT, NodeId(1));
        tree.inner.local_payload(NodeId(1), None).unwrap();
        tree.inner.local_delete(NodeId(1)).unwrap();
        for op in tree.inner.operations_from(0).unwrap() {
            let value = serde_json::to_value(TypedOperation::from_op(op.clone()).unwrap()).unwrap();
            assert!(value["meta"]["id"]["replica"].is_array());
            let restored =
                serde_json::from_value::<TypedOperation>(value).unwrap().into_op().unwrap();
            assert_eq!(restored, op);
        }
        assert!(hex_to_node("🙂").is_err());
        let mut oversized = tree.inner.operations_from(0).unwrap().remove(0);
        oversized.meta.lamport = 9_007_199_254_740_992;
        assert!(TypedOperation::from_op(oversized).unwrap().into_op().is_err());
    }
}
