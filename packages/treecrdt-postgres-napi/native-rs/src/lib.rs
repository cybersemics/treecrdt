#![forbid(unsafe_code)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use postgres::{Client, NoTls};
use treecrdt_core::{
    Error as CoreError, Lamport, NodeId, Operation, OperationId, OperationKind, ReplicaId,
    Result as CoreResult, VersionVector,
};

fn map_err(e: impl std::fmt::Display) -> napi::Error {
    napi::Error::new(Status::GenericFailure, format!("{e}"))
}

fn map_core_err(e: CoreError) -> napi::Error {
    map_err(format!("{e:?}"))
}

fn connect(url: &str) -> napi::Result<Client> {
    Client::connect(url, NoTls).map_err(map_err)
}

fn bytes16_to_node(bytes: &[u8]) -> CoreResult<NodeId> {
    if bytes.len() != 16 {
        return Err(CoreError::Storage("expected 16-byte node id".into()));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(NodeId(u128::from_be_bytes(arr)))
}

fn node_to_bytes16(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

fn vv_from_bytes(bytes: &[u8]) -> CoreResult<VersionVector> {
    serde_json::from_slice(bytes).map_err(|e| CoreError::Storage(e.to_string()))
}

fn vv_to_bytes(vv: &VersionVector) -> CoreResult<Vec<u8>> {
    serde_json::to_vec(vv).map_err(|e| CoreError::Storage(e.to_string()))
}

#[napi(object)]
pub struct NativeOp {
    pub lamport: BigInt,
    pub replica: Buffer,
    pub counter: BigInt,
    pub kind: String,
    pub parent: Option<Buffer>,
    pub node: Buffer,
    pub new_parent: Option<Buffer>,
    pub order_key: Option<Buffer>,
    pub payload: Option<Buffer>,
    pub known_state: Option<Buffer>,
}

#[napi(object)]
pub struct NativeTreeChildRow {
    pub node: Buffer,
    pub order_key: Option<Buffer>,
}

#[napi(object)]
pub struct NativeTreeRow {
    pub node: Buffer,
    pub parent: Option<Buffer>,
    pub order_key: Option<Buffer>,
    pub tombstone: bool,
}

fn bigint_to_u64(name: &str, v: BigInt) -> CoreResult<u64> {
    // napi BigInt is signed; reject negatives and overflow.
    let (is_negative, value, lossless) = v.get_u64();
    if is_negative {
        return Err(CoreError::Storage(format!("{name} must be non-negative")));
    }
    if !lossless {
        return Err(CoreError::Storage(format!("{name} out of u64 range")));
    }
    Ok(value)
}

fn native_to_core_op(op: NativeOp) -> CoreResult<Operation> {
    let lamport_u64 = bigint_to_u64("lamport", op.lamport)?;
    let counter_u64 = bigint_to_u64("counter", op.counter)?;

    let replica = ReplicaId(op.replica.to_vec());
    let id = OperationId::new(&replica, counter_u64);

    let known_state = match op.known_state {
        None => None,
        Some(b) if b.is_empty() => None,
        Some(b) => Some(vv_from_bytes(&b)?),
    };

    let meta = treecrdt_core::OperationMetadata {
        id,
        lamport: lamport_u64 as Lamport,
        known_state,
    };

    let node = bytes16_to_node(&op.node)?;

    let kind = match op.kind.as_str() {
        "insert" => {
            let parent =
                op.parent.ok_or_else(|| CoreError::Storage("insert op missing parent".into()))?;
            let order_key = op
                .order_key
                .ok_or_else(|| CoreError::Storage("insert op missing order_key".into()))?;

            OperationKind::Insert {
                parent: bytes16_to_node(&parent)?,
                node,
                order_key: order_key.to_vec(),
                payload: op.payload.map(|p| p.to_vec()),
            }
        }
        "move" => {
            let new_parent = op
                .new_parent
                .ok_or_else(|| CoreError::Storage("move op missing new_parent".into()))?;
            let order_key = op
                .order_key
                .ok_or_else(|| CoreError::Storage("move op missing order_key".into()))?;

            OperationKind::Move {
                node,
                new_parent: bytes16_to_node(&new_parent)?,
                order_key: order_key.to_vec(),
            }
        }
        "delete" => OperationKind::Delete { node },
        "tombstone" => OperationKind::Tombstone { node },
        "payload" => OperationKind::Payload {
            node,
            payload: op.payload.map(|p| p.to_vec()),
        },
        other => return Err(CoreError::Storage(format!("unknown op kind: {other}"))),
    };

    Ok(Operation { meta, kind })
}

fn core_to_native_op(op: Operation) -> CoreResult<NativeOp> {
    let lamport = BigInt::from(op.meta.lamport as u64);
    let counter = BigInt::from(op.meta.id.counter);

    let known_state = match op.meta.known_state.as_ref() {
        None => None,
        Some(vv) => Some(Buffer::from(vv_to_bytes(vv)?)),
    };

    match op.kind {
        OperationKind::Insert {
            parent,
            node,
            order_key,
            payload,
        } => Ok(NativeOp {
            lamport,
            replica: Buffer::from(op.meta.id.replica.as_bytes().to_vec()),
            counter,
            kind: "insert".to_string(),
            parent: Some(Buffer::from(node_to_bytes16(parent).to_vec())),
            node: Buffer::from(node_to_bytes16(node).to_vec()),
            new_parent: None,
            order_key: Some(Buffer::from(order_key)),
            payload: payload.map(Buffer::from),
            known_state,
        }),
        OperationKind::Move {
            node,
            new_parent,
            order_key,
        } => Ok(NativeOp {
            lamport,
            replica: Buffer::from(op.meta.id.replica.as_bytes().to_vec()),
            counter,
            kind: "move".to_string(),
            parent: None,
            node: Buffer::from(node_to_bytes16(node).to_vec()),
            new_parent: Some(Buffer::from(node_to_bytes16(new_parent).to_vec())),
            order_key: Some(Buffer::from(order_key)),
            payload: None,
            known_state,
        }),
        OperationKind::Delete { node } => Ok(NativeOp {
            lamport,
            replica: Buffer::from(op.meta.id.replica.as_bytes().to_vec()),
            counter,
            kind: "delete".to_string(),
            parent: None,
            node: Buffer::from(node_to_bytes16(node).to_vec()),
            new_parent: None,
            order_key: None,
            payload: None,
            known_state,
        }),
        OperationKind::Tombstone { node } => Ok(NativeOp {
            lamport,
            replica: Buffer::from(op.meta.id.replica.as_bytes().to_vec()),
            counter,
            kind: "tombstone".to_string(),
            parent: None,
            node: Buffer::from(node_to_bytes16(node).to_vec()),
            new_parent: None,
            order_key: None,
            payload: None,
            known_state,
        }),
        OperationKind::Payload { node, payload } => Ok(NativeOp {
            lamport,
            replica: Buffer::from(op.meta.id.replica.as_bytes().to_vec()),
            counter,
            kind: "payload".to_string(),
            parent: None,
            node: Buffer::from(node_to_bytes16(node).to_vec()),
            new_parent: None,
            order_key: None,
            payload: payload.map(Buffer::from),
            known_state,
        }),
    }
}

#[napi]
pub struct PgFactory {
    url: String,
}

#[napi]
impl PgFactory {
    #[napi(constructor)]
    pub fn new(url: String) -> Self {
        Self { url }
    }

    #[napi]
    pub fn ensure_schema(&self) -> napi::Result<()> {
        let mut client = connect(&self.url)?;
        treecrdt_postgres::ensure_schema(&mut client).map_err(map_core_err)?;
        Ok(())
    }

    #[napi]
    pub fn reset_for_tests(&self) -> napi::Result<()> {
        let mut client = connect(&self.url)?;
        // Test-only convenience: wipe all docs.
        client
            .batch_execute(
                "TRUNCATE treecrdt_oprefs_children, treecrdt_payload, treecrdt_nodes, treecrdt_ops, treecrdt_meta",
            )
            .map_err(map_err)?;
        Ok(())
    }

    #[napi]
    pub fn reset_doc_for_tests(&self, doc_id: String) -> napi::Result<()> {
        let mut client = connect(&self.url)?;
        treecrdt_postgres::reset_doc_for_tests(&mut client, &doc_id).map_err(map_core_err)?;
        Ok(())
    }

    #[napi]
    pub fn open(&self, doc_id: String) -> PgBackend {
        PgBackend {
            url: self.url.clone(),
            doc_id,
        }
    }
}

#[napi]
pub struct PgBackend {
    url: String,
    doc_id: String,
}

#[napi]
impl PgBackend {
    #[napi]
    pub fn max_lamport(&self) -> napi::Result<BigInt> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let lamport =
            treecrdt_postgres::max_lamport(&client, &self.doc_id).map_err(map_core_err)?;
        Ok(BigInt::from(lamport as u64))
    }

    #[napi]
    pub fn list_op_refs_all(&self) -> napi::Result<Vec<Buffer>> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let refs =
            treecrdt_postgres::list_op_refs_all(&client, &self.doc_id).map_err(map_core_err)?;
        Ok(refs.into_iter().map(|r| Buffer::from(r.to_vec())).collect())
    }

    #[napi]
    pub fn list_op_refs_children(&self, parent: Buffer) -> napi::Result<Vec<Buffer>> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let parent = bytes16_to_node(&parent).map_err(map_core_err)?;
        let refs = treecrdt_postgres::list_op_refs_children(&client, &self.doc_id, parent)
            .map_err(map_core_err)?;
        Ok(refs.into_iter().map(|r| Buffer::from(r.to_vec())).collect())
    }

    #[napi]
    pub fn ops_since(&self, lamport: BigInt, root: Option<Buffer>) -> napi::Result<Vec<NativeOp>> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let lamport_u64 = bigint_to_u64("lamport", lamport).map_err(map_core_err)?;
        let root_id = match root {
            None => None,
            Some(b) => Some(bytes16_to_node(&b).map_err(map_core_err)?),
        };

        let ops =
            treecrdt_postgres::ops_since(&client, &self.doc_id, lamport_u64 as Lamport, root_id)
                .map_err(map_core_err)?;
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            out.push(core_to_native_op(op).map_err(map_core_err)?);
        }
        Ok(out)
    }

    #[napi]
    pub fn tree_children(&self, parent: Buffer) -> napi::Result<Vec<Buffer>> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let parent = bytes16_to_node(&parent).map_err(map_core_err)?;
        let nodes = treecrdt_postgres::tree_children(&client, &self.doc_id, parent)
            .map_err(map_core_err)?;
        Ok(nodes.into_iter().map(|n| Buffer::from(node_to_bytes16(n).to_vec())).collect())
    }

    #[napi]
    pub fn tree_children_page(
        &self,
        parent: Buffer,
        cursor_order_key: Option<Buffer>,
        cursor_node: Option<Buffer>,
        limit: u32,
    ) -> napi::Result<Vec<NativeTreeChildRow>> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let parent = bytes16_to_node(&parent).map_err(map_core_err)?;

        let cursor = match (cursor_order_key, cursor_node) {
            (None, None) => None,
            (Some(k), Some(n)) => Some((k.to_vec(), n.to_vec())),
            _ => return Err(map_err("invalid cursor (expected both order_key and node)")),
        };

        let rows =
            treecrdt_postgres::tree_children_page(&client, &self.doc_id, parent, cursor, limit)
                .map_err(map_core_err)?;
        Ok(rows
            .into_iter()
            .map(|row| NativeTreeChildRow {
                node: Buffer::from(node_to_bytes16(row.node).to_vec()),
                order_key: row.order_key.map(Buffer::from),
            })
            .collect())
    }

    #[napi]
    pub fn tree_dump(&self) -> napi::Result<Vec<NativeTreeRow>> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let rows = treecrdt_postgres::tree_dump(&client, &self.doc_id).map_err(map_core_err)?;
        Ok(rows
            .into_iter()
            .map(|row| NativeTreeRow {
                node: Buffer::from(node_to_bytes16(row.node).to_vec()),
                parent: row.parent.map(|p| Buffer::from(node_to_bytes16(p).to_vec())),
                order_key: row.order_key.map(Buffer::from),
                tombstone: row.tombstone,
            })
            .collect())
    }

    #[napi]
    pub fn tree_node_count(&self) -> napi::Result<BigInt> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let cnt =
            treecrdt_postgres::tree_node_count(&client, &self.doc_id).map_err(map_core_err)?;
        Ok(BigInt::from(cnt))
    }

    #[napi]
    pub fn replica_max_counter(&self, replica: Buffer) -> napi::Result<BigInt> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));
        let cnt = treecrdt_postgres::replica_max_counter(&client, &self.doc_id, &replica)
            .map_err(map_core_err)?;
        Ok(BigInt::from(cnt))
    }

    #[napi]
    pub fn get_ops_by_op_refs(&self, op_refs: Vec<Buffer>) -> napi::Result<Vec<NativeOp>> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));

        let refs: Vec<[u8; 16]> = op_refs
            .into_iter()
            .map(|b| {
                if b.len() != 16 {
                    return Err(map_err("op_ref must be 16 bytes"));
                }
                let mut arr = [0u8; 16];
                arr.copy_from_slice(&b);
                Ok(arr)
            })
            .collect::<napi::Result<_>>()?;

        let ops = treecrdt_postgres::get_ops_by_op_refs(&client, &self.doc_id, &refs)
            .map_err(map_core_err)?;
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            out.push(core_to_native_op(op).map_err(map_core_err)?);
        }
        Ok(out)
    }

    #[napi]
    pub fn apply_ops(&self, ops: Vec<NativeOp>) -> napi::Result<()> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));

        let mut core_ops = Vec::with_capacity(ops.len());
        for op in ops {
            core_ops.push(native_to_core_op(op).map_err(map_core_err)?);
        }

        treecrdt_postgres::append_ops(&client, &self.doc_id, &core_ops).map_err(map_core_err)?;
        Ok(())
    }

    #[napi]
    pub fn local_insert(
        &self,
        replica: Buffer,
        parent: Buffer,
        node: Buffer,
        placement: String,
        after: Option<Buffer>,
        payload: Option<Buffer>,
    ) -> napi::Result<NativeOp> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));

        let replica = ReplicaId(replica.to_vec());
        let parent = bytes16_to_node(&parent).map_err(map_core_err)?;
        let node = bytes16_to_node(&node).map_err(map_core_err)?;
        let after_id = match after {
            None => None,
            Some(b) => Some(bytes16_to_node(&b).map_err(map_core_err)?),
        };

        let op = treecrdt_postgres::local_insert(
            &client,
            &self.doc_id,
            &replica,
            parent,
            node,
            &placement,
            after_id,
            payload.map(|p| p.to_vec()),
        )
        .map_err(map_core_err)?;
        core_to_native_op(op).map_err(map_core_err)
    }

    #[napi]
    pub fn local_move(
        &self,
        replica: Buffer,
        node: Buffer,
        new_parent: Buffer,
        placement: String,
        after: Option<Buffer>,
    ) -> napi::Result<NativeOp> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));

        let replica = ReplicaId(replica.to_vec());
        let node = bytes16_to_node(&node).map_err(map_core_err)?;
        let new_parent = bytes16_to_node(&new_parent).map_err(map_core_err)?;
        let after_id = match after {
            None => None,
            Some(b) => Some(bytes16_to_node(&b).map_err(map_core_err)?),
        };

        let op = treecrdt_postgres::local_move(
            &client,
            &self.doc_id,
            &replica,
            node,
            new_parent,
            &placement,
            after_id,
        )
        .map_err(map_core_err)?;
        core_to_native_op(op).map_err(map_core_err)
    }

    #[napi]
    pub fn local_delete(&self, replica: Buffer, node: Buffer) -> napi::Result<NativeOp> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));

        let replica = ReplicaId(replica.to_vec());
        let node = bytes16_to_node(&node).map_err(map_core_err)?;
        let op = treecrdt_postgres::local_delete(&client, &self.doc_id, &replica, node)
            .map_err(map_core_err)?;
        core_to_native_op(op).map_err(map_core_err)
    }

    #[napi]
    pub fn local_payload(
        &self,
        replica: Buffer,
        node: Buffer,
        payload: Option<Buffer>,
    ) -> napi::Result<NativeOp> {
        let client = connect(&self.url)?;
        let client = std::rc::Rc::new(std::cell::RefCell::new(client));

        let replica = ReplicaId(replica.to_vec());
        let node = bytes16_to_node(&node).map_err(map_core_err)?;
        let op = treecrdt_postgres::local_payload(
            &client,
            &self.doc_id,
            &replica,
            node,
            payload.map(|p| p.to_vec()),
        )
        .map_err(map_core_err)?;
        core_to_native_op(op).map_err(map_core_err)
    }
}
