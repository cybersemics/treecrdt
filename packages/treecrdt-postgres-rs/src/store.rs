use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use postgres::{Client, Row, Statement};

use treecrdt_core::{
    cmp_op_key, Error, Lamport, LamportClock, NodeId, NodeStore, Operation, OperationId,
    OperationKind, ParentOpIndex, PayloadStore, ReplicaId, Result, Storage, TreeCrdt,
    VersionVector,
};

use crate::opref::{derive_op_ref_v0, OPREF_V0_WIDTH};

fn storage_debug<E: std::fmt::Debug>(e: E) -> Error {
    Error::Storage(format!("{e:?}"))
}

fn node_to_bytes(node: NodeId) -> [u8; 16] {
    node.0.to_be_bytes()
}

fn bytes_to_node(bytes: &[u8]) -> Result<NodeId> {
    if bytes.len() != 16 {
        return Err(Error::Storage("expected 16-byte node id".into()));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(NodeId(u128::from_be_bytes(arr)))
}

fn op_ref_from_bytes(bytes: &[u8]) -> Result<[u8; OPREF_V0_WIDTH]> {
    if bytes.len() != OPREF_V0_WIDTH {
        return Err(Error::Storage("expected 16-byte op_ref".into()));
    }
    let mut arr = [0u8; OPREF_V0_WIDTH];
    arr.copy_from_slice(bytes);
    Ok(arr)
}

fn vv_to_bytes(vv: &VersionVector) -> Result<Vec<u8>> {
    serde_json::to_vec(vv).map_err(|e| Error::Storage(e.to_string()))
}

fn vv_from_bytes(bytes: &[u8]) -> Result<VersionVector> {
    serde_json::from_slice(bytes).map_err(|e| Error::Storage(e.to_string()))
}

#[derive(Clone, Debug)]
struct TreeMeta {
    dirty: bool,
    head_lamport: Lamport,
    head_replica: Vec<u8>,
    head_counter: u64,
    head_seq: u64,
}

fn ensure_doc_meta(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let mut c = client.borrow_mut();
    c.execute(
        // Default new docs to "clean" so we can incrementally maintain materialized state.
        "INSERT INTO treecrdt_meta(doc_id, dirty) VALUES ($1, FALSE) ON CONFLICT (doc_id) DO NOTHING",
        &[&doc_id],
    )
    .map_err(storage_debug)?;
    Ok(())
}

fn load_tree_meta(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<TreeMeta> {
    ensure_doc_meta(client, doc_id)?;

    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT dirty, head_lamport, head_replica, head_counter, head_seq FROM treecrdt_meta WHERE doc_id = $1 LIMIT 1",
            &[&doc_id],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;

    let row = rows.first().ok_or_else(|| Error::Storage("missing treecrdt_meta row".into()))?;

    Ok(TreeMeta {
        dirty: row.get::<_, bool>(0),
        head_lamport: row.get::<_, i64>(1).max(0) as Lamport,
        head_replica: row.get::<_, Vec<u8>>(2),
        head_counter: row.get::<_, i64>(3).max(0) as u64,
        head_seq: row.get::<_, i64>(4).max(0) as u64,
    })
}

fn load_tree_meta_for_update(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<TreeMeta> {
    ensure_doc_meta(client, doc_id)?;

    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT dirty, head_lamport, head_replica, head_counter, head_seq FROM treecrdt_meta WHERE doc_id = $1 FOR UPDATE",
            &[&doc_id],
        )
        .map_err(storage_debug)?;

    let row = rows.first().ok_or_else(|| Error::Storage("missing treecrdt_meta row".into()))?;

    Ok(TreeMeta {
        dirty: row.get::<_, bool>(0),
        head_lamport: row.get::<_, i64>(1).max(0) as Lamport,
        head_replica: row.get::<_, Vec<u8>>(2),
        head_counter: row.get::<_, i64>(3).max(0) as u64,
        head_seq: row.get::<_, i64>(4).max(0) as u64,
    })
}

fn set_tree_meta_dirty(client: &Rc<RefCell<Client>>, doc_id: &str, dirty: bool) -> Result<()> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    c.execute(
        "UPDATE treecrdt_meta SET dirty = $2 WHERE doc_id = $1",
        &[&doc_id, &dirty],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

fn update_tree_meta_head(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    lamport: Lamport,
    replica: &[u8],
    counter: u64,
    seq: u64,
) -> Result<()> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    c.execute(
        "UPDATE treecrdt_meta SET dirty = FALSE, head_lamport = $2, head_replica = $3, head_counter = $4, head_seq = $5 WHERE doc_id = $1",
        &[&doc_id, &(lamport as i64), &replica, &(counter as i64), &(seq as i64)],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

#[derive(Default)]
struct NoopStorage;

impl Storage for NoopStorage {
    fn apply(&mut self, _op: Operation) -> Result<bool> {
        Ok(true)
    }

    fn load_since(&self, _lamport: Lamport) -> Result<Vec<Operation>> {
        Ok(Vec::new())
    }

    fn latest_lamport(&self) -> Lamport {
        0
    }
}

#[derive(Clone)]
struct PgCtx {
    doc_id: String,
    client: Rc<RefCell<Client>>,
    stmts: Rc<RefCell<HashMap<&'static str, Statement>>>,
}

impl PgCtx {
    fn new(client: Rc<RefCell<Client>>, doc_id: &str) -> Result<Self> {
        ensure_doc_meta(&client, doc_id)?;
        Ok(Self {
            doc_id: doc_id.to_string(),
            client,
            stmts: Rc::new(RefCell::new(HashMap::new())),
        })
    }

    fn stmt(&self, c: &mut Client, sql: &'static str) -> Result<Statement> {
        if let Some(stmt) = self.stmts.borrow().get(sql) {
            return Ok(stmt.clone());
        }
        let stmt = c.prepare(sql).map_err(storage_debug)?;
        self.stmts.borrow_mut().insert(sql, stmt.clone());
        Ok(stmt)
    }
}

#[derive(Clone, Debug)]
struct CachedNodeRow {
    parent: Option<NodeId>,
    order_key: Option<Vec<u8>>,
    tombstone: bool,
    last_change: Option<Vec<u8>>,
    deleted_at: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct CachedPayloadRow {
    payload: Option<Vec<u8>>,
    last_lamport: Lamport,
    last_replica: Vec<u8>,
    last_counter: u64,
}

struct PgNodeStore {
    ctx: PgCtx,
    cache: RefCell<HashMap<NodeId, Option<CachedNodeRow>>>,
}

impl PgNodeStore {
    fn new(ctx: PgCtx) -> Self {
        Self {
            ctx,
            cache: RefCell::new(HashMap::new()),
        }
    }

    fn load_node_row(&self, node: NodeId) -> Result<Option<CachedNodeRow>> {
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT parent, order_key, tombstone, last_change, deleted_at \
             FROM treecrdt_nodes WHERE doc_id = $1 AND node = $2 LIMIT 1",
        )?;
        let rows = c
            .query(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let parent_bytes: Option<Vec<u8>> = row.get(0);
        let parent = match parent_bytes {
            None => None,
            Some(b) => Some(bytes_to_node(&b)?),
        };
        let order_key: Option<Vec<u8>> = row.get(1);
        let tombstone: bool = row.get(2);
        let last_change: Option<Vec<u8>> = row.get(3);
        let deleted_at: Option<Vec<u8>> = row.get(4);
        Ok(Some(CachedNodeRow {
            parent,
            order_key,
            tombstone,
            last_change,
            deleted_at,
        }))
    }

    fn node_row(&self, node: NodeId) -> Result<Option<CachedNodeRow>> {
        if let Some(cached) = self.cache.borrow().get(&node) {
            return Ok(cached.clone());
        }
        let loaded = self.load_node_row(node)?;
        self.cache.borrow_mut().insert(node, loaded.clone());
        Ok(loaded)
    }
}

impl treecrdt_core::NodeStore for PgNodeStore {
    fn reset(&mut self) -> Result<()> {
        self.cache.borrow_mut().clear();
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(&mut c, "DELETE FROM treecrdt_nodes WHERE doc_id = $1")?;
        c.execute(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;

        let root_bytes = node_to_bytes(NodeId::ROOT);
        let empty: &[u8] = &[];
        let stmt = self.ctx.stmt(
            &mut c,
            "INSERT INTO treecrdt_nodes(doc_id, node, parent, order_key, tombstone) VALUES ($1, $2, NULL, $3, FALSE) ON CONFLICT (doc_id, node) DO UPDATE SET parent = NULL, order_key = EXCLUDED.order_key, tombstone = FALSE",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &root_bytes.as_slice(), &empty])
            .map_err(storage_debug)?;

        self.cache.borrow_mut().insert(
            NodeId::ROOT,
            Some(CachedNodeRow {
                parent: None,
                order_key: Some(Vec::new()),
                tombstone: false,
                last_change: None,
                deleted_at: None,
            }),
        );
        Ok(())
    }

    fn ensure_node(&mut self, node: NodeId) -> Result<()> {
        if matches!(self.cache.borrow().get(&node), Some(Some(_))) {
            return Ok(());
        }

        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "INSERT INTO treecrdt_nodes(doc_id, node) VALUES ($1, $2) \
             ON CONFLICT (doc_id, node) DO NOTHING",
        )?;
        let inserted = c
            .execute(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        if inserted > 0 {
            self.cache.borrow_mut().insert(
                node,
                Some(CachedNodeRow {
                    parent: None,
                    order_key: None,
                    tombstone: false,
                    last_change: None,
                    deleted_at: None,
                }),
            );
        }
        Ok(())
    }

    fn exists(&self, node: NodeId) -> Result<bool> {
        Ok(self.node_row(node)?.is_some())
    }

    fn parent(&self, node: NodeId) -> Result<Option<NodeId>> {
        let Some(row) = self.node_row(node)? else {
            return Ok(None);
        };
        Ok(row.parent)
    }

    fn order_key(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        let Some(row) = self.node_row(node)? else {
            return Ok(None);
        };
        Ok(row.order_key)
    }

    fn children(&self, parent: NodeId) -> Result<Vec<NodeId>> {
        if parent == NodeId::TRASH {
            return Ok(Vec::new());
        }
        let parent_bytes = node_to_bytes(parent);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT node FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 \
             ORDER BY order_key, node",
        )?;
        let rows = c
            .query(&stmt, &[&self.ctx.doc_id, &parent_bytes.as_slice()])
            .map_err(storage_debug)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let node: Vec<u8> = row.get(0);
            out.push(bytes_to_node(&node)?);
        }
        Ok(out)
    }

    fn detach(&mut self, node: NodeId) -> Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET parent = NULL, order_key = NULL \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.parent = None;
            row.order_key = None;
        }
        Ok(())
    }

    fn attach(&mut self, node: NodeId, parent: NodeId, order_key: Vec<u8>) -> Result<()> {
        if node == NodeId::ROOT {
            return Ok(());
        }
        self.ensure_node(node)?;
        self.ensure_node(parent)?;

        let node_bytes = node_to_bytes(node);
        let parent_bytes = node_to_bytes(parent);
        let mut c = self.ctx.client.borrow_mut();

        if parent == NodeId::TRASH {
            let stmt = self.ctx.stmt(
                &mut c,
                "UPDATE treecrdt_nodes \
                 SET parent = $3, order_key = NULL \
                 WHERE doc_id = $1 AND node = $2",
            )?;
            c.execute(
                &stmt,
                &[
                    &self.ctx.doc_id,
                    &node_bytes.as_slice(),
                    &parent_bytes.as_slice(),
                ],
            )
            .map_err(storage_debug)?;

            if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
                row.parent = Some(parent);
                row.order_key = None;
            }
            return Ok(());
        }

        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET parent = $3, order_key = $4 \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(
            &stmt,
            &[
                &self.ctx.doc_id,
                &node_bytes.as_slice(),
                &parent_bytes.as_slice(),
                &order_key,
            ],
        )
        .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.parent = Some(parent);
            row.order_key = Some(order_key);
        }
        Ok(())
    }

    fn tombstone(&self, node: NodeId) -> Result<bool> {
        let Some(row) = self.node_row(node)? else {
            return Ok(false);
        };
        Ok(row.tombstone)
    }

    fn set_tombstone(&mut self, node: NodeId, tombstone: bool) -> Result<()> {
        self.ensure_node(node)?;
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET tombstone = $3 \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(
            &stmt,
            &[&self.ctx.doc_id, &node_bytes.as_slice(), &tombstone],
        )
        .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.tombstone = tombstone;
        }
        Ok(())
    }

    fn last_change(&self, node: NodeId) -> Result<VersionVector> {
        let Some(row) = self.node_row(node)? else {
            return Ok(VersionVector::new());
        };
        match row.last_change {
            None => Ok(VersionVector::new()),
            Some(b) if b.is_empty() => Ok(VersionVector::new()),
            Some(b) => vv_from_bytes(&b),
        }
    }

    fn merge_last_change(&mut self, node: NodeId, delta: &VersionVector) -> Result<()> {
        self.ensure_node(node)?;
        let mut vv = self.last_change(node)?;
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET last_change = $3 \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice(), &bytes])
            .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.last_change = Some(bytes);
        }
        Ok(())
    }

    fn deleted_at(&self, node: NodeId) -> Result<Option<VersionVector>> {
        let Some(row) = self.node_row(node)? else {
            return Ok(None);
        };
        match row.deleted_at {
            None => Ok(None),
            Some(b) if b.is_empty() => Ok(None),
            Some(b) => vv_from_bytes(&b).map(Some),
        }
    }

    fn merge_deleted_at(&mut self, node: NodeId, delta: &VersionVector) -> Result<()> {
        self.ensure_node(node)?;
        let mut vv = self.deleted_at(node)?.unwrap_or_else(VersionVector::new);
        vv.merge(delta);
        let bytes = vv_to_bytes(&vv)?;

        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "UPDATE treecrdt_nodes \
             SET deleted_at = $3 \
             WHERE doc_id = $1 AND node = $2",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice(), &bytes])
            .map_err(storage_debug)?;

        if let Some(Some(row)) = self.cache.borrow_mut().get_mut(&node) {
            row.deleted_at = Some(bytes);
        }
        Ok(())
    }

    fn all_nodes(&self) -> Result<Vec<NodeId>> {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(&mut c, "SELECT node FROM treecrdt_nodes WHERE doc_id = $1")?;
        let rows = c.query(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let node: Vec<u8> = row.get(0);
            out.push(bytes_to_node(&node)?);
        }
        Ok(out)
    }
}

struct PgPayloadStore {
    ctx: PgCtx,
    cache: RefCell<HashMap<NodeId, Option<CachedPayloadRow>>>,
}

impl PgPayloadStore {
    fn new(ctx: PgCtx) -> Self {
        Self {
            ctx,
            cache: RefCell::new(HashMap::new()),
        }
    }

    fn load_payload_row(&self, node: NodeId) -> Result<Option<CachedPayloadRow>> {
        let node_bytes = node_to_bytes(node);
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT payload, last_lamport, last_replica, last_counter \
             FROM treecrdt_payload WHERE doc_id = $1 AND node = $2 LIMIT 1",
        )?;
        let rows = c
            .query(&stmt, &[&self.ctx.doc_id, &node_bytes.as_slice()])
            .map_err(storage_debug)?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let payload: Option<Vec<u8>> = row.get(0);
        let last_lamport = row.get::<_, i64>(1).max(0) as Lamport;
        let last_replica: Vec<u8> = row.get(2);
        let last_counter = row.get::<_, i64>(3).max(0) as u64;
        Ok(Some(CachedPayloadRow {
            payload,
            last_lamport,
            last_replica,
            last_counter,
        }))
    }

    fn payload_row(&self, node: NodeId) -> Result<Option<CachedPayloadRow>> {
        if let Some(cached) = self.cache.borrow().get(&node) {
            return Ok(cached.clone());
        }
        let loaded = self.load_payload_row(node)?;
        self.cache.borrow_mut().insert(node, loaded.clone());
        Ok(loaded)
    }
}

impl treecrdt_core::PayloadStore for PgPayloadStore {
    fn reset(&mut self) -> Result<()> {
        self.cache.borrow_mut().clear();
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(&mut c, "DELETE FROM treecrdt_payload WHERE doc_id = $1")?;
        c.execute(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;
        Ok(())
    }

    fn payload(&self, node: NodeId) -> Result<Option<Vec<u8>>> {
        let Some(row) = self.payload_row(node)? else {
            return Ok(None);
        };
        Ok(row.payload)
    }

    fn last_writer(&self, node: NodeId) -> Result<Option<(Lamport, OperationId)>> {
        let Some(row) = self.payload_row(node)? else {
            return Ok(None);
        };
        Ok(Some((
            row.last_lamport,
            OperationId {
                replica: ReplicaId(row.last_replica),
                counter: row.last_counter,
            },
        )))
    }

    fn set_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
        writer: (Lamport, OperationId),
    ) -> Result<()> {
        let node_bytes = node_to_bytes(node);
        let (lamport, id) = writer;
        let OperationId { replica, counter } = id;
        let ReplicaId(replica_bytes) = replica;
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "INSERT INTO treecrdt_payload(doc_id, node, payload, last_lamport, last_replica, last_counter) VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT (doc_id, node) DO UPDATE SET payload = EXCLUDED.payload, last_lamport = EXCLUDED.last_lamport, last_replica = EXCLUDED.last_replica, last_counter = EXCLUDED.last_counter",
        )?;
        c.execute(
            &stmt,
            &[
                &self.ctx.doc_id,
                &node_bytes.as_slice(),
                &payload,
                &(lamport as i64),
                &replica_bytes,
                &(counter as i64),
            ],
        )
        .map_err(storage_debug)?;

        self.cache.borrow_mut().insert(
            node,
            Some(CachedPayloadRow {
                payload,
                last_lamport: lamport,
                last_replica: replica_bytes,
                last_counter: counter,
            }),
        );
        Ok(())
    }
}

struct PgParentOpIndex {
    ctx: PgCtx,
}

impl PgParentOpIndex {
    fn new(ctx: PgCtx) -> Self {
        Self { ctx }
    }
}

impl treecrdt_core::ParentOpIndex for PgParentOpIndex {
    fn reset(&mut self) -> Result<()> {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "DELETE FROM treecrdt_oprefs_children WHERE doc_id = $1",
        )?;
        c.execute(&stmt, &[&self.ctx.doc_id]).map_err(storage_debug)?;
        Ok(())
    }

    fn record(&mut self, parent: NodeId, op_id: &OperationId, seq: u64) -> Result<()> {
        if parent == NodeId::TRASH {
            return Ok(());
        }
        let parent_bytes = node_to_bytes(parent);
        let op_ref = derive_op_ref_v0(&self.ctx.doc_id, op_id.replica.as_bytes(), op_id.counter);
        let op_ref_bytes = op_ref.as_slice();

        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "INSERT INTO treecrdt_oprefs_children(doc_id, parent, op_ref, seq) VALUES ($1,$2,$3,$4) \
             ON CONFLICT (doc_id, parent, op_ref) DO NOTHING",
        )?;
        c.execute(
            &stmt,
            &[
                &self.ctx.doc_id,
                &parent_bytes.as_slice(),
                &op_ref_bytes,
                &(seq as i64),
            ],
        )
        .map_err(storage_debug)?;
        Ok(())
    }
}

struct PgOpStorage {
    ctx: PgCtx,
}

impl PgOpStorage {
    fn new(ctx: PgCtx) -> Self {
        Self { ctx }
    }
}

impl Storage for PgOpStorage {
    fn apply(&mut self, op: Operation) -> Result<bool> {
        let mut c = self.ctx.client.borrow_mut();
        let inserted = insert_op_in_tx(&self.ctx, &mut c, &op)?;
        Ok(inserted)
    }

    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state \
             FROM treecrdt_ops WHERE doc_id = $1 AND lamport > $2 ORDER BY lamport, replica, counter",
        )?;
        let rows = c.query(&stmt, &[&self.ctx.doc_id, &(lamport as i64)]).map_err(storage_debug)?;
        rows.into_iter().map(row_to_op).collect()
    }

    fn latest_lamport(&self) -> Lamport {
        let mut c = self.ctx.client.borrow_mut();
        let rows = c
            .query(
                "SELECT COALESCE(MAX(lamport), 0) FROM treecrdt_ops WHERE doc_id = $1",
                &[&self.ctx.doc_id],
            )
            .ok();
        match rows.and_then(|r| r.first().map(|row| row.get::<_, i64>(0))) {
            Some(v) => v.max(0) as Lamport,
            None => 0,
        }
    }

    fn latest_counter(&self, replica: &ReplicaId) -> Result<u64> {
        let mut c = self.ctx.client.borrow_mut();
        let stmt = self.ctx.stmt(
            &mut c,
            "SELECT COALESCE(MAX(counter), 0) \
             FROM treecrdt_ops WHERE doc_id = $1 AND replica = $2",
        )?;
        let rows = c.query(&stmt, &[&self.ctx.doc_id, &replica.0]).map_err(storage_debug)?;
        let row = rows.first().ok_or_else(|| Error::Storage("missing MAX(counter) row".into()))?;
        Ok(row.get::<_, i64>(0).max(0) as u64)
    }

    fn scan_since(
        &self,
        lamport: Lamport,
        visit: &mut dyn FnMut(Operation) -> Result<()>,
    ) -> Result<()> {
        let ops = self.load_since(lamport)?;
        for op in ops {
            visit(op)?;
        }
        Ok(())
    }
}

fn row_to_op(row: Row) -> Result<Operation> {
    let lamport = row.get::<_, i64>(0).max(0) as Lamport;
    let replica: Vec<u8> = row.get(1);
    let counter = row.get::<_, i64>(2).max(0) as u64;
    let kind: String = row.get(3);
    let parent: Option<Vec<u8>> = row.get(4);
    let node: Vec<u8> = row.get(5);
    let new_parent: Option<Vec<u8>> = row.get(6);
    let order_key: Option<Vec<u8>> = row.get(7);
    let payload: Option<Vec<u8>> = row.get(8);
    let known_state_bytes: Option<Vec<u8>> = row.get(9);
    let known_state = match known_state_bytes {
        None => None,
        Some(b) if b.is_empty() => None,
        Some(b) => Some(vv_from_bytes(&b)?),
    };

    let replica_id = ReplicaId(replica);
    let op_id = OperationId::new(&replica_id, counter);
    let meta = treecrdt_core::OperationMetadata {
        id: op_id,
        lamport,
        known_state,
    };

    let kind = match kind.as_str() {
        "insert" => {
            let parent = parent.ok_or_else(|| Error::Storage("insert op missing parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("insert op missing order_key".into()))?;
            OperationKind::Insert {
                parent: bytes_to_node(&parent)?,
                node: bytes_to_node(&node)?,
                order_key,
                payload,
            }
        }
        "move" => {
            let new_parent =
                new_parent.ok_or_else(|| Error::Storage("move op missing new_parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("move op missing order_key".into()))?;
            OperationKind::Move {
                node: bytes_to_node(&node)?,
                new_parent: bytes_to_node(&new_parent)?,
                order_key,
            }
        }
        "delete" => OperationKind::Delete {
            node: bytes_to_node(&node)?,
        },
        "tombstone" => OperationKind::Tombstone {
            node: bytes_to_node(&node)?,
        },
        "payload" => OperationKind::Payload {
            node: bytes_to_node(&node)?,
            payload,
        },
        other => return Err(Error::Storage(format!("unknown op kind: {other}"))),
    };

    Ok(Operation { meta, kind })
}

fn row_to_op_at(row: &Row, base: usize) -> Result<Operation> {
    let lamport = row.get::<_, i64>(base).max(0) as Lamport;
    let replica: Vec<u8> = row.get(base + 1);
    let counter = row.get::<_, i64>(base + 2).max(0) as u64;
    let kind: String = row.get(base + 3);
    let parent: Option<Vec<u8>> = row.get(base + 4);
    let node: Vec<u8> = row.get(base + 5);
    let new_parent: Option<Vec<u8>> = row.get(base + 6);
    let order_key: Option<Vec<u8>> = row.get(base + 7);
    let payload: Option<Vec<u8>> = row.get(base + 8);
    let known_state_bytes: Option<Vec<u8>> = row.get(base + 9);
    let known_state = match known_state_bytes {
        None => None,
        Some(b) if b.is_empty() => None,
        Some(b) => Some(vv_from_bytes(&b)?),
    };

    let replica_id = ReplicaId(replica);
    let op_id = OperationId::new(&replica_id, counter);
    let meta = treecrdt_core::OperationMetadata {
        id: op_id,
        lamport,
        known_state,
    };

    let kind = match kind.as_str() {
        "insert" => {
            let parent = parent.ok_or_else(|| Error::Storage("insert op missing parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("insert op missing order_key".into()))?;
            OperationKind::Insert {
                parent: bytes_to_node(&parent)?,
                node: bytes_to_node(&node)?,
                order_key,
                payload,
            }
        }
        "move" => {
            let new_parent =
                new_parent.ok_or_else(|| Error::Storage("move op missing new_parent".into()))?;
            let order_key =
                order_key.ok_or_else(|| Error::Storage("move op missing order_key".into()))?;
            OperationKind::Move {
                node: bytes_to_node(&node)?,
                new_parent: bytes_to_node(&new_parent)?,
                order_key,
            }
        }
        "delete" => OperationKind::Delete {
            node: bytes_to_node(&node)?,
        },
        "tombstone" => OperationKind::Tombstone {
            node: bytes_to_node(&node)?,
        },
        "payload" => OperationKind::Payload {
            node: bytes_to_node(&node)?,
            payload,
        },
        other => return Err(Error::Storage(format!("unknown op kind: {other}"))),
    };

    Ok(Operation { meta, kind })
}

struct OpDbFields {
    kind: &'static str,
    parent: Option<Vec<u8>>,
    node: Vec<u8>,
    new_parent: Option<Vec<u8>>,
    order_key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    known_state: Option<Vec<u8>>,
}

fn op_kind_to_db(op: &Operation) -> Result<OpDbFields> {
    let known_state = match op.meta.known_state.as_ref() {
        None => None,
        Some(vv) => Some(vv_to_bytes(vv)?),
    };

    match &op.kind {
        OperationKind::Insert {
            parent,
            node,
            order_key,
            payload,
        } => Ok(OpDbFields {
            kind: "insert",
            parent: Some(node_to_bytes(*parent).to_vec()),
            node: node_to_bytes(*node).to_vec(),
            new_parent: None,
            order_key: Some(order_key.clone()),
            payload: payload.clone(),
            known_state,
        }),
        OperationKind::Move {
            node,
            new_parent,
            order_key,
        } => Ok(OpDbFields {
            kind: "move",
            parent: None,
            node: node_to_bytes(*node).to_vec(),
            new_parent: Some(node_to_bytes(*new_parent).to_vec()),
            order_key: Some(order_key.clone()),
            payload: None,
            known_state,
        }),
        OperationKind::Delete { node } => {
            let Some(vv) = op.meta.known_state.as_ref() else {
                return Err(Error::InvalidOperation(
                    "treecrdt: delete operations require meta.known_state".into(),
                ));
            };
            let bytes = vv_to_bytes(vv)?;
            if bytes.is_empty() {
                return Err(Error::InvalidOperation(
                    "treecrdt: delete known_state must not be empty".into(),
                ));
            }
            Ok(OpDbFields {
                kind: "delete",
                parent: None,
                node: node_to_bytes(*node).to_vec(),
                new_parent: None,
                order_key: None,
                payload: None,
                known_state: Some(bytes),
            })
        }
        OperationKind::Tombstone { node } => Ok(OpDbFields {
            kind: "tombstone",
            parent: None,
            node: node_to_bytes(*node).to_vec(),
            new_parent: None,
            order_key: None,
            payload: None,
            known_state,
        }),
        OperationKind::Payload { node, payload } => Ok(OpDbFields {
            kind: "payload",
            parent: None,
            node: node_to_bytes(*node).to_vec(),
            new_parent: None,
            order_key: None,
            payload: payload.clone(),
            known_state,
        }),
    }
}

fn insert_op_in_tx(ctx: &PgCtx, c: &mut Client, op: &Operation) -> Result<bool> {
    let replica = op.meta.id.replica.as_bytes();
    let counter = op.meta.id.counter;
    let op_ref = derive_op_ref_v0(&ctx.doc_id, replica, counter);
    let row = op_kind_to_db(op)?;

    let stmt = ctx.stmt(
        c,
        "INSERT INTO treecrdt_ops (doc_id, op_ref, lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
         ON CONFLICT (doc_id, op_ref) DO NOTHING",
    )?;
    let inserted = c
        .execute(
            &stmt,
            &[
                &ctx.doc_id,
                &op_ref.as_slice(),
                &(op.meta.lamport as i64),
                &replica,
                &(counter as i64),
                &row.kind,
                &row.parent,
                &row.node,
                &row.new_parent,
                &row.order_key,
                &row.payload,
                &row.known_state,
            ],
        )
        .map_err(storage_debug)?;
    Ok(inserted > 0)
}

fn cmp_op_key_to_meta(op: &Operation, meta: &TreeMeta) -> Ordering {
    cmp_op_key(
        op.meta.lamport,
        op.meta.id.replica.as_bytes(),
        op.meta.id.counter,
        meta.head_lamport,
        &meta.head_replica,
        meta.head_counter,
    )
}

fn materialize_ops_in_order(ctx: PgCtx, meta: &TreeMeta, mut ops: Vec<Operation>) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }
    if meta.dirty {
        return Err(Error::Storage("materialize called while dirty".into()));
    }

    ops.sort_by(|a, b| {
        cmp_op_key(
            a.meta.lamport,
            a.meta.id.replica.as_bytes(),
            a.meta.id.counter,
            b.meta.lamport,
            b.meta.id.replica.as_bytes(),
            b.meta.id.counter,
        )
    });

    if let Some(first) = ops.first() {
        if cmp_op_key_to_meta(first, meta) == Ordering::Less {
            return Err(Error::Storage(
                "out-of-order op before materialized head".into(),
            ));
        }
    }

    let nodes = PgNodeStore::new(ctx.clone());
    let payloads = PgPayloadStore::new(ctx.clone());
    let mut index = PgParentOpIndex::new(ctx.clone());

    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"postgres"),
        NoopStorage,
        LamportClock::default(),
        nodes,
        payloads,
    )?;

    let mut seq = meta.head_seq;
    for op in ops {
        seq += 1;
        let applied = crdt.apply_remote_with_materialization(op, &mut index, seq)?;
        if applied.is_none() {
            seq -= 1;
        }
    }

    let last = crdt
        .head_op()
        .ok_or_else(|| Error::Storage("expected head op after materialization".into()))?;
    update_tree_meta_head(
        &ctx.client,
        &ctx.doc_id,
        last.meta.lamport,
        last.meta.id.replica.as_bytes(),
        last.meta.id.counter,
        seq,
    )?;
    Ok(())
}

pub fn append_ops(client: &Rc<RefCell<Client>>, doc_id: &str, ops: &[Operation]) -> Result<u64> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = append_ops_in_tx(client, doc_id, ops);

    match res {
        Ok(v) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(v)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

fn append_ops_in_tx(client: &Rc<RefCell<Client>>, doc_id: &str, ops: &[Operation]) -> Result<u64> {
    // Serialize per-doc writers across all server instances (incremental materialization updates
    // derived tables + head_seq and is not safe to run concurrently for the same doc_id).
    let meta = load_tree_meta_for_update(client, doc_id)?;
    let ctx = PgCtx::new(client.clone(), doc_id)?;

    let mut inserted_ops: Vec<Operation> = Vec::new();
    {
        let mut c = client.borrow_mut();
        for op in ops {
            if insert_op_in_tx(&ctx, &mut c, op)? {
                inserted_ops.push(op.clone());
            }
        }
    }

    if inserted_ops.is_empty() {
        return Ok(0);
    }

    let inserted = inserted_ops.len();
    if !meta.dirty {
        if materialize_ops_in_order(ctx, &meta, inserted_ops).is_err() {
            let _ = set_tree_meta_dirty(client, doc_id, true);
        }
    } else {
        let _ = set_tree_meta_dirty(client, doc_id, true);
    }

    Ok(inserted.min(u64::MAX as usize) as u64)
}

fn clear_materialized(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let mut c = client.borrow_mut();
    c.execute(
        "DELETE FROM treecrdt_oprefs_children WHERE doc_id = $1",
        &[&doc_id],
    )
    .map_err(|e| Error::Storage(e.to_string()))?;
    c.execute("DELETE FROM treecrdt_payload WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(e.to_string()))?;
    c.execute("DELETE FROM treecrdt_nodes WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

pub fn ensure_materialized(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = ensure_materialized_in_tx(client, doc_id);

    match res {
        Ok(()) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(())
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

fn ensure_materialized_in_tx(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<()> {
    let meta = load_tree_meta(client, doc_id)?;
    if !meta.dirty {
        return Ok(());
    }

    // Take a per-doc lock so rebuild can't race with concurrent append/materialization.
    let meta = load_tree_meta_for_update(client, doc_id)?;
    if !meta.dirty {
        return Ok(());
    }

    clear_materialized(client, doc_id)?;

    let ctx = PgCtx::new(client.clone(), doc_id)?;
    let storage = PgOpStorage::new(ctx.clone());
    let mut nodes = PgNodeStore::new(ctx.clone());
    let mut payloads = PgPayloadStore::new(ctx.clone());
    let mut index = PgParentOpIndex::new(ctx.clone());

    nodes.reset()?;
    payloads.reset()?;
    index.reset()?;

    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"postgres"),
        storage,
        LamportClock::default(),
        nodes,
        payloads,
    )?;
    crdt.replay_from_storage_with_materialization(&mut index)?;

    let seq = crdt.log_len().min(u64::MAX as usize) as u64;
    if let Some(last) = crdt.head_op() {
        update_tree_meta_head(
            client,
            doc_id,
            last.meta.lamport,
            last.meta.id.replica.as_bytes(),
            last.meta.id.counter,
            seq,
        )?;
    } else {
        update_tree_meta_head(client, doc_id, 0, &[], 0, 0)?;
    }

    Ok(())
}

pub fn max_lamport(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<Lamport> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT COALESCE(MAX(lamport), 0) FROM treecrdt_ops WHERE doc_id = $1",
            &[&doc_id],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    let row = rows.first().ok_or_else(|| Error::Storage("missing MAX(lamport) row".into()))?;
    Ok(row.get::<_, i64>(0).max(0) as Lamport)
}

pub fn list_op_refs_all(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
) -> Result<Vec<[u8; OPREF_V0_WIDTH]>> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT op_ref FROM treecrdt_ops WHERE doc_id = $1 ORDER BY lamport, replica, counter",
            &[&doc_id],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let bytes: Vec<u8> = row.get(0);
        out.push(op_ref_from_bytes(&bytes)?);
    }
    Ok(out)
}

pub fn list_op_refs_children(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
) -> Result<Vec<[u8; OPREF_V0_WIDTH]>> {
    ensure_materialized(client, doc_id)?;
    let parent_bytes = node_to_bytes(parent);
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT op_ref FROM treecrdt_oprefs_children WHERE doc_id = $1 AND parent = $2 ORDER BY seq",
            &[&doc_id, &parent_bytes.as_slice()],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let bytes: Vec<u8> = row.get(0);
        out.push(op_ref_from_bytes(&bytes)?);
    }
    Ok(out)
}

pub fn get_ops_by_op_refs(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    op_refs: &[[u8; OPREF_V0_WIDTH]],
) -> Result<Vec<Operation>> {
    ensure_doc_meta(client, doc_id)?;
    if op_refs.is_empty() {
        return Ok(Vec::new());
    }

    let refs: Vec<Vec<u8>> = op_refs.iter().map(|r| r.to_vec()).collect();

    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT \
              i.ord, \
              o.lamport, o.replica, o.counter, o.kind, o.parent, o.node, o.new_parent, o.order_key, o.payload, o.known_state \
             FROM unnest($2::bytea[]) WITH ORDINALITY AS i(op_ref, ord) \
             LEFT JOIN treecrdt_ops o \
               ON o.doc_id = $1 AND o.op_ref = i.op_ref \
             ORDER BY i.ord ASC",
            &[&doc_id, &refs],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let kind: Option<String> = row.get(4);
        if kind.is_none() {
            return Err(Error::Storage("opRef missing locally".into()));
        }
        out.push(row_to_op_at(&row, 1)?);
    }
    Ok(out)
}

pub fn ops_since(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    lamport: Lamport,
    root: Option<NodeId>,
) -> Result<Vec<Operation>> {
    ensure_doc_meta(client, doc_id)?;
    let root_bytes: Option<Vec<u8>> = root.map(|n| node_to_bytes(n).to_vec());
    let mut c = client.borrow_mut();
    let rows = c
        .query(
             "SELECT lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state \
              FROM treecrdt_ops \
              WHERE doc_id = $1 AND lamport > $2 \
               AND ($3::bytea IS NULL OR parent = $3 OR node = $3 OR new_parent = $3) \
              ORDER BY lamport, replica, counter",
             &[&doc_id, &(lamport as i64), &root_bytes],
         )
        .map_err(storage_debug)?;
    rows.into_iter().map(row_to_op).collect()
}

#[derive(Clone, Debug)]
pub struct TreeChildRow {
    pub node: NodeId,
    pub order_key: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct TreeRow {
    pub node: NodeId,
    pub parent: Option<NodeId>,
    pub order_key: Option<Vec<u8>>,
    pub tombstone: bool,
}

pub fn tree_children(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
) -> Result<Vec<NodeId>> {
    ensure_materialized(client, doc_id)?;
    if parent == NodeId::TRASH {
        return Ok(Vec::new());
    }
    let parent_bytes = node_to_bytes(parent);
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT node FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE \
             ORDER BY order_key, node",
            &[&doc_id, &parent_bytes.as_slice()],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let node: Vec<u8> = row.get(0);
        out.push(bytes_to_node(&node)?);
    }
    Ok(out)
}

pub fn tree_children_page(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
    cursor: Option<(Vec<u8>, Vec<u8>)>,
    limit: u32,
) -> Result<Vec<TreeChildRow>> {
    ensure_materialized(client, doc_id)?;
    if parent == NodeId::TRASH {
        return Ok(Vec::new());
    }
    let parent_bytes = node_to_bytes(parent);
    let after_order_key: Option<Vec<u8>> = cursor.as_ref().map(|(k, _n)| k.clone());
    let after_node: Option<Vec<u8>> = cursor.as_ref().map(|(_k, n)| n.clone());

    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT node, order_key \
             FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE \
               AND ($3 IS NULL OR (order_key > $3 OR (order_key = $3 AND node > $4))) \
             ORDER BY order_key, node \
             LIMIT $5",
            &[
                &doc_id,
                &parent_bytes.as_slice(),
                &after_order_key,
                &after_node,
                &(limit as i64),
            ],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let node: Vec<u8> = row.get(0);
        let order_key: Option<Vec<u8>> = row.get(1);
        out.push(TreeChildRow {
            node: bytes_to_node(&node)?,
            order_key,
        });
    }
    Ok(out)
}

pub fn tree_dump(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<Vec<TreeRow>> {
    ensure_materialized(client, doc_id)?;
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT node, parent, order_key, tombstone \
             FROM treecrdt_nodes \
             WHERE doc_id = $1 \
             ORDER BY node",
            &[&doc_id],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let node: Vec<u8> = row.get(0);
        let parent: Option<Vec<u8>> = row.get(1);
        let order_key: Option<Vec<u8>> = row.get(2);
        let tombstone: bool = row.get(3);
        out.push(TreeRow {
            node: bytes_to_node(&node)?,
            parent: match parent {
                None => None,
                Some(b) => Some(bytes_to_node(&b)?),
            },
            order_key,
            tombstone,
        });
    }
    Ok(out)
}

pub fn tree_node_count(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<u64> {
    ensure_materialized(client, doc_id)?;
    let root_bytes = node_to_bytes(NodeId::ROOT);
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT COUNT(*) FROM treecrdt_nodes \
             WHERE doc_id = $1 AND tombstone = FALSE AND node <> $2",
            &[&doc_id, &root_bytes.as_slice()],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    let row = rows.first().ok_or_else(|| Error::Storage("missing COUNT(*) row".into()))?;
    Ok(row.get::<_, i64>(0).max(0) as u64)
}

pub fn replica_max_counter(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &[u8],
) -> Result<u64> {
    ensure_doc_meta(client, doc_id)?;
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT COALESCE(MAX(counter), 0) FROM treecrdt_ops WHERE doc_id = $1 AND replica = $2",
            &[&doc_id, &replica],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    let row = rows.first().ok_or_else(|| Error::Storage("missing MAX(counter) row".into()))?;
    Ok(row.get::<_, i64>(0).max(0) as u64)
}

fn seed(replica: &[u8], counter: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(replica.len() + 8);
    out.extend_from_slice(replica);
    out.extend_from_slice(&counter.to_be_bytes());
    out
}

fn invalid_op_error(msg: &str) -> Error {
    Error::InvalidOperation(msg.to_string())
}

fn next_local_counter_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &[u8],
) -> Result<u64> {
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT COALESCE(MAX(counter), 0) FROM treecrdt_ops WHERE doc_id = $1 AND replica = $2",
            &[&doc_id, &replica],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    let row = rows.first().ok_or_else(|| Error::Storage("missing MAX(counter) row".into()))?;
    Ok(row.get::<_, i64>(0).max(0) as u64 + 1)
}

fn next_local_lamport_in_tx(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<Lamport> {
    Ok(max_lamport(client, doc_id)?.saturating_add(1))
}

fn order_key_for_local_after_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
    after: NodeId,
    exclude: NodeId,
) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
    let parent_bytes = node_to_bytes(parent);
    let after_bytes = node_to_bytes(after);
    let exclude_bytes = node_to_bytes(exclude);

    let mut c = client.borrow_mut();
    let left_rows = c
        .query(
            "SELECT order_key FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE AND node = $3 \
             LIMIT 1",
            &[&doc_id, &parent_bytes.as_slice(), &after_bytes.as_slice()],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;

    let left_row = left_rows
        .first()
        .ok_or_else(|| invalid_op_error("after node is not a child of parent"))?;
    let left: Option<Vec<u8>> = left_row.get(0);
    let Some(left) = left else {
        return Err(Error::Storage("after node missing order_key".into()));
    };

    let right_rows = c
        .query(
            "SELECT order_key FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE \
               AND node <> $3 \
               AND (order_key > $4 OR (order_key = $4 AND node > $5)) \
             ORDER BY order_key, node \
             LIMIT 1",
            &[
                &doc_id,
                &parent_bytes.as_slice(),
                &exclude_bytes.as_slice(),
                &left,
                &after_bytes.as_slice(),
            ],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;

    let right = right_rows.first().and_then(|row| row.get::<_, Option<Vec<u8>>>(0));
    Ok((left, right))
}

fn order_key_for_local_first_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
    exclude: NodeId,
) -> Result<Option<Vec<u8>>> {
    let parent_bytes = node_to_bytes(parent);
    let exclude_bytes = node_to_bytes(exclude);
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT order_key FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE AND node <> $3 \
             ORDER BY order_key, node \
             LIMIT 1",
            &[&doc_id, &parent_bytes.as_slice(), &exclude_bytes.as_slice()],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(rows.first().and_then(|row| row.get::<_, Option<Vec<u8>>>(0)))
}

fn order_key_for_local_last_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
    exclude: NodeId,
) -> Result<Option<Vec<u8>>> {
    let parent_bytes = node_to_bytes(parent);
    let exclude_bytes = node_to_bytes(exclude);
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT order_key FROM treecrdt_nodes \
             WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE AND node <> $3 \
             ORDER BY order_key DESC, node DESC \
             LIMIT 1",
            &[&doc_id, &parent_bytes.as_slice(), &exclude_bytes.as_slice()],
        )
        .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(rows.first().and_then(|row| row.get::<_, Option<Vec<u8>>>(0)))
}

fn allocate_local_order_key_in_tx(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
    node: NodeId,
    placement: &str,
    after: Option<NodeId>,
    seed: &[u8],
) -> Result<Vec<u8>> {
    if parent == NodeId::TRASH {
        return Ok(Vec::new());
    }

    // Mirror `TreeCrdt::allocate_child_key_after`: always exclude `node` when computing siblings.
    let exclude = node;

    let (left, right): (Option<Vec<u8>>, Option<Vec<u8>>) = match placement {
        "first" => (
            None,
            order_key_for_local_first_in_tx(client, doc_id, parent, exclude)?,
        ),
        "last" => (
            order_key_for_local_last_in_tx(client, doc_id, parent, exclude)?,
            None,
        ),
        "after" => {
            let Some(after) = after else {
                return Err(invalid_op_error("missing after for placement=after"));
            };
            if after == exclude {
                return Err(invalid_op_error("after cannot be excluded node"));
            }
            let (l, r) = order_key_for_local_after_in_tx(client, doc_id, parent, after, exclude)?;
            (Some(l), r)
        }
        _ => return Err(invalid_op_error("invalid placement")),
    };

    treecrdt_core::order_key::allocate_between(left.as_deref(), right.as_deref(), seed)
}

#[allow(clippy::too_many_arguments)]
pub fn local_insert(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    parent: NodeId,
    node: NodeId,
    placement: &str,
    after: Option<NodeId>,
    payload: Option<Vec<u8>>,
) -> Result<Operation> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = (|| -> Result<Operation> {
        ensure_materialized_in_tx(client, doc_id)?;
        let counter = next_local_counter_in_tx(client, doc_id, replica.as_bytes())?;
        let lamport = next_local_lamport_in_tx(client, doc_id)?;
        let seed = seed(replica.as_bytes(), counter);
        let order_key =
            allocate_local_order_key_in_tx(client, doc_id, parent, node, placement, after, &seed)?;

        let op = Operation::insert_with_optional_payload(
            replica, counter, lamport, parent, node, order_key, payload,
        );
        let _ = append_ops_in_tx(client, doc_id, std::slice::from_ref(&op))?;
        Ok(op)
    })();

    match res {
        Ok(op) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(op)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

pub fn local_move(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    new_parent: NodeId,
    placement: &str,
    after: Option<NodeId>,
) -> Result<Operation> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = (|| -> Result<Operation> {
        ensure_materialized_in_tx(client, doc_id)?;
        let counter = next_local_counter_in_tx(client, doc_id, replica.as_bytes())?;
        let lamport = next_local_lamport_in_tx(client, doc_id)?;
        let seed = seed(replica.as_bytes(), counter);
        let order_key = allocate_local_order_key_in_tx(
            client, doc_id, new_parent, node, placement, after, &seed,
        )?;

        let op = Operation::move_node(replica, counter, lamport, node, new_parent, order_key);
        let _ = append_ops_in_tx(client, doc_id, std::slice::from_ref(&op))?;
        Ok(op)
    })();

    match res {
        Ok(op) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(op)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

pub fn local_delete(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
) -> Result<Operation> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = (|| -> Result<Operation> {
        ensure_materialized_in_tx(client, doc_id)?;
        let counter = next_local_counter_in_tx(client, doc_id, replica.as_bytes())?;
        let lamport = next_local_lamport_in_tx(client, doc_id)?;

        let ctx = PgCtx::new(client.clone(), doc_id)?;
        let nodes = PgNodeStore::new(ctx);
        let known_state = Some(nodes.subtree_version_vector(node)?);

        let op = Operation::delete(replica, counter, lamport, node, known_state);
        let _ = append_ops_in_tx(client, doc_id, std::slice::from_ref(&op))?;
        Ok(op)
    })();

    match res {
        Ok(op) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(op)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}

pub fn local_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &ReplicaId,
    node: NodeId,
    payload: Option<Vec<u8>>,
) -> Result<Operation> {
    {
        let mut c = client.borrow_mut();
        c.batch_execute("BEGIN").map_err(|e| Error::Storage(e.to_string()))?;
    }

    let res = (|| -> Result<Operation> {
        ensure_materialized_in_tx(client, doc_id)?;
        let counter = next_local_counter_in_tx(client, doc_id, replica.as_bytes())?;
        let lamport = next_local_lamport_in_tx(client, doc_id)?;

        let op = Operation::payload(replica, counter, lamport, node, payload);
        let _ = append_ops_in_tx(client, doc_id, std::slice::from_ref(&op))?;
        Ok(op)
    })();

    match res {
        Ok(op) => {
            let mut c = client.borrow_mut();
            c.batch_execute("COMMIT").map_err(|e| Error::Storage(e.to_string()))?;
            Ok(op)
        }
        Err(e) => {
            let mut c = client.borrow_mut();
            let _ = c.batch_execute("ROLLBACK");
            Err(e)
        }
    }
}
