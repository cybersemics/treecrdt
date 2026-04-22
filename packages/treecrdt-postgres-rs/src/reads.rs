use std::cell::RefCell;
use std::rc::Rc;

use postgres::Client;

use treecrdt_core::{Error, Lamport, NodeId, Operation, Result};

use crate::opref::{derive_op_ref_v0, OPREF_V0_WIDTH};
use crate::store::{
    bytes_to_node, ensure_doc_meta, ensure_materialized, node_to_bytes, op_ref_from_bytes,
    replica_max_counter_in_tx, row_to_op, row_to_op_at, storage_debug, PgCtx,
};

pub fn max_lamport(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<Lamport> {
    ensure_doc_meta(client, doc_id)?;
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT COALESCE(MAX(lamport), 0) FROM treecrdt_ops WHERE doc_id = $1",
    )?;
    let rows = c.query(&stmt, &[&doc_id]).map_err(storage_debug)?;
    let row = rows.first().ok_or_else(|| Error::Storage("missing MAX(lamport) row".into()))?;
    Ok(row.get::<_, i64>(0).max(0) as Lamport)
}

pub fn list_op_refs_all(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
) -> Result<Vec<[u8; OPREF_V0_WIDTH]>> {
    ensure_doc_meta(client, doc_id)?;
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT op_ref FROM treecrdt_ops WHERE doc_id = $1 ORDER BY lamport, replica, counter",
    )?;
    let rows = c.query(&stmt, &[&doc_id]).map_err(storage_debug)?;
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
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let parent_bytes = node_to_bytes(parent);
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT op_ref FROM treecrdt_oprefs_children WHERE doc_id = $1 AND parent = $2 ORDER BY seq",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &parent_bytes.as_slice()]).map_err(storage_debug)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let bytes: Vec<u8> = row.get(0);
        out.push(op_ref_from_bytes(&bytes)?);
    }
    Ok(out)
}

pub fn list_op_refs_children_with_parent_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    parent: NodeId,
) -> Result<Vec<[u8; OPREF_V0_WIDTH]>> {
    ensure_materialized(client, doc_id)?;
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let parent_bytes = node_to_bytes(parent);
    let mut c = client.borrow_mut();

    let child_stmt = ctx.stmt(
        &mut c,
        "SELECT op_ref FROM treecrdt_oprefs_children WHERE doc_id = $1 AND parent = $2 ORDER BY seq",
    )?;
    let child_rows = c
        .query(&child_stmt, &[&doc_id, &parent_bytes.as_slice()])
        .map_err(storage_debug)?;
    let mut out = Vec::with_capacity(child_rows.len() + 1);
    for row in child_rows {
        let bytes: Vec<u8> = row.get(0);
        out.push(op_ref_from_bytes(&bytes)?);
    }

    let payload_stmt = ctx.stmt(
        &mut c,
        "SELECT last_replica, last_counter \
         FROM treecrdt_payload \
         WHERE doc_id = $1 AND node = $2 \
         LIMIT 1",
    )?;
    let payload_rows = c
        .query(&payload_stmt, &[&doc_id, &parent_bytes.as_slice()])
        .map_err(storage_debug)?;
    if let Some(row) = payload_rows.first() {
        let replica: Vec<u8> = row.get(0);
        let counter = row.get::<_, i64>(1).max(0) as u64;
        if counter > 0 {
            let op_ref = derive_op_ref_v0(doc_id, &replica, counter);
            if !out.contains(&op_ref) {
                out.push(op_ref);
            }
            return Ok(out);
        }
    }

    let fallback_stmt = ctx.stmt(
        &mut c,
        "SELECT op_ref \
         FROM treecrdt_ops \
         WHERE doc_id = $1 AND node = $2 \
           AND (kind = 'payload' OR (kind = 'insert' AND payload IS NOT NULL)) \
         ORDER BY lamport DESC, replica DESC, counter DESC \
         LIMIT 1",
    )?;
    let fallback_rows = c
        .query(&fallback_stmt, &[&doc_id, &parent_bytes.as_slice()])
        .map_err(storage_debug)?;
    if let Some(row) = fallback_rows.first() {
        let op_ref: Vec<u8> = row.get(0);
        let op_ref = op_ref_from_bytes(&op_ref)?;
        if !out.contains(&op_ref) {
            out.push(op_ref);
        }
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

    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let refs: Vec<Vec<u8>> = op_refs.iter().map(|r| r.to_vec()).collect();

    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT \
          i.ord, \
          o.lamport, o.replica, o.counter, o.kind, o.parent, o.node, o.new_parent, o.order_key, o.payload, o.known_state \
         FROM unnest($2::bytea[]) WITH ORDINALITY AS i(op_ref, ord) \
         LEFT JOIN treecrdt_ops o \
           ON o.doc_id = $1 AND o.op_ref = i.op_ref \
         ORDER BY i.ord ASC",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &refs]).map_err(storage_debug)?;

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
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let root_bytes: Option<Vec<u8>> = root.map(|n| node_to_bytes(n).to_vec());
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state \
         FROM treecrdt_ops \
         WHERE doc_id = $1 AND lamport > $2 \
           AND ($3::bytea IS NULL OR parent = $3 OR node = $3 OR new_parent = $3) \
         ORDER BY lamport, replica, counter",
    )?;
    let rows = c
        .query(&stmt, &[&doc_id, &(lamport as i64), &root_bytes])
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
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let parent_bytes = node_to_bytes(parent);
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT node FROM treecrdt_nodes \
         WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE \
         ORDER BY order_key, node",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &parent_bytes.as_slice()]).map_err(storage_debug)?;
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
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let parent_bytes = node_to_bytes(parent);
    let after_order_key: Option<Vec<u8>> = cursor.as_ref().map(|(k, _n)| k.clone());
    let after_node: Option<Vec<u8>> = cursor.as_ref().map(|(_k, n)| n.clone());

    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT node, order_key \
         FROM treecrdt_nodes \
         WHERE doc_id = $1 AND parent = $2 AND tombstone = FALSE \
           AND ($3::bytea IS NULL OR (order_key > $3::bytea OR (order_key = $3::bytea AND node > $4::bytea))) \
         ORDER BY order_key, node \
         LIMIT $5",
    )?;
    let rows = c
        .query(
            &stmt,
            &[
                &doc_id,
                &parent_bytes.as_slice(),
                &after_order_key,
                &after_node,
                &(limit as i64),
            ],
        )
        .map_err(storage_debug)?;

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
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT node, parent, order_key, tombstone \
         FROM treecrdt_nodes \
         WHERE doc_id = $1 \
         ORDER BY node",
    )?;
    let rows = c.query(&stmt, &[&doc_id]).map_err(storage_debug)?;

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

pub fn tree_payload(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    node: NodeId,
) -> Result<Option<Vec<u8>>> {
    ensure_materialized(client, doc_id)?;
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let node_bytes = node_to_bytes(node);
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT payload FROM treecrdt_payload WHERE doc_id = $1 AND node = $2 LIMIT 1",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &node_bytes.as_slice()]).map_err(storage_debug)?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let payload: Option<Vec<u8>> = row.get(0);
    Ok(payload)
}

pub fn tree_node_count(client: &Rc<RefCell<Client>>, doc_id: &str) -> Result<u64> {
    ensure_materialized(client, doc_id)?;
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let root_bytes = node_to_bytes(NodeId::ROOT);
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT COUNT(*) FROM treecrdt_nodes \
         WHERE doc_id = $1 AND tombstone = FALSE AND node <> $2",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &root_bytes.as_slice()]).map_err(storage_debug)?;
    let row = rows.first().ok_or_else(|| Error::Storage("missing COUNT(*) row".into()))?;
    Ok(row.get::<_, i64>(0).max(0) as u64)
}

pub fn tree_parent(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    node: NodeId,
) -> Result<Option<NodeId>> {
    ensure_materialized(client, doc_id)?;
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let node_bytes = node_to_bytes(node);
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT parent FROM treecrdt_nodes WHERE doc_id = $1 AND node = $2",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &node_bytes.as_slice()]).map_err(storage_debug)?;
    let row = match rows.first() {
        None => return Ok(None),
        Some(r) => r,
    };
    let parent: Option<Vec<u8>> = row.get(0);
    parent.map(|b| bytes_to_node(&b)).transpose()
}

pub fn tree_exists(client: &Rc<RefCell<Client>>, doc_id: &str, node: NodeId) -> Result<bool> {
    ensure_materialized(client, doc_id)?;
    let ctx = PgCtx::new_assume_doc_meta(client.clone(), doc_id)?;
    let node_bytes = node_to_bytes(node);
    let mut c = client.borrow_mut();
    let stmt = ctx.stmt(
        &mut c,
        "SELECT 1 FROM treecrdt_nodes WHERE doc_id = $1 AND node = $2 AND tombstone = FALSE LIMIT 1",
    )?;
    let rows = c.query(&stmt, &[&doc_id, &node_bytes.as_slice()]).map_err(storage_debug)?;
    Ok(!rows.is_empty())
}

pub fn replica_max_counter(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    replica: &[u8],
) -> Result<u64> {
    ensure_doc_meta(client, doc_id)?;
    replica_max_counter_in_tx(client, doc_id, replica)
}
