use rusqlite::{params, Connection, Row};
use treecrdt_core::{
    error::Error,
    ops::{Operation, OperationKind},
    traits::Snapshot,
    Lamport, NodeId, OperationId, ReplicaId, Storage,
};

/// SQLite-backed `Storage` implementation that persists operations in an op-log table.
pub struct SqliteStorage {
    conn: Connection,
}

impl SqliteStorage {
    pub fn new_in_memory() -> treecrdt_core::Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| Error::Storage(e.to_string()))?;
        let mut storage = Self { conn };
        storage.ensure_schema()?;
        Ok(storage)
    }

    pub fn new(path: &str) -> treecrdt_core::Result<Self> {
        let conn = Connection::open(path).map_err(|e| Error::Storage(e.to_string()))?;
        let mut storage = Self { conn };
        storage.ensure_schema()?;
        Ok(storage)
    }

    fn ensure_schema(&mut self) -> treecrdt_core::Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS ops (
                    replica BLOB NOT NULL,
                    counter INTEGER NOT NULL,
                    lamport INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    parent BLOB,
                    node BLOB NOT NULL,
                    new_parent BLOB,
                    position INTEGER,
                    payload BLOB,
                    PRIMARY KEY (replica, counter)
                );
                CREATE INDEX IF NOT EXISTS idx_ops_lamport ON ops(lamport, replica, counter);",
            )
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }
}

impl Storage for SqliteStorage {
    fn apply(&mut self, op: Operation) -> treecrdt_core::Result<()> {
        let (kind, parent, node, new_parent, position, payload) = match op.kind {
            OperationKind::Insert {
                parent,
                node,
                position,
                payload,
            } => ("insert", Some(parent), node, None, Some(position), payload),
            OperationKind::Move {
                node,
                new_parent,
                position,
            } => ("move", None, node, Some(new_parent), Some(position), None),
            OperationKind::Delete { node } => ("delete", None, node, None, None, None),
            OperationKind::Tombstone { node } => ("tombstone", None, node, None, None, None),
            OperationKind::Payload { node, payload } => {
                ("payload", None, node, None, None, payload)
            }
        };

        let lamport: i64 = op
            .meta
            .lamport
            .try_into()
            .map_err(|_| Error::Storage("lamport overflow".into()))?;
        let counter: i64 = op
            .meta
            .id
            .counter
            .try_into()
            .map_err(|_| Error::Storage("counter overflow".into()))?;

        self.conn
            .execute(
                "INSERT OR IGNORE INTO ops (replica, counter, lamport, kind, parent, node, new_parent, position, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    op.meta.id.replica.as_bytes(),
                    counter,
                    lamport,
                    kind,
                    parent.map(node_to_blob),
                    node_to_blob(node),
                    new_parent.map(node_to_blob),
                    position
                        .map(|p| i64::try_from(p).map_err(|_| Error::Storage("position overflow".into())))
                        .transpose()?,
                    payload,
                ],
            )
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    fn load_since(&self, lamport: Lamport) -> treecrdt_core::Result<Vec<Operation>> {
        let l: i64 = lamport.try_into().map_err(|_| Error::Storage("lamport overflow".into()))?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT replica, counter, lamport, kind, parent, node, new_parent, position, payload
                 FROM ops
                 WHERE lamport > ?
                 ORDER BY lamport ASC, replica ASC, counter ASC",
            )
            .map_err(|e| Error::Storage(e.to_string()))?;

        let rows = stmt
            .query_map([l], row_to_operation)
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut ops = Vec::new();
        for op in rows {
            ops.push(op.map_err(|e| Error::Storage(e.to_string()))?);
        }
        Ok(ops)
    }

    fn latest_lamport(&self) -> Lamport {
        let mut stmt = self
            .conn
            .prepare("SELECT MAX(lamport) FROM ops")
            .expect("prepare latest lamport");
        let val: Option<i64> = stmt.query_row([], |row| row.get(0)).unwrap_or(None);
        val.and_then(|v| u64::try_from(v).ok()).unwrap_or_default()
    }

    fn snapshot(&self) -> treecrdt_core::Result<Snapshot> {
        Ok(Snapshot {
            head: self.latest_lamport(),
        })
    }
}

fn row_to_operation(row: &Row<'_>) -> rusqlite::Result<Operation> {
    let replica: Vec<u8> = row.get(0)?;
    let counter: i64 = row.get(1)?;
    let lamport: i64 = row.get(2)?;
    let kind: String = row.get(3)?;
    let parent: Option<Vec<u8>> = row.get(4)?;
    let node: Vec<u8> = row.get(5)?;
    let new_parent: Option<Vec<u8>> = row.get(6)?;
    let position: Option<i64> = row.get(7)?;
    let payload: Option<Vec<u8>> = row.get(8)?;

    let op_id = OperationId {
        replica: ReplicaId::new(replica),
        counter: counter as u64,
    };

    let kind = match kind.as_str() {
        "insert" => OperationKind::Insert {
            parent: blob_to_node(parent.ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    4,
                    "parent".to_string(),
                    rusqlite::types::Type::Blob,
                )
            })?)?,
            node: blob_to_node(node)?,
            position: position.unwrap_or(0) as usize,
            payload,
        },
        "move" => OperationKind::Move {
            node: blob_to_node(node)?,
            new_parent: blob_to_node(new_parent.ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    6,
                    "new_parent".to_string(),
                    rusqlite::types::Type::Blob,
                )
            })?)?,
            position: position.unwrap_or(0) as usize,
        },
        "delete" => OperationKind::Delete {
            node: blob_to_node(node)?,
        },
        "tombstone" => OperationKind::Tombstone {
            node: blob_to_node(node)?,
        },
        "payload" => OperationKind::Payload {
            node: blob_to_node(node)?,
            payload,
        },
        other => {
            return Err(rusqlite::Error::InvalidColumnType(
                3,
                other.to_string(),
                rusqlite::types::Type::Text,
            ))
        }
    };

    Ok(Operation {
        meta: treecrdt_core::OperationMetadata {
            id: op_id,
            lamport: lamport as u64,
            known_state: None,
        },
        kind,
    })
}

fn node_to_blob(node: NodeId) -> Vec<u8> {
    node.0.to_be_bytes().to_vec()
}

fn blob_to_node(data: Vec<u8>) -> rusqlite::Result<NodeId> {
    if data.len() != 16 {
        return Err(rusqlite::Error::InvalidColumnType(
            0,
            "node".to_string(),
            rusqlite::types::Type::Blob,
        ));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&data);
    Ok(NodeId(u128::from_be_bytes(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use treecrdt_core::{LamportClock, TreeCrdt};

    #[test]
    fn apply_and_load_round_trip() {
        let mut storage = SqliteStorage::new_in_memory().unwrap();
        let replica = ReplicaId::new(b"r1");
        let insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), 0);
        let mov = Operation::move_node(&replica, 2, 2, NodeId(1), NodeId::ROOT, 0);
        let del = Operation::delete(&replica, 3, 3, NodeId(1), None);

        storage.apply(insert.clone()).unwrap();
        storage.apply(mov.clone()).unwrap();
        storage.apply(del.clone()).unwrap();

        let ops = storage.load_since(0).unwrap();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].kind, insert.kind);
        assert_eq!(ops[1].kind, mov.kind);
        assert_eq!(ops[2].kind, del.kind);
        assert_eq!(storage.latest_lamport(), 3);
    }

    #[test]
    fn snapshot_reflects_latest_lamport() {
        let mut storage = SqliteStorage::new_in_memory().unwrap();
        let replica = ReplicaId::new(b"r1");
        storage
            .apply(Operation::insert(
                &replica,
                1,
                10,
                NodeId::ROOT,
                NodeId(1),
                0,
            ))
            .unwrap();
        let snap = storage.snapshot().unwrap();
        assert_eq!(snap.head, 10);
    }

    #[test]
    fn tree_replay_with_sqlite_storage() {
        let mut storage = SqliteStorage::new_in_memory().unwrap();
        let replica = ReplicaId::new(b"r1");
        let parent = NodeId(2);
        let child = NodeId(3);

        // Persist operations out of order.
        storage.apply(Operation::move_node(&replica, 3, 3, child, parent, 0)).unwrap();
        storage
            .apply(Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, 0))
            .unwrap();
        storage
            .apply(Operation::insert(&replica, 2, 2, NodeId::ROOT, child, 0))
            .unwrap();

        let mut crdt = TreeCrdt::new(replica, storage, LamportClock::default());
        crdt.replay_from_storage().unwrap();

        assert_eq!(crdt.parent(child).unwrap(), Some(parent));
        assert_eq!(crdt.children(parent).unwrap(), &[child]);
    }
}
