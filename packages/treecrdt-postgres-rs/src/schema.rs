use postgres::Client;
use treecrdt_core::{Error, Result};

const SCHEMA_LOCK_KEY: i64 = 0x7472656563726474; // "treecrdt"

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS treecrdt_ops (
  doc_id TEXT NOT NULL,
  op_ref BYTEA NOT NULL,
  lamport BIGINT NOT NULL,
  replica BYTEA NOT NULL,
  counter BIGINT NOT NULL,
  kind TEXT NOT NULL,
  parent BYTEA,
  node BYTEA NOT NULL,
  new_parent BYTEA,
  order_key BYTEA,
  payload BYTEA,
  known_state BYTEA,
  PRIMARY KEY (doc_id, op_ref),
  UNIQUE (doc_id, replica, counter)
);

CREATE INDEX IF NOT EXISTS idx_treecrdt_ops_doc_order
  ON treecrdt_ops (doc_id, lamport, replica, counter);

CREATE INDEX IF NOT EXISTS idx_treecrdt_ops_doc_node_kind_order
  ON treecrdt_ops (doc_id, node, kind, lamport, replica, counter);

CREATE TABLE IF NOT EXISTS treecrdt_meta (
  doc_id TEXT PRIMARY KEY,
  head_lamport BIGINT NOT NULL DEFAULT 0,
  head_replica BYTEA NOT NULL DEFAULT ''::bytea,
  head_counter BIGINT NOT NULL DEFAULT 0,
  head_seq BIGINT NOT NULL DEFAULT 0,
  replay_lamport BIGINT,
  replay_replica BYTEA,
  replay_counter BIGINT
);

CREATE TABLE IF NOT EXISTS treecrdt_replica_meta (
  doc_id TEXT NOT NULL,
  replica BYTEA NOT NULL,
  max_counter BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (doc_id, replica)
);

CREATE TABLE IF NOT EXISTS treecrdt_nodes (
  doc_id TEXT NOT NULL,
  node BYTEA NOT NULL,
  parent BYTEA,
  order_key BYTEA,
  tombstone BOOLEAN NOT NULL DEFAULT FALSE,
  last_change BYTEA,
  deleted_at BYTEA,
  PRIMARY KEY (doc_id, node)
);

CREATE INDEX IF NOT EXISTS idx_treecrdt_nodes_doc_parent
  ON treecrdt_nodes (doc_id, parent, order_key, node);

CREATE TABLE IF NOT EXISTS treecrdt_payload (
  doc_id TEXT NOT NULL,
  node BYTEA NOT NULL,
  payload BYTEA,
  last_lamport BIGINT NOT NULL,
  last_replica BYTEA NOT NULL,
  last_counter BIGINT NOT NULL,
  PRIMARY KEY (doc_id, node)
);

CREATE TABLE IF NOT EXISTS treecrdt_oprefs_children (
  doc_id TEXT NOT NULL,
  parent BYTEA NOT NULL,
  op_ref BYTEA NOT NULL,
  seq BIGINT NOT NULL,
  PRIMARY KEY (doc_id, parent, op_ref)
);

CREATE INDEX IF NOT EXISTS idx_treecrdt_oprefs_children_doc_parent_seq
  ON treecrdt_oprefs_children (doc_id, parent, seq);

CREATE OR REPLACE FUNCTION treecrdt_update_replica_meta_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
  INSERT INTO treecrdt_replica_meta (doc_id, replica, max_counter)
  SELECT doc_id, replica, MAX(counter)
  FROM treecrdt_inserted_ops
  GROUP BY doc_id, replica
  ON CONFLICT (doc_id, replica) DO UPDATE
  SET max_counter = GREATEST(treecrdt_replica_meta.max_counter, EXCLUDED.max_counter)
  WHERE treecrdt_replica_meta.max_counter < EXCLUDED.max_counter;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS treecrdt_ops_replica_meta_after_insert ON treecrdt_ops;
CREATE TRIGGER treecrdt_ops_replica_meta_after_insert
AFTER INSERT ON treecrdt_ops
REFERENCING NEW TABLE AS treecrdt_inserted_ops
FOR EACH STATEMENT
EXECUTE FUNCTION treecrdt_update_replica_meta_after_insert();
"#;

pub fn ensure_schema(client: &mut Client) -> Result<()> {
    // `CREATE TABLE IF NOT EXISTS` is not fully concurrency-safe in Postgres; concurrent calls can
    // still fail with catalog uniqueness violations. Serialize schema creation across processes.
    client
        .query_one("SELECT pg_advisory_lock($1)", &[&SCHEMA_LOCK_KEY])
        .map_err(|e| Error::Storage(format!("{e:?}")))?;

    let res = client.batch_execute(SCHEMA_SQL).map_err(|e| Error::Storage(format!("{e:?}")));

    // Best-effort unlock. Locks are also released when the connection is dropped.
    let _ = client.query_one("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK_KEY]);

    res
}

pub fn reset_doc_for_tests(client: &mut Client, doc_id: &str) -> Result<()> {
    client
        .execute(
            "DELETE FROM treecrdt_oprefs_children WHERE doc_id = $1",
            &[&doc_id],
        )
        .map_err(|e| Error::Storage(format!("{e:?}")))?;
    client
        .execute("DELETE FROM treecrdt_payload WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(format!("{e:?}")))?;
    client
        .execute("DELETE FROM treecrdt_nodes WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(format!("{e:?}")))?;
    client
        .execute("DELETE FROM treecrdt_ops WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(format!("{e:?}")))?;
    client
        .execute("DELETE FROM treecrdt_meta WHERE doc_id = $1", &[&doc_id])
        .map_err(|e| Error::Storage(format!("{e:?}")))?;
    client
        .execute(
            "DELETE FROM treecrdt_replica_meta WHERE doc_id = $1",
            &[&doc_id],
        )
        .map_err(|e| Error::Storage(format!("{e:?}")))?;
    Ok(())
}
