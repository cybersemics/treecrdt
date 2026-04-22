use postgres::{Client, GenericClient};
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
"#;

fn configure_fast_test_tx(client: &mut Client) -> Result<()> {
    client
        .batch_execute(
            "SET LOCAL synchronous_commit = OFF;
             SET LOCAL statement_timeout = 0;",
        )
        .map_err(|e| Error::Storage(format!("{e:?}")))?;
    Ok(())
}

fn reset_doc_for_tests_in_tx(client: &mut impl GenericClient, doc_id: &str) -> Result<()> {
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
    client.batch_execute("BEGIN").map_err(|e| Error::Storage(format!("{e:?}")))?;
    let res = (|| {
        configure_fast_test_tx(client)?;
        reset_doc_for_tests_in_tx(client, doc_id)
    })();

    match res {
        Ok(()) => client.batch_execute("COMMIT").map_err(|e| Error::Storage(format!("{e:?}"))),
        Err(err) => {
            let _ = client.batch_execute("ROLLBACK");
            Err(err)
        }
    }
}

pub fn clone_doc_for_tests(
    client: &mut Client,
    source_doc_id: &str,
    target_doc_id: &str,
) -> Result<()> {
    if source_doc_id == target_doc_id {
        return Err(Error::Storage(
            "source_doc_id and target_doc_id must differ".to_string(),
        ));
    }

    let mut tx = client.transaction().map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.batch_execute(
        "SET LOCAL synchronous_commit = OFF;
         SET LOCAL statement_timeout = 0;",
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;

    tx.execute(
        "DELETE FROM treecrdt_oprefs_children WHERE doc_id = $1",
        &[&target_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "DELETE FROM treecrdt_payload WHERE doc_id = $1",
        &[&target_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "DELETE FROM treecrdt_nodes WHERE doc_id = $1",
        &[&target_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "DELETE FROM treecrdt_ops WHERE doc_id = $1",
        &[&target_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "DELETE FROM treecrdt_meta WHERE doc_id = $1",
        &[&target_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "DELETE FROM treecrdt_replica_meta WHERE doc_id = $1",
        &[&target_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;

    tx.execute(
        "INSERT INTO treecrdt_meta
           (doc_id, head_lamport, head_replica, head_counter, head_seq,
            replay_lamport, replay_replica, replay_counter)
         SELECT $1, head_lamport, head_replica, head_counter, head_seq,
                replay_lamport, replay_replica, replay_counter
           FROM treecrdt_meta
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_replica_meta (doc_id, replica, max_counter)
         SELECT $1, replica, max_counter
           FROM treecrdt_replica_meta
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_ops
           (doc_id, op_ref, lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state)
         SELECT $1, op_ref, lamport, replica, counter, kind, parent, node, new_parent, order_key, payload, known_state
           FROM treecrdt_ops
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_nodes
           (doc_id, node, parent, order_key, tombstone, last_change, deleted_at)
         SELECT $1, node, parent, order_key, tombstone, last_change, deleted_at
           FROM treecrdt_nodes
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_payload
           (doc_id, node, payload, last_lamport, last_replica, last_counter)
         SELECT $1, node, payload, last_lamport, last_replica, last_counter
           FROM treecrdt_payload
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_oprefs_children
           (doc_id, parent, op_ref, seq)
         SELECT $1, parent, op_ref, seq
           FROM treecrdt_oprefs_children
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;

    let copied = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM treecrdt_meta WHERE doc_id = $1)",
            &[&target_doc_id],
        )
        .map_err(|e| Error::Storage(format!("{e:?}")))?
        .get::<_, bool>(0);
    if !copied {
        return Err(Error::Storage(format!(
            "source doc not found for clone: {source_doc_id}"
        )));
    }

    tx.commit().map_err(|e| Error::Storage(format!("{e:?}")))?;
    Ok(())
}

pub fn clone_materialized_doc_for_tests(
    client: &mut Client,
    source_doc_id: &str,
    target_doc_id: &str,
) -> Result<()> {
    if source_doc_id == target_doc_id {
        return Err(Error::Storage(
            "source_doc_id and target_doc_id must differ".to_string(),
        ));
    }

    let mut tx = client.transaction().map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.batch_execute(
        "SET LOCAL synchronous_commit = OFF;
         SET LOCAL statement_timeout = 0;",
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;

    reset_doc_for_tests_in_tx(&mut tx, target_doc_id)?;

    tx.execute(
        "INSERT INTO treecrdt_meta
           (doc_id, head_lamport, head_replica, head_counter, head_seq,
            replay_lamport, replay_replica, replay_counter)
         SELECT $1, head_lamport, head_replica, head_counter, head_seq,
                replay_lamport, replay_replica, replay_counter
           FROM treecrdt_meta
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_replica_meta (doc_id, replica, max_counter)
         SELECT $1, replica, max_counter
           FROM treecrdt_replica_meta
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_nodes
           (doc_id, node, parent, order_key, tombstone, last_change, deleted_at)
         SELECT $1, node, parent, order_key, tombstone, last_change, deleted_at
           FROM treecrdt_nodes
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;
    tx.execute(
        "INSERT INTO treecrdt_payload
           (doc_id, node, payload, last_lamport, last_replica, last_counter)
         SELECT $1, node, payload, last_lamport, last_replica, last_counter
           FROM treecrdt_payload
          WHERE doc_id = $2",
        &[&target_doc_id, &source_doc_id],
    )
    .map_err(|e| Error::Storage(format!("{e:?}")))?;

    let copied = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM treecrdt_meta WHERE doc_id = $1)",
            &[&target_doc_id],
        )
        .map_err(|e| Error::Storage(format!("{e:?}")))?
        .get::<_, bool>(0);
    if !copied {
        return Err(Error::Storage(format!(
            "source doc not found for materialized clone: {source_doc_id}"
        )));
    }

    tx.commit().map_err(|e| Error::Storage(format!("{e:?}")))?;
    Ok(())
}
