#![cfg(feature = "ext-sqlite")]
use std::env;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;
use std::ptr::null_mut;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use treecrdt_core::{
    order_key::allocate_between, MaterializationChange, MaterializationOutcome,
    MaterializationSource, MaterializationSourceOperation, NodeId, Operation, OperationId,
    OperationKind, ReplicaId, VersionVector,
};
use treecrdt_test_support::{
    self as materialization_conformance, MaterializationConformanceHarness,
};

#[derive(Clone, Deserialize, Serialize)]
struct JsonOp {
    replica: Vec<u8>,
    counter: u64,
    lamport: u64,
    kind: String,
    parent: Option<[u8; 16]>,
    node: [u8; 16],
    new_parent: Option<[u8; 16]>,
    order_key: Option<Vec<u8>>,
    known_state: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    if_absent: bool,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonMaterializationOutcome {
    head_seq: u64,
    changes: Vec<JsonMaterializationChange>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum JsonMaterializationChange {
    Insert {
        node: String,
        parent_after: String,
        payload: Option<Vec<u8>>,
        source: Option<JsonMaterializationSource>,
    },
    Move {
        node: String,
        parent_before: Option<String>,
        parent_after: String,
        source: Option<JsonMaterializationSource>,
    },
    Delete {
        node: String,
        parent_before: Option<String>,
        source: Option<JsonMaterializationSource>,
    },
    Restore {
        node: String,
        parent_after: Option<String>,
        payload: Option<Vec<u8>>,
        source: Option<JsonMaterializationSource>,
    },
    Payload {
        node: String,
        payload: Option<Vec<u8>>,
        source: Option<JsonMaterializationSource>,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonMaterializationSource {
    operation: JsonMaterializationSourceOperation,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonMaterializationSourceOperation {
    id: JsonOperationId,
    lamport: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonOperationId {
    replica: Vec<u8>,
    counter: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonLocalOpResult {
    op: JsonOp,
    outcome: JsonMaterializationOutcome,
}

fn node_bytes_from_id(node: NodeId) -> Vec<u8> {
    node.0.to_be_bytes().to_vec()
}

fn bytes_to_node_id(bytes: &[u8]) -> NodeId {
    NodeId(u128::from_be_bytes(bytes.try_into().unwrap()))
}

fn hex_to_node_id(hex: &str) -> NodeId {
    NodeId(u128::from_str_radix(hex, 16).unwrap())
}

fn source_to_core(source: Option<JsonMaterializationSource>) -> Option<MaterializationSource> {
    source.map(|source| MaterializationSource {
        operation: MaterializationSourceOperation {
            id: OperationId {
                replica: ReplicaId::new(source.operation.id.replica),
                counter: source.operation.id.counter,
            },
            lamport: source.operation.lamport,
        },
    })
}

fn json_outcome_to_core(outcome: JsonMaterializationOutcome) -> MaterializationOutcome {
    MaterializationOutcome {
        head_seq: outcome.head_seq,
        changes: outcome
            .changes
            .into_iter()
            .map(|change| match change {
                JsonMaterializationChange::Insert {
                    node,
                    parent_after,
                    payload,
                    source,
                } => MaterializationChange::Insert {
                    node: hex_to_node_id(&node),
                    parent_after: hex_to_node_id(&parent_after),
                    payload,
                    source: source_to_core(source),
                },
                JsonMaterializationChange::Move {
                    node,
                    parent_before,
                    parent_after,
                    source,
                } => MaterializationChange::Move {
                    node: hex_to_node_id(&node),
                    parent_before: parent_before.as_deref().map(hex_to_node_id),
                    parent_after: hex_to_node_id(&parent_after),
                    source: source_to_core(source),
                },
                JsonMaterializationChange::Delete {
                    node,
                    parent_before,
                    source,
                } => MaterializationChange::Delete {
                    node: hex_to_node_id(&node),
                    parent_before: parent_before.as_deref().map(hex_to_node_id),
                    source: source_to_core(source),
                },
                JsonMaterializationChange::Restore {
                    node,
                    parent_after,
                    payload,
                    source,
                } => MaterializationChange::Restore {
                    node: hex_to_node_id(&node),
                    parent_after: parent_after.as_deref().map(hex_to_node_id),
                    payload,
                    source: source_to_core(source),
                },
                JsonMaterializationChange::Payload {
                    node,
                    payload,
                    source,
                } => MaterializationChange::Payload {
                    node: hex_to_node_id(&node),
                    payload,
                    source: source_to_core(source),
                },
            })
            .collect(),
    }
}

fn decode_ops_or_local_result(json: &str) -> Vec<JsonOp> {
    if let Ok(ops) = serde_json::from_str::<Vec<JsonOp>>(json) {
        return ops;
    }
    vec![serde_json::from_str::<JsonLocalOpResult>(json).unwrap().op]
}

fn json_op(op: &Operation) -> JsonOp {
    let if_absent = matches!(
        op.kind,
        OperationKind::Insert {
            if_absent: true,
            ..
        }
    );
    let (kind, parent, node, new_parent, order_key, payload) = match &op.kind {
        OperationKind::Insert {
            parent,
            node,
            order_key,
            payload,
            ..
        } => (
            "insert",
            Some(parent.0.to_be_bytes()),
            node.0.to_be_bytes(),
            None,
            Some(order_key.clone()),
            payload.clone(),
        ),
        OperationKind::Move {
            node,
            new_parent,
            order_key,
        } => (
            "move",
            None,
            node.0.to_be_bytes(),
            Some(new_parent.0.to_be_bytes()),
            Some(order_key.clone()),
            None,
        ),
        OperationKind::Delete { node } => ("delete", None, node.0.to_be_bytes(), None, None, None),
        OperationKind::Tombstone { node } => {
            ("tombstone", None, node.0.to_be_bytes(), None, None, None)
        }
        OperationKind::Payload { node, payload } => (
            "payload",
            None,
            node.0.to_be_bytes(),
            None,
            None,
            payload.clone(),
        ),
    };

    JsonOp {
        replica: op.meta.id.replica.as_bytes().to_vec(),
        counter: op.meta.id.counter,
        lamport: op.meta.lamport,
        kind: kind.into(),
        parent,
        node,
        new_parent,
        order_key,
        known_state: op.meta.known_state.as_ref().map(|vector| vector.encode().unwrap()),
        payload,
        if_absent,
    }
}

fn json_ops(ops: &[Operation]) -> Vec<JsonOp> {
    ops.iter().map(json_op).collect()
}

fn read_tree_meta(conn: &Connection) -> (i64, Vec<u8>, i64, i64) {
    conn.query_row(
        "SELECT head_lamport, head_replica, head_counter, head_seq FROM tree_meta WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
    .unwrap()
}

fn read_replay_frontier(conn: &Connection) -> (Option<i64>, Option<Vec<u8>>, Option<i64>) {
    conn.query_row(
        "SELECT replay_lamport, replay_replica, replay_counter FROM tree_meta WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn append_ops(conn: &Connection, ops: &[JsonOp]) -> (MaterializationOutcome, i64) {
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(ops, &mut encoded).unwrap();
    let affected_json: String = conn
        .query_row(
            "SELECT treecrdt_append_ops(?1)",
            rusqlite::params![encoded],
            |row| row.get(0),
        )
        .unwrap();
    let outcome: JsonMaterializationOutcome = serde_json::from_str(&affected_json).unwrap();
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
    (json_outcome_to_core(outcome), count)
}

fn visible_children(conn: &Connection, parent: &[u8]) -> Vec<Vec<u8>> {
    let parent_arr = <[u8; 16]>::try_from(parent).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT node FROM tree_nodes \
             WHERE parent = ?1 AND tombstone = 0 \
             ORDER BY order_key, node",
        )
        .unwrap();
    let rows = stmt
        .query_map(rusqlite::params![parent_arr], |row| {
            row.get::<_, Vec<u8>>(0)
        })
        .unwrap();
    rows.map(|row| row.unwrap()).collect()
}

fn payload_bytes(conn: &Connection, node: &[u8]) -> Option<Vec<u8>> {
    let node_arr = <[u8; 16]>::try_from(node).unwrap();
    conn.query_row(
        "SELECT payload FROM tree_payload WHERE node = ?1 LIMIT 1",
        rusqlite::params![node_arr],
        |row| row.get(0),
    )
    .unwrap()
}

fn oprefs_children(conn: &Connection, parent: &[u8]) -> Vec<Vec<u8>> {
    let parent_arr = <[u8; 16]>::try_from(parent).unwrap();
    let refs_json: String = conn
        .query_row(
            "SELECT treecrdt_oprefs_children(?1)",
            rusqlite::params![parent_arr],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&refs_json).unwrap()
}

fn ops_by_oprefs(conn: &Connection, refs: &[Vec<u8>]) -> Vec<JsonOp> {
    let refs_json = serde_json::to_string(refs).unwrap();
    let ops_json: String = conn
        .query_row(
            "SELECT treecrdt_ops_by_oprefs(?1)",
            rusqlite::params![refs_json],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&ops_json).unwrap()
}

struct SqliteConformanceHarness {
    conn: Connection,
}

impl MaterializationConformanceHarness for SqliteConformanceHarness {
    fn append_ops(&self, ops: &[Operation]) {
        append_ops(&self.conn, &json_ops(ops));
    }

    fn append_ops_with_materialization_outcome(&self, ops: &[Operation]) -> MaterializationOutcome {
        let (outcome, _) = append_ops(&self.conn, &json_ops(ops));
        outcome
    }

    fn visible_children(&self, parent: NodeId) -> Vec<NodeId> {
        visible_children(&self.conn, &node_bytes_from_id(parent))
            .iter()
            .map(|bytes| bytes_to_node_id(bytes))
            .collect()
    }

    fn payload(&self, node: NodeId) -> Option<Vec<u8>> {
        payload_bytes(&self.conn, &node_bytes_from_id(node))
    }

    fn op_count(&self) -> u64 {
        self.conn
            .query_row("SELECT COUNT(*) FROM ops", [], |row| row.get::<_, i64>(0))
            .unwrap()
            .max(0) as u64
    }

    fn replay_frontier(&self) -> Option<treecrdt_core::MaterializationFrontier> {
        match read_replay_frontier(&self.conn) {
            (Some(lamport), Some(replica), Some(counter)) => {
                Some(treecrdt_core::MaterializationFrontier {
                    lamport: lamport.max(0) as u64,
                    replica,
                    counter: counter.max(0) as u64,
                })
            }
            _ => None,
        }
    }

    fn head_seq(&self) -> u64 {
        let (_, _, _, head_seq) = read_tree_meta(&self.conn);
        head_seq.max(0) as u64
    }

    fn force_replay_from_start(&self) {
        self.conn
            .execute(
                "UPDATE tree_meta \
                 SET replay_lamport = 0, replay_replica = X'', replay_counter = 0 \
                 WHERE id = 1",
                [],
            )
            .unwrap();
    }

    fn ensure_materialized(&self) {
        let _: String = self
            .conn
            .query_row("SELECT treecrdt_ensure_materialized()", [], |row| {
                row.get(0)
            })
            .unwrap();
    }

    fn op_ref_counters_for_parent(&self, parent: NodeId) -> Vec<u64> {
        ops_by_oprefs(
            &self.conn,
            &oprefs_children(&self.conn, &node_bytes_from_id(parent)),
        )
        .iter()
        .map(|op| op.counter)
        .collect()
    }

    fn op_kinds_for_parent(&self, parent: NodeId) -> Vec<String> {
        ops_by_oprefs(
            &self.conn,
            &oprefs_children(&self.conn, &node_bytes_from_id(parent)),
        )
        .iter()
        .map(|op| op.kind.clone())
        .collect()
    }
}

fn setup_conformance_harness() -> SqliteConformanceHarness {
    SqliteConformanceHarness { conn: setup_conn() }
}

#[test]
fn append_and_fetch_ops_via_extension() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);
    let order_key = (1u16).to_be_bytes().to_vec();

    let _: String = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
            rusqlite::params![
                replica,
                1i64,
                1i64,
                "insert",
                parent,
                node,
                order_key.clone()
            ],
            |row| row.get(0),
        )
        .unwrap();

    // Move node to the end again
    let _: String = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, NULL)",
            rusqlite::params![
                b"r1".to_vec(),
                2i64,
                2i64,
                "move",
                node,
                parent,
                order_key.clone()
            ],
            |row| row.get(0),
        )
        .unwrap();

    let json: String =
        conn.query_row("SELECT treecrdt_ops_since(0)", [], |row| row.get(0)).unwrap();

    let ops: Vec<JsonOp> = decode_ops_or_local_result(&json);
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[0].kind, "insert");
    assert_eq!(ops[1].kind, "move");
    assert_eq!(ops[0].order_key, Some(order_key.clone()));
    assert_eq!(ops[1].order_key, Some(order_key.clone()));
    assert_eq!(
        ops[0].parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );
    assert_eq!(ops[0].node, <[u8; 16]>::try_from(node.as_slice()).unwrap());
    assert_eq!(
        ops[1].new_parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );

    // With root filter we still see the same ops when filtering by root.
    let json_filtered: String = conn
        .query_row(
            "SELECT treecrdt_ops_since(0, ?1)",
            rusqlite::params![parent],
            |row| row.get(0),
        )
        .unwrap();
    let filtered: Vec<JsonOp> = decode_ops_or_local_result(&json_filtered);
    assert_eq!(filtered.len(), 2);
}

#[test]
fn local_insert_returns_appended_insert_op() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica.clone(), parent.clone(), node.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let ops: Vec<JsonOp> = decode_ops_or_local_result(&json);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "insert");
    assert_eq!(op.replica, replica);
    assert_eq!(op.counter, 1);
    assert_eq!(op.lamport, 1);
    assert_eq!(
        op.parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );
    assert_eq!(op.node, <[u8; 16]>::try_from(node.as_slice()).unwrap());
    assert!(op.order_key.is_some());

    let seed = {
        let mut s = Vec::new();
        s.extend_from_slice(&replica);
        s.extend_from_slice(&1u64.to_be_bytes());
        s
    };
    let expected = allocate_between(None, None, &seed).expect("allocate_between");
    assert_eq!(op.order_key.as_ref().unwrap(), &expected);

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
    assert_eq!(count, 1);

    let (head_lamport, head_replica, head_counter, head_seq) = read_tree_meta(&conn);
    assert_eq!(head_lamport, 1);
    assert_eq!(head_replica, b"r1".to_vec());
    assert_eq!(head_counter, 1);
    assert_eq!(head_seq, 1);
}

#[test]
fn binary_append_validates_complete_batch_before_ingestion() {
    use ciborium::value::Value;
    use rusqlite::types::Value as SqlValue;

    let conn = setup_conn();
    let (empty, _) = append_ops(&conn, &[]);
    assert!(empty.changes.is_empty());
    let op = Value::Map(
        [
            ("replica", Value::Bytes(b"binary".to_vec())),
            ("counter", Value::Integer(1.into())),
            ("lamport", Value::Integer(1.into())),
            ("kind", Value::Text("insert".into())),
            ("parent", Value::Bytes(node_bytes(0))),
            ("node", Value::Bytes(node_bytes(1))),
            ("order_key", Value::Bytes(vec![0, 255])),
            ("payload", Value::Bytes(Vec::new())),
        ]
        .into_iter()
        .map(|(name, value)| (Value::Text(name.into()), value))
        .collect(),
    );
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&Value::Array(vec![op.clone()]), &mut encoded).unwrap();
    let mut trailing = encoded.clone();
    trailing.push(0);
    let mut invalid_second = Vec::new();
    ciborium::ser::into_writer(&Value::Array(vec![op, Value::Null]), &mut invalid_second).unwrap();

    for invalid in [
        SqlValue::Blob(encoded[..encoded.len() - 1].to_vec()),
        SqlValue::Blob(trailing),
        SqlValue::Blob(invalid_second),
        SqlValue::Blob(Vec::new()),
        SqlValue::Text("[]".into()),
        SqlValue::Null,
        SqlValue::Integer(0),
    ] {
        let result: rusqlite::Result<String> = conn.query_row(
            "SELECT treecrdt_append_ops(?1)",
            rusqlite::params![invalid],
            |row| row.get(0),
        );
        assert!(result.is_err());
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 0);
    }

    let outcome: String = conn
        .query_row(
            "SELECT treecrdt_append_ops(?1)",
            rusqlite::params![encoded],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<JsonMaterializationOutcome>(&outcome).unwrap().head_seq,
        1
    );
    assert_eq!(visible_children(&conn, &node_bytes(0)), vec![node_bytes(1)]);
    assert_eq!(payload_bytes(&conn, &node_bytes(1)), Some(Vec::new()));
}

#[test]
fn remote_append_materializes_only_inserted_ops() {
    let harness = setup_conformance_harness();
    materialization_conformance::append_batch_materializes_only_inserted_ops(&harness);
}

#[test]
fn remote_append_representative_batch_matches_postgres_shape() {
    let harness = setup_conformance_harness();
    materialization_conformance::representative_remote_batch_matches_shape(&harness);
}

#[test]
fn remote_append_out_of_order_catches_up_immediately_from_frontier() {
    let harness = setup_conformance_harness();
    materialization_conformance::out_of_order_append_catches_up_immediately_from_frontier(&harness);
}

#[test]
fn remote_append_out_of_order_losing_payload_skips_replay_frontier() {
    let harness = setup_conformance_harness();
    materialization_conformance::out_of_order_losing_payload_skips_replay_frontier(&harness);
}

#[test]
fn remote_append_out_of_order_move_with_later_payload_catches_up_immediately() {
    let harness = setup_conformance_harness();
    materialization_conformance::out_of_order_move_with_later_payload_catches_up_immediately(
        &harness,
    );
}

#[test]
fn remote_append_out_of_order_insert_and_move_before_head_catches_up_immediately() {
    let harness = setup_conformance_harness();
    materialization_conformance::out_of_order_insert_and_move_before_head_catches_up_immediately(
        &harness,
    );
}

#[test]
fn remote_replayed_conflicting_moves_report_final_parents() {
    for full_replay in [false, true] {
        materialization_conformance::replayed_conflicting_moves_report_final_parents(
            &setup_conformance_harness(),
            full_replay,
        );
    }
}

#[test]
fn remote_replayed_delete_is_reported_once() {
    materialization_conformance::replayed_delete_is_reported_once(&setup_conformance_harness());
}

#[test]
fn remote_late_op_after_rejected_self_move_keeps_accepted_parent() {
    let harness = setup_conformance_harness();
    materialization_conformance::late_op_after_rejected_self_move_keeps_accepted_parent(&harness);
}

#[test]
fn remote_late_op_after_cycle_rejected_move_keeps_accepted_parent() {
    let harness = setup_conformance_harness();
    materialization_conformance::late_op_after_cycle_rejected_move_keeps_accepted_parent(&harness);
}

#[test]
fn remote_append_replay_from_start_frontier_catches_up_immediately() {
    let harness = setup_conformance_harness();
    materialization_conformance::replay_from_start_frontier_catches_up_immediately(&harness);
}

#[test]
fn remote_deferred_recovery_from_replay_frontier_catches_up_on_ensure() {
    let harness = setup_conformance_harness();
    materialization_conformance::deferred_recovery_from_replay_frontier_catches_up_on_ensure(
        &harness,
    );
}

#[test]
fn remote_superseded_payload_gap_does_not_restore_after_replay() {
    let harness = setup_conformance_harness();
    materialization_conformance::superseded_payload_gap_does_not_restore_after_replay(&harness);
}

#[test]
fn remote_append_out_of_order_delete_suffix_falls_back_and_restores_parent() {
    let harness = setup_conformance_harness();
    materialization_conformance::out_of_order_delete_suffix_falls_back_and_restores_parent(
        &harness,
    );
}

#[test]
fn remote_late_create_only_insert_voids_the_shown_one() {
    let harness = setup_conformance_harness();
    materialization_conformance::late_create_only_insert_voids_the_shown_one(&harness);
}

#[test]
fn remote_failed_immediate_catch_up_rolls_back_inserted_ops_and_meta() {
    let conn = setup_conn();

    let replica = ReplicaId::new(b"rollback");
    let second = Operation::insert(
        &replica,
        2,
        2,
        NodeId::ROOT,
        materialization_conformance::node(2),
        materialization_conformance::order_key_from_position(1),
    );
    let first = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        materialization_conformance::node(1),
        materialization_conformance::order_key_from_position(0),
    );

    append_ops(&conn, &json_ops(&[second]));
    conn.execute_batch(
        "CREATE TRIGGER fail_tree_nodes_insert \
         BEFORE INSERT ON tree_nodes \
         BEGIN \
           SELECT RAISE(ROLLBACK, 'forced catch-up failure'); \
         END;",
    )
    .unwrap();

    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&json_ops(&[first]), &mut encoded).unwrap();
    let append_result: rusqlite::Result<String> = conn.query_row(
        "SELECT treecrdt_append_ops(?1)",
        rusqlite::params![encoded],
        |row| row.get(0),
    );
    assert!(append_result.is_err());

    let (_, _, _, head_seq) = read_tree_meta(&conn);
    let op_count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();

    assert_eq!(op_count, 1);
    assert_eq!(read_replay_frontier(&conn), (None, None, None));
    assert_eq!(head_seq, 1);
    assert_eq!(
        visible_children(&conn, &node_bytes(0)),
        vec![node_bytes_from_id(materialization_conformance::node(2))]
    );
}

#[test]
fn local_insert_after_is_deterministic_for_single_gap() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    let node_c = node_bytes(3);

    let key_a = (1u16).to_be_bytes().to_vec();
    let key_b = (3u16).to_be_bytes().to_vec();

    // A(1), B(3)
    for (counter, (node, order_key)) in [(1i64, (&node_a, &key_a)), (2i64, (&node_b, &key_b))] {
        let _: String = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![
                    replica.clone(),
                    counter,
                    counter,
                    "insert",
                    parent,
                    node,
                    order_key
                ],
                |row| row.get(0),
            )
            .unwrap();
    }

    // after(A) between 1 and 3 => 2 (deterministic)
    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'after', ?4, NULL)",
            rusqlite::params![replica, parent, node_c, node_a],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = decode_ops_or_local_result(&json);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "insert");
    assert_eq!(
        op.order_key.as_ref().unwrap(),
        &(2u16).to_be_bytes().to_vec()
    );
}

#[test]
fn local_insert_last_is_deterministic_for_single_gap() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);

    let key_a = (0xfffdu16).to_be_bytes().to_vec();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
            rusqlite::params![replica.clone(), 1i64, 1i64, "insert", parent, node_a, key_a],
            |row| row.get(0),
        )
        .unwrap();

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
            rusqlite::params![replica, parent, node_b],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = decode_ops_or_local_result(&json);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "insert");
    assert_eq!(
        op.order_key.as_ref().unwrap(),
        &(0xfffeu16).to_be_bytes().to_vec()
    );

    let parent_arr = <[u8; 16]>::try_from(parent.as_slice()).unwrap();
    let node_rows: Vec<Vec<u8>> = {
        let mut stmt = conn
            .prepare(
                "SELECT node FROM tree_nodes \
                 WHERE parent = ?1 AND tombstone = 0 \
                 ORDER BY order_key, node",
            )
            .unwrap();
        let rows = stmt
            .query_map(rusqlite::params![parent_arr], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    };
    assert_eq!(node_rows, vec![node_a.clone(), node_b.clone()]);
}

#[test]
fn local_move_allocates_key_excluding_self() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    let node_c = node_bytes(3);

    let key_a = (1u16).to_be_bytes().to_vec();
    let key_b = (3u16).to_be_bytes().to_vec();
    let key_c = (5u16).to_be_bytes().to_vec();

    // A(1), B(3), C(5)
    for (counter, (node, order_key)) in [
        (1i64, (&node_a, &key_a)),
        (2i64, (&node_b, &key_b)),
        (3i64, (&node_c, &key_c)),
    ] {
        let _: String = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![
                    replica.clone(),
                    counter,
                    counter,
                    "insert",
                    parent,
                    node,
                    order_key
                ],
                |row| row.get(0),
            )
            .unwrap();
    }

    // Move B after A, excluding B from sibling scan => allocate between A and C.
    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'after', ?4)",
            rusqlite::params![
                replica.clone(),
                node_b.clone(),
                parent.clone(),
                node_a.clone()
            ],
            |row| row.get(0),
        )
        .unwrap();

    let ops: Vec<JsonOp> = decode_ops_or_local_result(&json);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "move");
    assert_eq!(op.counter, 4);
    assert_eq!(op.lamport, 4);
    assert_eq!(
        op.new_parent,
        Some(<[u8; 16]>::try_from(parent.as_slice()).unwrap())
    );
    assert_eq!(op.node, <[u8; 16]>::try_from(node_b.as_slice()).unwrap());

    let seed = {
        let mut s = Vec::new();
        s.extend_from_slice(&replica);
        s.extend_from_slice(&4u64.to_be_bytes());
        s
    };
    let expected = allocate_between(Some(&key_a), Some(&key_c), &seed).expect("allocate_between");
    assert_eq!(op.order_key.as_ref().unwrap(), &expected);

    let (head_lamport, head_replica, head_counter, head_seq) = read_tree_meta(&conn);
    assert_eq!(head_lamport, 4);
    assert_eq!(head_replica, replica);
    assert_eq!(head_counter, 4);
    assert_eq!(head_seq, 4);
}

#[test]
fn local_move_to_trash_returns_empty_order_key() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);
    let trash = node_bytes(u128::MAX);

    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica.clone(), parent, node.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'first', NULL)",
            rusqlite::params![replica, node, trash],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = decode_ops_or_local_result(&json);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "move");
    assert!(op.order_key.as_ref().unwrap().is_empty());
}

#[test]
fn local_insert_and_move_place_between_bounds() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    // The allocated key, then the node id and a final 1 digit.
    let expected_key = |left: Option<&[u8]>, right: Option<&[u8]>, node: &[u8], counter: u64| {
        let seed = [replica.as_slice(), &counter.to_be_bytes()].concat();
        let key = allocate_between(left, right, &seed).expect("allocate_between");
        [key.as_slice(), node, &[0, 1]].concat()
    };
    let (left, right) = (vec![0x00, 0x05], vec![0x00, 0x09]);

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'between', NULL, NULL, ?4, ?5)",
            rusqlite::params![replica, parent, node_a, left, right],
            |row| row.get(0),
        )
        .unwrap();
    let op = &decode_ops_or_local_result(&json)[0];
    assert_eq!(op.kind, "insert");
    assert_eq!(
        op.order_key,
        Some(expected_key(Some(&left), Some(&right), &node_a, 1))
    );

    // NULL bounds are open ends.
    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'between', NULL, NULL, NULL, NULL)",
            rusqlite::params![replica, parent, node_b],
            |row| row.get(0),
        )
        .unwrap();
    let key_b = decode_ops_or_local_result(&json)[0].order_key.clone().unwrap();
    assert_eq!(key_b, expected_key(None, None, &node_b, 2));
    assert_eq!(
        visible_children(&conn, &parent),
        vec![node_a.clone(), node_b.clone()]
    );

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'between', NULL, ?4, NULL)",
            rusqlite::params![replica, node_a, parent, key_b],
            |row| row.get(0),
        )
        .unwrap();
    let op = &decode_ops_or_local_result(&json)[0];
    assert_eq!(op.kind, "move");
    assert_eq!(
        op.order_key,
        Some(expected_key(Some(&key_b), None, &node_a, 3))
    );
    assert_eq!(visible_children(&conn, &parent), vec![node_b, node_a]);
}

#[test]
fn local_between_rejects_missing_args_and_bounds_without_room() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica, parent, node],
            |row| row.get(0),
        )
        .unwrap();

    let error = |sql: &str| {
        conn.query_row(sql, rusqlite::params![replica, parent, node], |row| {
            row.get::<_, String>(0)
        })
        .unwrap_err()
        .to_string()
    };

    // 'between' needs both bound args.
    for sql in [
        "SELECT treecrdt_local_insert(?1, ?2, ?3, 'between', NULL, NULL)",
        "SELECT treecrdt_local_insert(?1, ?2, ?3, 'between', NULL, NULL, NULL)",
        "SELECT treecrdt_local_move(?1, ?3, ?2, 'between', NULL)",
        "SELECT treecrdt_local_move(?1, ?3, ?2, 'between', NULL, NULL)",
    ] {
        let message = "placement between takes two trailing args (left,right)";
        assert!(error(sql).contains(message), "{sql}: {}", error(sql));
    }
    // Bounds out of order, odd-length, or with no key between them. A zero-length BLOB is a
    // bound, not an open end, and no key is below it.
    for sql in [
        "SELECT treecrdt_local_insert(?1, ?2, ?3, 'between', NULL, NULL, X'0009', X'0005')",
        "SELECT treecrdt_local_move(?1, ?3, ?2, 'between', NULL, X'0005', X'0005')",
        "SELECT treecrdt_local_move(?1, ?3, ?2, 'between', NULL, X'000501', NULL)",
        "SELECT treecrdt_local_move(?1, ?3, ?2, 'between', NULL, X'0005', X'00050000')",
        "SELECT treecrdt_local_move(?1, ?3, ?2, 'between', NULL, NULL, X'')",
    ] {
        error(sql);
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn local_insert_and_move_place_at_a_key() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node_a = node_bytes(1);
    let node_b = node_bytes(2);
    let (key_a, key_b) = (vec![0x00, 0x05], vec![0x00, 0x03]);

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'at', NULL, NULL, ?4)",
            rusqlite::params![replica, parent, node_a, key_a],
            |row| row.get(0),
        )
        .unwrap();
    let op = &decode_ops_or_local_result(&json)[0];
    assert_eq!(op.kind, "insert");
    // The key, then the node id and a final 1 digit.
    assert_eq!(
        op.order_key,
        Some([key_a, node_a.clone(), vec![0x00, 0x01]].concat())
    );

    // Placements by sibling keep the 6-arg call.
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'after', ?4, NULL)",
            rusqlite::params![replica, parent, node_b, node_a],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        visible_children(&conn, &parent),
        vec![node_a.clone(), node_b.clone()]
    );

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'at', NULL, ?4)",
            rusqlite::params![replica, node_b, parent, key_b],
            |row| row.get(0),
        )
        .unwrap();
    let op = &decode_ops_or_local_result(&json)[0];
    assert_eq!(op.kind, "move");
    assert_eq!(
        op.order_key,
        Some([key_b, node_b.clone(), vec![0x00, 0x01]].concat())
    );
    assert_eq!(visible_children(&conn, &parent), vec![node_b, node_a]);
}

#[test]
fn local_at_rejects_wrong_args_and_keys_that_are_not_whole_digits() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica, parent, node],
            |row| row.get(0),
        )
        .unwrap();

    let error = |sql: &str| {
        conn.query_row(sql, rusqlite::params![replica, parent, node], |row| {
            row.get::<_, String>(0)
        })
        .unwrap_err()
        .to_string()
    };

    // 'at' takes exactly one key arg, the placements by sibling take none, and neither function
    // takes more args than 'between' does.
    for (sql, message) in [
        (
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'at', NULL, NULL)",
            "placement at takes one trailing arg (key)",
        ),
        (
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'at', NULL, NULL, X'0005', NULL)",
            "placement at takes one trailing arg (key)",
        ),
        (
            "SELECT treecrdt_local_move(?1, ?3, ?2, 'at', NULL)",
            "placement at takes one trailing arg (key)",
        ),
        (
            "SELECT treecrdt_local_move(?1, ?3, ?2, 'at', NULL, X'0005', NULL)",
            "placement at takes one trailing arg (key)",
        ),
        (
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'at', NULL, NULL, NULL)",
            "placement at needs a key, not NULL",
        ),
        (
            "SELECT treecrdt_local_move(?1, ?3, ?2, 'last', NULL, X'0005')",
            "only placements at and between take trailing args",
        ),
        (
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'at', NULL, NULL, X'0005', NULL, NULL)",
            "treecrdt_local_insert expects 6 args",
        ),
        (
            "SELECT treecrdt_local_move(?1, ?3, ?2, 'at')",
            "treecrdt_local_move expects 5 args",
        ),
    ] {
        assert!(error(sql).contains(message), "{sql}: {}", error(sql));
    }
    // Keys that are empty or odd-length.
    for sql in [
        "SELECT treecrdt_local_insert(?1, ?2, ?3, 'at', NULL, NULL, X'')",
        "SELECT treecrdt_local_move(?1, ?3, ?2, 'at', NULL, X'000501')",
    ] {
        error(sql);
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn local_insert_if_absent_writes_nothing_for_a_node_in_the_tree() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let root = node_bytes(0);
    let node = node_bytes(1);
    let other = node_bytes(2);
    let insert_if_absent = |sql: &str, node: &[u8]| -> Option<String> {
        conn.query_row(sql, rusqlite::params![replica, root, node], |row| {
            row.get(0)
        })
        .unwrap()
    };

    // It takes treecrdt_local_insert's args, placement args included.
    let json = insert_if_absent(
        "SELECT treecrdt_local_insert_if_absent(?1, ?2, ?3, 'at', NULL, X'05', X'0005')",
        &node,
    )
    .expect("an absent node is created");
    let op = &decode_ops_or_local_result(&json)[0];
    assert_eq!(op.kind, "insert");
    assert!(op.if_absent);
    assert_eq!(
        (op.counter, op.order_key.clone(), op.payload.clone()),
        (
            1,
            Some([vec![0x00, 0x05], node.clone(), vec![0x00, 0x01]].concat()),
            Some(vec![0x05])
        )
    );
    let outcome =
        json_outcome_to_core(serde_json::from_str::<JsonLocalOpResult>(&json).unwrap().outcome);
    assert_eq!(outcome.affected_nodes(), [NodeId::ROOT, NodeId(1)]);
    assert_eq!(visible_children(&conn, &root), vec![node.clone()]);

    // The node is in the tree now, deleted too, so the function returns NULL and writes nothing,
    // not even the catch-up of a pending replay.
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_delete(?1, ?2)",
            rusqlite::params![replica, node],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE tree_meta SET replay_lamport = 0, replay_replica = X'', replay_counter = 0",
        [],
    )
    .unwrap();
    let meta = read_tree_meta(&conn);
    for sql in [
        "SELECT treecrdt_local_insert_if_absent(?1, ?2, ?3, 'first', NULL, NULL)",
        "SELECT treecrdt_local_insert_if_absent(?1, ?2, ?3, 'between', NULL, NULL, NULL, NULL)",
    ] {
        assert_eq!(insert_if_absent(sql, &node), None, "{sql}");
    }
    assert_eq!(read_tree_meta(&conn), meta);
    assert_eq!(
        read_replay_frontier(&conn),
        (Some(0), Some(Vec::new()), Some(0))
    );
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
    assert_eq!(count, 2);

    // An absent node takes the next counter.
    let json = insert_if_absent(
        "SELECT treecrdt_local_insert_if_absent(?1, ?2, ?3, 'between', NULL, NULL, NULL, X'0005')",
        &other,
    )
    .expect("an absent node is created");
    let op = &decode_ops_or_local_result(&json)[0];
    assert!(op.if_absent);
    assert_eq!(op.counter, 3);
    assert!(op.order_key.as_deref().unwrap() < [0x00, 0x05].as_slice());

    let error = |sql: &str| {
        conn.query_row(sql, rusqlite::params![replica, root, other], |row| {
            row.get::<_, String>(0)
        })
        .unwrap_err()
        .to_string()
    };
    for (sql, message) in [
        (
            "SELECT treecrdt_local_insert_if_absent(?1, ?2, ?3, 'at', NULL, NULL)",
            "treecrdt_local_insert_if_absent: placement at takes one trailing arg (key)",
        ),
        (
            "SELECT treecrdt_local_insert_if_absent(?1, ?2, ?3, 'first')",
            "treecrdt_local_insert_if_absent expects 6 args",
        ),
    ] {
        assert!(error(sql).contains(message), "{sql}: {}", error(sql));
    }
}

#[test]
fn create_only_flag_survives_storage_and_both_append_paths() {
    let conn = setup_conn();

    let setup = ReplicaId::new(b"setup");
    let (p1, p2, n) = (NodeId(1), NodeId(2), NodeId(10));
    let shown = Operation::insert_if_absent(
        &ReplicaId::new(b"b"),
        1,
        11,
        p2,
        n,
        vec![0x00, 0x01],
        Some(vec![2]),
    );
    append_ops(
        &conn,
        &json_ops(&[
            Operation::insert(&setup, 1, 1, NodeId::ROOT, p1, vec![0x00, 0x01]),
            Operation::insert(&setup, 2, 2, NodeId::ROOT, p2, vec![0x00, 0x02]),
            shown,
        ]),
    );
    assert_eq!(
        visible_children(&conn, &node_bytes(2)),
        vec![node_bytes(10)]
    );

    // The single-op append takes the flag as a tenth arg. This earlier create arrives late, so the
    // append catches up from the stored ops: the shown create must come back create-only.
    let _: String = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, ?2, ?3, 'insert', ?4, ?5, NULL, ?6, ?7, 1)",
            rusqlite::params![
                b"a".to_vec(),
                1i64,
                10i64,
                node_bytes(1),
                node_bytes(10),
                vec![0x00u8, 0x01],
                vec![1u8]
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        visible_children(&conn, &node_bytes(1)),
        vec![node_bytes(10)]
    );
    assert!(visible_children(&conn, &node_bytes(2)).is_empty());
    assert_eq!(payload_bytes(&conn, &node_bytes(10)), Some(vec![1]));

    // Op reads write the flag for create-only inserts only.
    let json: String =
        conn.query_row("SELECT treecrdt_ops_since(0)", [], |row| row.get(0)).unwrap();
    assert_eq!(json.matches("if_absent").count(), 2, "{json}");
    let flags: Vec<(u64, bool)> = decode_ops_or_local_result(&json)
        .iter()
        .map(|op| (op.lamport, op.if_absent))
        .collect();
    assert_eq!(flags, [(1, false), (2, false), (10, true), (11, true)]);
    let indexed = ops_by_oprefs(&conn, &oprefs_children(&conn, &node_bytes(1)));
    assert_eq!(indexed.len(), 1);
    assert!(indexed[0].if_absent && indexed[0].lamport == 10);

    // Only an insert can be create-only.
    let error = conn
        .query_row(
            "SELECT treecrdt_append_op(?1, 2, 12, 'payload', NULL, ?2, NULL, NULL, NULL, 1)",
            rusqlite::params![b"a".to_vec(), node_bytes(10)],
            |row| row.get::<_, String>(0),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("only an insert can be create-only"),
        "{error}"
    );
    let mut payload = json_op(&Operation::set_payload(
        &ReplicaId::new(b"a"),
        2,
        12,
        n,
        vec![3],
    ));
    payload.if_absent = true;
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&[payload], &mut encoded).unwrap();
    let error = conn
        .query_row(
            "SELECT treecrdt_append_ops(?1)",
            rusqlite::params![encoded],
            |row| row.get::<_, String>(0),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("only an insert can be create-only"),
        "{error}"
    );
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
    assert_eq!(count, 4);
}

#[test]
fn local_delete_emits_canonical_known_state() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);

    // Insert first so subtree_known_state has something to report.
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica.clone(), parent, node.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let json: String = conn
        .query_row(
            "SELECT treecrdt_local_delete(?1, ?2)",
            rusqlite::params![replica.clone(), node],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = decode_ops_or_local_result(&json);
    assert_eq!(ops.len(), 1);
    let op = &ops[0];
    assert_eq!(op.kind, "delete");
    let bytes = op.known_state.as_ref().unwrap();
    assert!(!bytes.is_empty());
    let vv = VersionVector::decode(bytes).unwrap();
    assert!(vv.get(&ReplicaId::new(replica)) >= 1);

    let (head_lamport, head_replica, head_counter, head_seq) = read_tree_meta(&conn);
    assert_eq!(head_lamport, 2);
    assert_eq!(head_replica, b"r1".to_vec());
    assert_eq!(head_counter, 2);
    assert_eq!(head_seq, 2);
}

#[test]
fn materialization_repairs_invalid_deleted_at_bytes() {
    for deleted_at in [Vec::new(), br#"{"entries":[]}"#.to_vec()] {
        let conn = setup_conn();
        let replica = b"r1".to_vec();
        let root = node_bytes(0);
        let node = node_bytes(1);
        let order_key = (1u16).to_be_bytes().to_vec();

        let _: String = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
                rusqlite::params![replica, 1i64, 1i64, "insert", root, node, order_key],
                |row| row.get(0),
            )
            .unwrap();

        let updated = conn
            .execute(
                "UPDATE tree_nodes SET deleted_at = ?1 WHERE node = ?2",
                rusqlite::params![deleted_at, node],
            )
            .unwrap();
        assert_eq!(updated, 1);

        let _: String = conn
            .query_row(
                "SELECT treecrdt_append_op(?1, ?2, ?3, ?4, NULL, ?5, NULL, NULL, ?6)",
                rusqlite::params![
                    b"r1".to_vec(),
                    2i64,
                    2i64,
                    "payload",
                    node,
                    b"payload".to_vec()
                ],
                |row| row.get::<_, String>(0),
            )
            .unwrap();

        let repaired: Option<Vec<u8>> = conn
            .query_row(
                "SELECT deleted_at FROM tree_nodes WHERE node = ?1",
                rusqlite::params![node],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            repaired, None,
            "did not repair invalid deleted_at bytes: {deleted_at:?}"
        );
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM ops", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 2);
    }
}

#[test]
fn local_delete_after_move_updates_children_visibility() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let root = node_bytes(0);
    let a = node_bytes(1);
    let b = node_bytes(2);

    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
            rusqlite::params![replica.clone(), root.clone(), a.clone()],
            |row| row.get(0),
        )
        .unwrap();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
            rusqlite::params![replica.clone(), root.clone(), b.clone()],
            |row| row.get(0),
        )
        .unwrap();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'first', NULL)",
            rusqlite::params![replica.clone(), b.clone(), root.clone()],
            |row| row.get(0),
        )
        .unwrap();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_delete(?1, ?2)",
            rusqlite::params![replica, a.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let root_arr = <[u8; 16]>::try_from(root.as_slice()).unwrap();
    let visible_children: Vec<Vec<u8>> = {
        let mut stmt = conn
            .prepare(
                "SELECT node FROM tree_nodes \
                 WHERE parent = ?1 AND tombstone = 0 \
                 ORDER BY order_key, node",
            )
            .unwrap();
        let rows = stmt
            .query_map(rusqlite::params![root_arr], |row| row.get::<_, Vec<u8>>(0))
            .unwrap();
        rows.map(|r| r.unwrap()).collect()
    };
    assert_eq!(visible_children, vec![b]);
}

#[test]
fn local_payload_set_and_clear_updates_meta() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let parent = node_bytes(0);
    let node = node_bytes(1);

    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, NULL)",
            rusqlite::params![replica.clone(), parent, node.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let payload_bytes = vec![1u8, 2, 3];
    let set_json: String = conn
        .query_row(
            "SELECT treecrdt_local_payload(?1, ?2, ?3)",
            rusqlite::params![replica.clone(), node.clone(), payload_bytes.clone()],
            |row| row.get(0),
        )
        .unwrap();
    let set_ops: Vec<JsonOp> = decode_ops_or_local_result(&set_json);
    assert_eq!(set_ops.len(), 1);
    assert_eq!(set_ops[0].kind, "payload");
    assert_eq!(set_ops[0].payload, Some(payload_bytes));
    assert_eq!(set_ops[0].counter, 2);
    assert_eq!(set_ops[0].lamport, 2);

    let clear_json: String = conn
        .query_row(
            "SELECT treecrdt_local_payload(?1, ?2, ?3)",
            rusqlite::params![replica.clone(), node, Option::<Vec<u8>>::None],
            |row| row.get(0),
        )
        .unwrap();
    let clear_ops: Vec<JsonOp> = decode_ops_or_local_result(&clear_json);
    assert_eq!(clear_ops.len(), 1);
    assert_eq!(clear_ops[0].kind, "payload");
    assert_eq!(clear_ops[0].payload, None);
    assert_eq!(clear_ops[0].counter, 3);
    assert_eq!(clear_ops[0].lamport, 3);

    let (head_lamport, head_replica, head_counter, head_seq) = read_tree_meta(&conn);
    assert_eq!(head_lamport, 3);
    assert_eq!(head_replica, replica);
    assert_eq!(head_counter, 3);
    assert_eq!(head_seq, 3);
}

#[test]
fn oprefs_children_include_payload_after_move() {
    let conn = setup_conn();

    let replica = b"r1".to_vec();
    let root = node_bytes(0);
    let p1 = node_bytes(1);
    let p2 = node_bytes(2);
    let child = node_bytes(3);

    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
            rusqlite::params![replica.clone(), root.clone(), p1.clone()],
            |row| row.get(0),
        )
        .unwrap();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
            rusqlite::params![replica.clone(), root, p2.clone()],
            |row| row.get(0),
        )
        .unwrap();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
            rusqlite::params![replica.clone(), p1, child.clone()],
            |row| row.get(0),
        )
        .unwrap();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_payload(?1, ?2, ?3)",
            rusqlite::params![replica.clone(), child.clone(), vec![104u8, 105u8]], // "hi"
            |row| row.get(0),
        )
        .unwrap();
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_move(?1, ?2, ?3, 'last', NULL)",
            rusqlite::params![replica, child, p2.clone()],
            |row| row.get(0),
        )
        .unwrap();

    let p2_arr = <[u8; 16]>::try_from(p2.as_slice()).unwrap();
    let refs_json: String = conn
        .query_row(
            "SELECT treecrdt_oprefs_children(?1)",
            rusqlite::params![p2_arr],
            |row| row.get(0),
        )
        .unwrap();
    let refs: Vec<Vec<u8>> = serde_json::from_str(&refs_json).unwrap();
    assert_eq!(refs.len(), 2);
}

#[test]
fn repeated_local_ops_release_statements_before_connection_close() {
    let conn = setup_conn();

    let replica = b"lifecycle".to_vec();
    let root = node_bytes(0);
    let parent_a = node_bytes(1);
    let parent_b = node_bytes(2);

    for parent in [&parent_a, &parent_b] {
        let _: String = conn
            .query_row(
                "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, NULL)",
                rusqlite::params![replica, root, parent],
                |row| row.get(0),
            )
            .unwrap();
    }

    for id in 3..35u128 {
        let node = node_bytes(id);
        let payload = id.to_be_bytes().to_vec();

        let _: String = conn
            .query_row(
                "SELECT treecrdt_local_insert(?1, ?2, ?3, 'last', NULL, ?4)",
                rusqlite::params![replica, parent_a, node, payload],
                |row| row.get(0),
            )
            .unwrap();
        let _: String = conn
            .query_row(
                "SELECT treecrdt_local_payload(?1, ?2, ?3)",
                rusqlite::params![replica, node, vec![id as u8; 32]],
                |row| row.get(0),
            )
            .unwrap();
        let _: String = conn
            .query_row(
                "SELECT treecrdt_local_move(?1, ?2, ?3, 'last', NULL)",
                rusqlite::params![replica, node, parent_b],
                |row| row.get(0),
            )
            .unwrap();
        let _: String = conn
            .query_row(
                "SELECT treecrdt_local_delete(?1, ?2)",
                rusqlite::params![replica, node],
                |row| row.get(0),
            )
            .unwrap();
    }

    conn.close()
        .expect("local helper statements should be finalized before connection close");
}

#[test]
fn failed_local_op_releases_statements_before_the_next_write() {
    let conn = setup_conn();

    let replica = b"failure-recovery".to_vec();
    let root = node_bytes(0);
    let node = node_bytes(1);
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, ?4)",
            rusqlite::params![replica, root, node, vec![1u8]],
            |row| row.get(0),
        )
        .unwrap();

    conn.execute_batch(
        "CREATE TRIGGER fail_tree_payload_upsert \
         BEFORE INSERT ON tree_payload \
         BEGIN \
           SELECT RAISE(ABORT, 'forced local write failure'); \
         END;",
    )
    .unwrap();
    let failed: rusqlite::Result<String> = conn.query_row(
        "SELECT treecrdt_local_payload(?1, ?2, ?3)",
        rusqlite::params![replica, node, vec![2u8]],
        |row| row.get(0),
    );
    assert!(failed.is_err());

    conn.execute_batch("DROP TRIGGER fail_tree_payload_upsert").unwrap();
    let recovered: String = conn
        .query_row(
            "SELECT treecrdt_local_payload(?1, ?2, ?3)",
            rusqlite::params![replica, node, vec![3u8]],
            |row| row.get(0),
        )
        .unwrap();
    let ops: Vec<JsonOp> = decode_ops_or_local_result(&recovered);
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].payload, Some(vec![3u8]));

    conn.close()
        .expect("failed local helper statements should not leak into connection close");
}

unsafe extern "C" fn deny_tree_payload_delete(
    _context: *mut c_void,
    action: c_int,
    table: *const c_char,
    _column: *const c_char,
    _database: *const c_char,
    _trigger: *const c_char,
) -> c_int {
    let is_tree_payload =
        !table.is_null() && unsafe { CStr::from_ptr(table) }.to_bytes() == b"tree_payload";
    if action == rusqlite::ffi::SQLITE_DELETE && is_tree_payload {
        rusqlite::ffi::SQLITE_DENY
    } else {
        rusqlite::ffi::SQLITE_OK
    }
}

#[test]
fn local_payload_does_not_prepare_an_unused_delete_statement() {
    let conn = setup_conn();

    let replica = b"lazy-prepare".to_vec();
    let root = node_bytes(0);
    let node = node_bytes(1);
    let _: String = conn
        .query_row(
            "SELECT treecrdt_local_insert(?1, ?2, ?3, 'first', NULL, ?4)",
            rusqlite::params![replica, root, node, vec![1u8]],
            |row| row.get(0),
        )
        .unwrap();

    let set_authorizer_rc = unsafe {
        rusqlite::ffi::sqlite3_set_authorizer(
            conn.handle(),
            Some(deny_tree_payload_delete),
            null_mut(),
        )
    };
    assert_eq!(set_authorizer_rc, rusqlite::ffi::SQLITE_OK);

    let result: rusqlite::Result<String> = conn.query_row(
        "SELECT treecrdt_local_payload(?1, ?2, ?3)",
        rusqlite::params![replica, node, vec![2u8]],
        |row| row.get(0),
    );

    let clear_authorizer_rc =
        unsafe { rusqlite::ffi::sqlite3_set_authorizer(conn.handle(), None, null_mut()) };
    assert_eq!(clear_authorizer_rc, rusqlite::ffi::SQLITE_OK);
    result.expect("payload writes should not prepare the unused payload-delete statement");
}

#[test]
fn registration_only_entrypoint_defers_schema_and_matches_native_initialization() {
    let conn = Connection::open_in_memory().unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(find_extension().unwrap(), Some("sqlite3_treecrdt_register"))
            .unwrap();
    }
    let schema: String = conn.query_row("SELECT treecrdt_schema()", [], |row| row.get(0)).unwrap();
    assert!(objects(&conn).is_empty());
    conn.execute_batch(&schema).unwrap();

    fn objects(conn: &Connection) -> Vec<(String, String, Option<String>)> {
        conn.prepare("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
    assert_eq!(objects(&conn), objects(&setup_conn()));
    let root_count: i64 =
        conn.query_row("SELECT count(*) FROM tree_nodes", [], |row| row.get(0)).unwrap();
    assert_eq!(root_count, 1);
}

fn setup_conn() -> Connection {
    let ext_path = find_extension()
        .expect("run cargo build -p treecrdt-sqlite-ext --lib --features ext-sqlite first");
    let conn = Connection::open_in_memory().unwrap();
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(ext_path, Some("sqlite3_treecrdt_init")).unwrap();
    }
    conn.query_row(
        "SELECT treecrdt_set_doc_id('treecrdt-sqlite-ext-test')",
        [],
        |row| row.get::<_, i64>(0),
    )
    .unwrap();
    conn
}

fn node_bytes(id: u128) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn find_extension() -> Option<PathBuf> {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let target_dir = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| manifest.join("..").join("..").join("target"));

    let (name, alt_name) = extension_filenames();
    let candidates = [
        target_dir.join("debug").join(&name),
        target_dir.join("debug").join("deps").join(&name),
        target_dir.join("debug").join(&alt_name),
        target_dir.join("debug").join("deps").join(&alt_name),
    ];
    candidates.into_iter().find(|p| p.exists())
}

fn extension_filenames() -> (String, String) {
    #[cfg(target_os = "macos")]
    return (
        "libtreecrdt_sqlite_ext.dylib".into(),
        "libtreecrdt_sqlite_ext.dylib".into(),
    );
    #[cfg(target_os = "linux")]
    return (
        "libtreecrdt_sqlite_ext.so".into(),
        "libtreecrdt_sqlite_ext.so".into(),
    );
    #[cfg(target_os = "windows")]
    return (
        "treecrdt_sqlite_ext.dll".into(),
        "libtreecrdt_sqlite_ext.dll".into(),
    );
}
