use std::cell::RefCell;
use std::rc::Rc;
use std::sync::OnceLock;

use postgres::{Client, NoTls};
use uuid::Uuid;

use treecrdt_core::{NodeId, Operation, ReplicaId, VersionVector};
use treecrdt_postgres::{
    append_ops, clone_materialized_doc_for_tests, ensure_materialized, ensure_schema,
    get_ops_by_op_refs, list_op_refs_all, list_op_refs_children, local_delete, local_insert,
    local_move, local_payload, max_lamport, prime_balanced_fanout_doc_for_tests,
    prime_doc_for_tests, replica_max_counter, reset_doc_for_tests, tree_children, tree_node_count,
};

fn order_key_from_position(position: u16) -> Vec<u8> {
    let n = position.wrapping_add(1);
    n.to_be_bytes().to_vec()
}

fn node(n: u128) -> NodeId {
    NodeId(n)
}

fn balanced_parent_and_position(node_index: usize, fanout: usize) -> (NodeId, u16) {
    if node_index <= fanout {
        return (NodeId::ROOT, ((node_index - 1) % fanout) as u16);
    }
    (
        node((((node_index - (fanout + 1)) / fanout) + 1) as u128),
        ((node_index - 1) % fanout) as u16,
    )
}

fn build_balanced_fixture_ops(
    size: usize,
    fanout: usize,
    payload_bytes: usize,
    replica_label: &[u8],
) -> Vec<Operation> {
    let replica = ReplicaId::new(replica_label);
    let mut ops = Vec::with_capacity(size + usize::from(payload_bytes > 0));
    for node_index in 1..=size {
        let (parent, position) = balanced_parent_and_position(node_index, fanout);
        ops.push(Operation::insert(
            &replica,
            node_index as u64,
            node_index as u64,
            parent,
            node(node_index as u128),
            order_key_from_position(position),
        ));
    }
    if payload_bytes > 0 {
        ops.push(Operation::set_payload(
            &replica,
            (size + 1) as u64,
            (size + 1) as u64,
            node((fanout + 1) as u128),
            vec![0xAB; payload_bytes],
        ));
    }
    ops
}

fn connect() -> Option<Rc<RefCell<Client>>> {
    let url = std::env::var("TREECRDT_POSTGRES_URL").ok()?;
    let client = Client::connect(&url, NoTls).ok()?;
    Some(Rc::new(RefCell::new(client)))
}

fn ensure_schema_once(client: &Rc<RefCell<Client>>) {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let mut c = client.borrow_mut();
        ensure_schema(&mut c).unwrap();
    });
}

#[test]
fn postgres_backend_apply_is_idempotent_and_max_lamport_monotonic() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"a");
    let root = NodeId::ROOT;
    let n1 = node(1);

    let op1 = Operation::insert(&replica, 1, 1, root, n1, order_key_from_position(0));
    let op2 = Operation::set_payload(&replica, 2, 7, n1, vec![1, 2, 3]);

    let inserted = append_ops(&client, &doc_id, &[op1.clone(), op2.clone()]).unwrap();
    assert_eq!(inserted, 2);
    let inserted_again = append_ops(&client, &doc_id, &[op1, op2]).unwrap();
    assert_eq!(inserted_again, 0);

    let refs = list_op_refs_all(&client, &doc_id).unwrap();
    assert_eq!(refs.len(), 2);

    let max = max_lamport(&client, &doc_id).unwrap();
    assert_eq!(max, 7);
}

#[test]
fn postgres_backend_append_batch_materializes_only_inserted_ops() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"dup");
    let n1 = node(1);
    let op1 = Operation::insert(&replica, 1, 1, NodeId::ROOT, n1, order_key_from_position(0));
    let op2 = Operation::set_payload(&replica, 2, 2, n1, vec![9]);

    let inserted = append_ops(&client, &doc_id, &[op1.clone(), op1.clone(), op2.clone()]).unwrap();
    assert_eq!(inserted, 2);
    assert_eq!(list_op_refs_all(&client, &doc_id).unwrap().len(), 2);
    assert_eq!(
        tree_children(&client, &doc_id, NodeId::ROOT).unwrap(),
        vec![n1]
    );

    let head_seq = {
        let mut c = client.borrow_mut();
        let row = c
            .query_one(
                "SELECT head_seq FROM treecrdt_meta WHERE doc_id = $1",
                &[&doc_id],
            )
            .unwrap();
        row.get::<_, i64>(0).max(0) as u64
    };
    assert_eq!(head_seq, 2);
}

#[test]
fn postgres_backend_large_append_rebuilds_materialized_views_on_demand() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"bulk");
    let op_count = 2_500u64;
    let ops: Vec<Operation> = (0..op_count)
        .map(|index| {
            Operation::insert(
                &replica,
                index + 1,
                index + 1,
                NodeId::ROOT,
                node(10_000 + index as u128),
                order_key_from_position(index as u16),
            )
        })
        .collect();

    let inserted = append_ops(&client, &doc_id, &ops).unwrap();
    assert_eq!(inserted, op_count);
    assert_eq!(
        list_op_refs_all(&client, &doc_id).unwrap().len(),
        op_count as usize
    );
    assert_eq!(max_lamport(&client, &doc_id).unwrap(), op_count);

    let children = tree_children(&client, &doc_id, NodeId::ROOT).unwrap();
    assert_eq!(children.len(), op_count as usize);

    let refs_root = list_op_refs_children(&client, &doc_id, NodeId::ROOT).unwrap();
    assert_eq!(refs_root.len(), op_count as usize);
}

#[test]
fn postgres_backend_prime_doc_for_tests_builds_materialized_fixture() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    let replica = ReplicaId::new(b"fixture");
    let op_count = 2_500u64;
    let ops: Vec<Operation> = (0..op_count)
        .map(|index| {
            Operation::insert(
                &replica,
                index + 1,
                index + 1,
                NodeId::ROOT,
                node(20_000 + index as u128),
                order_key_from_position(index as u16),
            )
        })
        .collect();

    prime_doc_for_tests(&client, &doc_id, &ops).unwrap();

    assert_eq!(
        replica_max_counter(&client, &doc_id, replica.as_bytes()).unwrap(),
        op_count
    );
    assert_eq!(max_lamport(&client, &doc_id).unwrap(), op_count);
    assert_eq!(
        list_op_refs_all(&client, &doc_id).unwrap().len(),
        op_count as usize
    );
    assert_eq!(
        tree_children(&client, &doc_id, NodeId::ROOT).unwrap().len(),
        op_count as usize
    );
    assert_eq!(
        list_op_refs_children(&client, &doc_id, NodeId::ROOT).unwrap().len(),
        op_count as usize
    );
}

#[test]
fn postgres_backend_prime_balanced_fanout_doc_for_tests_generates_expected_shape() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    prime_balanced_fanout_doc_for_tests(&client, &doc_id, 25, 3, 8, "playground-seed").unwrap();

    assert_eq!(tree_node_count(&client, &doc_id).unwrap(), 25);
    assert_eq!(max_lamport(&client, &doc_id).unwrap(), 26);
    assert_eq!(
        tree_children(&client, &doc_id, NodeId::ROOT).unwrap().len(),
        3
    );
    assert_eq!(
        list_op_refs_children(&client, &doc_id, NodeId::ROOT).unwrap().len(),
        3
    );
    assert_eq!(
        treecrdt_postgres::tree_payload(&client, &doc_id, node(4))
            .unwrap()
            .unwrap()
            .len(),
        8
    );
}

#[test]
fn postgres_backend_prime_balanced_fanout_doc_for_tests_matches_replay_materialization() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let direct_doc_id = format!("test-direct-{}", Uuid::new_v4());
    let replay_doc_id = format!("test-replay-{}", Uuid::new_v4());
    let size = 25usize;
    let fanout = 3usize;
    let payload_bytes = 8usize;
    let ops = build_balanced_fixture_ops(size, fanout, payload_bytes, b"playground-seed");

    prime_doc_for_tests(&client, &replay_doc_id, &ops).unwrap();
    prime_balanced_fanout_doc_for_tests(
        &client,
        &direct_doc_id,
        size,
        fanout,
        payload_bytes,
        "playground-seed",
    )
    .unwrap();

    type NodeRow = (
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        bool,
        Option<Vec<u8>>,
    );

    let mut c = client.borrow_mut();
    let node_rows = |doc_id: &str, c: &mut Client| -> Vec<NodeRow> {
        c.query(
            "SELECT node, parent, order_key, tombstone, last_change \
             FROM treecrdt_nodes WHERE doc_id = $1 ORDER BY node",
            &[&doc_id],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
        .collect()
    };
    let payload_rows =
        |doc_id: &str, c: &mut Client| -> Vec<(Vec<u8>, Vec<u8>, i64, Vec<u8>, i64)> {
            c.query(
                "SELECT node, payload, last_lamport, last_replica, last_counter \
             FROM treecrdt_payload WHERE doc_id = $1 ORDER BY node",
                &[&doc_id],
            )
            .unwrap()
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
            .collect()
        };
    let index_rows = |doc_id: &str, c: &mut Client| -> Vec<(Vec<u8>, Vec<u8>, i64)> {
        c.query(
            "SELECT parent, op_ref, seq \
             FROM treecrdt_oprefs_children WHERE doc_id = $1 ORDER BY parent, seq, op_ref",
            &[&doc_id],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
    };
    let meta_row = |doc_id: &str, c: &mut Client| -> (bool, i64, Vec<u8>, i64, i64) {
        let row = c
            .query_one(
                "SELECT dirty, head_lamport, head_replica, head_counter, head_seq \
                 FROM treecrdt_meta WHERE doc_id = $1",
                &[&doc_id],
            )
            .unwrap();
        (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4))
    };
    let replica_rows = |doc_id: &str, c: &mut Client| -> Vec<(Vec<u8>, i64)> {
        c.query(
            "SELECT replica, max_counter \
             FROM treecrdt_replica_meta WHERE doc_id = $1 ORDER BY replica",
            &[&doc_id],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
    };

    assert_eq!(
        node_rows(&replay_doc_id, &mut c),
        node_rows(&direct_doc_id, &mut c)
    );
    assert_eq!(
        payload_rows(&replay_doc_id, &mut c),
        payload_rows(&direct_doc_id, &mut c)
    );
    assert_eq!(
        index_rows(&replay_doc_id, &mut c),
        index_rows(&direct_doc_id, &mut c)
    );
    assert_eq!(
        meta_row(&replay_doc_id, &mut c),
        meta_row(&direct_doc_id, &mut c)
    );
    assert_eq!(
        replica_rows(&replay_doc_id, &mut c),
        replica_rows(&direct_doc_id, &mut c)
    );
}

#[test]
fn postgres_backend_materialized_clone_supports_local_writes() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let source_doc_id = format!("test-source-{}", Uuid::new_v4());
    let target_doc_id = format!("test-target-{}", Uuid::new_v4());
    prime_balanced_fanout_doc_for_tests(&client, &source_doc_id, 25, 3, 8, "playground-seed")
        .unwrap();

    {
        let mut c = client.borrow_mut();
        clone_materialized_doc_for_tests(&mut c, &source_doc_id, &target_doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"writer");
    let inserted = local_insert(
        &client,
        &target_doc_id,
        &replica,
        NodeId::ROOT,
        node(999),
        "last",
        None,
        Some(vec![1, 2, 3]),
    )
    .unwrap();
    assert_eq!(inserted.kind.node(), node(999));
    assert_eq!(
        treecrdt_postgres::tree_parent(&client, &target_doc_id, node(999)).unwrap(),
        Some(NodeId::ROOT)
    );
    assert_eq!(
        treecrdt_postgres::tree_payload(&client, &target_doc_id, node(999))
            .unwrap()
            .unwrap(),
        vec![1, 2, 3]
    );

    let updated =
        local_payload(&client, &target_doc_id, &replica, node(4), Some(vec![9, 9])).unwrap();
    assert_eq!(updated.kind.node(), node(4));
    assert_eq!(
        treecrdt_postgres::tree_payload(&client, &target_doc_id, node(4))
            .unwrap()
            .unwrap(),
        vec![9, 9]
    );
}

#[test]
fn postgres_backend_doc_isolation() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_a = format!("test-a-{}", Uuid::new_v4());
    let doc_b = format!("test-b-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_a).unwrap();
        reset_doc_for_tests(&mut c, &doc_b).unwrap();
    }

    let replica_a = ReplicaId::new(b"a");
    let replica_b = ReplicaId::new(b"b");

    append_ops(
        &client,
        &doc_a,
        &[Operation::insert(
            &replica_a,
            1,
            1,
            NodeId::ROOT,
            node(11),
            order_key_from_position(0),
        )],
    )
    .unwrap();
    append_ops(
        &client,
        &doc_b,
        &[Operation::insert(
            &replica_b,
            1,
            1,
            NodeId::ROOT,
            node(22),
            order_key_from_position(0),
        )],
    )
    .unwrap();

    let refs_a = list_op_refs_all(&client, &doc_a).unwrap();
    let refs_b = list_op_refs_all(&client, &doc_b).unwrap();
    assert_eq!(refs_a.len(), 1);
    assert_eq!(refs_b.len(), 1);
    assert_ne!(refs_a[0], refs_b[0]);
}

#[test]
fn postgres_backend_get_ops_by_op_refs_preserves_order_and_errors_on_missing() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"a");

    append_ops(
        &client,
        &doc_id,
        &[
            Operation::insert(
                &replica,
                1,
                1,
                NodeId::ROOT,
                node(1),
                order_key_from_position(0),
            ),
            Operation::insert(
                &replica,
                2,
                2,
                NodeId::ROOT,
                node(2),
                order_key_from_position(1),
            ),
        ],
    )
    .unwrap();

    let refs = list_op_refs_all(&client, &doc_id).unwrap();
    assert_eq!(refs.len(), 2);

    let ops = get_ops_by_op_refs(&client, &doc_id, &[refs[1], refs[0]]).unwrap();
    assert_eq!(ops[0].meta.id.counter, 2);
    assert_eq!(ops[1].meta.id.counter, 1);

    let missing = [0u8; 16];
    let err = get_ops_by_op_refs(&client, &doc_id, &[missing]).unwrap_err();
    assert!(format!("{err:?}").contains("opRef missing locally"));
}

#[test]
fn postgres_backend_children_filter_includes_move_and_payload() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"a");
    let root = NodeId::ROOT;
    let p1 = node(101);
    let p2 = node(102);
    let n = node(103);

    append_ops(
        &client,
        &doc_id,
        &[
            Operation::insert(&replica, 1, 1, root, p1, order_key_from_position(0)),
            Operation::insert(&replica, 2, 2, root, p2, order_key_from_position(1)),
            Operation::insert(&replica, 3, 3, p1, n, order_key_from_position(0)),
            Operation::set_payload(&replica, 4, 4, n, vec![7]),
            Operation::move_node(&replica, 5, 5, n, p2, order_key_from_position(0)),
            Operation::set_payload(&replica, 6, 6, n, vec![8]),
        ],
    )
    .unwrap();

    let refs_p2 = list_op_refs_children(&client, &doc_id, p2).unwrap();
    let ops_p2 = get_ops_by_op_refs(&client, &doc_id, &refs_p2).unwrap();
    assert!(ops_p2
        .iter()
        .any(|op| matches!(op.kind, treecrdt_core::OperationKind::Move { .. })));
    assert!(ops_p2
        .iter()
        .any(|op| matches!(op.kind, treecrdt_core::OperationKind::Payload { .. })));

    let refs_p1 = list_op_refs_children(&client, &doc_id, p1).unwrap();
    let ops_p1 = get_ops_by_op_refs(&client, &doc_id, &refs_p1).unwrap();
    assert!(ops_p1
        .iter()
        .any(|op| matches!(op.kind, treecrdt_core::OperationKind::Move { .. })));
}

#[test]
fn postgres_backend_defensive_delete_restores_parent_after_child_insert() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"r1");
    let root = NodeId::ROOT;
    let parent = node(1);
    let child = node(2);

    let op1 = Operation::insert(&replica, 1, 1, root, parent, order_key_from_position(0));
    let mut vv = VersionVector::new();
    vv.observe(&replica, 1);
    let op2 = Operation::delete(&replica, 2, 2, parent, Some(vv));
    let op3 = Operation::insert(&replica, 3, 3, parent, child, order_key_from_position(0));

    append_ops(&client, &doc_id, &[op1, op2, op3]).unwrap();
    ensure_materialized(&client, &doc_id).unwrap();

    let parent_bytes = parent.0.to_be_bytes();
    let mut c = client.borrow_mut();
    let rows = c
        .query(
            "SELECT tombstone FROM treecrdt_nodes WHERE doc_id = $1 AND node = $2 LIMIT 1",
            &[&doc_id, &parent_bytes.as_slice()],
        )
        .unwrap();
    let tombstone: bool = rows[0].get(0);
    assert!(!tombstone);
}

#[test]
fn postgres_backend_local_ops_drive_core_materialization_flow() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"loc");
    let root = NodeId::ROOT;
    let parent = node(1001);
    let child = node(1002);
    let sibling = node(1003);

    let op1 = local_insert(
        &client, &doc_id, &replica, root, parent, "first", None, None,
    )
    .unwrap();
    let op2 = local_insert(
        &client,
        &doc_id,
        &replica,
        parent,
        child,
        "first",
        None,
        Some(vec![1]),
    )
    .unwrap();
    let op3 = local_insert(
        &client, &doc_id, &replica, root, sibling, "last", None, None,
    )
    .unwrap();
    let op4 = local_move(&client, &doc_id, &replica, child, root, "last", None).unwrap();
    let op5 = local_payload(&client, &doc_id, &replica, child, Some(vec![9])).unwrap();
    let op6 = local_delete(&client, &doc_id, &replica, parent).unwrap();

    assert_eq!(op1.meta.id.counter, 1);
    assert_eq!(op2.meta.id.counter, 2);
    assert_eq!(op3.meta.id.counter, 3);
    assert_eq!(op4.meta.id.counter, 4);
    assert_eq!(op5.meta.id.counter, 5);
    assert_eq!(op6.meta.id.counter, 6);

    ensure_materialized(&client, &doc_id).unwrap();

    let children = tree_children(&client, &doc_id, root).unwrap();
    assert_eq!(children, vec![sibling, child]);

    let refs_root = list_op_refs_children(&client, &doc_id, root).unwrap();
    let ops_root = get_ops_by_op_refs(&client, &doc_id, &refs_root).unwrap();
    assert!(ops_root
        .iter()
        .any(|op| matches!(op.kind, treecrdt_core::OperationKind::Move { .. })));
    assert!(ops_root
        .iter()
        .any(|op| matches!(op.kind, treecrdt_core::OperationKind::Payload { .. })));

    assert_eq!(max_lamport(&client, &doc_id).unwrap(), op6.meta.lamport);
    assert_eq!(
        replica_max_counter(&client, &doc_id, replica.as_bytes()).unwrap(),
        op6.meta.id.counter
    );
}
