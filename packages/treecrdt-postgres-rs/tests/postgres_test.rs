use std::cell::RefCell;
use std::rc::Rc;
use std::sync::OnceLock;

use postgres::{Client, NoTls};
use uuid::Uuid;

use treecrdt_core::{NodeId, Operation, ReplicaId, VersionVector};
use treecrdt_postgres::{
    append_ops, append_ops_with_affected_nodes, ensure_materialized, ensure_schema,
    get_ops_by_op_refs, list_op_refs_all, list_op_refs_children, local_delete, local_insert,
    local_move, local_payload, max_lamport, replica_max_counter, reset_doc_for_tests,
    tree_children, tree_payload,
};

fn order_key_from_position(position: u16) -> Vec<u8> {
    let n = position.wrapping_add(1);
    n.to_be_bytes().to_vec()
}

fn node(n: u128) -> NodeId {
    NodeId(n)
}

fn representative_remote_batch(replica: &ReplicaId) -> (NodeId, NodeId, NodeId, Vec<Operation>) {
    let p1 = node(1);
    let p2 = node(2);
    let child = node(3);
    (
        p1,
        p2,
        child,
        vec![
            Operation::insert(replica, 1, 1, NodeId::ROOT, p1, order_key_from_position(0)),
            Operation::insert(replica, 2, 2, NodeId::ROOT, p2, order_key_from_position(1)),
            Operation::insert(replica, 3, 3, p1, child, order_key_from_position(0)),
            Operation::set_payload(replica, 4, 4, child, vec![7]),
            Operation::move_node(replica, 5, 5, child, p2, order_key_from_position(0)),
            Operation::set_payload(replica, 6, 6, child, vec![8]),
        ],
    )
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
fn postgres_backend_append_with_affected_nodes_matches_representative_remote_batch() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"rep");
    let (p1, p2, child, ops) = representative_remote_batch(&replica);

    let affected = append_ops_with_affected_nodes(&client, &doc_id, &ops).unwrap();
    assert_eq!(affected, vec![NodeId::ROOT, p1, p2, child]);
    assert_eq!(
        tree_children(&client, &doc_id, NodeId::ROOT).unwrap(),
        vec![p1, p2]
    );
    assert_eq!(tree_children(&client, &doc_id, p2).unwrap(), vec![child]);
    assert_eq!(
        tree_payload(&client, &doc_id, child).unwrap(),
        Some(vec![8])
    );

    let refs_p2 = list_op_refs_children(&client, &doc_id, p2).unwrap();
    let ops_p2 = get_ops_by_op_refs(&client, &doc_id, &refs_p2).unwrap();
    assert!(ops_p2
        .iter()
        .any(|op| matches!(op.kind, treecrdt_core::OperationKind::Move { .. })));
    assert!(ops_p2
        .iter()
        .any(|op| matches!(op.kind, treecrdt_core::OperationKind::Payload { .. })));
}

#[test]
fn postgres_backend_out_of_order_append_uses_replay_frontier() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"ooo");
    let second = Operation::insert(
        &replica,
        2,
        2,
        NodeId::ROOT,
        node(2),
        order_key_from_position(1),
    );
    let first = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        node(1),
        order_key_from_position(0),
    );

    append_ops(&client, &doc_id, &[second]).unwrap();
    let affected = append_ops_with_affected_nodes(&client, &doc_id, &[first.clone()]).unwrap();
    assert!(affected.is_empty());

    let (replay_lamport, replay_replica, replay_counter, head_seq_before) = {
        let mut c = client.borrow_mut();
        let row = c
            .query_one(
                "SELECT replay_lamport, replay_replica, replay_counter, head_seq \
                 FROM treecrdt_meta WHERE doc_id = $1",
                &[&doc_id],
            )
            .unwrap();
        (
            row.get::<_, Option<i64>>(0).map(|v| v.max(0) as u64),
            row.get::<_, Option<Vec<u8>>>(1),
            row.get::<_, Option<i64>>(2).map(|v| v.max(0) as u64),
            row.get::<_, i64>(3).max(0) as u64,
        )
    };
    assert_eq!(replay_lamport, Some(first.meta.lamport));
    assert_eq!(
        replay_replica,
        Some(first.meta.id.replica.as_bytes().to_vec())
    );
    assert_eq!(replay_counter, Some(first.meta.id.counter));
    assert_eq!(head_seq_before, 1);

    assert_eq!(
        tree_children(&client, &doc_id, NodeId::ROOT).unwrap(),
        vec![node(1), node(2)]
    );

    let replay_after_read = {
        let mut c = client.borrow_mut();
        let row = c
            .query_one(
                "SELECT replay_lamport, head_seq FROM treecrdt_meta WHERE doc_id = $1",
                &[&doc_id],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(1).max(0) as u64, 2);
        row.get::<_, Option<i64>>(0)
    };
    assert_eq!(replay_after_read, None);

    let refs = list_op_refs_children(&client, &doc_id, NodeId::ROOT).unwrap();
    let ops = get_ops_by_op_refs(&client, &doc_id, &refs).unwrap();
    assert_eq!(
        ops.iter().map(|op| op.meta.id.counter).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn postgres_backend_replay_from_start_frontier_recovers_materialized_state() {
    let Some(client) = connect() else {
        return;
    };
    ensure_schema_once(&client);

    let doc_id = format!("test-{}", Uuid::new_v4());
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, &doc_id).unwrap();
    }

    let replica = ReplicaId::new(b"restart");
    let first = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        node(1),
        order_key_from_position(0),
    );
    let second = Operation::insert(
        &replica,
        2,
        2,
        NodeId::ROOT,
        node(2),
        order_key_from_position(1),
    );

    append_ops(&client, &doc_id, &[first]).unwrap();
    {
        let mut c = client.borrow_mut();
        c.execute(
            "UPDATE treecrdt_meta \
             SET replay_lamport = 0, replay_replica = ''::bytea, replay_counter = 0 \
             WHERE doc_id = $1",
            &[&doc_id],
        )
        .unwrap();
    }

    let affected = append_ops_with_affected_nodes(&client, &doc_id, &[second]).unwrap();
    assert!(affected.is_empty());

    let (replay_lamport, replay_replica, replay_counter) = {
        let mut c = client.borrow_mut();
        let row = c
            .query_one(
                "SELECT replay_lamport, replay_replica, replay_counter \
                 FROM treecrdt_meta WHERE doc_id = $1",
                &[&doc_id],
            )
            .unwrap();
        (
            row.get::<_, Option<i64>>(0).map(|v| v.max(0) as u64),
            row.get::<_, Option<Vec<u8>>>(1),
            row.get::<_, Option<i64>>(2).map(|v| v.max(0) as u64),
        )
    };
    assert_eq!(replay_lamport, Some(0));
    assert_eq!(replay_replica, Some(Vec::new()));
    assert_eq!(replay_counter, Some(0));

    assert_eq!(
        tree_children(&client, &doc_id, NodeId::ROOT).unwrap(),
        vec![node(1), node(2)]
    );

    let replay_after_read = {
        let mut c = client.borrow_mut();
        let row = c
            .query_one(
                "SELECT replay_lamport, head_seq FROM treecrdt_meta WHERE doc_id = $1",
                &[&doc_id],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(1).max(0) as u64, 2);
        row.get::<_, Option<i64>>(0)
    };
    assert_eq!(replay_after_read, None);
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
