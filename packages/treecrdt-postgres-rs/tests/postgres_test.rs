use std::cell::RefCell;
use std::rc::Rc;
use std::sync::OnceLock;

use postgres::{Client, NoTls};
use uuid::Uuid;

use treecrdt_core::{NodeId, Operation, ReplicaId, VersionVector};
use treecrdt_postgres::{
    append_ops, ensure_materialized, ensure_schema, get_ops_by_op_refs, list_op_refs_all,
    list_op_refs_children, max_lamport, reset_doc_for_tests,
};

fn order_key_from_position(position: u16) -> Vec<u8> {
    let n = position.wrapping_add(1);
    n.to_be_bytes().to_vec()
}

fn node(n: u128) -> NodeId {
    NodeId(n)
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
    assert_eq!(tombstone, false);
}
