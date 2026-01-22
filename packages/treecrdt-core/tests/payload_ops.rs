use treecrdt_core::{LamportClock, MemoryStorage, NodeId, Operation, ReplicaId, TreeCrdt};

#[test]
fn payload_lww_tie_breaker_converges() {
    let mut a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let node = NodeId(1);
    let insert = Operation::insert(&ReplicaId::new(b"s"), 1, 1, NodeId::ROOT, node, 0);
    a.apply_remote(insert.clone()).unwrap();
    b.apply_remote(insert).unwrap();

    // Same lamport, different replicas: tie-break deterministically by (replica, counter).
    let op_a = Operation::set_payload(&ReplicaId::new(b"a"), 1, 5, node, b"A");
    let op_b = Operation::set_payload(&ReplicaId::new(b"b"), 1, 5, node, b"B");

    a.apply_remote(op_a.clone()).unwrap();
    a.apply_remote(op_b.clone()).unwrap();

    b.apply_remote(op_b).unwrap();
    b.apply_remote(op_a).unwrap();

    assert_eq!(a.payload(node).unwrap(), Some(b"B".to_vec()));
    assert_eq!(b.payload(node).unwrap(), Some(b"B".to_vec()));
}

#[test]
fn payload_clear_is_last_writer_wins() {
    let mut a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let node = NodeId(1);
    let insert = Operation::insert(&ReplicaId::new(b"s"), 1, 1, NodeId::ROOT, node, 0);
    a.apply_remote(insert.clone()).unwrap();
    b.apply_remote(insert).unwrap();

    let set = Operation::set_payload(&ReplicaId::new(b"a"), 1, 5, node, b"hello");
    let clear = Operation::clear_payload(&ReplicaId::new(b"b"), 1, 6, node);

    a.apply_remote(set.clone()).unwrap();
    a.apply_remote(clear.clone()).unwrap();

    b.apply_remote(clear).unwrap();
    b.apply_remote(set).unwrap();

    assert_eq!(a.payload(node).unwrap(), None);
    assert_eq!(b.payload(node).unwrap(), None);
}

#[test]
fn payload_can_arrive_before_insert() {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let node = NodeId(1);
    let insert = Operation::insert(&ReplicaId::new(b"a"), 1, 1, NodeId::ROOT, node, 0);
    let payload = Operation::set_payload(&ReplicaId::new(b"a"), 2, 2, node, b"hello");

    // Receive payload first, then receive the earlier insert (out of order by lamport).
    tree.apply_remote(payload).unwrap();
    tree.apply_remote(insert).unwrap();

    assert_eq!(tree.parent(node).unwrap(), Some(NodeId::ROOT));
    assert_eq!(tree.payload(node).unwrap(), Some(b"hello".to_vec()));
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![node]);
}

#[test]
fn insert_with_payload_sets_value() {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let node = NodeId(1);
    let insert = Operation::insert_with_payload(
        &ReplicaId::new(b"a"),
        1,
        1,
        NodeId::ROOT,
        node,
        0,
        b"hello",
    );

    tree.apply_remote(insert).unwrap();

    assert_eq!(tree.parent(node).unwrap(), Some(NodeId::ROOT));
    assert_eq!(tree.payload(node).unwrap(), Some(b"hello".to_vec()));
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![node]);
}

#[test]
fn insert_payload_does_not_override_newer_payload() {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let node = NodeId(1);
    let insert =
        Operation::insert_with_payload(&ReplicaId::new(b"a"), 1, 1, NodeId::ROOT, node, 0, b"old");
    let payload = Operation::set_payload(&ReplicaId::new(b"a"), 2, 2, node, b"new");

    // Receive payload first, then receive the earlier insert-with-payload (out of order by lamport).
    tree.apply_remote(payload).unwrap();
    tree.apply_remote(insert).unwrap();

    assert_eq!(tree.parent(node).unwrap(), Some(NodeId::ROOT));
    assert_eq!(tree.payload(node).unwrap(), Some(b"new".to_vec()));
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![node]);
}
