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

    assert_eq!(a.payload(node), Some(&b"B"[..]));
    assert_eq!(b.payload(node), Some(&b"B"[..]));
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

    assert_eq!(a.payload(node), None);
    assert_eq!(b.payload(node), None);
}

