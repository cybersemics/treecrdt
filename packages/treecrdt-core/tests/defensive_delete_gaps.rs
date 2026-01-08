use treecrdt_core::{LamportClock, MemoryStorage, NodeId, ReplicaId, TreeCrdt};

#[test]
fn delete_unrelated_ops_should_not_prevent_restoration_when_child_insert_was_unseen() {
    // Demonstrates false awareness when known_state uses max Lamport per replica (gaps across subtrees).
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);
    let unrelated = NodeId(99);

    let parent_op = crdt_a.local_insert(NodeId::ROOT, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let insert_child_op = crdt_b.local_insert(parent, child, 0).unwrap();

    let unrelated_op = crdt_b.local_insert(NodeId::ROOT, unrelated, 1).unwrap();
    crdt_a.apply_remote(unrelated_op).unwrap();

    let delete_op = crdt_a.local_delete(parent).unwrap();
    crdt_b.apply_remote(delete_op.clone()).unwrap();

    crdt_a.apply_remote(insert_child_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent),
        "delete should be treated as unaware of the child insert"
    );
    assert!(
        !crdt_b.is_tombstoned(parent),
        "converged state should keep parent restorable"
    );
}

#[test]
fn delete_should_restore_when_earlier_child_op_from_same_replica_was_missing() {
    // Requires dotted/range version vectors: seeing B:2 must not imply seeing B:1.
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child1 = NodeId(2);
    let child2 = NodeId(3);

    let parent_op = crdt_a.local_insert(NodeId::ROOT, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let insert_child1_op = crdt_b.local_insert(parent, child1, 0).unwrap();
    let insert_child2_op = crdt_b.local_insert(parent, child2, 1).unwrap();

    crdt_a.apply_remote(insert_child2_op).unwrap();

    let delete_op = crdt_a.local_delete(parent).unwrap();
    crdt_b.apply_remote(delete_op.clone()).unwrap();

    crdt_a.apply_remote(insert_child1_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent),
        "delete should be treated as unaware of the missing child insert"
    );
    assert!(
        !crdt_b.is_tombstoned(parent),
        "converged state should keep parent restorable"
    );
}
