use treecrdt_core::{LamportClock, MemoryStorage, NodeId, Operation, ReplicaId, TreeCrdt};

#[test]
fn prevents_cycle_on_move() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let root = NodeId::ROOT;
    let a = NodeId(1);
    let b = NodeId(2);

    crdt.local_insert(root, a, 0).unwrap();
    crdt.local_insert(a, b, 0).unwrap();

    crdt.apply_remote(Operation::move_node(&ReplicaId::new(b"a"), 3, 3, a, b, 0))
        .unwrap();
    assert_eq!(crdt.parent(a), Some(root));
}

#[test]
fn cycles_are_blocked() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let root = NodeId::ROOT;
    let a = NodeId(1);
    let b = NodeId(2);

    let inserts = [
        Operation::insert(&ReplicaId::new(b"a"), 1, 1, root, a, 0),
        Operation::insert(&ReplicaId::new(b"a"), 2, 2, a, b, 0),
    ];
    for op in inserts {
        crdt.apply_remote(op).unwrap();
    }

    let bad_move = Operation::move_node(&ReplicaId::new(b"a"), 3, 3, a, b, 0);
    crdt.apply_remote(bad_move).unwrap();
    assert_eq!(crdt.parent(a), Some(root));
    assert_eq!(crdt.parent(b), Some(a));
    crdt.validate_invariants().unwrap();
}
