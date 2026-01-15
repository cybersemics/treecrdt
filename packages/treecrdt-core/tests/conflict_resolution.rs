use treecrdt_core::{LamportClock, MemoryStorage, NodeId, Operation, ReplicaId, TreeCrdt};

#[test]
fn higher_lamport_wins_on_conflict() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let root = NodeId::ROOT;
    let x = NodeId(1);
    let left = NodeId(10);
    let right = NodeId(11);

    let insert_left = crdt_a.local_insert(root, left, 0).unwrap();
    let insert_right = crdt_a.local_insert(root, right, 1).unwrap();
    let insert_x = crdt_a.local_insert(root, x, 2).unwrap();

    // replica a moves x under left (lamport 4)
    let move_left = crdt_a.local_move(x, left, 0).unwrap();

    // replica b moves x under right with higher lamport
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    crdt_b.apply_remote(insert_left.clone()).unwrap();
    crdt_b.apply_remote(insert_right.clone()).unwrap();
    crdt_b.apply_remote(insert_x.clone()).unwrap();
    let move_right = Operation::move_node(
        &ReplicaId::new(b"b"),
        1,
        move_left.meta.lamport + 1,
        x,
        right,
        0,
    );
    crdt_b.apply_remote(move_right.clone()).unwrap();

    // apply both moves to a; higher lamport should win
    crdt_a.apply_remote(move_right).unwrap();
    crdt_a.apply_remote(move_left).unwrap();

    assert_eq!(crdt_a.parent(x).unwrap(), Some(right));
}

#[test]
fn moves_reordered_by_lamport_and_id() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let root = NodeId::ROOT;
    let a = NodeId(1);
    let b = NodeId(2);
    let x = NodeId(3);

    let ops = [
        Operation::insert(&ReplicaId::new(b"a"), 1, 1, root, a, 0),
        Operation::insert(&ReplicaId::new(b"a"), 2, 2, root, b, 1),
        Operation::insert(&ReplicaId::new(b"a"), 3, 3, root, x, 2),
        // higher lamport move -> should win
        Operation::move_node(&ReplicaId::new(b"a"), 4, 5, x, a, 0),
        Operation::move_node(&ReplicaId::new(b"a"), 5, 4, x, b, 0),
    ];

    // apply out of order
    for op in ops.iter().rev() {
        crdt.apply_remote(op.clone()).unwrap();
    }

    assert_eq!(crdt.parent(x).unwrap(), Some(a));
    crdt.replay_from_storage().unwrap();
    assert_eq!(crdt.parent(x).unwrap(), Some(a));
}

#[test]
fn same_lamport_orders_by_op_id() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let root = NodeId::ROOT;
    let a = NodeId(1);
    let b = NodeId(2);
    let x = NodeId(3);

    let inserts = [
        Operation::insert(&ReplicaId::new(b"a"), 1, 1, root, a, 0),
        Operation::insert(&ReplicaId::new(b"a"), 2, 2, root, b, 1),
        Operation::insert(&ReplicaId::new(b"a"), 3, 3, root, x, 2),
    ];
    for op in inserts {
        crdt.apply_remote(op).unwrap();
    }

    let move_a = Operation::move_node(&ReplicaId::new(b"a"), 10, 5, x, a, 0);
    let move_b = Operation::move_node(&ReplicaId::new(b"b"), 10, 5, x, b, 0);

    crdt.apply_remote(move_b.clone()).unwrap();
    crdt.apply_remote(move_a.clone()).unwrap();
    // ReplicaId "b" > "a" so move_b wins at equal lamport
    assert_eq!(crdt.parent(x).unwrap(), Some(b));
}
