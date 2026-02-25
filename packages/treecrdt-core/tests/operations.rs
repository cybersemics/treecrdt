use treecrdt_core::{
    LamportClock, MemoryStorage, NodeId, NoopParentOpIndex, Operation, ReplicaId, TreeCrdt,
};

#[test]
fn inserts_and_moves_nodes() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let root = NodeId::ROOT;
    let a = NodeId(1);
    let b = NodeId(2);

    crdt.local_insert_after(root, a, None).unwrap();
    crdt.local_insert_after(a, b, None).unwrap();

    assert_eq!(crdt.parent(a).unwrap(), Some(root));
    assert_eq!(crdt.parent(b).unwrap(), Some(a));

    // move b under root
    crdt.local_move_after(b, root, None).unwrap();
    assert_eq!(crdt.parent(b).unwrap(), Some(root));
    assert_eq!(crdt.children(root).unwrap(), &[b, a]);
}

#[test]
fn duplicate_operations_are_ignored() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let op = crdt.local_insert_after(NodeId::ROOT, NodeId(1), None).unwrap();
    // applying again should be idempotent
    crdt.apply_remote(op.clone()).unwrap();
    crdt.apply_remote(op).unwrap();
    assert_eq!(crdt.children(NodeId::ROOT).unwrap(), &[NodeId(1)]);
}

#[test]
fn delete_marks_tombstone_and_removes_from_parent() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let child = NodeId(1);
    crdt.local_insert_after(NodeId::ROOT, child, None).unwrap();
    crdt.local_delete(child).unwrap();

    assert!(crdt.is_tombstoned(child).unwrap());
    assert_eq!(crdt.parent(child).unwrap(), Some(NodeId::TRASH));
    assert!(crdt.children(NodeId::ROOT).unwrap().is_empty());

    crdt.local_move_after(child, NodeId::ROOT, None).unwrap();
    assert!(!crdt.is_tombstoned(child).unwrap());
    assert_eq!(crdt.parent(child).unwrap(), Some(NodeId::ROOT));
}

#[test]
fn prevents_cycle_on_move() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let root = NodeId::ROOT;
    let a = NodeId(1);
    let b = NodeId(2);

    crdt.local_insert_after(root, a, None).unwrap();
    crdt.local_insert_after(a, b, None).unwrap();

    crdt.apply_remote(Operation::move_node(
        &ReplicaId::new(b"a"),
        3,
        3,
        a,
        b,
        Vec::new(),
    ))
    .unwrap();
    assert_eq!(crdt.parent(a).unwrap(), Some(root));
}

#[test]
fn cycles_are_blocked() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let root = NodeId::ROOT;
    let a = NodeId(1);
    let b = NodeId(2);

    let inserts = [
        Operation::insert(&ReplicaId::new(b"a"), 1, 1, root, a, Vec::new()),
        Operation::insert(&ReplicaId::new(b"a"), 2, 2, a, b, Vec::new()),
    ];
    for op in inserts {
        crdt.apply_remote(op).unwrap();
    }

    let bad_move = Operation::move_node(&ReplicaId::new(b"a"), 3, 3, a, b, Vec::new());
    crdt.apply_remote(bad_move).unwrap();
    assert_eq!(crdt.parent(a).unwrap(), Some(root));
    assert_eq!(crdt.parent(b).unwrap(), Some(a));
    crdt.validate_invariants().unwrap();
}

#[test]
fn materialization_seq_advances_only_for_new_ops() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut seq = 0;
    let mut index = NoopParentOpIndex;
    let op = Operation::insert(
        &ReplicaId::new(b"remote"),
        1,
        1,
        NodeId::ROOT,
        NodeId(1),
        Vec::new(),
    );

    let first = crdt
        .apply_remote_with_materialization_seq(op.clone(), &mut index, &mut seq)
        .unwrap();
    assert!(first.is_some());
    assert_eq!(seq, 1);

    let second = crdt.apply_remote_with_materialization_seq(op, &mut index, &mut seq).unwrap();
    assert!(second.is_none());
    assert_eq!(seq, 1);
}
