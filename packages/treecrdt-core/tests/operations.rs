use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryStorage, NodeId, NoopParentOpIndex, Operation, ReplicaId,
    TreeCrdt,
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

    crdt.local_insert(root, a, LocalPlacement::First, None).unwrap();
    crdt.local_insert(a, b, LocalPlacement::First, None).unwrap();

    assert_eq!(crdt.parent(a).unwrap(), Some(root));
    assert_eq!(crdt.parent(b).unwrap(), Some(a));

    crdt.local_move(b, root, LocalPlacement::First).unwrap();
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

    let (op, _) = crdt.local_insert(NodeId::ROOT, NodeId(1), LocalPlacement::First, None).unwrap();
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
    crdt.local_insert(NodeId::ROOT, child, LocalPlacement::First, None).unwrap();
    crdt.local_delete(child).unwrap();

    assert!(crdt.is_tombstoned(child).unwrap());
    assert_eq!(crdt.parent(child).unwrap(), Some(NodeId::TRASH));
    assert!(crdt.children(NodeId::ROOT).unwrap().is_empty());

    crdt.local_move(child, NodeId::ROOT, LocalPlacement::First).unwrap();
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

    crdt.local_insert(root, a, LocalPlacement::First, None).unwrap();
    crdt.local_insert(a, b, LocalPlacement::First, None).unwrap();

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

#[test]
fn apply_remote_with_materialization_reports_changes() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut seq = 0;
    let mut index = NoopParentOpIndex;
    let replica = ReplicaId::new(b"remote");

    let insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), Vec::new());
    let insert_delta = crdt
        .apply_remote_with_materialization_seq(insert, &mut index, &mut seq)
        .unwrap()
        .unwrap();
    assert_eq!(insert_delta.changes.len(), 1);
    assert_eq!(
        insert_delta.changes[0].affected_nodes(),
        vec![NodeId(1), NodeId::ROOT]
    );

    let payload = Operation::set_payload(&replica, 2, 2, NodeId(1), b"hello".to_vec());
    let payload_delta = crdt
        .apply_remote_with_materialization_seq(payload, &mut index, &mut seq)
        .unwrap()
        .unwrap();
    assert_eq!(payload_delta.changes.len(), 1);
    assert_eq!(payload_delta.changes[0].affected_nodes(), vec![NodeId(1)]);
}

#[test]
fn local_move_tracks_hint_and_payload_reindex() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let root = NodeId::ROOT;
    let parent_a = NodeId(10);
    let parent_b = NodeId(11);
    let node = NodeId(12);

    crdt.local_insert(root, parent_a, LocalPlacement::First, None).unwrap();
    crdt.local_insert(root, parent_b, LocalPlacement::First, None).unwrap();
    crdt.local_insert(parent_a, node, LocalPlacement::First, Some(vec![1])).unwrap();

    let expected_payload_writer = crdt.payload_last_writer(node).unwrap().unwrap().1;
    let (_op, plan) = crdt.local_move(node, parent_b, LocalPlacement::Last).unwrap();

    assert_eq!(plan.parent_hints, vec![parent_b, parent_a]);
    assert_eq!(
        plan.extra_index_records,
        vec![(parent_b, expected_payload_writer)]
    );
}

#[test]
fn local_placement_requires_after_for_after_variant() {
    let err = LocalPlacement::from_parts("after", None).unwrap_err();
    assert!(format!("{err:?}").contains("missing after"));
}

#[test]
fn resolve_after_rejects_excluded_node() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let root = NodeId::ROOT;
    let node = NodeId(42);
    crdt.local_insert(root, node, LocalPlacement::First, None).unwrap();

    let err = crdt
        .resolve_after_for_placement(root, LocalPlacement::After(node), Some(node))
        .unwrap_err();
    assert!(format!("{err:?}").contains("excluded"));
}

#[test]
fn finalize_local_advances_head_seq() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let root = NodeId::ROOT;
    let node = NodeId(7);
    let (op, plan) = crdt.local_insert(root, node, LocalPlacement::First, None).unwrap();
    let mut index = NoopParentOpIndex;

    let next_seq = crdt.finalize_local(&op, &mut index, 41, &plan).unwrap();
    assert_eq!(next_seq, 42);
}
