use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryNodeStore, MemoryPayloadStore, MemoryStorage, NodeId,
    NodeStore, NoopParentOpIndex, Operation, ReplicaId, TreeCrdt,
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
fn persisted_operations_require_portable_key_range() {
    let replica = ReplicaId::new(b"remote");
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let overflow = i64::MAX as u64 + 1;
    for (counter, lamport, node) in [
        (1, 0, NodeId(90)),
        (0, 1, NodeId(91)),
        (1, overflow, NodeId(92)),
        (overflow, 1, NodeId(93)),
    ] {
        let op = Operation::insert(&replica, counter, lamport, NodeId::ROOT, node, vec![0x10]);
        assert!(crdt.apply_remote(op).is_err());
    }
    assert!(crdt.children(NodeId::ROOT).unwrap().is_empty());
    assert!(crdt.operations_since(0).unwrap().is_empty());
}

#[test]
fn prepared_local_op_does_not_mutate_until_committed() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let node = NodeId(1);
    let prepared = crdt
        .prepare_local_insert(NodeId::ROOT, node, LocalPlacement::First, Some(vec![1]))
        .unwrap();

    assert_eq!(crdt.children(NodeId::ROOT).unwrap(), Vec::<NodeId>::new());
    assert_eq!(crdt.parent(node).unwrap(), None);

    let (op, plan) = crdt.commit_prepared_local(prepared).unwrap();

    assert_eq!(op.kind.node(), node);
    assert_eq!(plan.changes.len(), 1);
    assert_eq!(crdt.children(NodeId::ROOT).unwrap(), &[node]);
    assert_eq!(crdt.parent(node).unwrap(), Some(NodeId::ROOT));
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
fn rejected_cycle_move_emits_no_visible_change() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    crdt.local_insert(root, parent, LocalPlacement::First, None).unwrap();
    crdt.local_insert(parent, child, LocalPlacement::First, None).unwrap();

    let mut seq = 0;
    let mut index = NoopParentOpIndex;
    let delta = crdt
        .apply_remote_with_materialization_seq(
            Operation::move_node(&ReplicaId::new(b"remote"), 1, 3, parent, child, Vec::new()),
            &mut index,
            &mut seq,
        )
        .unwrap()
        .unwrap();

    assert!(delta.changes.is_empty());
    assert_eq!(crdt.parent(parent).unwrap(), Some(root));
    assert_eq!(crdt.parent(child).unwrap(), Some(parent));

    let rejected_insert = crdt
        .apply_remote_with_materialization_seq(
            Operation::insert(&ReplicaId::new(b"remote"), 2, 4, child, parent, Vec::new()),
            &mut index,
            &mut seq,
        )
        .unwrap()
        .unwrap();
    assert!(rejected_insert.changes.is_empty());
    assert_eq!(crdt.parent(parent).unwrap(), Some(root));

    let rejected_insert_with_payload = crdt
        .apply_remote_with_materialization_seq(
            Operation::insert_with_payload(
                &ReplicaId::new(b"remote"),
                3,
                5,
                child,
                child,
                Vec::new(),
                vec![9],
            ),
            &mut index,
            &mut seq,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        rejected_insert_with_payload.changes.as_slice(),
        [treecrdt_core::MaterializationChange::Payload {
            node,
            payload: Some(payload),
            ..
        }] if *node == child && payload == &[9]
    ));
    assert_eq!(crdt.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt.payload(child).unwrap(), Some(vec![9]));

    for op in [
        Operation::insert(
            &ReplicaId::new(b"remote"),
            4,
            6,
            NodeId::TRASH,
            NodeId::ROOT,
            Vec::new(),
        ),
        Operation::move_node(
            &ReplicaId::new(b"remote"),
            5,
            7,
            NodeId::TRASH,
            root,
            Vec::new(),
        ),
    ] {
        let delta = crdt
            .apply_remote_with_materialization_seq(op, &mut index, &mut seq)
            .unwrap()
            .unwrap();
        assert!(delta.changes.is_empty());
    }
    assert_eq!(crdt.parent(NodeId::ROOT).unwrap(), None);
    assert_eq!(crdt.parent(NodeId::TRASH).unwrap(), None);
}

#[test]
fn rejected_local_cycle_move_has_no_visible_change_plan() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    crdt.local_insert(root, parent, LocalPlacement::First, None).unwrap();
    crdt.local_insert(parent, child, LocalPlacement::First, None).unwrap();

    let (_op, plan) = crdt.local_move(parent, child, LocalPlacement::First).unwrap();

    assert!(plan.changes.is_empty());
    assert_eq!(crdt.parent(parent).unwrap(), Some(root));
    assert_eq!(crdt.parent(child).unwrap(), Some(parent));
}

#[test]
fn malformed_parent_cycle_rejects_move_without_looping() {
    let root = NodeId::ROOT;
    let cycle_a = NodeId(1);
    let cycle_b = NodeId(2);
    let node = NodeId(3);
    let mut nodes = MemoryNodeStore::default();
    nodes.ensure_node(cycle_a).unwrap();
    nodes.ensure_node(cycle_b).unwrap();
    nodes.ensure_node(node).unwrap();
    nodes.attach(cycle_a, cycle_b, vec![1]).unwrap();
    nodes.attach(cycle_b, cycle_a, vec![1]).unwrap();
    nodes.attach(node, root, vec![1]).unwrap();

    let mut crdt = TreeCrdt::with_stores(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
        nodes,
        MemoryPayloadStore::default(),
    )
    .unwrap();
    let mut seq = 0;
    let mut index = NoopParentOpIndex;
    let delta = crdt
        .apply_remote_with_materialization_seq(
            Operation::move_node(&ReplicaId::new(b"remote"), 1, 1, node, cycle_a, Vec::new()),
            &mut index,
            &mut seq,
        )
        .unwrap()
        .unwrap();

    assert!(delta.changes.is_empty());
    assert_eq!(crdt.parent(node).unwrap(), Some(root));
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
