use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation, OperationKind, ReplicaId,
    Storage, TreeCrdt, VersionVector,
};

fn assert_conflict(storage: &mut MemoryStorage, op: Operation) {
    let err = storage.apply(op).expect_err("conflicting operation id must fail");
    assert!(err.to_string().contains("conflicting contents"));
}

#[test]
fn memory_storage_only_deduplicates_identical_operations() {
    let replica = ReplicaId::new(b"replica");
    let original = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0, 1]);
    let mut storage = MemoryStorage::default();

    assert!(storage.apply(original.clone()).unwrap());
    assert!(!storage.apply(original.clone()).unwrap());

    let mut conflicts = Vec::new();

    let mut changed = original.clone();
    changed.meta.lamport = 2;
    conflicts.push(changed);

    let mut changed = original.clone();
    if let OperationKind::Insert { parent, .. } = &mut changed.kind {
        *parent = NodeId(2);
    }
    conflicts.push(changed);

    let mut changed = original.clone();
    if let OperationKind::Insert { node, .. } = &mut changed.kind {
        *node = NodeId(2);
    }
    conflicts.push(changed);

    let mut changed = original.clone();
    if let OperationKind::Insert { order_key, .. } = &mut changed.kind {
        *order_key = vec![0, 2];
    }
    conflicts.push(changed);

    let mut changed = original.clone();
    if let OperationKind::Insert { payload, .. } = &mut changed.kind {
        *payload = Some(Vec::new());
    }
    conflicts.push(changed);

    let mut changed = Operation::move_node(
        &replica,
        original.meta.id.counter,
        original.meta.lamport,
        NodeId(1),
        NodeId::ROOT,
        vec![0, 1],
    );
    changed.meta.id = original.meta.id.clone();
    conflicts.push(changed);

    for conflict in conflicts {
        assert_conflict(&mut storage, conflict);
    }

    assert_eq!(storage.load_since(0).unwrap(), vec![original]);
}

#[test]
fn memory_storage_distinguishes_payload_null_empty_and_known_state() {
    let replica = ReplicaId::new(b"replica");
    let mut storage = MemoryStorage::default();

    let cleared = Operation::payload(&replica, 1, 1, NodeId(1), None);
    assert!(storage.apply(cleared.clone()).unwrap());
    assert!(!storage.apply(cleared).unwrap());
    assert_conflict(
        &mut storage,
        Operation::payload(&replica, 1, 1, NodeId(1), Some(Vec::new())),
    );

    let tombstone = Operation::tombstone(&replica, 2, 2, NodeId(1));
    assert!(storage.apply(tombstone.clone()).unwrap());
    assert!(!storage.apply(tombstone.clone()).unwrap());

    let mut known_state = VersionVector::new();
    known_state.observe(&replica, 1);
    let mut changed = tombstone;
    changed.meta.known_state = Some(known_state);
    assert_conflict(&mut storage, changed);
}

#[test]
fn rejected_equivocation_does_not_advance_the_local_clock() {
    let remote = ReplicaId::new(b"remote");
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let original = Operation::insert(&remote, 1, 1, NodeId::ROOT, NodeId(1), vec![0, 1]);
    tree.apply_remote(original.clone()).unwrap();

    let mut conflict = original;
    conflict.meta.lamport = 100;
    assert!(tree.apply_remote(conflict).is_err());

    let (local, _) =
        tree.local_insert(NodeId::ROOT, NodeId(2), LocalPlacement::Last, None).unwrap();
    assert_eq!(local.meta.lamport, 2);
}
