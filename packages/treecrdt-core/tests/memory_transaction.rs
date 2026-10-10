use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation, ReplicaId, TreeCrdt,
};

fn tree() -> TreeCrdt<MemoryStorage, LamportClock> {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    tree.track_read_changes();
    tree.drain_read_changes().unwrap();
    tree
}

fn insert(tree: &mut TreeCrdt<MemoryStorage, LamportClock>, node: u128) -> Operation {
    tree.local_insert(
        NodeId::ROOT,
        NodeId(node),
        LocalPlacement::Last,
        Some(vec![node as u8]),
    )
    .unwrap()
    .0
}

#[test]
fn rollback_restores_log_pending_changes_clock_counter_and_visibility() {
    let mut actual = tree();
    let mut control = tree();
    let zero = Operation::insert(
        &ReplicaId::new(b"remote"),
        1,
        0,
        NodeId::ROOT,
        NodeId(99),
        vec![0, 99],
    );
    actual.apply_remote(zero.clone()).unwrap();
    control.apply_remote(zero).unwrap();
    insert(&mut actual, 1);
    insert(&mut control, 1);
    // Keep undrained records in the checkpoint, then drain provisional reads inside the transaction.
    let pending = actual.pending_read_changes().unwrap();
    let checkpoint = actual.memory_checkpoint();
    actual.drain_read_changes().unwrap();
    actual.local_delete(NodeId(1)).unwrap();
    insert(&mut actual, 2);
    actual.drain_read_changes().unwrap();
    // Even an ignored identity duplicate observes a newer clock/counter envelope.
    let mut duplicate = actual.operations_at(&[1]).unwrap().remove(0);
    duplicate.meta.lamport = 500;
    actual.apply_remote(duplicate).unwrap();
    actual.rollback_memory(checkpoint).unwrap();

    assert_eq!(actual.pending_read_changes().unwrap(), pending);
    assert_eq!(actual.operation_count(), 2);
    assert_eq!(
        actual.operations_from(0).unwrap(),
        control.operations_from(0).unwrap()
    );
    assert_eq!(actual.lamport(), control.lamport());
    assert_eq!(actual.nodes().unwrap(), control.nodes().unwrap());
    assert_eq!(
        actual.children(NodeId::ROOT).unwrap(),
        control.children(NodeId::ROOT).unwrap()
    );
    assert_eq!(actual.payload(NodeId(2)).unwrap(), None);
    assert_eq!(
        actual.subtree_version_vector(NodeId::ROOT).unwrap(),
        control.subtree_version_vector(NodeId::ROOT).unwrap()
    );
    assert_eq!(insert(&mut actual, 2), insert(&mut control, 2));
}

#[test]
fn rollback_after_failed_prepare_does_not_consume_an_operation_id() {
    let mut actual = tree();
    let mut control = tree();
    insert(&mut actual, 1);
    insert(&mut control, 1);
    let checkpoint = actual.memory_checkpoint();
    assert!(actual
        .local_move(NodeId(1), NodeId::ROOT, LocalPlacement::After(NodeId(99)))
        .is_err());
    actual.rollback_memory(checkpoint).unwrap();
    assert_eq!(insert(&mut actual, 2), insert(&mut control, 2));
}

#[test]
fn indexed_log_reads_preserve_arrival_order_and_historical_tail() {
    let mut actual = tree();
    let replica = ReplicaId::new(b"remote");
    let zero = Operation::insert(
        &ReplicaId::new(b"zero"),
        1,
        0,
        NodeId::ROOT,
        NodeId(3),
        vec![0, 3],
    );
    let high = Operation::insert(&replica, 2, 20, NodeId::ROOT, NodeId(2), vec![0, 2]);
    let low = Operation::insert(&replica, 1, 10, NodeId::ROOT, NodeId(1), vec![0, 1]);
    actual.apply_remote_batch([zero.clone(), high.clone()]).unwrap();
    let cursor = actual.operation_count();
    actual.apply_remote_batch(vec![low.clone(), high.clone()]).unwrap();
    assert_eq!(actual.operations_from(cursor).unwrap(), vec![low.clone()]);
    assert_eq!(actual.operations_range(1, 2).unwrap(), vec![high.clone()]);
    assert_eq!(actual.operations_range(2, 3).unwrap(), vec![low.clone()]);
    assert!(actual.operations_range(3, 3).unwrap().is_empty());
    assert!(actual.operations_range(2, 1).is_err());
    assert!(actual.operations_range(0, 4).is_err());
    assert_eq!(
        actual.operations_at(&[2, 0, 1, 2]).unwrap(),
        vec![low.clone(), zero, high, low]
    );
    assert!(actual.operations_at(&[3]).is_err());
    assert!(actual.operations_from(4).is_err());
    assert!(actual.operations_from(3).unwrap().is_empty());
    assert_eq!(actual.nodes().unwrap().len(), 3);
    assert_eq!(
        actual.children(NodeId::ROOT).unwrap(),
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );
}
