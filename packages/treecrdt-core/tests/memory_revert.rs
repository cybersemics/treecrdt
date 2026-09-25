use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation, OperationId, ReplicaId,
    TreeCrdt,
};

type Tree = TreeCrdt<MemoryStorage, LamportClock>;

fn tree() -> Tree {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    tree.track_snapshot_changes();
    tree
}

fn insert(tree: &mut Tree, parent: NodeId, node: u128, value: &[u8]) -> Operation {
    tree.local_insert(
        parent,
        NodeId(node),
        LocalPlacement::Last,
        Some(value.to_vec()),
    )
    .unwrap()
    .0
}

fn ids(operations: &[Operation]) -> Vec<OperationId> {
    operations.iter().map(|op| op.meta.id.clone()).collect()
}

#[test]
fn mixed_receipt_undo_and_redo_append_new_operations() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1, b"before");
    insert(&mut tree, NodeId::ROOT, 2, b"parent");
    let receipt = vec![
        tree.local_payload(NodeId(1), Some(b"after".to_vec())).unwrap().0,
        tree.local_move(NodeId(1), NodeId(2), LocalPlacement::First).unwrap().0,
        insert(&mut tree, NodeId(1), 3, b"child"),
    ];
    let committed = tree.operations_from(0).unwrap();
    let undo = tree.revert_operations(&ids(&receipt)).unwrap();
    assert!(!undo.is_empty());
    assert_eq!(
        tree.children(NodeId::ROOT).unwrap(),
        vec![NodeId(1), NodeId(2)]
    );
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"before".to_vec()));
    assert!(tree.is_tombstoned(NodeId(3)).unwrap());
    assert_eq!(
        &tree.operations_from(0).unwrap()[..committed.len()],
        committed
    );
    assert!(undo.iter().all(|op| op.meta.lamport > committed.last().unwrap().meta.lamport));

    let redo = tree.revert_operations(&ids(&undo)).unwrap();
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![NodeId(2)]);
    assert_eq!(tree.children(NodeId(2)).unwrap(), vec![NodeId(1)]);
    assert_eq!(tree.children(NodeId(1)).unwrap(), vec![NodeId(3)]);
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"after".to_vec()));
    assert_eq!(tree.payload(NodeId(3)).unwrap(), Some(b"child".to_vec()));
    assert_eq!(
        tree.operation_count(),
        committed.len() + undo.len() + redo.len()
    );
}

#[test]
fn restores_deleted_parent_and_descendants_before_redoing_the_delete() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1, b"parent");
    insert(&mut tree, NodeId(1), 2, b"child");
    insert(&mut tree, NodeId(2), 3, b"leaf");
    let deleted = [3, 2, 1]
        .into_iter()
        .map(|node| tree.local_delete(NodeId(node)).unwrap().0)
        .collect::<Vec<_>>();
    assert!(tree.children(NodeId::ROOT).unwrap().is_empty());
    let undo = tree.revert_operations(&ids(&deleted)).unwrap();
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), vec![NodeId(1)]);
    assert_eq!(tree.children(NodeId(1)).unwrap(), vec![NodeId(2)]);
    assert_eq!(tree.children(NodeId(2)).unwrap(), vec![NodeId(3)]);
    assert_eq!(tree.payload(NodeId(3)).unwrap(), Some(b"leaf".to_vec()));
    tree.revert_operations(&ids(&undo)).unwrap();
    assert!(tree.children(NodeId::ROOT).unwrap().is_empty());
    assert!(tree.is_tombstoned(NodeId(2)).unwrap());
    assert!(tree.is_tombstoned(NodeId(3)).unwrap());
}

#[test]
fn every_subtree_deletion_order_restores_selected_ancestors_and_supports_redo() {
    for order in [
        [1, 2, 3],
        [1, 3, 2],
        [2, 1, 3],
        [2, 3, 1],
        [3, 1, 2],
        [3, 2, 1],
    ] {
        let mut tree = tree();
        insert(&mut tree, NodeId::ROOT, 1, b"parent");
        insert(&mut tree, NodeId(1), 2, b"child");
        insert(&mut tree, NodeId(2), 3, b"leaf");
        let deleted = order
            .into_iter()
            .map(|node| tree.local_delete(NodeId(node)).unwrap().0)
            .collect::<Vec<_>>();
        let undo = tree.revert_operations(&ids(&deleted)).unwrap();
        assert_eq!(
            tree.children(NodeId::ROOT).unwrap(),
            vec![NodeId(1)],
            "{order:?}"
        );
        assert_eq!(
            tree.children(NodeId(1)).unwrap(),
            vec![NodeId(2)],
            "{order:?}"
        );
        assert_eq!(
            tree.children(NodeId(2)).unwrap(),
            vec![NodeId(3)],
            "{order:?}"
        );
        tree.revert_operations(&ids(&undo)).unwrap();
        assert!(tree.children(NodeId::ROOT).unwrap().is_empty(), "{order:?}");
        assert!(tree.is_tombstoned(NodeId(2)).unwrap(), "{order:?}");
        assert!(tree.is_tombstoned(NodeId(3)).unwrap(), "{order:?}");
    }
}

#[test]
fn force_revert_replaces_remote_payload_and_redo_restores_what_it_replaced() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1, b"before");
    let local = tree.local_payload(NodeId(1), Some(b"local".to_vec())).unwrap().0;
    let remote = Operation::set_payload(&ReplicaId::new(b"remote"), 1, 50, NodeId(1), b"remote");
    tree.apply_remote(remote.clone()).unwrap();
    let undo = tree.revert_operations(&ids(&[local])).unwrap();
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"before".to_vec()));
    assert!(undo[0].meta.lamport > remote.meta.lamport);
    tree.revert_operations(&ids(&undo)).unwrap();
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"remote".to_vec()));
}

#[test]
fn inversion_uses_canonical_history_not_arrival_or_receipt_order() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1, b"initial");
    let replica = ReplicaId::new(b"remote");
    let older = Operation::set_payload(&replica, 1, 10, NodeId(1), b"older");
    let newer = Operation::set_payload(&replica, 2, 20, NodeId(1), b"newer");
    tree.apply_remote(newer.clone()).unwrap();
    tree.apply_remote(older.clone()).unwrap();
    let undo = tree.revert_operations(&ids(&[newer.clone(), newer])).unwrap();
    assert_eq!(undo.len(), 1);
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"older".to_vec()));
    let payload = tree.local_payload(NodeId(1), Some(b"again".to_vec())).unwrap().0;
    tree.revert_operations(&ids(&[payload, older])).unwrap();
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"initial".to_vec()));
}

#[test]
fn missing_ids_reject_before_changes_and_empty_receipts_are_noops() {
    let mut tree = tree();
    let inserted = insert(&mut tree, NodeId::ROOT, 1, b"value");
    let before = tree.operations_from(0).unwrap();
    let clock = tree.lamport();
    let pending = tree.pending_snapshot_changes();
    let missing = OperationId::new(&ReplicaId::new(b"missing"), 1);
    assert!(tree.revert_operations(&[inserted.meta.id, missing]).is_err());
    assert!(tree.revert_operations(&[]).unwrap().is_empty());
    assert_eq!(tree.operations_from(0).unwrap(), before);
    assert_eq!(tree.lamport(), clock);
    assert_eq!(tree.pending_snapshot_changes(), pending);
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"value".to_vec()));
}

#[test]
fn moved_and_deleted_anchors_reject_and_roll_back_an_earlier_inverse_payload() {
    for delete_anchor in [false, true] {
        let mut tree = tree();
        insert(&mut tree, NodeId::ROOT, 1, b"anchor");
        insert(&mut tree, NodeId::ROOT, 2, b"target");
        insert(&mut tree, NodeId::ROOT, 3, b"destination");
        let moved = tree.local_move(NodeId(2), NodeId(3), LocalPlacement::First).unwrap().0;
        let payload = tree.local_payload(NodeId(2), Some(b"changed".to_vec())).unwrap().0;
        if delete_anchor {
            tree.local_delete(NodeId(1)).unwrap();
        } else {
            tree.local_move(NodeId(1), NodeId(3), LocalPlacement::Last).unwrap();
        }
        let before = tree.operations_from(0).unwrap();
        let nodes = tree.nodes().unwrap();
        let clock = tree.lamport();
        let pending = tree.pending_snapshot_changes();
        let error = tree.revert_operations(&ids(&[moved, payload])).unwrap_err();
        assert!(error.to_string().contains("revert anchor"));
        assert_eq!(tree.operations_from(0).unwrap(), before);
        assert_eq!(tree.nodes().unwrap(), nodes);
        assert_eq!(tree.payload(NodeId(2)).unwrap(), Some(b"changed".to_vec()));
        assert_eq!(tree.lamport(), clock);
        assert_eq!(tree.pending_snapshot_changes(), pending);
        let next = tree.local_payload(NodeId(2), Some(b"next".to_vec())).unwrap().0;
        assert_eq!(
            next.meta.id.counter,
            before.last().unwrap().meta.id.counter + 1
        );
    }
}

#[test]
fn deleted_destinations_and_new_cycles_reject_without_silent_restoration() {
    for delete_parent in [false, true] {
        let mut tree = tree();
        insert(&mut tree, NodeId::ROOT, 1, b"parent");
        insert(&mut tree, NodeId(1), 2, b"target");
        let moved = tree.local_move(NodeId(2), NodeId::ROOT, LocalPlacement::Last).unwrap().0;
        if delete_parent {
            tree.local_delete(NodeId(1)).unwrap();
        } else {
            tree.local_move(NodeId(1), NodeId(2), LocalPlacement::First).unwrap();
        }
        let before = tree.operations_from(0).unwrap();
        let error = tree.revert_operations(&ids(&[moved])).unwrap_err();
        assert!(error.to_string().contains(if delete_parent {
            "destination"
        } else {
            "cycle"
        }));
        assert_eq!(tree.operations_from(0).unwrap(), before);
        assert_eq!(tree.parent(NodeId(2)).unwrap(), Some(NodeId::ROOT));
        if delete_parent {
            assert!(tree.is_tombstoned(NodeId(1)).unwrap());
        }
    }
}

#[test]
fn undo_payload_that_revived_a_deleted_node_hides_it_again() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1, b"original");
    tree.local_delete(NodeId(1)).unwrap();
    let revived = tree.local_payload(NodeId(1), Some(b"revived".to_vec())).unwrap().0;
    assert!(!tree.is_tombstoned(NodeId(1)).unwrap());
    let undo = tree.revert_operations(&ids(&[revived])).unwrap();
    assert!(tree.is_tombstoned(NodeId(1)).unwrap());
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"original".to_vec()));
    tree.revert_operations(&ids(&undo)).unwrap();
    assert!(!tree.is_tombstoned(NodeId(1)).unwrap());
    assert_eq!(tree.payload(NodeId(1)).unwrap(), Some(b"revived".to_vec()));
}
