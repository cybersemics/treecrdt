use std::collections::{BTreeMap, BTreeSet};

use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation, ReadChange, ReadNode,
    ReplicaId, TreeCrdt,
};

type Tree = TreeCrdt<MemoryStorage, LamportClock>;

fn tree() -> Tree {
    Tree::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap()
}

fn insert(tree: &mut Tree, parent: NodeId, id: u128) -> Operation {
    tree.local_insert(
        parent,
        NodeId(id),
        LocalPlacement::Last,
        Some(vec![id as u8]),
    )
    .unwrap()
    .0
}

fn rows(tree: &Tree) -> BTreeMap<NodeId, ReadNode> {
    tree.node_ids()
        .unwrap()
        .into_iter()
        .map(|id| (id, tree.read_node(id).unwrap().unwrap()))
        .collect()
}

fn expected_changes(before: &BTreeMap<NodeId, ReadNode>, tree: &Tree) -> Vec<ReadChange> {
    let after = rows(tree);
    let ids: BTreeSet<_> = before.keys().chain(after.keys()).copied().collect();
    ids.into_iter()
        .filter_map(|id| {
            let before = before.get(&id).cloned();
            let after = after.get(&id).cloned();
            (before != after).then_some(ReadChange { id, before, after })
        })
        .collect()
}

fn assert_delta(tree: &mut Tree, before: &BTreeMap<NodeId, ReadNode>, ids: &[NodeId]) {
    let changes = tree.drain_read_changes().unwrap();
    assert!(!changes.reset);
    assert_eq!(changes.changes, expected_changes(before, tree));
    assert_eq!(
        changes.changes.iter().map(|change| change.id).collect::<Vec<_>>(),
        ids
    );
    assert!(tree.drain_read_changes().unwrap().changes.is_empty());
}

#[test]
fn current_reads_and_sparse_rows_cover_rename_move_delete_and_defensive_restore() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1);
    insert(&mut tree, NodeId::ROOT, 2);
    insert(&mut tree, NodeId(1), 3);
    insert(&mut tree, NodeId(3), 4);
    tree.track_read_changes();
    let reset = tree.drain_read_changes().unwrap();
    assert!(reset.reset);
    assert!(reset.changes.is_empty());

    let before = rows(&tree);
    let retained = tree.read_node(NodeId(3)).unwrap().unwrap();
    tree.local_payload(NodeId(3), Some(b"renamed".to_vec())).unwrap();
    assert_delta(&mut tree, &before, &[NodeId(3)]);
    assert_eq!(retained.payload, Some(vec![3]));

    let before = rows(&tree);
    tree.local_move(NodeId(3), NodeId(2), LocalPlacement::Last).unwrap();
    assert_delta(&mut tree, &before, &[NodeId(1), NodeId(2), NodeId(3)]);

    let before = rows(&tree);
    tree.local_delete(NodeId(3)).unwrap();
    assert_delta(&mut tree, &before, &[NodeId(2), NodeId(3)]);
    assert_eq!(tree.read_node(NodeId(3)).unwrap(), None);
    assert!(!tree.node_ids().unwrap().contains(&NodeId(3)));

    let before = rows(&tree);
    tree.local_payload(NodeId(4), Some(b"new descendant content".to_vec())).unwrap();
    assert_delta(&mut tree, &before, &[NodeId(2), NodeId(3), NodeId(4)]);
}

#[test]
fn duplicate_rejected_and_final_noop_writes_have_no_visible_delta() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1);
    let initial = insert(&mut tree, NodeId(1), 2);
    tree.track_read_changes();
    tree.drain_read_changes().unwrap();
    let before = rows(&tree);
    tree.apply_remote(initial).unwrap();
    tree.local_move(NodeId(1), NodeId(2), LocalPlacement::Last).unwrap();
    tree.local_payload(NodeId(2), Some(vec![2])).unwrap();
    tree.local_payload(NodeId(2), Some(b"temporary".to_vec())).unwrap();
    tree.local_payload(NodeId(2), Some(vec![2])).unwrap();
    assert_delta(&mut tree, &before, &[]);
}

#[test]
fn replay_retains_first_before_images_and_discards_unchanged_rows() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1);
    insert(&mut tree, NodeId::ROOT, 2);
    tree.track_read_changes();
    tree.drain_read_changes().unwrap();
    let before = rows(&tree);
    tree.local_payload(NodeId(1), Some(b"first undrained edit".to_vec())).unwrap();
    tree.apply_remote(Operation::insert(
        &ReplicaId::new(b"remote"),
        1,
        0,
        NodeId::ROOT,
        NodeId(3),
        vec![0, 3],
    ))
    .unwrap();
    assert_delta(&mut tree, &before, &[NodeId::ROOT, NodeId(1), NodeId(3)]);

    let before = rows(&tree);
    tree.apply_remote(Operation::set_payload(
        &ReplicaId::new(b"remote"),
        2,
        0,
        NodeId(1),
        b"superseded".to_vec(),
    ))
    .unwrap();
    assert_delta(&mut tree, &before, &[]);
    tree.replay_from_storage().unwrap();
    assert_delta(&mut tree, &before, &[]);
}

#[test]
fn rollback_restores_undrained_records_even_after_provisional_drains_and_replay() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1);
    insert(&mut tree, NodeId::ROOT, 2);
    tree.track_read_changes();
    tree.drain_read_changes().unwrap();
    tree.local_payload(NodeId(1), Some(b"pending".to_vec())).unwrap();
    let before_transaction = rows(&tree);
    let pending = tree.pending_read_changes().unwrap();
    let checkpoint = tree.memory_checkpoint();
    tree.drain_read_changes().unwrap();
    tree.local_delete(NodeId(1)).unwrap();
    tree.apply_remote(Operation::insert(
        &ReplicaId::new(b"remote"),
        1,
        0,
        NodeId::ROOT,
        NodeId(3),
        vec![0, 3],
    ))
    .unwrap();
    tree.drain_read_changes().unwrap();
    tree.rollback_memory(checkpoint).unwrap();
    assert_eq!(rows(&tree), before_transaction);
    assert_eq!(tree.drain_read_changes().unwrap(), pending);

    let before = rows(&tree);
    tree.local_payload(NodeId(2), Some(b"next edit".to_vec())).unwrap();
    assert_delta(&mut tree, &before, &[NodeId(2)]);
}

#[test]
fn per_operation_deltas_match_full_reads_for_canonical_and_reversed_ingestion() {
    let mut source = tree();
    let mut operations = Vec::new();
    for id in 1..=12 {
        let parent = if id <= 3 {
            NodeId::ROOT
        } else {
            NodeId(id / 2)
        };
        operations.push(insert(&mut source, parent, id));
    }
    operations.push(source.local_delete(NodeId(2)).unwrap().0);
    operations.push(source.local_delete(NodeId(4)).unwrap().0);
    operations.push(source.local_payload(NodeId(8), Some(vec![42])).unwrap().0);
    operations.push(source.local_move(NodeId(4), NodeId(3), LocalPlacement::Last).unwrap().0);
    operations.push(source.local_move(NodeId(3), NodeId(8), LocalPlacement::Last).unwrap().0);
    operations.push(source.local_move(NodeId(12), NodeId::TRASH, LocalPlacement::Last).unwrap().0);
    operations.push(source.local_move(NodeId(11), NodeId(99), LocalPlacement::Last).unwrap().0);
    operations.push(source.local_payload(NodeId(50), None).unwrap().0);

    for reverse in [false, true] {
        let mut receiver = tree();
        receiver.track_read_changes();
        receiver.drain_read_changes().unwrap();
        let mut ordered = operations.clone();
        if reverse {
            ordered.reverse();
        }
        for operation in ordered {
            let before = rows(&receiver);
            receiver.apply_remote(operation).unwrap();
            let actual = receiver.drain_read_changes().unwrap();
            assert!(!actual.reset);
            assert_eq!(actual.changes, expected_changes(&before, &receiver));
        }
        assert_eq!(rows(&receiver), rows(&source));
    }
}

#[test]
fn relocated_trash_row_does_not_hide_its_old_parent_or_ancestor_restoration() {
    let mut tree = tree();
    insert(&mut tree, NodeId::ROOT, 1);
    insert(&mut tree, NodeId::ROOT, 2);
    tree.local_insert(NodeId(1), NodeId::TRASH, LocalPlacement::Last, None).unwrap();
    tree.track_read_changes();
    tree.drain_read_changes().unwrap();
    let before = rows(&tree);
    tree.local_move(NodeId::TRASH, NodeId(2), LocalPlacement::Last).unwrap();
    assert_delta(&mut tree, &before, &[NodeId(1), NodeId(2), NodeId::TRASH]);

    tree.local_delete(NodeId(2)).unwrap();
    tree.drain_read_changes().unwrap();
    let before = rows(&tree);
    tree.local_payload(NodeId::TRASH, Some(b"restore parent".to_vec())).unwrap();
    assert_delta(
        &mut tree,
        &before,
        &[NodeId::ROOT, NodeId(2), NodeId::TRASH],
    );
}
