use treecrdt_core::{
    LamportClock, LocalPlacement, MemoryStorage, NodeId, NoopParentOpIndex, ReplicaId, TreeCrdt,
};

#[test]
fn defensive_delete_parent_then_insert_child_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (insert_child_op, _) = crdt_b.local_insert(parent, child, LocalPlacement::First, None).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_parent_then_move_child_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);
    let other_parent = NodeId(3);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (other_parent_op, _) =
        crdt_a.local_insert(NodeId::ROOT, other_parent, LocalPlacement::After(parent), None).unwrap();
    crdt_b.apply_remote(other_parent_op).unwrap();

    let (child_op, _) = crdt_a.local_insert(other_parent, child, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(child_op).unwrap();

    let (move_op, _) = crdt_b.local_move(child, parent, LocalPlacement::First).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(move_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_a.children(other_parent).unwrap(), &[]);
    assert_eq!(crdt_b.children(other_parent).unwrap(), &[]);

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_sibling_moved_same_parent_then_deleted_restores_node() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let first = NodeId(2);
    let middle = NodeId(3);
    let last = NodeId(4);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (first_op, _) = crdt_a.local_insert(parent, first, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(first_op).unwrap();

    let (middle_op, _) = crdt_a.local_insert(parent, middle, LocalPlacement::After(first), None).unwrap();
    crdt_b.apply_remote(middle_op).unwrap();

    let (last_op, _) = crdt_a.local_insert(parent, last, LocalPlacement::After(middle), None).unwrap();
    crdt_b.apply_remote(last_op).unwrap();

    assert_eq!(crdt_a.children(parent).unwrap(), &[first, middle, last]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[first, middle, last]);

    let (move_op, _) = crdt_b.local_move(middle, parent, LocalPlacement::After(last)).unwrap();
    assert_eq!(crdt_b.parent(middle).unwrap(), Some(parent));
    assert_eq!(crdt_b.children(parent).unwrap(), &[first, last, middle]);
    assert!(!crdt_b.is_tombstoned(middle).unwrap());

    let (delete_op, _) = crdt_a.local_delete(middle).unwrap();
    assert!(crdt_a.is_tombstoned(middle).unwrap());

    crdt_a.apply_remote(move_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(middle).unwrap(),
        "Node should be restored after sync"
    );
    assert!(
        !crdt_b.is_tombstoned(middle).unwrap(),
        "Node should be restored after sync"
    );
    assert_eq!(crdt_a.parent(middle).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(middle).unwrap(), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[first, last, middle]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[first, last, middle]);

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_parent_when_sibling_moved_same_parent_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let first = NodeId(2);
    let middle = NodeId(3);
    let last = NodeId(4);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (first_op, _) = crdt_a.local_insert(parent, first, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(first_op).unwrap();

    let (middle_op, _) = crdt_a.local_insert(parent, middle, LocalPlacement::After(first), None).unwrap();
    crdt_b.apply_remote(middle_op).unwrap();

    let (last_op, _) = crdt_a.local_insert(parent, last, LocalPlacement::After(middle), None).unwrap();
    crdt_b.apply_remote(last_op).unwrap();

    assert_eq!(crdt_a.children(parent).unwrap(), &[first, middle, last]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[first, middle, last]);

    let (move_op, _) = crdt_a.local_move(middle, parent, LocalPlacement::After(last)).unwrap();
    assert_eq!(crdt_a.parent(middle).unwrap(), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[first, last, middle]);
    assert!(!crdt_a.is_tombstoned(parent).unwrap());

    let (delete_op, _) = crdt_b.local_delete(parent).unwrap();
    assert!(crdt_b.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(delete_op.clone()).unwrap();
    crdt_b.apply_remote(move_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored after sync"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored after sync"
    );
    assert_eq!(crdt_a.parent(first).unwrap(), Some(parent));
    assert_eq!(crdt_a.parent(middle).unwrap(), Some(parent));
    assert_eq!(crdt_a.parent(last).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(first).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(middle).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(last).unwrap(), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[first, last, middle]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[first, last, middle]);

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_parent_then_multiple_children_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child1 = NodeId(2);
    let child2 = NodeId(3);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (insert_child1_op, _) = crdt_b.local_insert(parent, child1, LocalPlacement::First, None).unwrap();

    let (insert_child2_op, _) = crdt_b.local_insert(parent, child2, LocalPlacement::After(child1), None).unwrap();
    assert!(!crdt_b.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_b.children(parent).unwrap(), &[child1, child2]);

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(insert_child1_op.clone()).unwrap();
    crdt_a.apply_remote(insert_child2_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert_eq!(crdt_a.parent(child1).unwrap(), Some(parent));
    assert_eq!(crdt_a.parent(child2).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child1).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child2).unwrap(), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child1, child2]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child1, child2]);

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_insert_then_delete_no_restoration() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (child_op, _) = crdt_b.local_insert(parent, child, LocalPlacement::First, None).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(child_op.clone()).unwrap();
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert!(!crdt_a.is_tombstoned(parent).unwrap());

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_b.apply_remote(delete_op).unwrap();

    assert!(crdt_a.is_tombstoned(parent).unwrap());
    assert!(crdt_b.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_parent_then_payload_change_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (set_payload_op, _) = crdt_b.local_payload(parent, Some(b"hello".to_vec())).unwrap();
    assert_eq!(crdt_b.payload(parent).unwrap(), Some(b"hello".to_vec()));

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(set_payload_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert_eq!(crdt_a.payload(parent).unwrap(), Some(b"hello".to_vec()));
    assert_eq!(crdt_b.payload(parent).unwrap(), Some(b"hello".to_vec()));

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_parent_then_payload_change_no_restoration_when_aware() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (set_payload_op, _) = crdt_b.local_payload(parent, Some(b"hello".to_vec())).unwrap();
    crdt_a.apply_remote(set_payload_op).unwrap();

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_b.apply_remote(delete_op).unwrap();

    assert!(crdt_a.is_tombstoned(parent).unwrap());
    assert!(crdt_b.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_a.payload(parent).unwrap(), Some(b"hello".to_vec()));
    assert_eq!(crdt_b.payload(parent).unwrap(), Some(b"hello".to_vec()));

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_later_delete_unaware_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (insert_child_op, _) = crdt_b.local_insert(parent, child, LocalPlacement::First, None).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();

    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_insert_delete_sequence() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (insert_child_op, _) = crdt_b.local_insert(parent, child, LocalPlacement::First, None).unwrap();
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    let (delete1_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(delete1_op.clone()).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored after first insert"
    );
    assert!(!crdt_b.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_multiple_deletes_then_insert_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_c = TreeCrdt::new(
        ReplicaId::new(b"c"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op.clone()).unwrap();
    crdt_c.apply_remote(parent_op).unwrap();

    let (insert_child_op, _) = crdt_c.local_insert(parent, child, LocalPlacement::First, None).unwrap();
    assert!(!crdt_c.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_c.children(parent).unwrap(), &[child]);

    let (delete_a, _) = crdt_a.local_delete(parent).unwrap();
    let (delete_b, _) = crdt_b.local_delete(parent).unwrap();

    assert!(crdt_a.is_tombstoned(parent).unwrap());
    assert!(crdt_b.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(delete_b.clone()).unwrap();
    crdt_b.apply_remote(delete_a.clone()).unwrap();
    crdt_c.apply_remote(delete_a.clone()).unwrap();
    crdt_c.apply_remote(delete_b.clone()).unwrap();

    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(insert_child_op.clone()).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_c.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_c.parent(child).unwrap(), Some(parent));

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    assert_eq!(crdt_b.nodes().unwrap(), crdt_c.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
    crdt_c.validate_invariants().unwrap();
}

#[test]
fn defensive_delete_parent_then_modify_grandchild_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);
    let grandchild = NodeId(3);
    let other_parent = NodeId(4);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (child_op, _) = crdt_a.local_insert(parent, child, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(child_op).unwrap();

    let (grandchild_op, _) = crdt_a.local_insert(child, grandchild, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(grandchild_op).unwrap();

    let (other_parent_op, _) =
        crdt_a.local_insert(NodeId::ROOT, other_parent, LocalPlacement::After(parent), None).unwrap();
    crdt_b.apply_remote(other_parent_op).unwrap();

    let (move_grandchild_op, _) = crdt_b.local_move(grandchild, other_parent, LocalPlacement::First).unwrap();
    assert_eq!(crdt_b.parent(grandchild).unwrap(), Some(other_parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(other_parent).unwrap(), &[grandchild]);

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_a.apply_remote(move_grandchild_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();

    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "Parent should be restored"
    );
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert_eq!(crdt_a.parent(grandchild).unwrap(), Some(other_parent));
    assert_eq!(crdt_b.parent(grandchild).unwrap(), Some(other_parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_a.children(other_parent).unwrap(), &[grandchild]);
    assert_eq!(crdt_b.children(other_parent).unwrap(), &[grandchild]);

    assert_eq!(crdt_a.nodes().unwrap(), crdt_b.nodes().unwrap());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn delete_unrelated_ops_should_not_prevent_restoration_when_child_insert_was_unseen() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child = NodeId(2);
    let unrelated = NodeId(99);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (insert_child_op, _) = crdt_b.local_insert(parent, child, LocalPlacement::First, None).unwrap();

    let (unrelated_op, _) = crdt_b.local_insert(NodeId::ROOT, unrelated, LocalPlacement::After(parent), None).unwrap();
    crdt_a.apply_remote(unrelated_op).unwrap();

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    crdt_b.apply_remote(delete_op.clone()).unwrap();

    crdt_a.apply_remote(insert_child_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "delete should be treated as unaware of the child insert"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "converged state should keep parent restorable"
    );
}

#[test]
fn delete_should_restore_when_earlier_child_op_from_same_replica_was_missing() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(1);
    let child1 = NodeId(2);
    let child2 = NodeId(3);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let (insert_child1_op, _) = crdt_b.local_insert(parent, child1, LocalPlacement::First, None).unwrap();
    let (insert_child2_op, _) = crdt_b.local_insert(parent, child2, LocalPlacement::After(child1), None).unwrap();

    crdt_a.apply_remote(insert_child2_op).unwrap();

    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    crdt_b.apply_remote(delete_op.clone()).unwrap();

    crdt_a.apply_remote(insert_child1_op).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "delete should be treated as unaware of the missing child insert"
    );
    assert!(
        !crdt_b.is_tombstoned(parent).unwrap(),
        "converged state should keep parent restorable"
    );
}

#[test]
fn materialized_apply_delta_includes_parent_restored_by_unseen_payload_change() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut seq_a = 0;
    let mut seq_b = 0;
    let mut index_a = NoopParentOpIndex;
    let mut index_b = NoopParentOpIndex;

    let parent = NodeId(1);
    let child = NodeId(2);

    let (parent_op, _) = crdt_a.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    crdt_b
        .apply_remote_with_materialization_seq(parent_op, &mut index_b, &mut seq_b)
        .unwrap()
        .unwrap();

    let (child_op, _) = crdt_a.local_insert(parent, child, LocalPlacement::First, None).unwrap();
    crdt_b
        .apply_remote_with_materialization_seq(child_op, &mut index_b, &mut seq_b)
        .unwrap()
        .unwrap();

    let (payload_op, _) = crdt_b.local_payload(child, Some(b"x".to_vec())).unwrap();
    let (delete_op, _) = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    let delta = crdt_a
        .apply_remote_with_materialization_seq(payload_op, &mut index_a, &mut seq_a)
        .unwrap()
        .unwrap();
    let _ = crdt_b
        .apply_remote_with_materialization_seq(delete_op, &mut index_b, &mut seq_b)
        .unwrap();

    assert!(
        !crdt_a.is_tombstoned(parent).unwrap(),
        "parent should be restored by unseen payload change"
    );
    assert!(delta.affected_nodes.contains(&child));
    assert!(
        delta.affected_nodes.contains(&parent),
        "delta should include ancestor tombstone flip"
    );
}
