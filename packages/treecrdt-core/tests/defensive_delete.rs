use treecrdt_core::{LamportClock, MemoryStorage, NodeId, ReplicaId, TreeCrdt};

#[test]
fn defensive_delete_parent_then_insert_child_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child first, then Client A deletes without awareness
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let insert_child_op = crdt_b.local_insert_after(parent, child, None).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    // Client A deletes without having seen the insert
    let delete_op = crdt_a.local_delete(parent).unwrap();
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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);
    let other_parent = NodeId(3);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let other_parent_op =
        crdt_a.local_insert_after(NodeId::ROOT, other_parent, Some(parent)).unwrap();
    crdt_b.apply_remote(other_parent_op).unwrap();

    let child_op = crdt_a.local_insert_after(other_parent, child, None).unwrap();
    crdt_b.apply_remote(child_op).unwrap();

    // Client B moves child first, then Client A deletes without awareness
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let move_op = crdt_b.local_move_after(child, parent, None).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    // Client A deletes without having seen the move
    let delete_op = crdt_a.local_delete(parent).unwrap();
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
fn defensive_delete_parent_then_multiple_children_restores_parent() {
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child1 = NodeId(2);
    let child2 = NodeId(3);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let insert_child1_op = crdt_b.local_insert_after(parent, child1, None).unwrap();

    // Client B inserts children first, then Client A deletes without awareness
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let insert_child2_op = crdt_b.local_insert_after(parent, child2, Some(child1)).unwrap();
    assert!(!crdt_b.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_b.children(parent).unwrap(), &[child1, child2]);

    // Client A deletes without having seen the second child insert
    let delete_op = crdt_a.local_delete(parent).unwrap();
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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child first
    let child_op = crdt_b.local_insert_after(parent, child, None).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    // Client A receives insert first (has full awareness), then deletes with higher lamport
    // Since Client A is aware of all changes, delete should succeed and parent should stay tombstoned
    crdt_a.apply_remote(child_op.clone()).unwrap();
    assert_eq!(crdt_a.parent(child).unwrap(), Some(parent));
    assert!(!crdt_a.is_tombstoned(parent).unwrap());

    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_b.apply_remote(delete_op).unwrap();

    // Parent should stay tombstoned because delete happened with full awareness of modifications
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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B sets payload first, then Client A deletes without awareness.
    // Defensive delete: parent should be restored because delete was unaware of modifications.
    let set_payload_op = crdt_b.local_set_payload(parent, b"hello".to_vec()).unwrap();
    assert_eq!(crdt_b.payload(parent).unwrap(), Some(b"hello".to_vec()));

    let delete_op = crdt_a.local_delete(parent).unwrap();
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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let set_payload_op = crdt_b.local_set_payload(parent, b"hello".to_vec()).unwrap();
    crdt_a.apply_remote(set_payload_op).unwrap();

    // Client A deletes with full awareness of payload.
    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    crdt_b.apply_remote(delete_op).unwrap();

    // Parent should stay tombstoned because delete happened with full awareness.
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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child first
    let insert_child_op = crdt_b.local_insert_after(parent, child, None).unwrap();
    assert_eq!(crdt_b.parent(child).unwrap(), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    // Client A deletes without having seen the insert
    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    // Synchronize: Client A receives the insert, Client B receives the delete
    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();

    // Defensive delete: parent should be restored because delete was unaware of modifications
    // even though it happened later in time (higher lamport)
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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child first, then Client A deletes without awareness
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let insert_child_op = crdt_b.local_insert_after(parent, child, None).unwrap();
    assert!(!crdt_b.is_tombstoned(parent).unwrap());

    // Client A deletes without having seen the insert
    let delete1_op = crdt_a.local_delete(parent).unwrap();
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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut crdt_c = TreeCrdt::new(
        ReplicaId::new(b"c"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op.clone()).unwrap();
    crdt_c.apply_remote(parent_op).unwrap();

    // Client C inserts child first, then Clients A and B delete concurrently without awareness
    // Defensive delete: parent should be restored because deletes were unaware of modifications
    let insert_child_op = crdt_c.local_insert_after(parent, child, None).unwrap();
    assert!(!crdt_c.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_c.children(parent).unwrap(), &[child]);

    // Clients A and B delete without having seen the insert
    let delete_a = crdt_a.local_delete(parent).unwrap();
    let delete_b = crdt_b.local_delete(parent).unwrap();

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
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);
    let grandchild = NodeId(3);
    let other_parent = NodeId(4);

    // Setup: root -> parent -> child -> grandchild
    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let child_op = crdt_a.local_insert_after(parent, child, None).unwrap();
    crdt_b.apply_remote(child_op).unwrap();

    let grandchild_op = crdt_a.local_insert_after(child, grandchild, None).unwrap();
    crdt_b.apply_remote(grandchild_op).unwrap();

    // Create another parent for moving the grandchild
    let other_parent_op =
        crdt_a.local_insert_after(NodeId::ROOT, other_parent, Some(parent)).unwrap();
    crdt_b.apply_remote(other_parent_op).unwrap();

    let move_grandchild_op = crdt_b.local_move_after(grandchild, other_parent, None).unwrap();
    assert_eq!(crdt_b.parent(grandchild).unwrap(), Some(other_parent));
    assert!(!crdt_b.is_tombstoned(parent).unwrap());
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(other_parent).unwrap(), &[grandchild]);

    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent).unwrap());

    // Synchronize: Client A receives the move, Client B receives the delete
    crdt_a.apply_remote(move_grandchild_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();

    // Defensive delete: parent should be restored because delete was unaware of grandchild modification
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
    // Demonstrates false awareness when known_state uses max Lamport per replica (gaps across subtrees).
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);
    let unrelated = NodeId(99);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let insert_child_op = crdt_b.local_insert_after(parent, child, None).unwrap();

    let unrelated_op = crdt_b.local_insert_after(NodeId::ROOT, unrelated, Some(parent)).unwrap();
    crdt_a.apply_remote(unrelated_op).unwrap();

    let delete_op = crdt_a.local_delete(parent).unwrap();
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
    // Requires dotted/range version vectors: seeing B:2 must not imply seeing B:1.
    let mut crdt_a = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );
    let mut crdt_b = TreeCrdt::new(
        ReplicaId::new(b"b"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child1 = NodeId(2);
    let child2 = NodeId(3);

    let parent_op = crdt_a.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let insert_child1_op = crdt_b.local_insert_after(parent, child1, None).unwrap();
    let insert_child2_op = crdt_b.local_insert_after(parent, child2, Some(child1)).unwrap();

    crdt_a.apply_remote(insert_child2_op).unwrap();

    let delete_op = crdt_a.local_delete(parent).unwrap();
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
