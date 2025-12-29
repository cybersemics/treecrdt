use treecrdt_core::{LamportClock, MemoryStorage, NodeId, Operation, ReplicaId, TreeCrdt};

#[test]
fn delete_parent_then_insert_child_unaware() {
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    let insert_parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(insert_parent_op).unwrap();

    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_a.children(parent).unwrap().is_empty());

    // Client B inserts child with higher lamport (happens after delete)
    let insert_child = Operation::insert(
        &ReplicaId::new(b"b"),
        1,
        delete_op.meta.lamport + 1,
        parent,
        child,
        0,
    );
    crdt_b.apply_remote(insert_child.clone()).unwrap();
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent));

    crdt_b.apply_remote(delete_op.clone()).unwrap();
    crdt_a.apply_remote(insert_child).unwrap();

    // Parent stays tombstoned because delete happened first (lower lamport)
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert!(!crdt_a.is_tombstoned(child));
    assert!(!crdt_b.is_tombstoned(child));
    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn delete_parent_then_move_child_unaware() {
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);
    let other_parent = NodeId(3);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let other_parent_op = crdt_a.local_insert(root, other_parent, 1).unwrap();
    crdt_b.apply_remote(other_parent_op).unwrap();

    let child_op = crdt_a.local_insert(other_parent, child, 0).unwrap();
    crdt_b.apply_remote(child_op).unwrap();

    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent));

    // Client B moves child with higher lamport (happens after delete)
    let move_op = Operation::move_node(
        &ReplicaId::new(b"b"),
        1,
        delete_op.meta.lamport + 1,
        child,
        parent,
        0,
    );
    crdt_b.apply_remote(move_op.clone()).unwrap();
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent));

    crdt_b.apply_remote(delete_op.clone()).unwrap();
    crdt_a.apply_remote(move_op).unwrap();

    // Parent stays tombstoned because delete happened first (lower lamport)
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert!(!crdt_a.is_tombstoned(child));
    assert!(!crdt_b.is_tombstoned(child));
    assert_eq!(crdt_a.children(other_parent).unwrap(), &[]);
    assert_eq!(crdt_b.children(other_parent).unwrap(), &[]);

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn insert_child_then_delete_parent() {
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let child_op = crdt_b.local_insert(parent, child, 0).unwrap();
    crdt_a.apply_remote(child_op.clone()).unwrap();
    // Delete happens after insert (higher lamport), so parent should be tombstoned
    let delete_op = crdt_a.local_delete(parent).unwrap();

    crdt_b.apply_remote(delete_op.clone()).unwrap();

    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn delete_parent_then_multiple_children_operations() {
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child1 = NodeId(2);
    let child2 = NodeId(3);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let delete_op = crdt_a.local_delete(parent).unwrap();

    // Client B inserts children with higher lamports (happen after delete)
    let insert_child1 = Operation::insert(
        &ReplicaId::new(b"b"),
        1,
        delete_op.meta.lamport + 1,
        parent,
        child1,
        0,
    );
    let insert_child2 = Operation::insert(
        &ReplicaId::new(b"b"),
        2,
        delete_op.meta.lamport + 2,
        parent,
        child2,
        1,
    );

    crdt_b.apply_remote(insert_child1.clone()).unwrap();
    crdt_b.apply_remote(insert_child2.clone()).unwrap();

    crdt_b.apply_remote(delete_op.clone()).unwrap();
    crdt_a.apply_remote(insert_child1).unwrap();
    crdt_a.apply_remote(insert_child2).unwrap();

    // Parent stays tombstoned because delete happened first (lower lamport)
    assert_eq!(crdt_a.parent(child1), Some(parent));
    assert_eq!(crdt_a.parent(child2), Some(parent));
    assert_eq!(crdt_b.parent(child1), Some(parent));
    assert_eq!(crdt_b.parent(child2), Some(parent));
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child1, child2]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child1, child2]);
    assert!(!crdt_a.is_tombstoned(child1));
    assert!(!crdt_a.is_tombstoned(child2));
    assert!(!crdt_b.is_tombstoned(child1));
    assert!(!crdt_b.is_tombstoned(child2));

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
fn delete_insert_delete_sequence() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    crdt.local_insert(root, parent, 0).unwrap();

    let _delete1 = crdt.local_delete(parent).unwrap();
    assert!(crdt.is_tombstoned(parent));

    // Insert happens after delete (higher lamport), parent stays tombstoned
    let insert_child = Operation::insert(&ReplicaId::new(b"b"), 1, 3, parent, child, 0);
    crdt.apply_remote(insert_child).unwrap();
    assert!(crdt.is_tombstoned(parent));
    assert_eq!(crdt.parent(child), Some(parent));
    assert!(!crdt.is_tombstoned(child));

    // Second delete on already-tombstoned node
    let _delete2 = crdt.local_delete(parent).unwrap();
    assert!(crdt.is_tombstoned(parent));
    assert_eq!(crdt.parent(child), Some(parent));
    assert_eq!(crdt.children(parent).unwrap(), &[child]);

    crdt.validate_invariants().unwrap();
}

#[test]
fn concurrent_deletes_then_insert() {
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Both clients delete concurrently (same lamport, different replica IDs)
    let delete_a_op = crdt_a.local_delete(parent).unwrap();
    let delete_a = Operation::delete(
        &ReplicaId::new(b"a"),
        delete_a_op.meta.id.counter,
        delete_a_op.meta.lamport,
        parent,
    );
    let delete_b = Operation::delete(&ReplicaId::new(b"b"), 1, delete_a_op.meta.lamport, parent);

    crdt_a.apply_remote(delete_a.clone()).unwrap();
    crdt_b.apply_remote(delete_b.clone()).unwrap();

    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));

    crdt_a.apply_remote(delete_b).unwrap();
    crdt_b.apply_remote(delete_a).unwrap();

    // Insert happens after deletes (higher lamport), parent stays tombstoned
    let insert_child = Operation::insert(
        &ReplicaId::new(b"a"),
        3,
        delete_a_op.meta.lamport + 1,
        parent,
        child,
        0,
    );
    crdt_a.apply_remote(insert_child.clone()).unwrap();
    crdt_b.apply_remote(insert_child).unwrap();

    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_a.is_tombstoned(child));
    assert!(!crdt_b.is_tombstoned(child));

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
#[should_panic(expected = "Parent should be restored")]
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child first, then Client A deletes without awareness (same lamport)
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let insert_child_op = crdt_b.local_insert(parent, child, 0).unwrap();
    let insert_child_lamport = insert_child_op.meta.lamport;
    crdt_b.apply_remote(insert_child_op.clone()).unwrap();
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent));

    let delete_op = Operation::delete(
        &ReplicaId::new(b"a"),
        2,
        insert_child_lamport,
        parent,
    );
    crdt_a.apply_remote(delete_op.clone()).unwrap();
    assert!(crdt_a.is_tombstoned(parent));

    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(!crdt_a.is_tombstoned(parent), "Parent should be restored");
    assert!(!crdt_b.is_tombstoned(parent), "Parent should be restored");
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
#[should_panic(expected = "Parent should be restored")]
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);
    let other_parent = NodeId(3);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let other_parent_op = crdt_a.local_insert(root, other_parent, 1).unwrap();
    crdt_b.apply_remote(other_parent_op).unwrap();

    let child_op = crdt_a.local_insert(other_parent, child, 0).unwrap();
    crdt_b.apply_remote(child_op).unwrap();

    // Client B moves child first, then Client A deletes without awareness (lower lamport)
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let move_op = crdt_b.local_move(child, parent, 0).unwrap();
    let move_lamport = move_op.meta.lamport;
    crdt_b.apply_remote(move_op.clone()).unwrap();
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent));

    let delete_op = Operation::delete(
        &ReplicaId::new(b"a"),
        4,
        move_lamport - 1,
        parent,
    );
    crdt_a.apply_remote(delete_op.clone()).unwrap();
    assert!(crdt_a.is_tombstoned(parent));

    crdt_a.apply_remote(move_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(!crdt_a.is_tombstoned(parent), "Parent should be restored");
    assert!(!crdt_b.is_tombstoned(parent), "Parent should be restored");
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_a.children(other_parent).unwrap(), &[]);
    assert_eq!(crdt_b.children(other_parent).unwrap(), &[]);

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
#[should_panic(expected = "Parent should be restored")]
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child1 = NodeId(2);
    let child2 = NodeId(3);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    let insert_child1_op = crdt_b.local_insert(parent, child1, 0).unwrap();
    crdt_b.apply_remote(insert_child1_op.clone()).unwrap();

    // Client B inserts children first, then Client A deletes without awareness (same lamport)
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let insert_child2_op = crdt_b.local_insert(parent, child2, 1).unwrap();
    let insert_child2_lamport = insert_child2_op.meta.lamport;
    crdt_b.apply_remote(insert_child2_op.clone()).unwrap();
    assert!(!crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_b.children(parent).unwrap(), &[child1, child2]);

    let delete_op = Operation::delete(
        &ReplicaId::new(b"a"),
        2,
        insert_child2_lamport,
        parent,
    );
    crdt_a.apply_remote(delete_op.clone()).unwrap();
    assert!(crdt_a.is_tombstoned(parent));

    crdt_a.apply_remote(insert_child1_op.clone()).unwrap();
    crdt_a.apply_remote(insert_child2_op.clone()).unwrap();
    crdt_b.apply_remote(delete_op).unwrap();
    assert!(!crdt_a.is_tombstoned(parent), "Parent should be restored");
    assert!(!crdt_b.is_tombstoned(parent), "Parent should be restored");
    assert_eq!(crdt_a.parent(child1), Some(parent));
    assert_eq!(crdt_a.parent(child2), Some(parent));
    assert_eq!(crdt_b.parent(child1), Some(parent));
    assert_eq!(crdt_b.parent(child2), Some(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child1, child2]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child1, child2]);

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child first
    let child_op = crdt_b.local_insert(parent, child, 0).unwrap();
    crdt_b.apply_remote(child_op.clone()).unwrap();
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent));

    // Client A receives insert first (has full awareness), then deletes with higher lamport
    // Since Client A is aware of all changes, delete should succeed and parent should stay tombstoned
    crdt_a.apply_remote(child_op.clone()).unwrap();
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert!(!crdt_a.is_tombstoned(parent));
    
    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent));

    crdt_b.apply_remote(delete_op).unwrap();

    // Parent should stay tombstoned because delete happened with full awareness of modifications
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
#[should_panic(expected = "Parent should be restored after first insert")]
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child first, then Client A deletes without awareness (same lamport)
    // Defensive delete: parent should be restored because delete was unaware of modifications
    let insert_child_op = crdt_b.local_insert(parent, child, 0).unwrap();
    let insert_lamport = insert_child_op.meta.lamport;
    crdt_b.apply_remote(insert_child_op.clone()).unwrap();
    assert!(!crdt_b.is_tombstoned(parent));

    let delete1_op = Operation::delete(
        &ReplicaId::new(b"a"),
        2,
        insert_lamport,
        parent,
    );
    crdt_a.apply_remote(delete1_op.clone()).unwrap();
    assert!(crdt_a.is_tombstoned(parent));

    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(delete1_op.clone()).unwrap();
    assert!(
        !crdt_a.is_tombstoned(parent),
        "Parent should be restored after first insert"
    );
    assert!(!crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

#[test]
#[should_panic(expected = "Parent should be restored")]
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

    let root = NodeId::ROOT;
    let parent = NodeId(1);
    let child = NodeId(2);

    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op.clone()).unwrap();
    crdt_c.apply_remote(parent_op).unwrap();

    // Client C inserts child first, then Clients A and B delete concurrently without awareness
    // Defensive delete: parent should be restored because deletes were unaware of modifications
    let insert_child_op = crdt_c.local_insert(parent, child, 0).unwrap();
    let insert_lamport = insert_child_op.meta.lamport;
    crdt_c.apply_remote(insert_child_op.clone()).unwrap();
    assert!(!crdt_c.is_tombstoned(parent));
    assert_eq!(crdt_c.children(parent).unwrap(), &[child]);

    let delete_a = Operation::delete(
        &ReplicaId::new(b"a"),
        2,
        insert_lamport,
        parent,
    );
    let delete_b = Operation::delete(
        &ReplicaId::new(b"b"),
        1,
        insert_lamport,
        parent,
    );

    crdt_a.apply_remote(delete_a.clone()).unwrap();
    crdt_b.apply_remote(delete_b.clone()).unwrap();

    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));

    crdt_a.apply_remote(delete_b.clone()).unwrap();
    crdt_b.apply_remote(delete_a.clone()).unwrap();
    crdt_c.apply_remote(delete_a.clone()).unwrap();
    crdt_c.apply_remote(delete_b.clone()).unwrap();

    crdt_a.apply_remote(insert_child_op.clone()).unwrap();
    crdt_b.apply_remote(insert_child_op.clone()).unwrap();
    assert!(!crdt_a.is_tombstoned(parent), "Parent should be restored");
    assert!(!crdt_b.is_tombstoned(parent), "Parent should be restored");
    assert!(!crdt_c.is_tombstoned(parent), "Parent should be restored");
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert_eq!(crdt_c.parent(child), Some(parent));

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    assert_eq!(crdt_b.nodes(), crdt_c.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
    crdt_c.validate_invariants().unwrap();
}
