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

    // Both clients start with the same tree: parent under root
    // Use local_insert to ensure proper setup
    let insert_parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(insert_parent_op).unwrap();

    // Client A deletes the parent
    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_a.children(parent).unwrap().is_empty());

    // Client B doesn't know about deletion yet, inserts child under parent
    // We need to ensure Client B's clock is advanced so the insert has higher lamport
    // First, let Client B observe the delete's lamport (but don't apply it yet)
    // Actually, we'll create the insert with a manually set higher lamport
    let insert_child = Operation::insert(&ReplicaId::new(b"b"), 1, delete_op.meta.lamport + 1, parent, child, 0);
    crdt_b.apply_remote(insert_child.clone()).unwrap();
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent)); // Parent not tombstoned in B's view

    // Now synchronize: Client B receives the delete
    crdt_b.apply_remote(delete_op.clone()).unwrap();
    
    // Client A receives the insert
    crdt_a.apply_remote(insert_child).unwrap();

    // Both should converge to the same state
    // The delete happened first (lamport 2), then insert (lamport 3)
    // Parent remains tombstoned, but child is attached to parent
    // (attach clears tombstone on the child, not the parent)
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(crdt_a.is_tombstoned(parent)); // Parent stays tombstoned
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert!(!crdt_a.is_tombstoned(child)); // Child is not tombstoned
    assert!(!crdt_b.is_tombstoned(child));
    
    // Both should have consistent state
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

    // Setup: parent and other_parent under root, child under other_parent
    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();
    
    let other_parent_op = crdt_a.local_insert(root, other_parent, 1).unwrap();
    crdt_b.apply_remote(other_parent_op).unwrap();
    
    let child_op = crdt_a.local_insert(other_parent, child, 0).unwrap();
    crdt_b.apply_remote(child_op).unwrap();

    // Client A deletes parent (lamport 4)
    let delete_op = crdt_a.local_delete(parent).unwrap();
    assert!(crdt_a.is_tombstoned(parent));

    // Client B doesn't know, moves child from other_parent to deleted parent
    // Use a lamport higher than the delete
    let move_op = Operation::move_node(&ReplicaId::new(b"b"), 1, delete_op.meta.lamport + 1, child, parent, 0);
    crdt_b.apply_remote(move_op.clone()).unwrap();
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_b.is_tombstoned(parent));

    // Synchronize
    crdt_b.apply_remote(delete_op.clone()).unwrap();
    crdt_a.apply_remote(move_op).unwrap();

    // Both should converge: move happened after delete
    // Parent stays tombstoned, but child is attached to it
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(crdt_a.is_tombstoned(parent)); // Parent stays tombstoned
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child]);
    assert!(!crdt_a.is_tombstoned(child)); // Child is not tombstoned
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

    // Setup: parent exists
    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client B inserts child under parent
    let child_op = crdt_b.local_insert(parent, child, 0).unwrap();
    
    // Client A deletes parent - happens after insert (higher lamport)
    // But we need to ensure A's clock observes B's insert first
    crdt_a.apply_remote(child_op.clone()).unwrap();
    let delete_op = crdt_a.local_delete(parent).unwrap();

    // Synchronize - delete has higher lamport, so it comes after insert
    crdt_b.apply_remote(delete_op.clone()).unwrap();

    // Delete happened after insert, so parent should be tombstoned
    // Note: children remain attached to tombstoned parents
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.parent(child), Some(parent)); // Child stays attached
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

    // Setup: parent exists
    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Client A deletes parent
    let delete_op = crdt_a.local_delete(parent).unwrap();

    // Client B doesn't know, inserts two children with higher lamports
    let insert_child1 = Operation::insert(&ReplicaId::new(b"b"), 1, delete_op.meta.lamport + 1, parent, child1, 0);
    let insert_child2 = Operation::insert(&ReplicaId::new(b"b"), 2, delete_op.meta.lamport + 2, parent, child2, 1);
    
    crdt_b.apply_remote(insert_child1.clone()).unwrap();
    crdt_b.apply_remote(insert_child2.clone()).unwrap();

    // Synchronize
    crdt_b.apply_remote(delete_op.clone()).unwrap();
    crdt_a.apply_remote(insert_child1).unwrap();
    crdt_a.apply_remote(insert_child2).unwrap();

    // Both children should be attached to parent
    // Parent stays tombstoned even with children attached
    assert_eq!(crdt_a.parent(child1), Some(parent));
    assert_eq!(crdt_a.parent(child2), Some(parent));
    assert_eq!(crdt_b.parent(child1), Some(parent));
    assert_eq!(crdt_b.parent(child2), Some(parent));
    assert!(crdt_a.is_tombstoned(parent)); // Parent stays tombstoned
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.children(parent).unwrap(), &[child1, child2]);
    assert_eq!(crdt_b.children(parent).unwrap(), &[child1, child2]);
    assert!(!crdt_a.is_tombstoned(child1)); // Children are not tombstoned
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

    // Setup
    crdt.local_insert(root, parent, 0).unwrap();

    // Delete parent (lamport 2)
    let _delete1 = crdt.local_delete(parent).unwrap();
    assert!(crdt.is_tombstoned(parent));

    // Insert child under parent (lamport 3) - parent stays tombstoned
    let insert_child = Operation::insert(&ReplicaId::new(b"b"), 1, 3, parent, child, 0);
    crdt.apply_remote(insert_child).unwrap();
    assert!(crdt.is_tombstoned(parent)); // Parent stays tombstoned
    assert_eq!(crdt.parent(child), Some(parent));
    assert!(!crdt.is_tombstoned(child)); // Child is not tombstoned

    // Delete parent again (lamport 4)
    // Note: deleting an already-tombstoned node doesn't change children
    let _delete2 = crdt.local_delete(parent).unwrap();
    assert!(crdt.is_tombstoned(parent));
    assert_eq!(crdt.parent(child), Some(parent)); // Child stays attached
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

    // Setup
    let parent_op = crdt_a.local_insert(root, parent, 0).unwrap();
    crdt_b.apply_remote(parent_op).unwrap();

    // Both clients delete parent concurrently (same lamport, different replica IDs)
    // Client A deletes locally
    let delete_a_op = crdt_a.local_delete(parent).unwrap();
    let delete_a = Operation::delete(&ReplicaId::new(b"a"), delete_a_op.meta.id.counter, delete_a_op.meta.lamport, parent);
    // Client B deletes with same lamport but different replica
    let delete_b = Operation::delete(&ReplicaId::new(b"b"), 1, delete_a_op.meta.lamport, parent);

    crdt_a.apply_remote(delete_a.clone()).unwrap();
    crdt_b.apply_remote(delete_b.clone()).unwrap();

    // Both should have parent tombstoned
    assert!(crdt_a.is_tombstoned(parent));
    assert!(crdt_b.is_tombstoned(parent));

    // Now synchronize and Client A inserts child (lamport 3)
    crdt_a.apply_remote(delete_b).unwrap();
    crdt_b.apply_remote(delete_a).unwrap();

    let insert_child = Operation::insert(&ReplicaId::new(b"a"), 3, delete_a_op.meta.lamport + 1, parent, child, 0);
    crdt_a.apply_remote(insert_child.clone()).unwrap();
    crdt_b.apply_remote(insert_child).unwrap();

    // Parent stays tombstoned, but child is attached to it
    assert!(crdt_a.is_tombstoned(parent)); // Parent stays tombstoned
    assert!(crdt_b.is_tombstoned(parent));
    assert_eq!(crdt_a.parent(child), Some(parent));
    assert_eq!(crdt_b.parent(child), Some(parent));
    assert!(!crdt_a.is_tombstoned(child)); // Child is not tombstoned
    assert!(!crdt_b.is_tombstoned(child));

    assert_eq!(crdt_a.nodes(), crdt_b.nodes());
    crdt_a.validate_invariants().unwrap();
    crdt_b.validate_invariants().unwrap();
}

