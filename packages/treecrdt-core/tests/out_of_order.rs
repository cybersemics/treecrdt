use treecrdt_core::{LamportClock, MemoryStorage, NodeId, Operation, ReplicaId, Storage, TreeCrdt};

#[test]
fn applies_insert_after_parent_arrives_out_of_order() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let child = NodeId(2);
    let replica = ReplicaId::new(b"r1");

    let child_first = Operation::insert(&replica, 1, 1, parent, child, 0);
    crdt.apply_remote(child_first).unwrap();

    let parent_op = Operation::insert(&replica, 2, 2, NodeId::ROOT, parent, 0);
    crdt.apply_remote(parent_op).unwrap();

    assert_eq!(crdt.parent(child), Some(parent));
    assert_eq!(crdt.children(parent).unwrap(), &[child]);
}

#[test]
fn move_applied_after_insert_when_delivered_out_of_order() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"a"),
        MemoryStorage::default(),
        LamportClock::default(),
    );

    let parent = NodeId(1);
    let node = NodeId(2);
    let replica = ReplicaId::new(b"r1");

    // Move arrives first (references node + parent that do not yet exist)
    let move_op = Operation::move_node(&replica, 3, 3, node, parent, 0);
    crdt.apply_remote(move_op).unwrap();

    // Later, parent and node inserts arrive
    let parent_insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, 0);
    let node_insert = Operation::insert(&replica, 2, 2, NodeId::ROOT, node, 0);
    crdt.apply_remote(parent_insert).unwrap();
    crdt.apply_remote(node_insert).unwrap();

    assert_eq!(crdt.parent(node), Some(parent));
    assert_eq!(crdt.children(parent).unwrap(), &[node]);
}

#[test]
fn replay_rebuilds_state_and_advanced_clock() {
    let mut storage = MemoryStorage::default();
    let replica = ReplicaId::new(b"r1");
    let parent = NodeId(10);
    let node = NodeId(20);

    // out-of-order arrival persisted to storage
    let move_first = Operation::move_node(&replica, 3, 4, node, parent, 0);
    let node_insert = Operation::insert(&replica, 1, 2, NodeId::ROOT, node, 0);
    let parent_insert = Operation::insert(&replica, 2, 5, NodeId::ROOT, parent, 0);
    storage.apply(move_first.clone()).unwrap();
    storage.apply(node_insert.clone()).unwrap();
    storage.apply(parent_insert.clone()).unwrap();

    let mut crdt = TreeCrdt::new(replica.clone(), storage, LamportClock::default());
    crdt.replay_from_storage().unwrap();

    assert_eq!(crdt.parent(node), Some(parent));
    assert_eq!(crdt.children(parent).unwrap(), &[node]);
    assert_eq!(crdt.lamport(), 5);

    // applying an already-seen op should be ignored
    crdt.apply_remote(move_first).unwrap();
    assert_eq!(crdt.children(parent).unwrap(), &[node]);
}
