use treecrdt_core::{LamportClock, LocalPlacement, MemoryStorage, NodeId, ReplicaId, TreeCrdt};

#[test]
fn current_rows_are_owned_and_exclude_deleted_or_unknown_nodes() {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    for (id, parent) in [(1, NodeId::ROOT), (2, NodeId::ROOT), (3, NodeId(1))] {
        tree.local_insert(
            parent,
            NodeId(id),
            LocalPlacement::Last,
            Some(vec![id as u8]),
        )
        .unwrap();
    }
    let retained = tree.read_node(NodeId(1)).unwrap().unwrap();
    tree.local_payload(NodeId(1), Some(b"renamed".to_vec())).unwrap();
    tree.local_move(NodeId(3), NodeId(2), LocalPlacement::Last).unwrap();

    assert_eq!(retained.payload, Some(vec![1]));
    assert_eq!(retained.children, vec![NodeId(3)]);
    let current = tree.read_node(NodeId(1)).unwrap().unwrap();
    assert_eq!(current.parent, Some(NodeId::ROOT));
    assert_eq!(current.payload, Some(b"renamed".to_vec()));
    assert!(current.children.is_empty());
    assert_eq!(
        tree.read_node(NodeId(3)).unwrap().unwrap().parent,
        Some(NodeId(2))
    );

    tree.local_delete(NodeId(1)).unwrap();
    assert_eq!(tree.read_node(NodeId(1)).unwrap(), None);
    assert_eq!(tree.read_node(NodeId(99)).unwrap(), None);
    let ids = tree.node_ids().unwrap();
    assert!(!ids.contains(&NodeId(1)));
    assert!(ids.contains(&NodeId(2)));
    assert!(ids.contains(&NodeId(3)));
}
