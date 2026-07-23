use treecrdt_core::{MemoryNodeStore, NodeId, NodeStore, ReplicaId, VersionVector};

fn version(replica: &ReplicaId, counter: u64) -> VersionVector {
    let mut vv = VersionVector::new();
    vv.observe(replica, counter);
    vv
}

#[test]
fn structural_subtree_walk_terminates_on_cycle_and_merges_each_node() {
    let replica_a = ReplicaId::new(b"a");
    let replica_b = ReplicaId::new(b"b");
    let node_a = NodeId(1);
    let node_b = NodeId(2);
    let mut nodes = MemoryNodeStore::default();

    nodes.attach(node_a, node_b, vec![0x10]).unwrap();
    nodes.attach(node_b, node_a, vec![0x20]).unwrap();
    nodes.merge_last_change(node_a, &version(&replica_a, 1)).unwrap();
    nodes.merge_last_change(node_b, &version(&replica_b, 2)).unwrap();

    let subtree = nodes.structural_subtree_version_vector(node_a).unwrap();

    assert_eq!(subtree.get(&replica_a), 1);
    assert_eq!(subtree.get(&replica_b), 2);
}

#[test]
fn structural_subtree_walk_handles_a_deep_chain_without_recursion() {
    const DEPTH: u64 = 50_000;

    let replica = ReplicaId::new(b"deep-chain");
    let mut nodes = MemoryNodeStore::default();
    let mut parent = NodeId::ROOT;

    for counter in 1..=DEPTH {
        let child = NodeId(counter as u128);
        nodes.attach(child, parent, vec![0x10]).unwrap();
        nodes.merge_last_change(child, &version(&replica, counter)).unwrap();
        parent = child;
    }

    let subtree = nodes.structural_subtree_version_vector(NodeId::ROOT).unwrap();

    assert_eq!(subtree.frontier(&replica), DEPTH);
}
