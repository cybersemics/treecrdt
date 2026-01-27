use proptest::prelude::*;
use treecrdt_core::{Lamport, LamportClock, MemoryStorage, NodeId, Operation, ReplicaId, TreeCrdt};

proptest! {
    #[test]
    fn permutations_converge_property(ops in {
        // Generate up to 5 operations with lamports 1..=5 over a small node set.
        let nodes = [NodeId::ROOT, NodeId(1), NodeId(2), NodeId(3)];
        let replicas = [ReplicaId::new(b"a"), ReplicaId::new(b"b")];
        prop::collection::vec(
            (0usize..5).prop_map(move |i| {
                let lamport = (i + 1) as Lamport;
                let replica = replicas[i % replicas.len()].clone();
                let node = nodes[(i + 1) % nodes.len()];
                let parent = nodes[i % nodes.len()];
                match i % 3 {
                    0 => Operation::insert(&replica, (i + 1) as u64, lamport, parent, node, Vec::new()),
                    1 => Operation::move_node(&replica, (i + 1) as u64, lamport, node, parent, Vec::new()),
                    _ => Operation::delete(&replica, (i + 1) as u64, lamport, node, None),
                }
            }),
            1..=5,
        )
    }) {
        // Generate all permutations using Heap's algorithm
        fn heap_permute(k: usize, items: &mut [Operation], res: &mut Vec<Vec<Operation>>) {
            if k == 1 {
                res.push(items.to_vec());
                return;
            }
            heap_permute(k - 1, items, res);
            for i in 0..(k - 1) {
                if k.is_multiple_of(2) {
                    items.swap(i, k - 1);
                } else {
                    items.swap(0, k - 1);
                }
                heap_permute(k - 1, items, res);
            }
        }
        let mut permutations = Vec::new();
        heap_permute(ops.len(), &mut ops.clone(), &mut permutations);

        let mut baseline: Option<Vec<(NodeId, Option<NodeId>)>> = None;
        for perm in permutations {
            let mut crdt = TreeCrdt::new(
                ReplicaId::new(b"p"),
                MemoryStorage::default(),
                LamportClock::default(),
            )
            .unwrap();
            for op in &perm {
                crdt.apply_remote(op.clone()).unwrap();
            }
            crdt.validate_invariants().unwrap();
            let snapshot = crdt.nodes().unwrap();
            if let Some(base) = &baseline {
                prop_assert_eq!(snapshot, base.clone());
            } else {
                baseline = Some(snapshot);
            }
        }
    }
}
