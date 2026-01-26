use treecrdt_core::{LamportClock, MemoryStorage, NodeId, Operation, ReplicaId, TreeCrdt};

#[test]
fn permutations_converge() {
    let ops = vec![
        Operation::insert(
            &ReplicaId::new(b"a"),
            1,
            1,
            NodeId::ROOT,
            NodeId(1),
            Vec::new(),
        ),
        Operation::insert(
            &ReplicaId::new(b"a"),
            2,
            2,
            NodeId::ROOT,
            NodeId(2),
            Vec::new(),
        ),
        Operation::insert(
            &ReplicaId::new(b"a"),
            3,
            3,
            NodeId::ROOT,
            NodeId(3),
            Vec::new(),
        ),
        Operation::move_node(
            &ReplicaId::new(b"a"),
            4,
            4,
            NodeId(3),
            NodeId(1),
            Vec::new(),
        ),
        Operation::move_node(
            &ReplicaId::new(b"a"),
            5,
            5,
            NodeId(3),
            NodeId(2),
            Vec::new(),
        ),
    ];

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
            assert_eq!(snapshot, *base);
        } else {
            baseline = Some(snapshot);
        }
    }
}
