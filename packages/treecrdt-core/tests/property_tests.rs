use proptest::prelude::*;
use treecrdt_core::{
    order_key::allocate_between, Lamport, LamportClock, MemoryStorage, NodeId, Operation,
    ReplicaId, TreeCrdt,
};

proptest! {
    #[test]
    fn order_key_output_is_strictly_inside_valid_digit_bounds(
        left in 1u16..=u16::MAX,
        right in 1u16..=u16::MAX,
        seed in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        prop_assume!(left < right);
        let left = left.to_be_bytes();
        let right = right.to_be_bytes();
        let key = allocate_between(Some(&left), Some(&right), &seed).unwrap();
        prop_assert!(key.as_slice() > left.as_slice());
        prop_assert!(key.as_slice() < right.as_slice());
    }

    #[test]
    fn order_key_allocation_never_panics_and_is_deterministic(
        left in prop::option::of(prop::collection::vec(any::<u8>(), 0..32)),
        right in prop::option::of(prop::collection::vec(any::<u8>(), 0..32)),
        seed in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let first = allocate_between(left.as_deref(), right.as_deref(), &seed);
        let second = allocate_between(left.as_deref(), right.as_deref(), &seed);

        match (first, second) {
            (Ok(first), Ok(second)) => {
                prop_assert_eq!(&first, &second);
                prop_assert!(!first.is_empty());
                prop_assert!(first.len().is_multiple_of(2));
                let terminator = u16::from_be_bytes([
                    first[first.len() - 2],
                    first[first.len() - 1],
                ]);
                prop_assert_ne!(terminator, 0);
                if let Some(left) = left.as_deref() {
                    prop_assert!(first.as_slice() > left);
                }
                if let Some(right) = right.as_deref() {
                    prop_assert!(first.as_slice() < right);
                }
            }
            (Err(first), Err(second)) => prop_assert_eq!(first.to_string(), second.to_string()),
            _ => prop_assert!(false, "same inputs returned different result variants"),
        }
    }

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
                    0 => Operation::insert(&replica, (i + 1) as u64, lamport, parent, node, vec![0, (i + 1) as u8]),
                    1 => Operation::move_node(&replica, (i + 1) as u64, lamport, node, parent, vec![0, (i + 1) as u8]),
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
