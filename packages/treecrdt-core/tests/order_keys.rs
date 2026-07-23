use std::collections::HashSet;

use treecrdt_core::{
    order_key::allocate_between, LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation,
    OperationKind, ReplicaId, TreeCrdt,
};

fn assert_canonical(key: &[u8]) {
    assert!(!key.is_empty());
    assert!(key.len().is_multiple_of(2));
    assert_ne!(&key[key.len() - 2..], &[0, 0]);
}

#[test]
fn generated_keys_have_high_seed_diversity() {
    let mut initial_keys = HashSet::new();
    let mut tight_interval_keys = HashSet::new();
    for counter in 0..65_536_u64 {
        let mut seed = b"replica-a".to_vec();
        seed.extend_from_slice(&counter.to_be_bytes());

        let initial = allocate_between(None, None, &seed).unwrap();
        assert_canonical(&initial);
        assert!(initial_keys.insert(initial));

        let tight = allocate_between(Some(&[0, 1]), Some(&[0, 2]), &seed).unwrap();
        assert_canonical(&tight);
        assert!(tight_interval_keys.insert(tight));
    }
}

#[test]
fn allocator_requires_canonical_increasing_bounds() {
    for (left, right, expected) in [
        (Some([].as_slice()), None, "non-empty"),
        (Some([1].as_slice()), None, "complete"),
        (Some([0, 0].as_slice()), None, "non-zero"),
        (
            Some([0, 1].as_slice()),
            Some([0, 1].as_slice()),
            "strictly increasing",
        ),
        (
            Some([0, 2].as_slice()),
            Some([0, 1].as_slice()),
            "strictly increasing",
        ),
    ] {
        let error = allocate_between(left, right, b"seed").unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn max_digit_is_allocatable() {
    let max = [0xff, 0xff];
    let after = allocate_between(Some(&max), None, b"seed").unwrap();
    let before = allocate_between(Some(&[0xff, 0xfe]), Some(&max), b"seed").unwrap();

    assert!(after.as_slice() > max.as_slice());
    assert!(before.as_slice() > [0xff, 0xfe].as_slice());
    assert!(before.as_slice() < max.as_slice());
    assert_canonical(&after);
    assert_canonical(&before);
}

#[test]
fn repeated_exact_after_insertions_remain_strictly_ordered() {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let anchor = NodeId(1);
    let (anchor_op, _) =
        tree.local_insert(NodeId::ROOT, anchor, LocalPlacement::First, None).unwrap();
    let OperationKind::Insert {
        order_key: anchor_key,
        ..
    } = anchor_op.kind
    else {
        unreachable!();
    };

    let mut keys = HashSet::from([anchor_key.clone()]);
    for id in 2..=258 {
        let node = NodeId(id);
        let (op, _) = tree
            .local_insert(NodeId::ROOT, node, LocalPlacement::After(anchor), None)
            .unwrap();
        let OperationKind::Insert { order_key, .. } = op.kind else {
            unreachable!();
        };
        assert_canonical(&order_key);
        assert!(order_key > anchor_key);
        assert!(keys.insert(order_key));
        assert_eq!(tree.children(NodeId::ROOT).unwrap()[..2], [anchor, node]);
    }
}

#[test]
fn equal_keys_use_node_tiebreak_and_exact_after_fails_closed() {
    let mut tree = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let remote = ReplicaId::new(b"remote");
    let first = NodeId(10);
    let second = NodeId(20);
    let key = vec![0x12, 0x34];

    tree.apply_remote(Operation::insert(
        &remote,
        1,
        1,
        NodeId::ROOT,
        second,
        key.clone(),
    ))
    .unwrap();
    tree.apply_remote(Operation::insert(&remote, 2, 2, NodeId::ROOT, first, key))
        .unwrap();

    assert_eq!(tree.children(NodeId::ROOT).unwrap(), &[first, second]);
    let error = tree
        .local_insert(NodeId::ROOT, NodeId(30), LocalPlacement::After(first), None)
        .unwrap_err();
    assert!(error.to_string().contains("same order_key"));
    assert_eq!(tree.children(NodeId::ROOT).unwrap(), &[first, second]);
}
