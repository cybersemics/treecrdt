use treecrdt_core::{
    Error, LamportClock, LocalPlacement, MaterializationChange, MemoryStorage, NodeId, Operation,
    OperationKind, ReplicaId, TreeCrdt,
};

type Tree = TreeCrdt<MemoryStorage, LamportClock>;

fn tree(replica: &[u8]) -> Tree {
    TreeCrdt::new(
        ReplicaId::new(replica),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap()
}

fn between(left: Option<&[u8]>, right: Option<&[u8]>) -> LocalPlacement {
    LocalPlacement::Between {
        left: left.map(<[u8]>::to_vec),
        right: right.map(<[u8]>::to_vec),
    }
}

fn order_key(op: &Operation) -> &[u8] {
    match &op.kind {
        OperationKind::Insert { order_key, .. } | OperationKind::Move { order_key, .. } => {
            order_key
        }
        _ => unreachable!("only inserts and moves carry an order key"),
    }
}

fn assert_between(key: &[u8], left: Option<&[u8]>, right: Option<&[u8]>) {
    assert!(
        left.is_none_or(|left| key > left) && right.is_none_or(|right| key < right),
        "{key:02x?} is not between {left:02x?} and {right:02x?}"
    );
    assert!(
        key.len().is_multiple_of(2) && key[key.len() - 2..] != [0, 0],
        "{key:02x?} is not canonical"
    );
}

/// Inserts `node` between the bounds and returns its key.
fn place(crdt: &mut Tree, node: NodeId, left: Option<&[u8]>, right: Option<&[u8]>) -> Vec<u8> {
    let (op, _) = crdt.local_insert(NodeId::ROOT, node, between(left, right), None).unwrap();
    let key = order_key(&op).to_vec();
    assert_between(&key, left, right);
    key
}

#[test]
fn places_strictly_between_open_and_closed_bounds() {
    let mut crdt = tree(b"a");
    let (a, b, c, d) = (NodeId(1), NodeId(2), NodeId(3), NodeId(4));

    let key_a = place(&mut crdt, a, None, None);
    place(&mut crdt, b, Some(&key_a), None);
    let key_c = place(&mut crdt, c, None, Some(&key_a));
    place(&mut crdt, d, Some(&key_c), Some(&key_a));

    assert_eq!(crdt.children(NodeId::ROOT).unwrap(), [c, d, a, b]);
}

#[test]
fn bounds_need_not_be_sibling_keys_and_may_end_in_a_zero_digit() {
    // Three-digit prefixes like these order nodes by a rank kept outside the tree.
    let rank = |rank: u64| rank.to_be_bytes()[2..].to_vec();
    let mut crdt = tree(b"a");

    for (node, left, right) in [
        (1, Some(rank(0xffff)), Some(rank(0x1_0000))),
        (2, None, Some(rank(0x1_0000))),
        (3, Some(rank(0x1_0000)), None),
        (4, Some(rank(0x1_0000)), Some(rank(0x2_0000))),
    ] {
        place(&mut crdt, NodeId(node), left.as_deref(), right.as_deref());
    }
}

#[test]
fn rejects_bounds_with_no_key_between_them() {
    let mut crdt = tree(b"a");
    let (node, other) = (NodeId(1), NodeId(2));
    crdt.local_insert(NodeId::ROOT, node, LocalPlacement::First, None).unwrap();
    let (low, high) = ([0x00, 0x05], [0x00, 0x09]);
    let (out_of_order, odd, no_room) = (
        "left must be below right",
        "bounds must have even length",
        "no key fits between left and right",
    );
    /// The bounds, and the reason a placement between them fails.
    type Case<'a> = (Option<&'a [u8]>, Option<&'a [u8]>, &'a str);
    let cases: [Case<'_>; 7] = [
        (Some(&high), Some(&low), out_of_order),
        (Some(&low), Some(&low), out_of_order),
        (Some(&[0x00, 0x05, 0x01]), None, odd),
        (None, Some(&[0x05]), odd),
        // Only keys ending in a zero digit lie between `left` and `left` followed by zero digits.
        (Some(&low), Some(&[0x00, 0x05, 0x00, 0x00]), no_room),
        (None, Some(&[0x00, 0x00]), no_room),
        (None, Some(&[]), no_room),
    ];

    for (left, right, reason) in cases {
        let insert = crdt.local_insert(NodeId::ROOT, other, between(left, right), None);
        let moved = crdt.local_move(node, NodeId::ROOT, between(left, right));
        for result in [insert, moved] {
            assert!(
                matches!(&result, Err(Error::InvalidOperation(message)) if message.contains(reason)),
                "{left:02x?}..{right:02x?}: {result:?}"
            );
        }
    }
    // No rejected placement, of any kind, uses up a counter or ticks the clock.
    let after_unknown = LocalPlacement::After(NodeId(99));
    assert!(matches!(
        crdt.local_insert(NodeId::ROOT, other, after_unknown, None),
        Err(Error::InvalidOperation(_))
    ));
    let (op, _) = crdt.local_insert(NodeId::ROOT, other, LocalPlacement::Last, None).unwrap();
    assert_eq!((op.meta.id.counter, op.meta.lamport), (2, 2));
    // Engines that name placements cannot pass bounds.
    assert!(matches!(
        LocalPlacement::from_parts("between", None),
        Err(Error::InvalidOperation(_))
    ));
}

#[test]
fn concurrent_placements_in_one_gap_get_distinct_keys_and_converge() {
    let (mut a, mut b) = (tree(b"a"), tree(b"b"));
    let (left, right, x, y) = (NodeId(1), NodeId(2), NodeId(3), NodeId(4));
    let (insert_left, _) = a.local_insert(NodeId::ROOT, left, LocalPlacement::First, None).unwrap();
    let (insert_right, _) =
        a.local_insert(NodeId::ROOT, right, LocalPlacement::After(left), None).unwrap();
    b.apply_remote_batch([insert_left.clone(), insert_right.clone()]).unwrap();
    let gap = between(
        Some(order_key(&insert_left)),
        Some(order_key(&insert_right)),
    );

    let (insert_x, _) = a.local_insert(NodeId::ROOT, x, gap.clone(), None).unwrap();
    let (insert_y, _) = b.local_insert(NodeId::ROOT, y, gap, None).unwrap();
    assert_ne!(order_key(&insert_x), order_key(&insert_y));
    a.apply_remote(insert_y).unwrap();
    b.apply_remote(insert_x).unwrap();

    let children = a.children(NodeId::ROOT).unwrap();
    assert_eq!(children, b.children(NodeId::ROOT).unwrap());
    assert_eq!((children[0], children[3]), (left, right));
}

#[test]
fn placements_with_one_seed_get_distinct_keys() {
    // Two trees with one replica id seed their first ops alike, so both allocate the same key in
    // a gap; the node id after it keeps the keys apart.
    let (mut a, mut b) = (tree(b"r"), tree(b"r"));
    let (insert_x, _) = a.local_insert(NodeId::ROOT, NodeId(1), between(None, None), None).unwrap();
    let (insert_y, _) = b.local_insert(NodeId::ROOT, NodeId(2), between(None, None), None).unwrap();

    // The node id and the final digit are the last 18 bytes.
    let (key_x, key_y) = (order_key(&insert_x), order_key(&insert_y));
    assert_eq!(key_x[..key_x.len() - 18], key_y[..key_y.len() - 18]);
    assert_ne!(key_x, key_y);
}

#[test]
fn move_between_keeps_the_node_and_its_subtree() {
    let mut crdt = tree(b"a");
    let root = NodeId::ROOT;
    let [from, to, node, child, first, last] = [1, 2, 3, 4, 5, 6].map(NodeId);
    crdt.local_insert(root, from, LocalPlacement::First, None).unwrap();
    crdt.local_insert(root, to, LocalPlacement::Last, None).unwrap();
    crdt.local_insert(from, node, LocalPlacement::First, Some(b"x".to_vec()))
        .unwrap();
    crdt.local_insert(node, child, LocalPlacement::First, None).unwrap();
    let (insert_first, _) = crdt.local_insert(to, first, LocalPlacement::First, None).unwrap();
    let (insert_last, _) = crdt.local_insert(to, last, LocalPlacement::Last, None).unwrap();
    let (left, right) = (order_key(&insert_first), order_key(&insert_last));

    let (op, plan) = crdt.local_move(node, to, between(Some(left), Some(right))).unwrap();

    assert!(matches!(op.kind, OperationKind::Move { new_parent, .. } if new_parent == to));
    assert_eq!(op.kind.node(), node);
    assert_between(order_key(&op), Some(left), Some(right));
    assert_eq!(
        plan.changes,
        [MaterializationChange::Move {
            node,
            parent_before: Some(from),
            parent_after: to,
            source: None,
        }]
    );
    assert_eq!(crdt.children(to).unwrap(), [first, node, last]);
    assert_eq!(crdt.children(from).unwrap(), []);
    assert_eq!(crdt.children(node).unwrap(), [child]);
    assert_eq!(crdt.payload(node).unwrap(), Some(b"x".to_vec()));

    // A move under the node's own descendant is ignored, as with any placement.
    crdt.local_move(node, child, between(None, None)).unwrap();
    assert_eq!(crdt.parent(node).unwrap(), Some(to));
}

#[test]
fn move_between_to_trash_gets_an_empty_key_like_other_placements() {
    let mut crdt = tree(b"a");
    let (a, b) = (NodeId(1), NodeId(2));
    crdt.local_insert(NodeId::ROOT, a, LocalPlacement::Last, None).unwrap();
    crdt.local_insert(NodeId::ROOT, b, LocalPlacement::Last, None).unwrap();

    let (last, _) = crdt.local_move(a, NodeId::TRASH, LocalPlacement::Last).unwrap();
    // Trashed nodes have no order, so not even bounds without room between them matter.
    let low = [0x00, 0x05];
    let (trashed, _) = crdt.local_move(b, NodeId::TRASH, between(Some(&low), Some(&low))).unwrap();
    assert!(order_key(&last).is_empty());
    assert_eq!(order_key(&trashed), order_key(&last));
}

/// The root's children with their keys, in order.
type Order = Vec<(Vec<u8>, NodeId)>;

/// Places 1,000 times between the neighbors at the index `gap` picks, given a random number, and
/// returns the longest key those placements got. Each placement inserts a new node, or with
/// `moves`, moves a random one of the `initial` nodes.
fn max_key_bytes(initial: usize, moves: bool, gap: fn(&Order, u64) -> usize) -> usize {
    let mut crdt = tree(b"replica");
    let mut rng = 0x9e37_79b9_7f4a_7c15_u64;
    let mut random = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut order = Order::new();
    let mut max = 0;
    for step in 0..initial + 1_000 {
        let setup = step < initial;
        // Random ids, as applications use: a key can extend its left bound, node id included.
        let node = if moves && !setup {
            order.remove((random() % order.len() as u64) as usize).1
        } else {
            NodeId(u128::from(random()) << 64 | u128::from(random()))
        };
        let at = if setup {
            order.len()
        } else {
            gap(&order, random())
        };
        let left = at.checked_sub(1).map(|i| order[i].0.as_slice());
        let right = order.get(at).map(|(key, _)| key.as_slice());
        let (op, _) = if moves && !setup {
            crdt.local_move(node, NodeId::ROOT, between(left, right)).unwrap()
        } else {
            crdt.local_insert(NodeId::ROOT, node, between(left, right), None).unwrap()
        };
        let key = order_key(&op).to_vec();
        assert_between(&key, left, right);
        if !setup {
            max = max.max(key.len());
        }
        order.insert(at, (key, node));
    }
    let children: Vec<NodeId> = order.iter().map(|(_, node)| *node).collect();
    assert_eq!(crdt.children(NodeId::ROOT).unwrap(), children);
    max
}

#[test]
fn repeated_placements_keep_keys_short() {
    type Gap = fn(&Order, u64) -> usize;
    let cases: [(&str, usize, bool, Gap); 5] = [
        ("top", 2, false, |_, _| 0),
        ("bottom", 2, false, |order, _| order.len()),
        ("after one key", 2, false, |_, _| 1),
        ("before one key", 2, false, |order, _| order.len() - 1),
        ("random among 50", 50, true, |order, random| {
            (random % (order.len() as u64 + 1)) as usize
        }),
    ];
    for (case, initial, moves, gap) in cases {
        let max = max_key_bytes(initial, moves, gap);
        // At most 40 bytes today; a digit more per placement would reach 2 KB.
        assert!(max <= 64, "{case}: {max} bytes");
    }
}
