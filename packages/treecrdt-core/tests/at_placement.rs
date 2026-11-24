use treecrdt_core::{
    Error, LamportClock, LocalPlacement, MemoryStorage, NodeId, Operation, OperationKind,
    ReplicaId, TreeCrdt,
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

fn at(key: &[u8]) -> LocalPlacement {
    LocalPlacement::At { key: key.to_vec() }
}

fn order_key(op: &Operation) -> &[u8] {
    match &op.kind {
        OperationKind::Insert { order_key, .. } | OperationKind::Move { order_key, .. } => {
            order_key
        }
        _ => unreachable!("only inserts and moves carry an order key"),
    }
}

#[test]
fn inserts_and_moves_get_exactly_the_key() {
    let mut crdt = tree(b"a");
    let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));

    let (insert_a, _) = crdt.local_insert(NodeId::ROOT, a, at(&[0x00, 0x05]), None).unwrap();
    let (insert_b, _) = crdt.local_insert(NodeId::ROOT, b, at(&[0x00, 0x03]), None).unwrap();
    assert_eq!(order_key(&insert_a), [0x00, 0x05]);
    assert_eq!(order_key(&insert_b), [0x00, 0x03]);
    assert_eq!(crdt.children(NodeId::ROOT).unwrap(), [b, a]);

    crdt.local_insert(NodeId::ROOT, c, LocalPlacement::Last, None).unwrap();
    let (move_c, _) = crdt.local_move(c, NodeId::ROOT, at(&[0x00, 0x04])).unwrap();
    assert!(matches!(move_c.kind, OperationKind::Move { node, .. } if node == c));
    assert_eq!(order_key(&move_c), [0x00, 0x04]);
    assert_eq!(crdt.children(NodeId::ROOT).unwrap(), [b, c, a]);
}

#[test]
fn other_placements_find_room_next_to_exact_keys() {
    let mut crdt = tree(b"a");
    let [a, b, first, after_a, last] = [1, 2, 3, 4, 5].map(NodeId);
    // Only keys longer than both lie between these two.
    crdt.local_insert(NodeId::ROOT, a, at(&[0x00, 0x05]), None).unwrap();
    crdt.local_insert(NodeId::ROOT, b, at(&[0x00, 0x05, 0x00, 0x01]), None).unwrap();

    crdt.local_insert(NodeId::ROOT, first, LocalPlacement::First, None).unwrap();
    crdt.local_insert(NodeId::ROOT, after_a, LocalPlacement::After(a), None)
        .unwrap();
    crdt.local_insert(NodeId::ROOT, last, LocalPlacement::Last, None).unwrap();

    assert_eq!(
        crdt.children(NodeId::ROOT).unwrap(),
        [first, a, after_a, b, last]
    );
}

#[test]
fn rejects_keys_that_are_not_canonical() {
    let mut crdt = tree(b"a");
    let (node, other) = (NodeId(1), NodeId(2));
    crdt.local_insert(NodeId::ROOT, node, LocalPlacement::First, None).unwrap();
    let (empty, odd, zero) = (
        "key must not be empty",
        "key must have even length",
        "key must not end in a zero digit",
    );
    let cases: [(&[u8], &str); 5] = [
        (&[], empty),
        (&[0x05], odd),
        (&[0x00, 0x05, 0x01], odd),
        (&[0x00, 0x00], zero),
        (&[0x00, 0x05, 0x00, 0x00], zero),
    ];

    for (key, reason) in cases {
        let insert = crdt.local_insert(NodeId::ROOT, other, at(key), None);
        let moved = crdt.local_move(node, NodeId::ROOT, at(key));
        for result in [insert, moved] {
            assert!(
                matches!(&result, Err(Error::InvalidOperation(message)) if message.contains(reason)),
                "{key:02x?}: {result:?}"
            );
        }
    }
    // No rejected key uses up a counter or ticks the clock.
    let (op, _) = crdt.local_insert(NodeId::ROOT, other, at(&[0x00, 0x01]), None).unwrap();
    assert_eq!((op.meta.id.counter, op.meta.lamport), (2, 2));
    // Engines that name placements cannot pass a key.
    assert!(matches!(
        LocalPlacement::from_parts("at", None),
        Err(Error::InvalidOperation(_))
    ));
}

#[test]
fn insert_or_move_at_a_key_into_trash_gets_an_empty_key_like_other_placements() {
    let mut crdt = tree(b"a");
    let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));
    crdt.local_insert(NodeId::ROOT, a, LocalPlacement::Last, None).unwrap();
    crdt.local_insert(NodeId::ROOT, b, LocalPlacement::Last, None).unwrap();

    let (last, _) = crdt.local_move(a, NodeId::TRASH, LocalPlacement::Last).unwrap();
    let (moved, _) = crdt.local_move(b, NodeId::TRASH, at(&[0x00, 0x05])).unwrap();
    // Trashed nodes have no order, so not even a key that is not canonical matters.
    let (inserted, _) = crdt.local_insert(NodeId::TRASH, c, at(&[]), None).unwrap();
    assert!(order_key(&last).is_empty());
    assert_eq!(order_key(&moved), order_key(&last));
    assert_eq!(order_key(&inserted), order_key(&last));
}

#[test]
fn replicas_placing_at_one_key_converge() {
    let (mut a, mut b) = (tree(b"a"), tree(b"b"));
    let (shared, x, y) = (NodeId(1), NodeId(3), NodeId(2));
    let key = [0x00, 0x05];

    // Both create the same node at the same key, as automatic writes do, and each another node at
    // that key.
    let (a_shared, _) = a.local_insert(NodeId::ROOT, shared, at(&key), None).unwrap();
    let (b_shared, _) = b.local_insert(NodeId::ROOT, shared, at(&key), None).unwrap();
    assert_eq!(a_shared.kind, b_shared.kind);
    let (insert_x, _) = a.local_insert(NodeId::ROOT, x, at(&key), None).unwrap();
    let (insert_y, _) = b.local_insert(NodeId::ROOT, y, at(&key), None).unwrap();
    a.apply_remote_batch([b_shared, insert_y]).unwrap();
    b.apply_remote_batch([a_shared, insert_x]).unwrap();

    // Equal keys go in node-id order.
    assert_eq!(a.children(NodeId::ROOT).unwrap(), [shared, y, x]);
    assert_eq!(b.children(NodeId::ROOT).unwrap(), [shared, y, x]);
}
