use std::slice;

use treecrdt_core::{MaterializationFrontier, NodeId, Operation, ReplicaId};

pub trait MaterializationConformanceHarness {
    fn append_ops(&self, ops: &[Operation]);
    fn append_ops_with_affected_nodes(&self, ops: &[Operation]) -> Vec<NodeId>;
    fn visible_children(&self, parent: NodeId) -> Vec<NodeId>;
    fn payload(&self, node: NodeId) -> Option<Vec<u8>>;
    fn replay_frontier(&self) -> Option<MaterializationFrontier>;
    fn head_seq(&self) -> u64;
    fn force_replay_from_start(&self);
    fn ensure_materialized(&self);
    fn op_ref_counters_for_parent(&self, parent: NodeId) -> Vec<u64>;
}

pub fn order_key_from_position(position: u16) -> Vec<u8> {
    let n = position.wrapping_add(1);
    n.to_be_bytes().to_vec()
}

pub fn node(n: u128) -> NodeId {
    NodeId(n)
}

pub fn representative_remote_batch(
    replica: &ReplicaId,
) -> (NodeId, NodeId, NodeId, Vec<Operation>) {
    let p1 = node(1);
    let p2 = node(2);
    let child = node(3);
    (
        p1,
        p2,
        child,
        vec![
            Operation::insert(replica, 1, 1, NodeId::ROOT, p1, order_key_from_position(0)),
            Operation::insert(replica, 2, 2, NodeId::ROOT, p2, order_key_from_position(1)),
            Operation::insert(replica, 3, 3, p1, child, order_key_from_position(0)),
            Operation::set_payload(replica, 4, 4, child, vec![7]),
            Operation::move_node(replica, 5, 5, child, p2, order_key_from_position(0)),
            Operation::set_payload(replica, 6, 6, child, vec![8]),
        ],
    )
}

fn assert_replay_cleared<H: MaterializationConformanceHarness>(harness: &H) {
    assert_eq!(harness.replay_frontier(), None);
}

pub fn out_of_order_append_catches_up_immediately_from_frontier<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"ooo");
    let second = Operation::insert(
        &replica,
        2,
        2,
        NodeId::ROOT,
        node(2),
        order_key_from_position(1),
    );
    let first = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        node(1),
        order_key_from_position(0),
    );

    harness.append_ops(&[second]);
    let affected = harness.append_ops_with_affected_nodes(slice::from_ref(&first));
    assert_eq!(affected, vec![NodeId::ROOT, node(1), node(2)]);
    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 2);
    assert_eq!(
        harness.visible_children(NodeId::ROOT),
        vec![node(1), node(2)]
    );
    assert_eq!(harness.op_ref_counters_for_parent(NodeId::ROOT), vec![1, 2]);
}

pub fn out_of_order_losing_payload_skips_replay_frontier<H: MaterializationConformanceHarness>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"payload-shortcut");
    let payload_node = node(7);
    let insert = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        payload_node,
        order_key_from_position(0),
    );
    let winning_payload = Operation::set_payload(&replica, 3, 3, payload_node, vec![9]);
    let losing_payload = Operation::set_payload(&replica, 2, 2, payload_node, vec![4]);

    harness.append_ops(&[insert, winning_payload]);
    let affected = harness.append_ops_with_affected_nodes(slice::from_ref(&losing_payload));
    assert_eq!(affected, vec![payload_node]);
    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 3);
    assert_eq!(harness.payload(payload_node), Some(vec![9]));
}

pub fn out_of_order_move_with_later_payload_catches_up_immediately<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"mixed-move");
    let p1 = node(1);
    let p2 = node(2);
    let child = node(3);
    let insert_p1 = Operation::insert(&replica, 1, 1, NodeId::ROOT, p1, order_key_from_position(0));
    let insert_p2 = Operation::insert(&replica, 2, 2, NodeId::ROOT, p2, order_key_from_position(1));
    let insert_child = Operation::insert(&replica, 3, 3, p1, child, order_key_from_position(0));
    let earlier_payload = Operation::set_payload(&replica, 5, 5, child, vec![7]);
    let out_of_order_move =
        Operation::move_node(&replica, 4, 4, child, p2, order_key_from_position(0));
    let later_payload = Operation::set_payload(&replica, 6, 6, child, vec![9]);

    harness.append_ops(&[insert_p1, insert_p2, insert_child, earlier_payload]);
    let affected = harness.append_ops_with_affected_nodes(&[later_payload, out_of_order_move]);
    assert_eq!(affected, vec![p1, p2, child]);
    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 6);
    assert_eq!(harness.visible_children(p1), Vec::<NodeId>::new());
    assert_eq!(harness.visible_children(p2), vec![child]);
    assert_eq!(harness.payload(child), Some(vec![9]));
}

pub fn out_of_order_insert_and_move_before_head_catches_up_immediately<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"mixed-insert");
    let p1 = node(1);
    let p2 = node(2);
    let child = node(3);
    let insert_p1 = Operation::insert(&replica, 1, 1, NodeId::ROOT, p1, order_key_from_position(0));
    let insert_p2 = Operation::insert(&replica, 2, 2, NodeId::ROOT, p2, order_key_from_position(1));
    let unrelated_head = Operation::set_payload(&replica, 5, 5, p2, vec![4]);
    let out_of_order_insert =
        Operation::insert(&replica, 3, 3, p1, child, order_key_from_position(0));
    let out_of_order_move =
        Operation::move_node(&replica, 4, 4, child, p2, order_key_from_position(0));

    harness.append_ops(&[insert_p1, insert_p2, unrelated_head]);
    let affected =
        harness.append_ops_with_affected_nodes(&[out_of_order_move, out_of_order_insert]);
    assert_eq!(affected, vec![p1, p2, child]);
    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 5);
    assert_eq!(harness.visible_children(p1), Vec::<NodeId>::new());
    assert_eq!(harness.visible_children(p2), vec![child]);
    assert_eq!(harness.payload(p2), Some(vec![4]));
}

pub fn replay_from_start_frontier_catches_up_immediately<H: MaterializationConformanceHarness>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"restart");
    let first = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        node(1),
        order_key_from_position(0),
    );
    let second = Operation::insert(
        &replica,
        2,
        2,
        NodeId::ROOT,
        node(2),
        order_key_from_position(1),
    );

    harness.append_ops(&[first]);
    harness.force_replay_from_start();

    let affected = harness.append_ops_with_affected_nodes(&[second]);
    assert_eq!(affected, vec![NodeId::ROOT, node(1), node(2)]);
    assert_replay_cleared(harness);
    assert_eq!(
        harness.visible_children(NodeId::ROOT),
        vec![node(1), node(2)]
    );
    assert_eq!(harness.head_seq(), 2);
}

pub fn deferred_recovery_from_replay_frontier_catches_up_on_ensure<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"ensure");
    let first = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        node(1),
        order_key_from_position(0),
    );
    let second = Operation::insert(
        &replica,
        2,
        2,
        NodeId::ROOT,
        node(2),
        order_key_from_position(1),
    );

    harness.append_ops(&[first, second]);
    harness.force_replay_from_start();
    harness.ensure_materialized();

    assert_replay_cleared(harness);
    assert_eq!(
        harness.visible_children(NodeId::ROOT),
        vec![node(1), node(2)]
    );
    assert_eq!(harness.head_seq(), 2);
    assert_eq!(harness.op_ref_counters_for_parent(NodeId::ROOT), vec![1, 2]);
}

pub fn out_of_order_delete_suffix_falls_back_and_restores_parent<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"delete-fallback");
    let parent = node(1);
    let child = node(2);

    let insert_parent =
        Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, order_key_from_position(0));
    let insert_child = Operation::insert(&replica, 2, 2, parent, child, order_key_from_position(0));

    let mut vv = treecrdt_core::VersionVector::new();
    vv.observe(&replica, 1);
    let delete_parent = Operation::delete(&replica, 3, 3, parent, Some(vv));

    harness.append_ops(&[insert_parent, delete_parent]);
    let _ = harness.append_ops_with_affected_nodes(&[insert_child]);

    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 3);
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![parent]);
    assert_eq!(harness.visible_children(parent), vec![child]);
}
