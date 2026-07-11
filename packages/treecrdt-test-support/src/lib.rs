use std::{collections::HashSet, slice};

use treecrdt_core::{
    MaterializationChange, MaterializationFrontier, MaterializationOutcome, MaterializationSource,
    NodeId, Operation, ReplicaId, VersionVector,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedNodeState {
    pub parent: Option<NodeId>,
    pub tombstone: bool,
    pub deleted_at: Option<VersionVector>,
}

pub trait MaterializationConformanceHarness {
    fn append_ops(&self, ops: &[Operation]);
    fn append_ops_with_materialization_outcome(&self, ops: &[Operation]) -> MaterializationOutcome;
    fn visible_children(&self, parent: NodeId) -> Vec<NodeId>;
    fn payload(&self, node: NodeId) -> Option<Vec<u8>>;
    fn op_count(&self) -> u64;
    fn replay_frontier(&self) -> Option<MaterializationFrontier>;
    fn materialization_head(&self) -> MaterializationFrontier;
    fn head_seq(&self) -> u64;
    fn node_state(&self, node: NodeId) -> MaterializedNodeState;
    fn force_replay_from_start(&self);
    fn ensure_materialized(&self);
    fn op_ref_counters_for_parent(&self, parent: NodeId) -> Vec<u64>;
    fn op_kinds_for_parent(&self, parent: NodeId) -> Vec<String>;
}

fn changed_nodes(outcome: &MaterializationOutcome) -> Vec<NodeId> {
    outcome.affected_nodes()
}

fn visible_payload_source(
    change: &MaterializationChange,
    target: NodeId,
    expected_payload: &[u8],
) -> Option<Option<MaterializationSource>> {
    match change {
        MaterializationChange::Insert {
            node,
            payload,
            source,
            ..
        }
        | MaterializationChange::Restore {
            node,
            payload,
            source,
            ..
        }
        | MaterializationChange::Payload {
            node,
            payload,
            source,
        } if *node == target && payload.as_deref() == Some(expected_payload) => {
            Some(source.clone())
        }
        _ => None,
    }
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

pub fn append_batch_materializes_only_inserted_ops<H: MaterializationConformanceHarness>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"dup");
    let node = node(1);
    let insert = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        node,
        order_key_from_position(0),
    );
    let payload = Operation::set_payload(&replica, 2, 2, node, vec![9]);

    let before = harness.op_count();
    harness.append_ops(&[insert.clone(), insert, payload]);

    assert_eq!(harness.op_count().saturating_sub(before), 2);
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![node]);
    assert_eq!(harness.head_seq(), 2);
}

pub fn representative_remote_batch_matches_shape<H: MaterializationConformanceHarness>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"rep");
    let (p1, p2, child, ops) = representative_remote_batch(&replica);

    let outcome = harness.append_ops_with_materialization_outcome(&ops);
    assert_eq!(changed_nodes(&outcome), vec![NodeId::ROOT, p1, p2, child]);
    let payload_source = outcome
        .changes
        .iter()
        .find_map(|change| visible_payload_source(change, child, &[8]))
        .expect("expected final child payload in materialization outcome");
    assert_eq!(
        payload_source,
        Some(MaterializationSource::from_op(ops.last().unwrap()))
    );
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![p1, p2]);
    assert_eq!(harness.visible_children(p2), vec![child]);
    assert_eq!(harness.payload(child), Some(vec![8]));

    let kinds = harness.op_kinds_for_parent(p2);
    assert!(kinds.iter().any(|kind| kind == "move"));
    assert!(kinds.iter().any(|kind| kind == "payload"));
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
    let outcome = harness.append_ops_with_materialization_outcome(slice::from_ref(&first));
    assert_eq!(
        changed_nodes(&outcome),
        vec![NodeId::ROOT, node(1), node(2)]
    );
    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 2);
    assert_eq!(
        harness.visible_children(NodeId::ROOT),
        vec![node(1), node(2)]
    );
    assert_eq!(harness.op_ref_counters_for_parent(NodeId::ROOT), vec![1, 2]);
}

pub fn out_of_order_losing_payload_replays_causal_metadata<H: MaterializationConformanceHarness>(
    harness: &H,
) {
    let replica = ReplicaId::new(b"payload-replay");
    let deleter = ReplicaId::new(b"payload-delete");
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
    harness.append_ops(&[losing_payload]);
    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 3);
    assert_eq!(harness.payload(payload_node), Some(vec![9]));

    // The losing payload is still causally significant. A delete that knows counters 1 and 3 but
    // not the late counter 2 must remain concurrent with the node's canonical last-change vector.
    let mut known_state = VersionVector::new();
    known_state.observe(&replica, 1);
    known_state.observe(&replica, 3);
    let delete = Operation::delete(&deleter, 1, 4, payload_node, Some(known_state));
    harness.append_ops(&[delete]);

    assert!(!harness.node_state(payload_node).tombstone);
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![payload_node]);
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
    let outcome =
        harness.append_ops_with_materialization_outcome(&[later_payload, out_of_order_move]);
    assert_eq!(changed_nodes(&outcome), vec![p1, p2, child]);
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
    let outcome =
        harness.append_ops_with_materialization_outcome(&[out_of_order_move, out_of_order_insert]);
    assert_eq!(changed_nodes(&outcome), vec![p2, child]);
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

    let outcome = harness.append_ops_with_materialization_outcome(&[second]);
    assert_eq!(
        changed_nodes(&outcome),
        vec![NodeId::ROOT, node(1), node(2)]
    );
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

    let insert_parent = Operation::insert(
        &replica,
        1,
        1,
        NodeId::ROOT,
        parent,
        order_key_from_position(0),
    );
    let insert_child = Operation::insert(&replica, 2, 2, parent, child, order_key_from_position(0));

    let mut vv = treecrdt_core::VersionVector::new();
    vv.observe(&replica, 1);
    let delete_parent = Operation::delete(&replica, 3, 3, parent, Some(vv));

    harness.append_ops(&[insert_parent, delete_parent]);
    let _ = harness.append_ops_with_materialization_outcome(&[insert_child]);

    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 3);
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![parent]);
    assert_eq!(harness.visible_children(parent), vec![child]);
}

fn assert_parent_chain_acyclic<H: MaterializationConformanceHarness>(harness: &H, start: NodeId) {
    let mut seen = HashSet::new();
    let mut current = Some(start);

    while let Some(node) = current {
        if node == NodeId::ROOT || node == NodeId::TRASH {
            return;
        }
        assert!(
            seen.insert(node),
            "cycle detected from {start:?} at {node:?}"
        );
        current = harness.node_state(node).parent;
    }
}

pub fn out_of_order_append_after_cycle_rejected_moves_keeps_canonical_tree_acyclic<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let replicas: Vec<_> = (1u8..=6).map(|byte| ReplicaId::new(vec![byte; 32])).collect();
    let parent = node(1);
    let child = node(2);
    let unrelated = node(3);

    let insert_parent = Operation::insert(
        &replicas[0],
        1,
        1,
        NodeId::ROOT,
        parent,
        order_key_from_position(0),
    );
    let insert_child = Operation::insert(
        &replicas[1],
        1,
        2,
        parent,
        child,
        order_key_from_position(1),
    );
    let rejected_move = Operation::move_node(
        &replicas[2],
        1,
        3,
        parent,
        child,
        order_key_from_position(2),
    );
    let mut known_state = VersionVector::new();
    known_state.observe(&replicas[0], 1);
    let delete_parent = Operation::delete(&replicas[3], 1, 4, parent, Some(known_state));
    let delayed_payload = Operation::set_payload(&replicas[4], 1, 5, unrelated, vec![5]);
    let later_rejected_move = Operation::move_node(
        &replicas[5],
        1,
        6,
        parent,
        child,
        order_key_from_position(5),
    );

    harness.append_ops(&[
        insert_parent,
        insert_child,
        rejected_move,
        delete_parent,
        later_rejected_move,
    ]);
    let outcome = harness.append_ops_with_materialization_outcome(&[delayed_payload]);
    assert_eq!(changed_nodes(&outcome), vec![unrelated]);

    assert_eq!(harness.op_count(), 6);
    assert_replay_cleared(harness);
    assert_eq!(
        harness.materialization_head(),
        MaterializationFrontier {
            lamport: 6,
            replica: replicas[5].as_bytes().to_vec(),
            counter: 1,
        }
    );
    assert_eq!(harness.head_seq(), 6);

    let parent_state = harness.node_state(parent);
    let child_state = harness.node_state(child);
    assert_eq!(parent_state.parent, Some(NodeId::ROOT));
    assert_eq!(child_state.parent, Some(parent));
    assert!(!parent_state.tombstone);
    assert!(!child_state.tombstone);
    assert_parent_chain_acyclic(harness, parent);
    assert_parent_chain_acyclic(harness, child);
}

pub fn out_of_order_concurrent_delete_converges_internal_node_metadata<
    H: MaterializationConformanceHarness,
>(
    canonical: &H,
    out_of_order: &H,
) {
    let replicas: Vec<_> = (1u8..=4).map(|byte| ReplicaId::new(vec![byte; 32])).collect();
    let parent = node(1);
    let child = node(2);
    let unrelated = node(3);

    let insert_parent = Operation::insert(
        &replicas[0],
        1,
        1,
        NodeId::ROOT,
        parent,
        order_key_from_position(0),
    );
    let insert_child = Operation::insert(
        &replicas[1],
        1,
        2,
        parent,
        child,
        order_key_from_position(1),
    );
    let mut known_state = VersionVector::new();
    known_state.observe(&replicas[0], 1);
    let concurrent_delete = Operation::delete(&replicas[2], 1, 3, parent, Some(known_state));
    let later_payload = Operation::set_payload(&replicas[3], 1, 4, unrelated, vec![4]);

    canonical.append_ops(&[
        insert_parent.clone(),
        insert_child.clone(),
        concurrent_delete.clone(),
        later_payload.clone(),
    ]);
    out_of_order.append_ops(&[insert_parent, insert_child, later_payload]);
    out_of_order.append_ops(&[concurrent_delete]);

    let canonical_state = canonical.node_state(parent);
    let out_of_order_state = out_of_order.node_state(parent);
    let deleted_at = canonical_state
        .deleted_at
        .as_ref()
        .expect("concurrent delete must be retained even when the node stays visible");
    assert_eq!(deleted_at.get(&replicas[0]), 1);
    assert_eq!(deleted_at.get(&replicas[2]), 1);
    assert!(!canonical_state.tombstone);
    assert_eq!(out_of_order_state, canonical_state);

    let expected_head = MaterializationFrontier {
        lamport: 4,
        replica: replicas[3].as_bytes().to_vec(),
        counter: 1,
    };
    assert_eq!(canonical.materialization_head(), expected_head);
    assert_eq!(out_of_order.materialization_head(), expected_head);
    assert_eq!(canonical.head_seq(), 4);
    assert_eq!(out_of_order.head_seq(), 4);
    assert_replay_cleared(canonical);
    assert_replay_cleared(out_of_order);
}
