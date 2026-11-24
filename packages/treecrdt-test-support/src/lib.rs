use std::slice;

use treecrdt_core::{
    MaterializationChange, MaterializationFrontier, MaterializationOutcome, MaterializationSource,
    NodeId, Operation, ReplicaId,
};

pub trait MaterializationConformanceHarness {
    fn append_ops(&self, ops: &[Operation]);
    fn append_ops_with_materialization_outcome(&self, ops: &[Operation]) -> MaterializationOutcome;
    fn visible_children(&self, parent: NodeId) -> Vec<NodeId>;
    fn payload(&self, node: NodeId) -> Option<Vec<u8>>;
    fn op_count(&self) -> u64;
    fn replay_frontier(&self) -> Option<MaterializationFrontier>;
    fn head_seq(&self) -> u64;
    fn force_replay_from_start(&self);
    fn ensure_materialized(&self);
    fn op_ref_counters_for_parent(&self, parent: NodeId) -> Vec<u64>;
    fn op_kinds_for_parent(&self, parent: NodeId) -> Vec<String>;
    /// Appends the ops `source` lists for `children(parent)`, as a peer that syncs only that
    /// filter receives them.
    fn pull_children(&self, source: &Self, parent: NodeId);
}

fn changed_nodes(outcome: &MaterializationOutcome) -> Vec<NodeId> {
    outcome.affected_nodes()
}

/// Counters of the ops listed for `children(parent)`, in counter order: ops recorded by one apply
/// share a seq, so the listing does not order them.
fn sorted_op_ref_counters<H: MaterializationConformanceHarness>(
    harness: &H,
    parent: NodeId,
) -> Vec<u64> {
    let mut counters = harness.op_ref_counters_for_parent(parent);
    counters.sort_unstable();
    counters
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
    // Replay inserts node 2 again, but it was already shown where it stays.
    assert_eq!(changed_nodes(&outcome), vec![NodeId::ROOT, node(1)]);
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
    let outcome = harness.append_ops_with_materialization_outcome(slice::from_ref(&losing_payload));
    assert!(outcome.changes.is_empty());
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

pub fn replayed_conflicting_moves_report_final_parents<H: MaterializationConformanceHarness>(
    harness: &H,
    full_replay: bool,
) {
    let replica = ReplicaId::new(b"conflicting-moves");
    let a = node(1);
    let b = node(2);
    harness.append_ops(&[
        Operation::insert(&replica, 1, 1, NodeId::ROOT, a, order_key_from_position(0)),
        Operation::insert(&replica, 2, 2, NodeId::ROOT, b, order_key_from_position(1)),
        Operation::move_node(&replica, 4, 4, b, a, order_key_from_position(0)),
    ]);
    if full_replay {
        let c = node(3);
        let mut known_state = treecrdt_core::VersionVector::new();
        known_state.observe(&replica, 5);
        harness.append_ops(&[
            Operation::insert(&replica, 5, 5, NodeId::ROOT, c, order_key_from_position(2)),
            Operation::delete(&replica, 6, 6, c, Some(known_state)),
        ]);
    }

    // Canonical replay accepts the earlier A -> B move and rejects B -> A as a cycle.
    let late_move = Operation::move_node(&replica, 3, 3, a, b, order_key_from_position(0));
    let outcome = harness.append_ops_with_materialization_outcome(&[late_move]);
    let moves: Vec<_> = outcome
        .changes
        .iter()
        .filter_map(|change| match change {
            MaterializationChange::Move {
                node,
                parent_before,
                parent_after,
                source,
            } => Some((*node, *parent_before, *parent_after, source.is_none())),
            _ => None,
        })
        .collect();
    assert_eq!(
        moves,
        vec![
            (a, Some(NodeId::ROOT), b, false),
            (b, Some(a), NodeId::ROOT, true)
        ]
    );
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![b]);
    assert_eq!(harness.visible_children(b), vec![a]);
}

pub fn replayed_delete_is_reported_once<H: MaterializationConformanceHarness>(harness: &H) {
    let replica = ReplicaId::new(b"delete-events");
    let a = node(1);
    let b = node(2);
    let mut known_state = treecrdt_core::VersionVector::new();
    known_state.observe(&replica, 1);
    harness.append_ops(&[
        Operation::insert(&replica, 1, 1, NodeId::ROOT, a, order_key_from_position(0)),
        Operation::insert(&replica, 2, 2, NodeId::ROOT, b, order_key_from_position(1)),
        Operation::set_payload(&replica, 4, 4, b, vec![9]),
    ]);

    // A delete in the replay suffix requires the full-replay fallback.
    let late_delete = Operation::delete(&replica, 3, 3, a, Some(known_state));
    let outcome = harness.append_ops_with_materialization_outcome(&[late_delete]);
    let deletions: Vec<_> = outcome
        .changes
        .iter()
        .filter_map(|change| match change {
            MaterializationChange::Delete {
                node,
                parent_before,
                ..
            } => Some((*node, *parent_before)),
            _ => None,
        })
        .collect();
    assert_eq!(deletions, vec![(a, Some(NodeId::ROOT))]);
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![b]);
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

pub fn superseded_payload_gap_does_not_restore_after_replay<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let structure_replica = ReplicaId::new(b"structure");
    let payload_replica = ReplicaId::new(b"payload");
    let delete_replica = ReplicaId::new(b"delete");
    let parent = node(1);

    let insert_parent = Operation::insert(
        &structure_replica,
        1,
        1,
        NodeId::ROOT,
        parent,
        order_key_from_position(0),
    );
    let superseded_payload =
        Operation::set_payload(&payload_replica, 1, 2, parent, b"old".to_vec());
    let winning_payload = Operation::clear_payload(&payload_replica, 2, 3, parent);

    let mut known_state = treecrdt_core::VersionVector::new();
    known_state.observe(&structure_replica, 1);
    known_state.observe(&payload_replica, 2);
    let delete_parent = Operation::delete(&delete_replica, 1, 4, parent, Some(known_state));

    // The delete sees the current payload writer but has a receipt gap for its predecessor.
    harness.append_ops(&[insert_parent, winning_payload, delete_parent]);
    assert_eq!(harness.visible_children(NodeId::ROOT), Vec::<NodeId>::new());

    // The late predecessor is an LWW no-op. Incremental materialization and a canonical replay
    // must agree that it cannot restore the node.
    harness.append_ops(&[superseded_payload]);
    assert_eq!(harness.visible_children(NodeId::ROOT), Vec::<NodeId>::new());
    harness.force_replay_from_start();
    harness.ensure_materialized();

    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 4);
    assert_eq!(harness.visible_children(NodeId::ROOT), Vec::<NodeId>::new());
    assert_eq!(harness.payload(parent), None);
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

pub fn late_op_after_rejected_self_move_keeps_accepted_parent<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let local = ReplicaId::new(b"b");
    let remote = ReplicaId::new(b"a");
    let parent = node(1);
    let child = node(2);
    let late_node = node(3);
    harness.append_ops(&[
        Operation::insert(
            &local,
            1,
            1,
            NodeId::ROOT,
            parent,
            order_key_from_position(0),
        ),
        Operation::move_node(&local, 2, 2, parent, parent, order_key_from_position(0)),
        Operation::insert(&local, 3, 3, parent, child, order_key_from_position(0)),
        Operation::move_node(
            &local,
            4,
            4,
            parent,
            NodeId::ROOT,
            order_key_from_position(1),
        ),
    ]);

    // Sorts before the child insert, so the append replays the later ops on `parent` and `child`.
    harness.append_ops(&[Operation::insert(
        &remote,
        1,
        3,
        NodeId::ROOT,
        late_node,
        order_key_from_position(2),
    )]);

    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 5);
    assert_eq!(
        harness.visible_children(NodeId::ROOT),
        vec![parent, late_node]
    );
    assert_eq!(harness.visible_children(parent), vec![child]);
}

pub fn late_op_after_cycle_rejected_move_keeps_accepted_parent<
    H: MaterializationConformanceHarness,
>(
    harness: &H,
) {
    let local = ReplicaId::new(b"b");
    let concurrent = ReplicaId::new(b"c");
    let remote = ReplicaId::new(b"a");
    let a = node(1);
    let b = node(2);
    let late_node = node(3);
    harness.append_ops(&[
        Operation::insert(&local, 1, 1, NodeId::ROOT, a, order_key_from_position(0)),
        Operation::insert(&local, 2, 2, NodeId::ROOT, b, order_key_from_position(1)),
        Operation::move_node(&local, 3, 3, a, b, order_key_from_position(0)),
        // Sorts after A -> B, so B -> A would close a cycle and is rejected.
        Operation::move_node(&concurrent, 1, 3, b, a, order_key_from_position(0)),
        Operation::move_node(&local, 4, 4, b, NodeId::ROOT, order_key_from_position(2)),
    ]);

    harness.append_ops(&[Operation::insert(
        &remote,
        1,
        4,
        a,
        late_node,
        order_key_from_position(0),
    )]);

    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 6);
    assert_eq!(harness.visible_children(NodeId::ROOT), vec![b]);
    assert_eq!(harness.visible_children(b), vec![a]);
    assert_eq!(harness.visible_children(a), vec![late_node]);
}

/// A create-only insert is a no-op wherever its node is already placed. The backend shows one
/// create, then an earlier one arrives: catch-up replays the stored ops, the shown create among
/// them, so it reports a move to the earlier create's parent only if storage kept the flag. The
/// voided create stays listed under its parent, now with the create that placed the node. A
/// replay from the start agrees.
pub fn late_create_only_insert_voids_the_shown_one<H: MaterializationConformanceHarness>(
    harness: &H,
) {
    let setup = ReplicaId::new(b"setup");
    let (p1, p2, n) = (node(1), node(2), node(10));
    harness.append_ops(&[
        Operation::insert(&setup, 1, 1, NodeId::ROOT, p1, order_key_from_position(0)),
        Operation::insert(&setup, 2, 2, NodeId::ROOT, p2, order_key_from_position(1)),
    ]);
    let shown = Operation::insert_if_absent(
        &ReplicaId::new(b"b"),
        7,
        11,
        p2,
        n,
        order_key_from_position(0),
        Some(vec![2]),
    );
    let late = Operation::insert_if_absent(
        &ReplicaId::new(b"a"),
        3,
        10,
        p1,
        n,
        order_key_from_position(0),
        Some(vec![1]),
    );
    harness.append_ops(slice::from_ref(&shown));
    assert_eq!(harness.visible_children(p2), vec![n]);
    assert_eq!(harness.op_ref_counters_for_parent(p2), vec![7]);

    let outcome = harness.append_ops_with_materialization_outcome(slice::from_ref(&late));
    assert_eq!(
        outcome.changes,
        vec![
            MaterializationChange::Move {
                node: n,
                parent_before: Some(p2),
                parent_after: p1,
                source: Some(MaterializationSource::from_op(&late)),
            },
            MaterializationChange::Payload {
                node: n,
                payload: Some(vec![1]),
                source: None,
            },
        ]
    );
    assert_replay_cleared(harness);
    assert_eq!(harness.head_seq(), 4);
    let assert_late_create_won = || {
        assert_eq!(harness.visible_children(p1), vec![n]);
        assert!(harness.visible_children(p2).is_empty());
        assert_eq!(harness.payload(n), Some(vec![1]));
        assert_eq!(harness.op_ref_counters_for_parent(p1), vec![3]);
        assert_eq!(sorted_op_ref_counters(harness, p2), vec![3, 7]);
    };
    assert_late_create_won();

    harness.force_replay_from_start();
    harness.ensure_materialized();
    assert_replay_cleared(harness);
    assert_late_create_won();
}

/// A peer that syncs only `children(P)` holds a create-only insert of a node under P. An earlier
/// insert of the node under Q then reaches the server and voids the create there. The server lists
/// that insert under P with the create, so the peer replays it first and drops the node from P too.
pub fn scoped_peer_learns_its_create_only_insert_was_voided<
    H: MaterializationConformanceHarness,
>(
    server: &H,
    peer: &H,
) {
    let setup = ReplicaId::new(b"setup");
    let (p, q, n) = (node(1), node(2), node(10));
    server.append_ops(&[
        Operation::insert(&setup, 1, 1, NodeId::ROOT, p, order_key_from_position(0)),
        Operation::insert(&setup, 2, 2, NodeId::ROOT, q, order_key_from_position(1)),
    ]);
    let create = Operation::insert_if_absent(
        &ReplicaId::new(b"b"),
        7,
        11,
        p,
        n,
        order_key_from_position(0),
        None,
    );
    server.append_ops(slice::from_ref(&create));
    let sync = || {
        peer.pull_children(server, NodeId::ROOT);
        peer.pull_children(server, p);
    };
    sync();
    assert_eq!(peer.visible_children(p), vec![n]);

    let placed = Operation::insert(
        &ReplicaId::new(b"a"),
        3,
        10,
        q,
        n,
        order_key_from_position(0),
    );
    server.append_ops(slice::from_ref(&placed));
    assert!(server.visible_children(p).is_empty());
    assert_eq!(sorted_op_ref_counters(server, p), vec![3, 7]);

    sync();
    assert!(peer.visible_children(p).is_empty());
    assert_eq!(sorted_op_ref_counters(peer, p), vec![3, 7]);
}

/// A peer that syncs only `children(P)` never sees a node under Q, so it creates the node under P.
/// Its create-only insert sorts after the insert under Q and changes nothing on the server, which
/// lists that insert under P with the create, so the peer drops the node from P once it syncs.
pub fn scoped_peer_learns_its_own_create_only_insert_changed_nothing<
    H: MaterializationConformanceHarness,
>(
    server: &H,
    peer: &H,
) {
    let setup = ReplicaId::new(b"setup");
    let (p, q, n) = (node(1), node(2), node(10));
    server.append_ops(&[
        Operation::insert(&setup, 1, 1, NodeId::ROOT, p, order_key_from_position(0)),
        Operation::insert(&setup, 2, 2, NodeId::ROOT, q, order_key_from_position(1)),
        Operation::insert(
            &ReplicaId::new(b"a"),
            3,
            3,
            q,
            n,
            order_key_from_position(0),
        ),
    ]);
    let sync = || {
        peer.pull_children(server, NodeId::ROOT);
        peer.pull_children(server, p);
    };
    sync();

    let create = Operation::insert_if_absent(
        &ReplicaId::new(b"peer"),
        7,
        21,
        p,
        n,
        order_key_from_position(0),
        None,
    );
    peer.append_ops(slice::from_ref(&create));
    assert_eq!(peer.visible_children(p), vec![n]);
    server.append_ops(slice::from_ref(&create));
    assert!(server.visible_children(p).is_empty());
    assert_eq!(sorted_op_ref_counters(server, p), vec![3, 7]);

    sync();
    assert!(peer.visible_children(p).is_empty());
    assert_eq!(sorted_op_ref_counters(peer, p), vec![3, 7]);
}
