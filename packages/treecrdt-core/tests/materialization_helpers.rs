use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;
use treecrdt_core::NodeStore;
use treecrdt_core::{
    apply_incremental_ops_with_delta, apply_persisted_remote_ops_with_delta,
    catch_up_materialized_state, materialize_persisted_remote_ops_with_delta,
    orchestrate_persisted_remote_append, try_direct_rewind_catch_up_materialized_state,
    try_shortcut_out_of_order_payload_noops, CatchUpResult, ExactPayloadStore,
    IncrementalApplyResult, Lamport, LamportClock, LocalFinalizePlan, LocalPlacement,
    MaterializationChange, MaterializationCursor, MaterializationHead, MaterializationKey,
    MaterializationOutcome, MaterializationState, MemoryNodeStore, MemoryPayloadStore,
    MemoryStorage, NodeId, NoopParentOpIndex, Operation, OperationId, ParentOpIndex, PayloadStore,
    PersistedRemoteStores, ReplicaId, Storage, TreeCrdt, TruncatingParentOpIndex, VersionVector,
};

#[derive(Clone, Default)]
struct RecordingIndex {
    records: Vec<(NodeId, OperationId, u64)>,
}

#[derive(Default)]
struct Cursor {
    head_lamport: u64,
    head_replica: Vec<u8>,
    head_counter: u64,
    head_seq: u64,
    replay_lamport: Option<u64>,
    replay_replica: Option<Vec<u8>>,
    replay_counter: Option<u64>,
}

impl MaterializationCursor for Cursor {
    fn state(&self) -> MaterializationState<&[u8]> {
        let head = if self.head_seq == 0
            && self.head_lamport == 0
            && self.head_replica.is_empty()
            && self.head_counter == 0
        {
            None
        } else {
            Some(MaterializationHead {
                at: MaterializationKey {
                    lamport: self.head_lamport,
                    replica: self.head_replica.as_slice(),
                    counter: self.head_counter,
                },
                seq: self.head_seq,
            })
        };
        let replay_from = match (
            self.replay_lamport,
            self.replay_replica.as_deref(),
            self.replay_counter,
        ) {
            (Some(lamport), Some(replica), Some(counter)) => Some(MaterializationKey {
                lamport,
                replica,
                counter,
            }),
            _ => None,
        };

        MaterializationState { head, replay_from }
    }
}

impl ParentOpIndex for RecordingIndex {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        self.records.clear();
        Ok(())
    }

    fn record(
        &mut self,
        parent: NodeId,
        op_id: &OperationId,
        seq: u64,
    ) -> treecrdt_core::Result<()> {
        self.records.push((parent, op_id.clone(), seq));
        Ok(())
    }
}

impl TruncatingParentOpIndex for RecordingIndex {
    fn truncate_from(&mut self, seq: u64) -> treecrdt_core::Result<()> {
        self.records.retain(|(_, _, existing_seq)| *existing_seq < seq);
        Ok(())
    }
}

struct CountingStorage {
    inner: MemoryStorage,
    scan_count: Rc<Cell<u64>>,
}

impl Storage for CountingStorage {
    fn apply(&mut self, op: Operation) -> treecrdt_core::Result<bool> {
        self.inner.apply(op)
    }

    fn load_since(&self, lamport: Lamport) -> treecrdt_core::Result<Vec<Operation>> {
        self.inner.load_since(lamport)
    }

    fn latest_lamport(&self) -> Lamport {
        self.inner.latest_lamport()
    }

    fn latest_counter(&self, replica: &ReplicaId) -> treecrdt_core::Result<u64> {
        self.inner.latest_counter(replica)
    }

    fn scan_since(
        &self,
        lamport: Lamport,
        visit: &mut dyn FnMut(Operation) -> treecrdt_core::Result<()>,
    ) -> treecrdt_core::Result<()> {
        self.scan_count.set(self.scan_count.get() + 1);
        self.inner.scan_since(lamport, visit)
    }
}

#[test]
fn finalize_local_records_unique_hints_and_extras() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(10);
    let node = NodeId(11);
    crdt.local_insert(NodeId::ROOT, parent, LocalPlacement::First, None).unwrap();
    let (op, _) = crdt.local_insert(parent, node, LocalPlacement::First, None).unwrap();

    let extra_op_id = OperationId {
        replica: ReplicaId::new(b"extra"),
        counter: 7,
    };

    let plan = LocalFinalizePlan {
        parent_hints: vec![parent, parent, NodeId::TRASH],
        extra_index_records: vec![
            (parent, extra_op_id.clone()),
            (NodeId::TRASH, extra_op_id.clone()),
        ],
        changes: Vec::new(),
    };

    let mut index = RecordingIndex::default();
    let seq = crdt.finalize_local(&op, &mut index, 41, &plan).unwrap();

    assert_eq!(seq, 42);
    assert_eq!(index.records.len(), 2);
    assert_eq!(index.records[0], (parent, op.meta.id.clone(), 42));
    assert_eq!(index.records[1], (parent, extra_op_id, 42));
}

#[test]
fn apply_incremental_ops_with_delta_sorts_and_returns_head() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");

    let first = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let second = Operation::insert(&replica, 2, 2, NodeId::ROOT, NodeId(2), vec![0x20]);

    let next =
        apply_incremental_ops_with_delta(&mut crdt, &mut index, &cursor, vec![second, first])
            .unwrap()
            .head
            .expect("expected materialization head");

    assert_eq!(next.at.lamport, 2);
    assert_eq!(next.at.replica, replica.as_bytes());
    assert_eq!(next.at.counter, 2);
    assert_eq!(next.seq, 2);
    assert_eq!(index.records.len(), 2);
    assert_eq!(index.records[0].2, 1);
    assert_eq!(index.records[1].2, 2);
}

#[test]
fn apply_incremental_ops_with_delta_keeps_head_for_duplicate_ops() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let replica = ReplicaId::new(b"remote");
    let op = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);

    let first_result = apply_incremental_ops_with_delta(
        &mut crdt,
        &mut index,
        &Cursor::default(),
        vec![op.clone()],
    )
    .unwrap();
    let first_head = first_result.head.expect("expected first head");

    let duplicate_cursor = Cursor {
        head_lamport: first_head.at.lamport,
        head_replica: first_head.at.replica.to_vec(),
        head_counter: first_head.at.counter,
        head_seq: first_head.seq,
        ..Cursor::default()
    };
    let duplicate_result =
        apply_incremental_ops_with_delta(&mut crdt, &mut index, &duplicate_cursor, vec![op])
            .unwrap();

    assert_eq!(
        duplicate_result.head.expect("expected duplicate head").seq,
        1
    );
    assert_eq!(duplicate_result.outcome.head_seq, 1);
    assert!(duplicate_result.outcome.changes.is_empty());
}

#[test]
fn apply_incremental_ops_with_delta_rejects_before_materialized_head() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let cursor = Cursor {
        head_lamport: 5,
        head_replica: b"r".to_vec(),
        head_counter: 3,
        head_seq: 12,
        ..Cursor::default()
    };

    let op = Operation::insert(
        &ReplicaId::new(b"r"),
        2,
        4,
        NodeId::ROOT,
        NodeId(9),
        vec![0x10],
    );

    let res = apply_incremental_ops_with_delta(&mut crdt, &mut index, &cursor, vec![op]);
    assert!(res.is_err());
}

#[test]
fn apply_incremental_ops_with_delta_rejects_pending_replay_frontier() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let cursor = Cursor {
        head_lamport: 5,
        head_replica: b"r".to_vec(),
        head_counter: 3,
        head_seq: 12,
        replay_lamport: Some(4),
        replay_replica: Some(b"r".to_vec()),
        replay_counter: Some(2),
    };
    let op = Operation::insert(
        &ReplicaId::new(b"r"),
        4,
        6,
        NodeId::ROOT,
        NodeId(9),
        vec![0x10],
    );

    let res = apply_incremental_ops_with_delta(&mut crdt, &mut index, &cursor, vec![op]);
    assert!(res.is_err());
}

#[test]
fn apply_incremental_ops_with_delta_returns_affected_union() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();
    let mut index = RecordingIndex::default();
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");

    let parent = NodeId(10);
    let child = NodeId(11);
    let parent_insert = Operation::insert(&replica, 1, 1, NodeId::ROOT, parent, vec![0x10]);
    let child_insert = Operation::insert(&replica, 2, 2, parent, child, vec![0x20]);

    let res = apply_incremental_ops_with_delta(
        &mut crdt,
        &mut index,
        &cursor,
        vec![child_insert, parent_insert],
    )
    .unwrap();

    let head = res.head.expect("expected materialization head");
    assert_eq!(head.at.counter, 2);
    assert_eq!(
        res.outcome.affected_nodes(),
        vec![NodeId::ROOT, parent, child],
    );
}

#[test]
fn apply_persisted_remote_ops_materializes_only_inserted_entries() {
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");
    let op2 = Operation::insert(&replica, 2, 2, NodeId::ROOT, NodeId(2), vec![0x20]);
    let op3 = Operation::set_payload(&replica, 3, 3, NodeId(2), vec![9]);

    let mut seen_counters = Vec::new();
    let mut updated_head = None;
    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![op2.clone(), op3.clone()],
        |ops| {
            seen_counters = ops.iter().map(|op| op.meta.id.counter).collect();
            Ok::<_, ()>(treecrdt_core::IncrementalApplyResult {
                head: Some(MaterializationHead {
                    at: MaterializationKey {
                        lamport: op3.meta.lamport,
                        replica: op3.meta.id.replica.as_bytes().to_vec(),
                        counter: op3.meta.id.counter,
                    },
                    seq: 2,
                }),
                outcome: MaterializationOutcome {
                    head_seq: 2,
                    changes: vec![MaterializationChange::Payload {
                        node: NodeId(2),
                        payload: Some(vec![9]),
                        source: None,
                    }],
                },
            })
        },
        |head| {
            updated_head = Some(head.clone());
            Ok::<_, ()>(())
        },
        |_| Ok::<_, ()>(()),
    )
    .unwrap();

    assert_eq!(seen_counters, vec![2, 3]);
    assert_eq!(result.inserted_count, 2);
    assert_eq!(result.outcome.affected_nodes(), vec![NodeId(2)]);
    assert!(!result.catch_up_needed);
    assert_eq!(
        updated_head,
        Some(MaterializationHead {
            at: MaterializationKey {
                lamport: 3,
                replica: replica.as_bytes().to_vec(),
                counter: 3,
            },
            seq: 2,
        })
    );
}

#[test]
fn apply_persisted_remote_ops_schedules_replay_from_start_when_head_is_missing() {
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");
    let op = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let mut runs = 0u64;
    let mut scheduled_replay = 0u64;

    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![op],
        |_| {
            runs += 1;
            Ok::<_, ()>(treecrdt_core::IncrementalApplyResult {
                head: None,
                outcome: MaterializationOutcome::empty(0),
            })
        },
        |_| Ok::<_, ()>(()),
        |_| {
            scheduled_replay += 1;
            Ok::<_, ()>(())
        },
    )
    .unwrap();

    assert_eq!(runs, 1);
    assert_eq!(scheduled_replay, 1);
    assert_eq!(result.inserted_count, 1);
    assert_eq!(result.outcome.affected_nodes(), Vec::<NodeId>::new());
    assert!(result.catch_up_needed);
}

#[test]
fn apply_persisted_remote_ops_schedules_full_replay_when_update_head_fails() {
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");
    let op = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let mut scheduled_replay = 0u64;

    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![op],
        |_| {
            Ok::<_, ()>(treecrdt_core::IncrementalApplyResult {
                head: Some(MaterializationHead {
                    at: MaterializationKey {
                        lamport: 7,
                        replica: b"r".to_vec(),
                        counter: 4,
                    },
                    seq: 9,
                }),
                outcome: MaterializationOutcome {
                    head_seq: 9,
                    changes: vec![
                        MaterializationChange::Payload {
                            node: NodeId(1),
                            payload: None,
                            source: None,
                        },
                        MaterializationChange::Payload {
                            node: NodeId(2),
                            payload: None,
                            source: None,
                        },
                    ],
                },
            })
        },
        |_| Err::<(), ()>(()),
        |_| {
            scheduled_replay += 1;
            Ok::<(), ()>(())
        },
    )
    .unwrap();

    assert_eq!(scheduled_replay, 1);
    assert_eq!(result.inserted_count, 1);
    assert!(result.outcome.changes.is_empty());
    assert!(result.catch_up_needed);
}

#[test]
fn apply_persisted_remote_ops_schedules_replay_frontier_for_out_of_order_ops() {
    let cursor = Cursor {
        head_lamport: 5,
        head_replica: b"r".to_vec(),
        head_counter: 5,
        head_seq: 5,
        ..Cursor::default()
    };
    let replica = ReplicaId::new(b"r");
    let out_of_order = Operation::insert(&replica, 2, 2, NodeId::ROOT, NodeId(2), vec![0x20]);
    let later = Operation::insert(&replica, 6, 6, NodeId::ROOT, NodeId(6), vec![0x60]);
    let mut materialize_runs = 0u64;
    let mut replay_frontier = None;

    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![later, out_of_order.clone()],
        |_| {
            materialize_runs += 1;
            Ok::<_, ()>(treecrdt_core::IncrementalApplyResult {
                head: None,
                outcome: MaterializationOutcome::empty(0),
            })
        },
        |_| Ok::<_, ()>(()),
        |frontier| {
            replay_frontier = Some(frontier.clone());
            Ok::<_, ()>(())
        },
    )
    .unwrap();

    assert_eq!(materialize_runs, 0);
    assert_eq!(result.inserted_count, 2);
    assert!(result.outcome.changes.is_empty());
    assert!(result.catch_up_needed);
    assert_eq!(
        replay_frontier,
        Some(treecrdt_core::MaterializationFrontier {
            lamport: out_of_order.meta.lamport,
            replica: out_of_order.meta.id.replica.as_bytes().to_vec(),
            counter: out_of_order.meta.id.counter,
        })
    );
}

#[test]
fn apply_persisted_remote_ops_keeps_earliest_existing_replay_frontier() {
    let cursor = Cursor {
        head_lamport: 5,
        head_replica: b"r".to_vec(),
        head_counter: 5,
        head_seq: 5,
        replay_lamport: Some(2),
        replay_replica: Some(b"r".to_vec()),
        replay_counter: Some(2),
    };
    let replica = ReplicaId::new(b"r");
    let later = Operation::insert(&replica, 4, 4, NodeId::ROOT, NodeId(4), vec![0x40]);
    let mut replay_frontier = None;

    let result = apply_persisted_remote_ops_with_delta(
        &cursor,
        vec![later],
        |_| unreachable!("pending replay frontier should bypass incremental materialization"),
        |_| Ok::<_, ()>(()),
        |frontier| {
            replay_frontier = Some(frontier.clone());
            Ok::<_, ()>(())
        },
    )
    .unwrap();

    assert_eq!(result.inserted_count, 1);
    assert!(result.outcome.changes.is_empty());
    assert!(result.catch_up_needed);
    assert_eq!(
        replay_frontier,
        Some(treecrdt_core::MaterializationFrontier {
            lamport: 2,
            replica: b"r".to_vec(),
            counter: 2,
        })
    );
}

#[test]
fn materialize_persisted_remote_ops_with_delta_runs_prepare_and_flush_hooks() {
    let cursor = Cursor::default();
    let replica = ReplicaId::new(b"remote");
    let parent = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(10), vec![0x10]);
    let child = Operation::insert(&replica, 2, 2, NodeId(10), NodeId(11), vec![0x20]);

    let mut prepared = 0u64;
    let mut flushed_nodes = 0u64;
    let mut flushed_index = 0u64;

    let result = materialize_persisted_remote_ops_with_delta(
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"adapter"),
            clock: LamportClock::default(),
            nodes: MemoryNodeStore::default(),
            payloads: MemoryPayloadStore::default(),
            index: NoopParentOpIndex,
        },
        &cursor,
        vec![child, parent],
        |_, ops| {
            prepared += ops.len() as u64;
            Ok(())
        },
        |_| {
            flushed_nodes += 1;
            Ok(())
        },
        |_| {
            flushed_index += 1;
            Ok(())
        },
    )
    .unwrap();

    let head = result.head.expect("expected head");
    assert_eq!(prepared, 2);
    assert_eq!(flushed_nodes, 1);
    assert_eq!(flushed_index, 1);
    assert_eq!(head.at.counter, 2);
    assert_eq!(
        result.outcome.affected_nodes(),
        vec![NodeId::ROOT, NodeId(10), NodeId(11)]
    );
}

#[test]
fn payload_noop_shortcut_skips_out_of_order_payload_dominated_by_current_winner() {
    let cursor = Cursor {
        head_lamport: 10,
        head_replica: b"r".to_vec(),
        head_counter: 10,
        head_seq: 5,
        ..Cursor::default()
    };
    let replica = ReplicaId::new(b"r");
    let node = NodeId(7);
    let op = Operation::set_payload(&replica, 4, 4, node, vec![1]);

    let shortcut = try_shortcut_out_of_order_payload_noops(&cursor, vec![op.clone()], |lookup| {
        assert_eq!(lookup, node);
        Ok::<_, ()>(Some((
            9,
            OperationId {
                replica: replica.clone(),
                counter: 9,
            },
        )))
    })
    .unwrap()
    .expect("expected payload noop shortcut");

    assert_eq!(shortcut.resumed_head.at.counter, 10);
    assert_eq!(shortcut.resumed_head.seq, 6);
    assert!(shortcut.remaining_ops.is_empty());
    assert!(shortcut.outcome.changes.is_empty());
}

#[test]
fn payload_noop_shortcut_keeps_later_in_order_payload_for_incremental_materialization() {
    let cursor = Cursor {
        head_lamport: 10,
        head_replica: b"r".to_vec(),
        head_counter: 10,
        head_seq: 5,
        ..Cursor::default()
    };
    let replica = ReplicaId::new(b"r");
    let node = NodeId(8);
    let older = Operation::set_payload(&replica, 4, 4, node, vec![1]);
    let newer = Operation::set_payload(&replica, 12, 12, node, vec![2]);

    let shortcut = try_shortcut_out_of_order_payload_noops(
        &cursor,
        vec![newer.clone(), older.clone()],
        |_| Ok::<_, ()>(None),
    )
    .unwrap()
    .expect("expected payload noop shortcut");

    assert_eq!(shortcut.resumed_head.seq, 6);
    assert_eq!(shortcut.remaining_ops, vec![newer]);
    assert!(shortcut.outcome.changes.is_empty());
}

#[test]
fn payload_noop_shortcut_rejects_out_of_order_payload_that_becomes_final_winner() {
    let cursor = Cursor {
        head_lamport: 10,
        head_replica: b"r".to_vec(),
        head_counter: 10,
        head_seq: 5,
        ..Cursor::default()
    };
    let replica = ReplicaId::new(b"r");
    let node = NodeId(9);
    let op = Operation::set_payload(&replica, 4, 4, node, vec![1]);

    let shortcut = try_shortcut_out_of_order_payload_noops(&cursor, vec![op], |_| {
        Ok::<_, ()>(Some((
            2,
            OperationId {
                replica: ReplicaId::new(b"old"),
                counter: 2,
            },
        )))
    })
    .unwrap();

    assert!(shortcut.is_none());
}

#[test]
fn payload_noop_shortcut_rejects_out_of_order_move() {
    let cursor = Cursor {
        head_lamport: 10,
        head_replica: b"r".to_vec(),
        head_counter: 10,
        head_seq: 5,
        ..Cursor::default()
    };
    let replica = ReplicaId::new(b"r");
    let move_op = Operation::move_node(&replica, 4, 4, NodeId(3), NodeId::ROOT, vec![0x10]);
    let called = Cell::new(false);

    let shortcut = try_shortcut_out_of_order_payload_noops(
        &cursor,
        vec![move_op],
        |_| -> Result<Option<(u64, OperationId)>, ()> {
            called.set(true);
            Ok(None)
        },
    )
    .unwrap();

    assert!(shortcut.is_none());
    assert!(!called.get());
}

#[test]
fn catch_up_materialized_state_scans_storage_once() {
    let replica = ReplicaId::new(b"scan-once");
    let first = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let second = Operation::insert(&replica, 2, 2, NodeId::ROOT, NodeId(2), vec![0x20]);

    let mut inner = MemoryStorage::default();
    inner.apply(first.clone()).unwrap();
    inner.apply(second.clone()).unwrap();
    let scan_count = Rc::new(Cell::new(0));
    let storage = CountingStorage {
        inner,
        scan_count: scan_count.clone(),
    };
    let meta = Cursor {
        replay_lamport: Some(first.meta.lamport),
        replay_replica: Some(first.meta.id.replica.as_bytes().to_vec()),
        replay_counter: Some(first.meta.id.counter),
        ..Cursor::default()
    };

    let result = catch_up_materialized_state(
        storage,
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"adapter"),
            clock: LamportClock::default(),
            nodes: MemoryNodeStore::default(),
            payloads: MemoryPayloadStore::default(),
            index: NoopParentOpIndex,
        },
        &meta,
        |_| Ok(()),
        |_| Ok(()),
    )
    .unwrap();

    assert_eq!(scan_count.get(), 1);
    assert_eq!(result.head.expect("expected head").seq, 2);
    assert_eq!(
        result.outcome.affected_nodes(),
        vec![NodeId::ROOT, NodeId(1), NodeId(2)]
    );
}

#[test]
fn catch_up_materialized_state_reports_only_invalidated_suffix_changes() {
    let replica = ReplicaId::new(b"suffix-only");
    let prefix = NodeId(1);
    let suffix = NodeId(2);
    let prefix_op = Operation::insert(&replica, 1, 1, NodeId::ROOT, prefix, vec![0x10]);
    let suffix_op = Operation::insert(&replica, 2, 2, NodeId::ROOT, suffix, vec![0x20]);

    let mut storage = MemoryStorage::default();
    storage.apply(prefix_op.clone()).unwrap();
    storage.apply(suffix_op.clone()).unwrap();

    // Simulate the backend state at the replay boundary: prefix rows are already materialized,
    // so conservative catch-up should not re-emit them as materialization changes.
    let mut nodes = MemoryNodeStore::default();
    nodes.ensure_node(prefix).unwrap();
    nodes.attach(prefix, NodeId::ROOT, vec![0x10]).unwrap();
    let mut index = RecordingIndex::default();
    index.record(NodeId::ROOT, &prefix_op.meta.id, 1).unwrap();

    let meta = Cursor {
        head_lamport: suffix_op.meta.lamport,
        head_replica: suffix_op.meta.id.replica.as_bytes().to_vec(),
        head_counter: suffix_op.meta.id.counter,
        head_seq: 2,
        replay_lamport: Some(suffix_op.meta.lamport),
        replay_replica: Some(suffix_op.meta.id.replica.as_bytes().to_vec()),
        replay_counter: Some(suffix_op.meta.id.counter),
    };

    let mut flushed_nodes = false;
    let result = catch_up_materialized_state(
        storage,
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"adapter"),
            clock: LamportClock::default(),
            nodes,
            payloads: MemoryPayloadStore::default(),
            index,
        },
        &meta,
        |nodes| {
            flushed_nodes = true;
            assert!(nodes.exists(prefix)?);
            assert!(nodes.exists(suffix)?);
            Ok(())
        },
        |index| {
            assert_eq!(
                index.records,
                vec![
                    (NodeId::ROOT, prefix_op.meta.id.clone(), 1),
                    (NodeId::ROOT, suffix_op.meta.id.clone(), 2)
                ]
            );
            Ok(())
        },
    )
    .unwrap();

    assert!(flushed_nodes);
    assert_eq!(result.head.expect("expected head").seq, 2);
    assert_eq!(result.outcome.affected_nodes(), vec![NodeId::ROOT, suffix]);
}

#[test]
fn catch_up_materialized_state_reports_rows_restored_by_replay_patch() {
    let author = ReplicaId::new(b"author");
    let deleter = ReplicaId::new(b"deleter");
    let parent = NodeId(10);
    let child = NodeId(11);

    let parent_op = Operation::insert(&author, 1, 1, NodeId::ROOT, parent, vec![0x10]);
    let child_op = Operation::insert(&author, 2, 2, parent, child, vec![0x20]);
    let mut known_state = VersionVector::new();
    known_state.observe(&author, 1);
    let delete_op = Operation::delete(&deleter, 1, 3, parent, Some(known_state.clone()));

    let mut storage = MemoryStorage::default();
    storage.apply(parent_op.clone()).unwrap();
    storage.apply(child_op.clone()).unwrap();
    storage.apply(delete_op.clone()).unwrap();

    let mut deleted_at = known_state;
    deleted_at.observe(&deleter, 1);

    // Simulate the stale materialized backend state before catch-up: the parent insert and later
    // delete were materialized, but the out-of-order child insert has not been replayed yet.
    let mut nodes = MemoryNodeStore::default();
    nodes.ensure_node(parent).unwrap();
    nodes.attach(parent, NodeId::ROOT, vec![0x10]).unwrap();
    nodes.merge_deleted_at(parent, &deleted_at).unwrap();
    nodes.set_tombstone(parent, true).unwrap();

    let meta = Cursor {
        head_lamport: delete_op.meta.lamport,
        head_replica: delete_op.meta.id.replica.as_bytes().to_vec(),
        head_counter: delete_op.meta.id.counter,
        head_seq: 2,
        replay_lamport: Some(child_op.meta.lamport),
        replay_replica: Some(child_op.meta.id.replica.as_bytes().to_vec()),
        replay_counter: Some(child_op.meta.id.counter),
    };

    let result = catch_up_materialized_state(
        storage,
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"adapter"),
            clock: LamportClock::default(),
            nodes,
            payloads: MemoryPayloadStore::default(),
            index: NoopParentOpIndex,
        },
        &meta,
        |_| Ok(()),
        |_| Ok(()),
    )
    .unwrap();

    assert!(
        result.outcome.changes.contains(&MaterializationChange::Restore {
            node: parent,
            parent_after: Some(NodeId::ROOT),
            payload: None,
            source: None,
        }),
        "catch-up must report rows restored by patching stale backend state"
    );
    assert_eq!(
        result.outcome.affected_nodes(),
        vec![NodeId::ROOT, parent, child],
    );
}

/// Payload writes go straight through, as in the SQL payload stores; core has no payload flush.
struct SharedPayloads<'a>(&'a RefCell<MemoryPayloadStore>);

impl PayloadStore for SharedPayloads<'_> {
    fn reset(&mut self) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().reset()
    }

    fn payload(&self, node: NodeId) -> treecrdt_core::Result<Option<Vec<u8>>> {
        self.0.borrow().payload(node)
    }

    fn last_writer(&self, node: NodeId) -> treecrdt_core::Result<Option<(Lamport, OperationId)>> {
        self.0.borrow().last_writer(node)
    }

    fn set_payload(
        &mut self,
        node: NodeId,
        payload: Option<Vec<u8>>,
        writer: (Lamport, OperationId),
    ) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().set_payload(node, payload, writer)
    }
}

impl ExactPayloadStore for SharedPayloads<'_> {
    fn clear_payload(&mut self, node: NodeId) -> treecrdt_core::Result<()> {
        self.0.borrow_mut().clear_payload(node)
    }
}

#[derive(Debug, PartialEq)]
struct MaterializedRow {
    node: NodeId,
    parent: Option<NodeId>,
    order_key: Option<Vec<u8>>,
    tombstone: bool,
    last_change: VersionVector,
    deleted_at: Option<VersionVector>,
    payload: Option<Vec<u8>>,
    payload_writer: Option<(Lamport, OperationId)>,
}

/// In-memory adapter: an op log plus materialized rows, appended through the same orchestration
/// as the SQLite and Postgres adapters.
struct MemoryBackend {
    log: RefCell<MemoryStorage>,
    nodes: RefCell<MemoryNodeStore>,
    payloads: RefCell<MemoryPayloadStore>,
    index: RefCell<RecordingIndex>,
    meta: RefCell<MaterializationState>,
}

impl MemoryBackend {
    fn new() -> Self {
        Self {
            log: RefCell::default(),
            nodes: RefCell::default(),
            payloads: RefCell::default(),
            index: RefCell::default(),
            meta: RefCell::new(MaterializationState {
                head: None,
                replay_from: None,
            }),
        }
    }

    fn replayed(ops: &[Operation]) -> Self {
        let backend = Self::new();
        backend.append(ops);
        backend
    }

    fn append(&self, ops: &[Operation]) {
        let inserted: Vec<_> = ops
            .iter()
            .filter(|op| self.log.borrow_mut().apply((*op).clone()).unwrap())
            .cloned()
            .collect();
        let stores = || PersistedRemoteStores {
            replica_id: ReplicaId::new(b"memory-backend"),
            clock: LamportClock::default(),
            nodes: self.nodes.borrow().clone(),
            payloads: SharedPayloads(&self.payloads),
            index: self.index.borrow().clone(),
        };
        let flush_nodes = |nodes: &mut MemoryNodeStore| {
            *self.nodes.borrow_mut() = nodes.clone();
            Ok(())
        };
        let flush_index = |index: &mut RecordingIndex| {
            *self.index.borrow_mut() = index.clone();
            Ok(())
        };
        let log_copy = || {
            let mut copy = MemoryStorage::default();
            for op in self.log.borrow().load_since(0).unwrap() {
                copy.apply(op).unwrap();
            }
            copy
        };
        let meta = self.meta.borrow().clone();
        orchestrate_persisted_remote_append(
            &meta,
            inserted,
            |node| self.payloads.borrow().last_writer(node),
            |meta, ops| {
                materialize_persisted_remote_ops_with_delta(
                    stores(),
                    &meta,
                    ops,
                    |_, _| Ok(()),
                    flush_nodes,
                    flush_index,
                )
            },
            |head| {
                *self.meta.borrow_mut() = MaterializationState {
                    head: Some(head.clone()),
                    replay_from: None,
                };
                Ok(())
            },
            |frontier| {
                self.meta.borrow_mut().replay_from = Some(frontier.clone());
                Ok(())
            },
            || Ok(self.meta.borrow().clone()),
            |meta, inserted_op_ids| {
                try_direct_rewind_catch_up_materialized_state(
                    &*self.log.borrow(),
                    inserted_op_ids,
                    stores(),
                    &meta,
                    flush_nodes,
                    flush_index,
                )
            },
            |meta| {
                catch_up_materialized_state(log_copy(), stores(), &meta, flush_nodes, flush_index)
            },
            |message| treecrdt_core::Error::Storage(message.into()),
        )
        .unwrap();
    }

    fn rows(&self) -> Vec<MaterializedRow> {
        let nodes = self.nodes.borrow();
        let payloads = self.payloads.borrow();
        let mut ids = nodes.all_nodes().unwrap();
        ids.sort();
        ids.into_iter()
            .map(|node| MaterializedRow {
                node,
                parent: nodes.parent(node).unwrap(),
                order_key: nodes.order_key(node).unwrap(),
                tombstone: nodes.tombstone(node).unwrap(),
                last_change: nodes.last_change(node).unwrap(),
                deleted_at: nodes.deleted_at(node).unwrap(),
                payload: payloads.payload(node).unwrap(),
                payload_writer: payloads.last_writer(node).unwrap(),
            })
            .collect()
    }

    fn assert_matches_replay_of(&self, ops: &[Operation]) {
        let canonical = Self::replayed(ops);
        assert_eq!(self.rows(), canonical.rows());
        assert_eq!(*self.meta.borrow(), *canonical.meta.borrow());
    }
}

/// A native loop blocks the test thread, so run append scenarios on a worker with a deadline.
fn finishes_within_deadline(scenario: impl FnOnce() + Send + 'static) {
    const DEADLINE: Duration = Duration::from_secs(30);
    let (done, finished) = mpsc::channel();
    let worker = thread::spawn(move || {
        scenario();
        let _ = done.send(());
    });
    assert!(
        !matches!(
            finished.recv_timeout(DEADLINE),
            Err(RecvTimeoutError::Timeout)
        ),
        "append did not finish within {DEADLINE:?}"
    );
    if let Err(panic) = worker.join() {
        std::panic::resume_unwind(panic);
    }
}

fn assert_late_append_matches_replay(history: Vec<Operation>, late: Operation) {
    finishes_within_deadline(move || {
        let backend = MemoryBackend::replayed(&history);
        backend.append(std::slice::from_ref(&late));
        backend.assert_matches_replay_of(&[history, vec![late]].concat());
    });
}

#[test]
fn late_append_after_rejected_self_move_matches_replay() {
    let local = ReplicaId::new(b"b");
    let remote = ReplicaId::new(b"a");
    let (node, child, late_node) = (NodeId(1), NodeId(2), NodeId(3));
    let history = vec![
        Operation::insert(&local, 1, 1, NodeId::ROOT, node, vec![0x10]),
        // Rejected: a node cannot be its own parent.
        Operation::move_node(&local, 2, 2, node, node, vec![0x10]),
        Operation::insert(&local, 3, 3, node, child, vec![0x10]),
        Operation::move_node(&local, 4, 4, node, NodeId::ROOT, vec![0x20]),
    ];
    // Sorts before the child insert, so the later ops on `node` and `child` replay after it.
    let late = Operation::insert(&remote, 1, 3, NodeId::ROOT, late_node, vec![0x30]);
    assert_late_append_matches_replay(history, late);
}

#[test]
fn late_append_after_cycle_rejected_move_matches_replay() {
    let local = ReplicaId::new(b"b");
    let concurrent = ReplicaId::new(b"c");
    let remote = ReplicaId::new(b"a");
    let (a, b, late_node) = (NodeId(1), NodeId(2), NodeId(3));
    let history = vec![
        Operation::insert(&local, 1, 1, NodeId::ROOT, a, vec![0x10]),
        Operation::insert(&local, 2, 2, NodeId::ROOT, b, vec![0x20]),
        Operation::move_node(&local, 3, 3, a, b, vec![0x10]),
        // Sorts after A -> B, so B -> A would close a cycle and is rejected.
        Operation::move_node(&concurrent, 1, 3, b, a, vec![0x10]),
        Operation::move_node(&local, 4, 4, b, NodeId::ROOT, vec![0x30]),
    ];
    let late = Operation::insert(&remote, 1, 4, a, late_node, vec![0x10]);
    assert_late_append_matches_replay(history, late);
}

#[test]
fn late_append_after_rejected_move_whose_cycle_was_undone_matches_replay() {
    let local = ReplicaId::new(b"b");
    let remote = ReplicaId::new(b"a");
    let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));
    let history = vec![
        Operation::insert(&local, 1, 1, NodeId::ROOT, a, vec![0x10]),
        Operation::insert(&local, 2, 2, a, b, vec![0x10]),
        Operation::insert(&local, 3, 3, NodeId::ROOT, c, vec![0x20]),
        // Rejected: B is A's child.
        Operation::move_node(&local, 4, 4, a, b, vec![0x10]),
        // Once B is back under the root, the rejected move no longer looks like a cycle.
        Operation::move_node(&local, 5, 5, b, NodeId::ROOT, vec![0x30]),
        Operation::move_node(&local, 7, 7, a, c, vec![0x10]),
    ];
    // Places C under A, which turns the later A -> C move into a rejected cycle.
    let late = Operation::move_node(&remote, 1, 6, c, a, vec![0x10]);
    assert_late_append_matches_replay(history, late);
}

#[test]
fn late_append_through_an_intermediate_parent_matches_replay() {
    let local = ReplicaId::new(b"b");
    let remote = ReplicaId::new(b"a");
    let (p, q, t, x) = (NodeId(1), NodeId(2), NodeId(3), NodeId(4));
    let history = vec![
        Operation::insert(&local, 1, 1, NodeId::ROOT, p, vec![0x10]),
        Operation::insert(&local, 2, 2, NodeId::ROOT, q, vec![0x20]),
        Operation::insert(&local, 3, 3, NodeId::ROOT, t, vec![0x30]),
        Operation::insert(&local, 4, 4, p, x, vec![0x10]),
        Operation::move_node(&local, 6, 6, x, t, vec![0x10]),
    ];
    // Replays as X: P -> Q -> T, which coalesces to P -> T but still changes Q's row.
    let late = Operation::move_node(&remote, 1, 5, x, q, vec![0x10]);
    assert_late_append_matches_replay(history, late);
}

#[test]
fn orchestrated_append_skips_catch_up_after_direct_rewind() {
    let replica = ReplicaId::new(b"lazy");
    let late = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(1), vec![0x10]);
    let head = |seq| MaterializationHead {
        at: MaterializationKey {
            lamport: 2,
            replica: replica.as_bytes().to_vec(),
            counter: 2,
        },
        seq,
    };
    let rewound = CatchUpResult {
        head: Some(head(2)),
        outcome: MaterializationOutcome::empty(2),
    };

    let result = orchestrate_persisted_remote_append(
        &MaterializationState {
            head: Some(head(1)),
            replay_from: None,
        },
        vec![late.clone()],
        |_| Ok(None),
        |_, _| -> Result<IncrementalApplyResult, &str> { Err("late op cannot apply in order") },
        |_| Ok(()),
        |_| Ok(()),
        || {
            Ok(MaterializationState {
                head: Some(head(1)),
                replay_from: Some(MaterializationKey {
                    lamport: late.meta.lamport,
                    replica: replica.as_bytes().to_vec(),
                    counter: late.meta.id.counter,
                }),
            })
        },
        |_, inserted_op_ids: &HashSet<OperationId>| {
            assert!(inserted_op_ids.contains(&late.meta.id));
            Ok(Some(rewound.clone()))
        },
        |_| Err("catch-up must not run after a successful direct rewind"),
        |message| message,
    )
    .unwrap();

    assert_eq!(result.outcome, rewound.outcome);
}

struct XorShift(u64);

impl XorShift {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

/// Replicas edit concurrently, including self-moves and cycle-closing moves, and sync in shuffled
/// batches. After every sync, the receiving backend must match a canonical replay of its ops.
#[test]
fn random_out_of_order_appends_match_replay() {
    finishes_within_deadline(|| {
        for seed in 1..=64 {
            random_sync_walk(seed, 40);
        }
    });
}

fn random_sync_walk(seed: u64, steps: usize) {
    let mut rng = XorShift(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let replicas = [b"a", b"b", b"c"].map(ReplicaId::new);
    let mut engines = replicas.clone().map(|replica| {
        TreeCrdt::new(replica, MemoryStorage::default(), LamportClock::default()).unwrap()
    });
    let backends = replicas.clone().map(|_| MemoryBackend::new());
    let mut logs: [Vec<Operation>; 3] = Default::default();
    let mut next_node = 1;

    for _ in 0..steps {
        let at = rng.below(replicas.len());
        let live: Vec<_> = engines[at].nodes().unwrap().into_iter().map(|(node, _)| node).collect();
        let parents: Vec<_> = std::iter::once(NodeId::ROOT).chain(live.iter().copied()).collect();
        let byte = rng.below(4) as u8;
        let engine = &mut engines[at];
        let op = match rng.below(9) {
            0 | 1 => {
                next_node += 1;
                let payload = (byte > 1).then(|| vec![byte]);
                let parent = rng.pick(&parents);
                engine.local_insert(parent, NodeId(next_node), LocalPlacement::Last, payload)
            }
            2 | 3 => {
                let to = (at + 1 + rng.below(replicas.len() - 1)) % replicas.len();
                let known: HashSet<_> = logs[to].iter().map(|op| op.meta.id.clone()).collect();
                let mut missing: Vec<_> =
                    logs[at].iter().filter(|op| !known.contains(&op.meta.id)).cloned().collect();
                for i in (1..missing.len()).rev() {
                    missing.swap(i, rng.below(i + 1));
                }
                let split = rng.below(missing.len() + 1);
                for batch in [&missing[..split], &missing[split..]] {
                    engines[to].apply_remote_batch(batch.to_vec()).unwrap();
                    backends[to].append(batch);
                    logs[to].extend_from_slice(batch);
                    backends[to].assert_matches_replay_of(&logs[to]);
                }
                continue;
            }
            _ if live.is_empty() => continue,
            // The new parent may be the node itself or one of its descendants.
            4 | 5 => engine.local_move(rng.pick(&live), rng.pick(&parents), LocalPlacement::First),
            6 | 7 => engine.local_payload(rng.pick(&live), (byte > 0).then(|| vec![byte])),
            _ => engine.local_delete(rng.pick(&live)),
        };
        let (op, _) = op.unwrap();
        backends[at].append(std::slice::from_ref(&op));
        logs[at].push(op);
    }
}
