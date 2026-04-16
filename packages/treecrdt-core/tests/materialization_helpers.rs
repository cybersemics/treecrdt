use treecrdt_core::{
    apply_incremental_ops_with_delta, apply_persisted_remote_ops_with_delta,
    catch_up_materialized_state, cmp_op_key, materialize_persisted_remote_ops_with_delta,
    LamportClock, MaterializationCursor, MaterializationFrontier, MaterializationHead,
    MaterializationKey, MaterializationState, MemoryNodeStore, MemoryPayloadStore, MemoryStorage,
    NodeId, NodeStore, NoopParentOpIndex, Operation, OperationId, ParentOpIndex, PayloadStore,
    PersistedRemoteStores, ReplicaId, Storage, TreeCrdt, VersionVector,
};

#[derive(Default)]
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

#[derive(Default)]
struct ScanAfterStorage {
    ops: Vec<Operation>,
}

impl Storage for ScanAfterStorage {
    fn apply(&mut self, op: Operation) -> treecrdt_core::Result<bool> {
        self.ops.push(op);
        Ok(true)
    }

    fn load_since(&self, lamport: u64) -> treecrdt_core::Result<Vec<Operation>> {
        Ok(self.ops.iter().filter(|op| op.meta.lamport > lamport).cloned().collect())
    }

    fn latest_lamport(&self) -> u64 {
        self.ops.iter().map(|op| op.meta.lamport).max().unwrap_or_default()
    }

    fn scan_after(
        &self,
        after: Option<(u64, &[u8], u64)>,
        visit: &mut dyn FnMut(Operation) -> treecrdt_core::Result<()>,
    ) -> treecrdt_core::Result<()> {
        let mut ops = self.ops.clone();
        ops.sort_by(treecrdt_core::cmp_ops);
        for op in ops {
            if let Some((lamport, replica, counter)) = after {
                if cmp_op_key(
                    op.meta.lamport,
                    op.meta.id.replica.as_bytes(),
                    op.meta.id.counter,
                    lamport,
                    replica,
                    counter,
                ) != std::cmp::Ordering::Greater
                {
                    continue;
                }
            }
            visit(op)?;
        }
        Ok(())
    }
}

#[test]
fn finalize_local_materialization_records_unique_hints_and_extras() {
    let mut crdt = TreeCrdt::new(
        ReplicaId::new(b"local"),
        MemoryStorage::default(),
        LamportClock::default(),
    )
    .unwrap();

    let parent = NodeId(10);
    let node = NodeId(11);
    crdt.local_insert_after(NodeId::ROOT, parent, None).unwrap();
    let op = crdt.local_insert_after(parent, node, None).unwrap();

    let extra_op_id = OperationId {
        replica: ReplicaId::new(b"extra"),
        counter: 7,
    };

    let mut index = RecordingIndex::default();
    crdt.finalize_local_materialization(
        &op,
        &mut index,
        42,
        &[parent, parent, NodeId::TRASH],
        &[
            (parent, extra_op_id.clone()),
            (NodeId::TRASH, extra_op_id.clone()),
        ],
    )
    .unwrap();

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
    assert_eq!(res.affected_nodes, vec![NodeId::ROOT, parent, child],);
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
                affected_nodes: vec![NodeId(2)],
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
    assert_eq!(result.affected_nodes, vec![NodeId(2)]);
    assert!(!result.frontier_recorded);
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
                affected_nodes: Vec::new(),
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
    assert_eq!(result.affected_nodes, Vec::<NodeId>::new());
    assert!(result.frontier_recorded);
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
                affected_nodes: vec![NodeId(1), NodeId(2)],
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
    assert!(result.affected_nodes.is_empty());
    assert!(result.frontier_recorded);
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
                affected_nodes: Vec::new(),
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
    assert!(result.affected_nodes.is_empty());
    assert!(result.frontier_recorded);
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
    assert!(result.affected_nodes.is_empty());
    assert!(result.frontier_recorded);
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
        result.affected_nodes,
        vec![NodeId::ROOT, NodeId(10), NodeId(11)]
    );
}

#[test]
fn catch_up_materialized_state_restores_checkpoint_and_replays_suffix() {
    let replica = ReplicaId::new(b"remote");
    let op1 = Operation::insert(&replica, 1, 1, NodeId::ROOT, NodeId(10), vec![0x10]);
    let op2 = Operation::insert(&replica, 2, 2, NodeId(10), NodeId(11), vec![0x20]);
    let op3 = Operation::insert(&replica, 3, 3, NodeId::ROOT, NodeId(12), vec![0x30]);

    let mut storage = ScanAfterStorage::default();
    storage.apply(op1.clone()).unwrap();
    storage.apply(op2.clone()).unwrap();
    storage.apply(op3.clone()).unwrap();

    let cursor = Cursor {
        head_lamport: 3,
        head_replica: replica.as_bytes().to_vec(),
        head_counter: 3,
        head_seq: 3,
        replay_lamport: Some(2),
        replay_replica: Some(replica.as_bytes().to_vec()),
        replay_counter: Some(2),
    };
    let checkpoint = MaterializationHead {
        at: MaterializationKey {
            lamport: 1,
            replica: replica.as_bytes().to_vec(),
            counter: 1,
        },
        seq: 1,
    };

    let mut restored = false;
    let mut flushed_children_root = Vec::new();
    let mut flushed_child_parent = None;
    let head = catch_up_materialized_state(
        storage,
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"adapter"),
            clock: LamportClock::default(),
            nodes: MemoryNodeStore::default(),
            payloads: MemoryPayloadStore::default(),
            index: RecordingIndex::default(),
        },
        &cursor,
        |frontier| {
            assert_eq!(
                frontier,
                &MaterializationFrontier {
                    lamport: 2,
                    replica: replica.as_bytes().to_vec(),
                    counter: 2,
                }
            );
            Ok(Some(checkpoint.clone()))
        },
        |checkpoint, nodes, payloads, index| {
            nodes.reset()?;
            payloads.reset()?;
            index.reset()?;
            if checkpoint.is_some() {
                restored = true;
                nodes.ensure_node(NodeId(10))?;
                nodes.attach(NodeId(10), NodeId::ROOT, vec![0x10])?;
                let mut vv = VersionVector::new();
                vv.observe(&replica, 1);
                nodes.merge_last_change(NodeId(10), &vv)?;
                nodes.merge_last_change(NodeId::ROOT, &vv)?;
                index.record(NodeId::ROOT, &op1.meta.id, 1)?;
            }
            Ok(())
        },
        |nodes| {
            flushed_children_root = nodes.children(NodeId::ROOT)?;
            flushed_child_parent = nodes.parent(NodeId(11))?;
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap()
    .expect("head after checkpoint catch-up");

    assert!(restored);
    assert_eq!(flushed_children_root, vec![NodeId(10), NodeId(12)]);
    assert_eq!(flushed_child_parent, Some(NodeId(10)));
    assert_eq!(head.at.counter, 3);
    assert_eq!(head.seq, 3);
}

#[test]
fn catch_up_materialized_state_is_noop_without_replay_frontier() {
    let load_called = std::cell::Cell::new(false);
    let restore_called = std::cell::Cell::new(false);
    let flush_nodes_called = std::cell::Cell::new(false);
    let flush_index_called = std::cell::Cell::new(false);
    let replica = ReplicaId::new(b"remote");
    let cursor = Cursor {
        head_lamport: 7,
        head_replica: replica.as_bytes().to_vec(),
        head_counter: 4,
        head_seq: 9,
        ..Cursor::default()
    };

    let head = catch_up_materialized_state(
        ScanAfterStorage::default(),
        PersistedRemoteStores {
            replica_id: ReplicaId::new(b"adapter"),
            clock: LamportClock::default(),
            nodes: MemoryNodeStore::default(),
            payloads: MemoryPayloadStore::default(),
            index: RecordingIndex::default(),
        },
        &cursor,
        |_| {
            load_called.set(true);
            Ok(None)
        },
        |_, _, _, _| {
            restore_called.set(true);
            Ok(())
        },
        |_| {
            flush_nodes_called.set(true);
            Ok(())
        },
        |_| {
            flush_index_called.set(true);
            Ok(())
        },
    )
    .unwrap()
    .expect("current head");

    assert!(!load_called.get());
    assert!(!restore_called.get());
    assert!(!flush_nodes_called.get());
    assert!(!flush_index_called.get());
    assert_eq!(
        head,
        MaterializationHead {
            at: MaterializationKey {
                lamport: 7,
                replica: replica.as_bytes().to_vec(),
                counter: 4,
            },
            seq: 9,
        }
    );
}
