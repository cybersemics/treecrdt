use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use treecrdt_core::{
    Lamport, LamportClock, NodeId, Operation, OperationId, ReplicaId, Result, Storage, TreeCrdt,
};

#[derive(Clone, Default)]
struct SharedMemoryStorage {
    ops: Arc<Mutex<Vec<Operation>>>,
    ids: Arc<Mutex<HashSet<OperationId>>>,
}

impl Storage for SharedMemoryStorage {
    fn apply(&mut self, op: Operation) -> Result<bool> {
        let mut ids = self.ids.lock().expect("lock ids");
        if ids.contains(&op.meta.id) {
            return Ok(false);
        }
        ids.insert(op.meta.id.clone());
        drop(ids);

        let mut ops = self.ops.lock().expect("lock ops");
        ops.push(op);
        Ok(true)
    }

    fn load_since(&self, lamport: Lamport) -> Result<Vec<Operation>> {
        let ops = self.ops.lock().expect("lock ops");
        Ok(ops.iter().filter(|op| op.meta.lamport > lamport).cloned().collect())
    }

    fn latest_lamport(&self) -> Lamport {
        let ops = self.ops.lock().expect("lock ops");
        ops.iter().map(|op| op.meta.lamport).max().unwrap_or_default()
    }

    fn latest_counter(&self, replica: &ReplicaId) -> Result<u64> {
        let ops = self.ops.lock().expect("lock ops");
        Ok(ops
            .iter()
            .filter(|op| &op.meta.id.replica == replica)
            .map(|op| op.meta.id.counter)
            .max()
            .unwrap_or(0))
    }
}

#[test]
fn local_meta_survives_restart() {
    let storage = SharedMemoryStorage::default();
    let replica = ReplicaId::new(b"a");

    let mut a = TreeCrdt::new(replica.clone(), storage.clone(), LamportClock::default()).unwrap();
    let op1 = a.local_insert_after(NodeId::ROOT, NodeId(1), None).unwrap();
    let op2 = a.local_insert_after(NodeId::ROOT, NodeId(2), Some(NodeId(1))).unwrap();
    assert_eq!(op2.meta.id.counter, op1.meta.id.counter + 1);
    assert_eq!(op2.meta.lamport, op1.meta.lamport + 1);

    drop(a);

    let mut b = TreeCrdt::new(replica.clone(), storage.clone(), LamportClock::default()).unwrap();
    b.replay_from_storage().unwrap();

    let op3 = b.local_insert_after(NodeId::ROOT, NodeId(3), Some(NodeId(2))).unwrap();
    assert_eq!(op3.meta.id.counter, op2.meta.id.counter + 1);
    assert_eq!(op3.meta.lamport, op2.meta.lamport + 1);
}
