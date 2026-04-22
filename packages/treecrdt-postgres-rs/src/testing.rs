use std::cell::RefCell;
use std::rc::Rc;

use postgres::Client;

use treecrdt_core::{Error, NodeId, Operation, ReplicaId, Result};

use crate::schema::reset_doc_for_tests;
use crate::store::append_ops;

fn balanced_parent_and_position(node_index: usize, fanout: usize) -> (NodeId, u16) {
    if node_index <= fanout {
        return (NodeId::ROOT, ((node_index - 1) % fanout) as u16);
    }
    (
        NodeId((((node_index - (fanout + 1)) / fanout) + 1) as u128),
        ((node_index - 1) % fanout) as u16,
    )
}

fn order_key_from_position(position: u16) -> Vec<u8> {
    position.wrapping_add(1).to_be_bytes().to_vec()
}

pub fn prime_doc_for_tests(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    ops: &[Operation],
) -> Result<()> {
    {
        let mut c = client.borrow_mut();
        reset_doc_for_tests(&mut c, doc_id)?;
    }

    if ops.is_empty() {
        return Ok(());
    }

    append_ops(client, doc_id, ops)?;
    Ok(())
}

pub fn prime_balanced_fanout_doc_for_tests(
    client: &Rc<RefCell<Client>>,
    doc_id: &str,
    size: usize,
    fanout: usize,
    payload_bytes: usize,
    replica_label: &str,
) -> Result<()> {
    if size == 0 {
        return Err(Error::Storage("fixture size must be positive".into()));
    }
    if fanout == 0 {
        return Err(Error::Storage("fixture fanout must be positive".into()));
    }
    if payload_bytes > 0 && size <= fanout {
        return Err(Error::Storage(format!(
            "payload fixture requires size > fanout ({fanout})"
        )));
    }

    let replica = ReplicaId::new(replica_label.as_bytes());
    let mut ops = Vec::with_capacity(size + usize::from(payload_bytes > 0));
    for node_index in 1..=size {
        let (parent, position) = balanced_parent_and_position(node_index, fanout);
        ops.push(Operation::insert(
            &replica,
            node_index as u64,
            node_index as u64,
            parent,
            NodeId(node_index as u128),
            order_key_from_position(position),
        ));
    }
    if payload_bytes > 0 {
        let payload_counter = (size + 1) as u64;
        ops.push(Operation::set_payload(
            &replica,
            payload_counter,
            payload_counter,
            NodeId((fanout + 1) as u128),
            vec![0xAB; payload_bytes],
        ));
    }

    prime_doc_for_tests(client, doc_id, &ops)
}
