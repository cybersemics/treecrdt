use super::sqlite_api::*;

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use std::slice;

use treecrdt_core::{
    Lamport, MaterializationCursor, MaterializationHead, MaterializationKey, MaterializationState,
};

#[derive(Clone, Debug)]
pub(super) struct TreeMeta(pub(super) MaterializationState);

impl MaterializationCursor for TreeMeta {
    fn state(&self) -> MaterializationState<&[u8]> {
        self.0.as_borrowed()
    }
}

pub(super) fn load_doc_id(db: *mut sqlite3) -> Result<Option<Vec<u8>>, c_int> {
    let sql =
        CString::new("SELECT value FROM meta WHERE key = 'doc_id' LIMIT 1").expect("doc id sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc == SQLITE_ROW as c_int {
        let ptr = unsafe { sqlite_column_text(stmt, 0) } as *const u8;
        let len = unsafe { sqlite_column_bytes(stmt, 0) } as usize;
        let value = if ptr.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
        };
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
        Ok(Some(value))
    } else if step_rc == SQLITE_DONE as c_int {
        let finalize_rc = unsafe { sqlite_finalize(stmt) };
        if finalize_rc != SQLITE_OK as c_int {
            return Err(finalize_rc);
        }
        Ok(None)
    } else {
        unsafe { sqlite_finalize(stmt) };
        Err(step_rc)
    }
}

pub(super) fn load_tree_meta(db: *mut sqlite3) -> Result<TreeMeta, c_int> {
    let sql = CString::new(
        "SELECT head_lamport, head_replica, head_counter, head_seq, \
                replay_lamport, replay_replica, replay_counter \
         FROM tree_meta WHERE id = 1 LIMIT 1",
    )
    .expect("tree meta sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    if step_rc != SQLITE_ROW as c_int {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }

    let head_lamport = unsafe { sqlite_column_int64(stmt, 0) } as Lamport;
    let rep_ptr = unsafe { sqlite_column_blob(stmt, 1) } as *const u8;
    let rep_len = unsafe { sqlite_column_bytes(stmt, 1) } as usize;
    let head_replica = if rep_ptr.is_null() || rep_len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(rep_ptr, rep_len) }.to_vec()
    };
    let head_counter = unsafe { sqlite_column_int64(stmt, 2) } as u64;
    let head_seq = unsafe { sqlite_column_int64(stmt, 3) } as u64;
    let replay_lamport = if unsafe { sqlite_column_type(stmt, 4) } == SQLITE_NULL as c_int {
        None
    } else {
        Some(unsafe { sqlite_column_int64(stmt, 4).max(0) as Lamport })
    };
    let replay_replica = if unsafe { sqlite_column_type(stmt, 5) } == SQLITE_NULL as c_int {
        None
    } else {
        let ptr = unsafe { sqlite_column_blob(stmt, 5) } as *const u8;
        let len = unsafe { sqlite_column_bytes(stmt, 5) } as usize;
        Some(if ptr.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { slice::from_raw_parts(ptr, len) }.to_vec()
        })
    };
    let replay_counter = if unsafe { sqlite_column_type(stmt, 6) } == SQLITE_NULL as c_int {
        None
    } else {
        Some(unsafe { sqlite_column_int64(stmt, 6).max(0) as u64 })
    };

    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }

    let head = if head_seq == 0 && head_lamport == 0 && head_replica.is_empty() && head_counter == 0
    {
        None
    } else {
        Some(MaterializationHead {
            at: MaterializationKey {
                lamport: head_lamport,
                replica: head_replica,
                counter: head_counter,
            },
            seq: head_seq,
        })
    };
    let replay_from = match (replay_lamport, replay_replica, replay_counter) {
        (Some(lamport), Some(replica), Some(counter)) => Some(MaterializationKey {
            lamport,
            replica,
            counter,
        }),
        _ => None,
    };

    Ok(TreeMeta(MaterializationState { head, replay_from }))
}

pub(super) fn set_tree_meta_replay_frontier(
    db: *mut sqlite3,
    frontier: &treecrdt_core::MaterializationFrontier,
) -> Result<(), c_int> {
    let sql = CString::new(
        "UPDATE tree_meta \
         SET replay_lamport = ?1, replay_replica = ?2, replay_counter = ?3 \
         WHERE id = 1",
    )
    .expect("tree meta replay sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_int64(stmt, 1, frontier.lamport as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            2,
            frontier.replica.as_ptr() as *const c_void,
            frontier.replica.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 3, frontier.counter as i64) != SQLITE_OK as c_int;
    }
    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(())
}

pub(super) fn update_tree_meta_head<R: AsRef<[u8]>>(
    db: *mut sqlite3,
    head: Option<&MaterializationHead<R>>,
) -> Result<(), c_int> {
    let (lamport, replica, counter, seq): (Lamport, &[u8], u64, u64) = match head {
        Some(head) => (
            head.at.lamport,
            head.at.replica.as_ref(),
            head.at.counter,
            head.seq,
        ),
        None => (0, &[], 0, 0),
    };
    let sql = CString::new(
        "UPDATE tree_meta \
         SET head_lamport = ?1, \
             head_replica = ?2, \
             head_counter = ?3, \
             head_seq = ?4, \
             replay_lamport = NULL, \
             replay_replica = NULL, \
             replay_counter = NULL \
         WHERE id = 1",
    )
    .expect("tree meta head sql");
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    if rc != SQLITE_OK as c_int {
        return Err(rc);
    }

    let mut bind_err = false;
    unsafe {
        bind_err |= sqlite_bind_int64(stmt, 1, lamport as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_blob(
            stmt,
            2,
            replica.as_ptr() as *const c_void,
            replica.len() as c_int,
            None,
        ) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 3, counter as i64) != SQLITE_OK as c_int;
        bind_err |= sqlite_bind_int64(stmt, 4, seq as i64) != SQLITE_OK as c_int;
    }
    if bind_err {
        unsafe { sqlite_finalize(stmt) };
        return Err(SQLITE_ERROR as c_int);
    }

    let step_rc = unsafe { sqlite_step(stmt) };
    let finalize_rc = unsafe { sqlite_finalize(stmt) };
    if step_rc != SQLITE_DONE as c_int {
        return Err(step_rc);
    }
    if finalize_rc != SQLITE_OK as c_int {
        return Err(finalize_rc);
    }
    Ok(())
}

pub(super) const SCHEMA_SQL: &str = include_str!("schema.sql");

pub(super) fn ensure_schema(db: *mut sqlite3) -> Result<(), c_int> {
    ensure_api_initialized()?;
    let sql = CString::new(SCHEMA_SQL).expect("schema SQL");
    let rc = sqlite_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    if rc == SQLITE_OK as c_int {
        Ok(())
    } else {
        Err(rc)
    }
}
