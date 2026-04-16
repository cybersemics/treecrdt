use std::sync::OnceLock;

// Bench/debug-only upload profiling. This stays off in normal operation and
// emits one JSON line per append batch when explicitly enabled.
pub(crate) fn append_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("TREECRDT_PG_PROFILE_UPLOAD").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
        )
    })
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PgAppendProfile {
    pub(crate) batch_ops: usize,
    pub(crate) frontier_pending_before: bool,
    pub(crate) head_seq_before: u64,
    pub(crate) bulk_insert_ms: f64,
    pub(crate) bulk_inserted_ops: usize,
    pub(crate) dedupe_filter_ms: f64,
    pub(crate) materialize_ms: f64,
    pub(crate) update_head_ms: f64,
    pub(crate) frontier_recorded: bool,
    pub(crate) node_load_count: u64,
    pub(crate) node_load_ms: f64,
    pub(crate) node_ensure_count: u64,
    pub(crate) node_ensure_ms: f64,
    pub(crate) node_detach_count: u64,
    pub(crate) node_detach_ms: f64,
    pub(crate) node_attach_count: u64,
    pub(crate) node_attach_ms: f64,
    pub(crate) node_tombstone_count: u64,
    pub(crate) node_tombstone_ms: f64,
    pub(crate) node_last_change_count: u64,
    pub(crate) node_last_change_ms: f64,
    pub(crate) node_deleted_at_count: u64,
    pub(crate) node_deleted_at_ms: f64,
    pub(crate) payload_load_count: u64,
    pub(crate) payload_load_ms: f64,
    pub(crate) payload_set_count: u64,
    pub(crate) payload_set_ms: f64,
    pub(crate) index_record_count: u64,
    pub(crate) index_record_ms: f64,
}

impl PgAppendProfile {
    pub(crate) fn new(
        batch_ops: usize,
        frontier_pending_before: bool,
        head_seq_before: u64,
    ) -> Self {
        Self {
            batch_ops,
            frontier_pending_before,
            head_seq_before,
            ..Self::default()
        }
    }

    pub(crate) fn log(&self, doc_id: &str, inserted: usize) {
        eprintln!(
            "{}",
            serde_json::json!({
                "kind": "treecrdt_postgres_append_profile",
                "docId": doc_id,
                "batchOps": self.batch_ops,
                "insertedOps": inserted,
                "frontierPendingBefore": self.frontier_pending_before,
                "headSeqBefore": self.head_seq_before,
                "bulkInsertMs": self.bulk_insert_ms,
                "bulkInsertedOps": self.bulk_inserted_ops,
                "dedupeFilterMs": self.dedupe_filter_ms,
                "materializeMs": self.materialize_ms,
                "updateHeadMs": self.update_head_ms,
                "frontierRecorded": self.frontier_recorded,
                "nodeLoadCount": self.node_load_count,
                "nodeLoadMs": self.node_load_ms,
                "nodeEnsureCount": self.node_ensure_count,
                "nodeEnsureMs": self.node_ensure_ms,
                "nodeDetachCount": self.node_detach_count,
                "nodeDetachMs": self.node_detach_ms,
                "nodeAttachCount": self.node_attach_count,
                "nodeAttachMs": self.node_attach_ms,
                "nodeTombstoneCount": self.node_tombstone_count,
                "nodeTombstoneMs": self.node_tombstone_ms,
                "nodeLastChangeCount": self.node_last_change_count,
                "nodeLastChangeMs": self.node_last_change_ms,
                "nodeDeletedAtCount": self.node_deleted_at_count,
                "nodeDeletedAtMs": self.node_deleted_at_ms,
                "payloadLoadCount": self.payload_load_count,
                "payloadLoadMs": self.payload_load_ms,
                "payloadSetCount": self.payload_set_count,
                "payloadSetMs": self.payload_set_ms,
                "indexRecordCount": self.index_record_count,
                "indexRecordMs": self.index_record_ms,
            })
        );
    }
}
