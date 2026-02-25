/// Run incremental materialization when possible; otherwise mark the document as dirty.
///
/// Returns `true` when incremental materialization succeeded, `false` when the caller
/// should rely on a full rebuild path later.
pub fn try_incremental_materialization<E>(
    already_dirty: bool,
    incremental: impl FnOnce() -> std::result::Result<(), E>,
    mut mark_dirty: impl FnMut(),
) -> bool {
    if already_dirty {
        mark_dirty();
        return false;
    }

    if incremental().is_err() {
        mark_dirty();
        return false;
    }

    true
}
