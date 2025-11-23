use crate::buffer_manager::BufferManager;

#[test]
fn exmap_snapshot_consistency() {
    let mut cfg = crate::config::Config::default();
    cfg.use_exmap = true;
    let bm = BufferManager::new(cfg).expect("buffer manager init");
    let snap = bm.stats_snapshot();
    assert!(snap.exmap.requested);
    if snap.exmap.active {
        assert!(snap.exmap.reason.is_none());
    } else {
        assert!(snap.exmap.reason.is_some());
    }
}
