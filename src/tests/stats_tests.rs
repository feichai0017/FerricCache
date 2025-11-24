use crate::buffer_manager::BufferManager;

/// Verify per-worker stats capture writes/evictions triggered by pressure.
#[test]
fn stats_capture_eviction_activity() {
    // 1 phys page so second alloc forces eviction/write of first dirty page.
    let bm = BufferManager::new_with_pages("/tmp/ferric_stats".to_string(), 4, 1, 1).unwrap();
    let p1 = bm.alloc_page().unwrap();
    // Make page dirty with user data.
    let ptr = bm.fix_x(p1);
    unsafe {
        let buf = std::slice::from_raw_parts_mut(ptr as *mut u8, crate::memory::PAGE_SIZE);
        buf[8] = 0xAA;
    }
    bm.unfix_x(p1);
    let _p2 = bm.alloc_page().unwrap(); // should evict/write p1
    bm.evict(); // force eviction pass

    let snap = bm.stats_snapshot();
    assert!(snap.writes > 0, "expected at least one write from eviction");
    if let Some(worker) = snap.worker {
        // All work ran on thread-local id 0 by default.
        assert!(!worker.is_empty());
        assert!(
            worker[0].writes > 0,
            "per-worker write stats should record eviction writes"
        );
        assert!(
            worker[0].evicts > 0,
            "per-worker evict stats should record eviction count"
        );
    }
}
