use crate::buffer_manager::BufferManager;

#[test]
fn bgwrite_config_sets_flag() {
    let mut cfg = crate::config::Config::default();
    cfg.bg_write = true;
    let bm = BufferManager::new(cfg).unwrap();
    assert!(bm.bg_write);
}

#[test]
fn bgwrite_async_write_preserves_data() {
    let mut cfg = crate::config::Config::default();
    cfg.bg_write = true;
    // Use small sizes via new_with_pages helper
    let bm = BufferManager::new_with_pages("/tmp/ferric_bgwrite".to_string(), 8, 4, 1).unwrap();
    let pid = bm.alloc_page().unwrap();
    let ptr = bm.fix_x(pid);
    unsafe {
        let buf = std::slice::from_raw_parts_mut(ptr as *mut u8, crate::memory::PAGE_SIZE);
        buf[0] = 0xAB;
    }
    bm.unfix_x(pid);
    bm.evict();
    // wait a bit for background write to complete
    std::thread::sleep(std::time::Duration::from_millis(10));
    // fault in and verify
    let ptr2 = bm.fix_s(pid);
    unsafe {
        let buf = std::slice::from_raw_parts(ptr2 as *const u8, crate::memory::PAGE_SIZE);
        assert_eq!(buf[0], 0xAB);
    }
    bm.unfix_s(pid);
}
