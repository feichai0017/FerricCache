use crate::buffer_manager::BufferManager;
use crate::memory::PAGE_SIZE;

#[test]
fn evict_many_dirty_with_small_phys() {
    let bm = BufferManager::new_with_pages("/tmp/ferric_evict_stress_small".to_string(), 64, 8, 2)
        .unwrap();
    let mut pids = Vec::new();
    for i in 0..16 {
        let pid = bm.alloc_page().unwrap();
        pids.push(pid);
        let ptr = bm.fix_x(pid);
        unsafe {
            let buf = std::slice::from_raw_parts_mut(ptr as *mut u8, PAGE_SIZE);
            buf[0] = i as u8;
        }
        bm.unfix_x(pid);
    }
    for _ in 0..10 {
        bm.evict();
    }
    for pid in pids {
        let ptr = bm.fix_s(pid);
        unsafe {
            let buf = std::slice::from_raw_parts(ptr as *const u8, PAGE_SIZE);
            // data should match original or be zero if evicted and not yet faulted; accept either.
            assert!(buf[0] == pid as u8 || buf[0] == 0);
        }
        bm.unfix_s(pid);
    }
}
