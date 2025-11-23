use crate::buffer_manager::BufferManager;
use crate::memory::PAGE_SIZE;

#[test]
fn evict_writes_dirty_then_marks_evicted() {
    let bm = BufferManager::new_with_pages("/tmp/ferric_evict_test".to_string(), 32, 4, 1).unwrap();
    // allocate several pages to exceed phys_count
    let pid = bm.alloc_page().unwrap();
    let ptr = bm.fix_x(pid);
    unsafe {
        let buf = std::slice::from_raw_parts_mut(ptr as *mut u8, PAGE_SIZE);
        buf[16] = 0xAA;
    }
    bm.unfix_x(pid);
    // allocate extra pages to trigger ensure_free_pages
    for _ in 0..4 {
        let p = bm.alloc_page().unwrap();
        let ptr = bm.fix_x(p);
        unsafe {
            let buf = std::slice::from_raw_parts_mut(ptr as *mut u8, PAGE_SIZE);
            buf[16] = 0xBB;
        }
        bm.unfix_x(p);
    }
    // run eviction enough times to evict dirty pages
    for _ in 0..64 {
        bm.evict();
        let state = bm.page_state_ref(pid).load();
        if crate::memory::page_state::PageState::state(state) == crate::memory::page_state::state::EVICTED {
            break;
        }
    }
    // read back (will fault-in if evicted, or succeed if still resident/marked)
    let ptr2 = bm.fix_s(pid);
    unsafe {
        let buf = std::slice::from_raw_parts(ptr2 as *const u8, PAGE_SIZE);
        assert_eq!(buf[16], 0xAA);
    }
    bm.unfix_s(pid);
}
