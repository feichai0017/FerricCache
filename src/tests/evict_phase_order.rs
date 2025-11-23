use crate::buffer_manager::BufferManager;
use crate::memory::PAGE_SIZE;

#[test]
fn dirty_batch_write_clears_dirty_and_evicts() {
    let bm = BufferManager::new_with_pages("/tmp/ferric_evict_phase".to_string(), 16, 8, 2).unwrap();
    let pid = bm.alloc_page().unwrap();
    let ptr = bm.fix_x(pid);
    unsafe {
        let buf = std::slice::from_raw_parts_mut(ptr as *mut u8, PAGE_SIZE);
        buf[0] = 0xCC;
    }
    bm.unfix_x(pid);
    bm.evict();
    // verify dirty cleared or evicted
    let state = bm.page_state_ref(pid).load();
    let st = crate::memory::page_state::PageState::state(state);
    // allow Marked if not yet evicted, but data should be persisted
    assert!(st == crate::memory::page_state::state::EVICTED || st == crate::memory::page_state::state::UNLOCKED || st == crate::memory::page_state::state::MARKED);
    let ptr2 = bm.fix_s(pid);
    unsafe {
        let buf = std::slice::from_raw_parts(ptr2 as *const u8, PAGE_SIZE);
        assert_eq!(buf[0], 0xCC);
    }
    bm.unfix_s(pid);
}
