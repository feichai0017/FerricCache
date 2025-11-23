use crate::memory::page_state::{state, PageState};
use crate::memory::ResidentPageSet;
use crate::buffer_manager::BufferManager;
use crate::memory::PAGE_SIZE;
use std::fs::OpenOptions;
use std::io::Write;

mod btree_tests;
mod scan_tests;
mod delete_tests;
mod evict_tests;
mod bgwrite_toggle;
mod evict_phase_order;
mod evict_stress_small;
mod mixed_stress;
mod worker_id;
mod stats_tests;
mod insert_regression;
mod exmap_status;

#[test]
fn page_state_shared_and_mark_unlocks() {
    let ps = PageState::new();
    ps.init_evicted();
    // set to unlocked
    let _v = PageState::with_same_version(ps.load(), state::UNLOCKED);
    ps.try_lock_x(ps.load()); // lock X
    ps.unlock_x(); // now unlocked with version+1

    // mark then lock S
    let v = ps.load();
    assert_eq!(PageState::state(v), state::UNLOCKED);
    assert!(ps.try_mark(v));
    let v2 = ps.load();
    assert_eq!(PageState::state(v2), state::MARKED);
    assert!(ps.try_lock_s(v2)); // marked -> shared
    assert_eq!(PageState::state(ps.load()), 1);
    ps.unlock_s();
    // After releasing the only shared lock, state returns to UNLOCKED.
    assert_eq!(PageState::state(ps.load()), state::UNLOCKED);
}

#[test]
fn resident_page_set_insert_remove() {
    let set = ResidentPageSet::new(8);
    set.insert(42);
    set.insert(7);
    assert!(set.remove(42));
    assert!(!set.remove(42));
    assert!(set.remove(7));
}

/// Smoke test: allocate a page, write data, evict, then fault it back in.
#[test]
fn alloc_write_evict_fault_cycle() {
    // Use a temp file-backed block device.
    let path = "/tmp/ferric_cache_test_block";
    let mut f = OpenOptions::new().create(true).write(true).open(path).unwrap();
    // Pre-size the file to a few pages.
    f.set_len((PAGE_SIZE * 8) as u64).unwrap();
    f.write_all(&vec![0u8; PAGE_SIZE * 8]).unwrap();

    let virt_pages = 8u64;
    let phys_pages = 4u64;
    let bm = BufferManager::new_with_pages(path.to_string(), virt_pages, phys_pages, 1).expect("bm init");

    let pid = bm.alloc_page().unwrap();
    let ptr = bm.fix_x(pid);
    let data_offset = 16; // avoid clobbering the dirty flag at offset 0
    unsafe {
        let buf = std::slice::from_raw_parts_mut(ptr as *mut u8, PAGE_SIZE);
        buf[data_offset] = 0xAB;
    }
    bm.unfix_x(pid);

    for _ in 0..4 {
        bm.evict();
        if PageState::state(bm.page_state_ref(pid).load()) == state::EVICTED {
            break;
        }
    }
    assert_eq!(PageState::state(bm.page_state_ref(pid).load()), state::EVICTED);

    // Fault in via fix_s and verify data.
    let ptr2 = bm.fix_s(pid);
    unsafe {
        let buf = std::slice::from_raw_parts(ptr2 as *const u8, PAGE_SIZE);
        assert_eq!(buf[data_offset], 0xAB);
    }
    bm.unfix_s(pid);
}
