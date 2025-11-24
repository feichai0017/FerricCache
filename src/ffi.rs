use crate::buffer_manager::{BgStatsSnapshot, BufferManager, StatsSnapshot};
use crate::config::Config;
use crate::thread_local::set_worker_id;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::Arc;

/// C-friendly configuration mirroring `Config`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FerricConfig {
    pub block_path: *const c_char,
    pub virt_gb: u64,
    pub phys_gb: u64,
    pub use_exmap: u8,
    pub batch: u64,
    pub run_for: u64,
    pub threads: u64,
    pub data_size: u64,
    pub random_read: u8,
    pub bg_write: u8,
    pub bg_write_threads: u64,
    pub io_depth: u64,
    pub io_workers: u64,
    pub io_affinity: *const c_char,
}

impl Default for FerricConfig {
    fn default() -> Self {
        let cfg = Config::default();
        Self {
            block_path: std::ptr::null(),
            virt_gb: cfg.virt_gb,
            phys_gb: cfg.phys_gb,
            use_exmap: cfg.use_exmap as u8,
            batch: cfg.batch,
            run_for: cfg.run_for,
            threads: cfg.threads,
            data_size: cfg.data_size,
            random_read: cfg.random_read as u8,
            bg_write: cfg.bg_write as u8,
            bg_write_threads: cfg.bg_write_threads,
            io_depth: cfg.io_depth,
            io_workers: cfg.io_workers,
            io_affinity: std::ptr::null(),
        }
    }
}

impl From<&FerricConfig> for Config {
    fn from(c: &FerricConfig) -> Self {
        let mut cfg = Config::default();
        cfg.virt_gb = c.virt_gb;
        cfg.phys_gb = c.phys_gb;
        cfg.use_exmap = c.use_exmap != 0;
        cfg.batch = c.batch;
        cfg.run_for = c.run_for;
        cfg.threads = c.threads;
        cfg.data_size = c.data_size;
        cfg.random_read = c.random_read != 0;
        cfg.bg_write = c.bg_write != 0;
        cfg.bg_write_threads = c.bg_write_threads;
        cfg.io_depth = c.io_depth;
        cfg.io_workers = c.io_workers;
        // best-effort parse block path/affinity from C strings if provided
        if !c.block_path.is_null() {
            if let Ok(s) = unsafe { CStr::from_ptr(c.block_path) }.to_str() {
                cfg.block_path = s.to_string();
            }
        }
        if !c.io_affinity.is_null() {
            if let Ok(s) = unsafe { CStr::from_ptr(c.io_affinity) }.to_str() {
                cfg.io_affinity = Some(s.to_string());
            }
        }
        cfg
    }
}

/// Handle returned to C callers.
#[repr(C)]
pub struct FerricHandle {
    bm: Arc<BufferManager>,
}

/// C-friendly stats snapshot (aggregated).
#[repr(C)]
#[derive(Default)]
pub struct FerricStats {
    pub reads: u64,
    pub writes: u64,
    pub phys_used: u64,
    pub alloc: u64,
    pub bg_enqueued: u64,
    pub bg_completed: u64,
    pub bg_errors: u64,
    pub bg_errors_enospc: u64,
    pub bg_errors_eio: u64,
    pub bg_queue_len: u64,
    pub bg_inflight: u64,
    pub bg_retries: u64,
    pub bg_wait_park: u64,
    pub io_submit: u64,
    pub io_fail: u64,
    pub io_timeout: u64,
    pub io_retries: u64,
    pub exmap_requested: u8,
    pub exmap_active: u8,
}

fn aggregate_bg(bg: &BgStatsSnapshot, out: &mut FerricStats) {
    out.bg_enqueued = bg.enqueued;
    out.bg_completed = bg.completed;
    out.bg_errors = bg.errors;
    out.bg_errors_enospc = bg.errors_enospc;
    out.bg_errors_eio = bg.errors_eio;
    out.bg_queue_len = bg.queue_len;
    out.bg_inflight = bg.inflight;
    out.bg_retries = bg.retries;
    out.bg_wait_park = bg.wait_park;
    if let Some(ioq) = &bg.io_queues {
        for q in &ioq.queues {
            out.io_submit += q.submit;
            out.io_fail += q.fail;
            out.io_timeout += q.timeout;
            out.io_retries += q.retries;
        }
    }
}

fn fill_stats(snap: &StatsSnapshot, out: &mut FerricStats) {
    out.reads = snap.reads;
    out.writes = snap.writes;
    out.phys_used = snap.phys_used;
    out.alloc = snap.alloc;
    out.exmap_requested = snap.exmap.requested as u8;
    out.exmap_active = snap.exmap.active as u8;
    if let Some(bg) = &snap.bgwrite {
        aggregate_bg(bg, out);
    }
}

fn with_handle<T>(handle: *mut FerricHandle, f: impl FnOnce(&FerricHandle) -> T) -> Option<T> {
    if handle.is_null() {
        return None;
    }
    // Safety: caller promises a valid handle pointer returned by ferric_init.
    let h = unsafe { &*handle };
    Some(f(h))
}

#[cfg_attr(feature = "capi", export_name = "ferric_init")]
pub unsafe extern "C" fn ferric_init(cfg: *const FerricConfig) -> *mut FerricHandle {
    let fc = unsafe { cfg.as_ref() }
        .copied()
        .unwrap_or_else(FerricConfig::default);
    let config = Config::from(&fc);
    match BufferManager::new(config) {
        Ok(bm) => Box::into_raw(Box::new(FerricHandle { bm: Arc::new(bm) })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[cfg_attr(feature = "capi", export_name = "ferric_destroy")]
pub unsafe extern "C" fn ferric_destroy(handle: *mut FerricHandle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

#[cfg_attr(feature = "capi", export_name = "ferric_set_worker_id")]
pub unsafe extern "C" fn ferric_set_worker_id(id: u16) {
    set_worker_id(id);
}

#[cfg_attr(feature = "capi", export_name = "ferric_alloc_page")]
pub unsafe extern "C" fn ferric_alloc_page(handle: *mut FerricHandle) -> u64 {
    with_handle(handle, |h| h.bm.alloc_page().unwrap_or(0)).unwrap_or(0)
}

#[cfg_attr(feature = "capi", export_name = "ferric_fix_s")]
pub unsafe extern "C" fn ferric_fix_s(handle: *mut FerricHandle, pid: u64) -> *mut u8 {
    with_handle(handle, |h| h.bm.fix_s(pid) as *mut u8).unwrap_or(std::ptr::null_mut())
}

#[cfg_attr(feature = "capi", export_name = "ferric_fix_x")]
pub unsafe extern "C" fn ferric_fix_x(handle: *mut FerricHandle, pid: u64) -> *mut u8 {
    with_handle(handle, |h| h.bm.fix_x(pid) as *mut u8).unwrap_or(std::ptr::null_mut())
}

#[cfg_attr(feature = "capi", export_name = "ferric_unfix_s")]
pub unsafe extern "C" fn ferric_unfix_s(handle: *mut FerricHandle, pid: u64) {
    if let Some(_) = with_handle(handle, |h| h.bm.unfix_s(pid)) {}
}

#[cfg_attr(feature = "capi", export_name = "ferric_unfix_x")]
pub unsafe extern "C" fn ferric_unfix_x(handle: *mut FerricHandle, pid: u64) {
    if let Some(_) = with_handle(handle, |h| h.bm.unfix_x(pid)) {}
}

#[cfg_attr(feature = "capi", export_name = "ferric_mark_dirty")]
pub unsafe extern "C" fn ferric_mark_dirty(handle: *mut FerricHandle, pid: u64) {
    if let Some(_) = with_handle(handle, |h| h.bm.mark_dirty(pid)) {}
}

#[cfg_attr(feature = "capi", export_name = "ferric_evict")]
pub unsafe extern "C" fn ferric_evict(handle: *mut FerricHandle) {
    if let Some(_) = with_handle(handle, |h| h.bm.evict()) {}
}

#[cfg_attr(feature = "capi", export_name = "ferric_poll_bg")]
pub unsafe extern "C" fn ferric_poll_bg(handle: *mut FerricHandle) {
    if let Some(_) = with_handle(handle, |h| h.bm.poll_bg_completions()) {}
}

#[cfg_attr(feature = "capi", export_name = "ferric_stats")]
pub unsafe extern "C" fn ferric_stats(handle: *mut FerricHandle, out: *mut FerricStats) -> i32 {
    if out.is_null() {
        return -1;
    }
    if let Some(_) = with_handle(handle, |h| {
        let snap = h.bm.stats_snapshot();
        let mut s = FerricStats::default();
        fill_stats(&snap, &mut s);
        unsafe {
            *out = s;
        }
    }) {
        0
    } else {
        -1
    }
}
