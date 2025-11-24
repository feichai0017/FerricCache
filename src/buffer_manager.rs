use crate::Result;
use crate::Result as FerricResult;
use crate::config::Config;
use crate::io::PageIo;
#[cfg(not(feature = "libaio"))]
use crate::io::SyncFileIo;
#[cfg(feature = "libaio")]
use crate::io::lbaio::LibaioIo;
use crate::memory::{
    PAGE_SIZE, RegionKind, ResidentPageSet, VirtualRegion, create_region,
    page::Page,
    page_state::{PageState, state},
};
use crate::thread_local::{get_worker_id, set_worker_id};
use crossbeam_channel::{Receiver as CbReceiver, Sender as CbSender, TrySendError};
use std::cell::Cell;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Core buffer manager responsible for page lifecycle.
///
/// This is a skeleton; IO, eviction, and page fault handling will be filled in
/// following the original vmcache design.
pub struct BufferManager {
    pub config: Config,
    /// Total virtual memory size in bytes.
    pub virt_size: u64,
    /// Physical buffer size in bytes.
    pub phys_size: u64,
    /// Virtual pages count.
    pub virt_count: u64,
    /// Physical pages count (buffer pool size).
    pub phys_count: u64,
    /// Counter of pages currently mapped/used.
    pub phys_used_count: AtomicU64,
    /// Next PID to allocate (PID 0 reserved for metadata).
    pub alloc_count: AtomicU64,
    /// Access counters for stats.
    pub read_count: AtomicU64,
    pub write_count: AtomicU64,
    /// Virtual memory region (mmap or exmap).
    pub virt_region: VirtualRegion,
    /// Region kind used (mmap or exmap).
    pub region_kind: RegionKind,
    /// Optional reason when exmap requested but not active.
    pub exmap_reason: Option<String>,
    /// Per-page state array.
    pub page_state: Vec<PageState>,
    /// Tracks currently resident pages for eviction.
    pub resident_set: ResidentPageSet,
    /// Eviction batch size.
    pub batch: u64,
    /// Page IO backend.
    pub io: Arc<dyn PageIo>,
    /// Inflight async IO count for libaio submissions.
    pub io_inflight_count: Arc<AtomicU64>,
    /// Condvar to park/unpark when inflight is saturated.
    pub io_inflight_cv: Arc<(Mutex<()>, std::sync::Condvar)>,
    /// Condvar to park/unpark eviction when IO inflight is high.
    pub evict_cv: Arc<(Mutex<()>, std::sync::Condvar)>,
    /// Simple free list for reclaimed page IDs.
    pub free_list: Mutex<Vec<u64>>,
    /// Background write toggle.
    pub bg_write: bool,
    /// Background writer handle (not used yet).
    pub bg_handle: Option<std::thread::JoinHandle<()>>,
    /// Channel to send background write tasks.
    pub bg_tx: Option<CbSender<BgRequest>>,
    /// BGWRITE stats.
    pub bg_stats: Arc<BgStats>,
    /// Tracking inflight async writes (pid -> completion rx).
    pub bg_inflight: Mutex<std::collections::HashMap<u64, Receiver<FerricResult<()>>>>,
    /// Optional per-worker stats.
    pub worker_stats: Option<Arc<PerWorkerStats>>,
    /// Allocator for worker ids.
    pub worker_id_alloc: AtomicU64,
}

// Safety: BufferManager manages an mmap-backed region and internal atomics.
// We rely on external synchronization (page states + guards) for correctness.
unsafe impl Send for BufferManager {}
unsafe impl Sync for BufferManager {}

impl BufferManager {
    pub fn new(config: Config) -> Result<Self> {
        let virt_size = config.virt_gb * GB;
        let phys_size = config.phys_gb * GB;
        Self::new_with_sizes(config, virt_size, phys_size)
    }

    fn new_with_sizes(config: Config, virt_size: u64, phys_size: u64) -> Result<Self> {
        if virt_size == 0 || phys_size == 0 {
            return Err("virt/phys size must be > 0".into());
        }
        if virt_size < phys_size {
            return Err("VIRTGB must be >= PHYSGB".into());
        }
        let virt_count = virt_size / PAGE_SIZE as u64;
        let phys_count = phys_size / PAGE_SIZE as u64;
        let batch = config.batch;

        #[cfg(feature = "libaio")]
        let io: Arc<dyn PageIo> = {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&config.block_path)?;
            file.set_len(virt_size)?;
            let affinity = config.io_affinity.as_ref().map(|s| {
                s.split(',')
                    .filter_map(|t| t.trim().parse::<usize>().ok())
                    .collect::<Vec<_>>()
            });
            LibaioIo::open(
                file,
                config.io_depth as usize,
                config.io_workers as usize,
                affinity,
            )?
        };
        #[cfg(not(feature = "libaio"))]
        let io: Arc<dyn PageIo> = SyncFileIo::open_with_len(&config.block_path, virt_size)?;

        let page_count =
            usize::try_from(virt_count).map_err(|_| "virt_count does not fit in usize")?;
        let (virt_region, region_kind, exmap_reason) = create_region(page_count, config.use_exmap)?;
        if config.use_exmap && region_kind != RegionKind::Exmap {
            if let Some(msg) = &exmap_reason {
                eprintln!("exmap requested but unavailable; falling back to mmap ({msg})");
            } else {
                eprintln!("exmap requested but unavailable; falling back to mmap");
            }
        }
        let mut page_state = Vec::with_capacity(virt_count as usize);
        for _ in 0..virt_count {
            let ps = PageState::new();
            ps.init_evicted();
            page_state.push(ps);
        }

        let bg_write = config.bg_write;
        let bg_stats = Arc::new(BgStats::default());
        let io_inflight_count = Arc::new(AtomicU64::new(0));
        let io_inflight_cv = Arc::new((Mutex::new(()), std::sync::Condvar::new()));
        let evict_cv = Arc::new((Mutex::new(()), std::sync::Condvar::new()));
        let (bg_handle, bg_tx) = if bg_write {
            let (tx, rx) = crossbeam_channel::bounded::<BgRequest>(batch as usize * 2 + 1);
            let stats_clone = bg_stats.clone();
            let inflight_clone = io_inflight_count.clone();
            let inflight_cv_clone = io_inflight_cv.clone();
            let evict_cv_clone = evict_cv.clone();
            // Dedicated IO context for background writes (libaio if enabled, else sync).
            let bg_io = Self::make_io_for_bg(&config, virt_size)?;
            let bg_worker_id = if config.threads > 0 {
                config.threads as u16
            } else {
                0
            };
            let handle = std::thread::spawn(move || {
                set_worker_id(bg_worker_id);
                Self::bg_worker(
                    rx,
                    bg_io,
                    stats_clone,
                    inflight_clone,
                    inflight_cv_clone,
                    evict_cv_clone,
                    config.io_depth,
                );
            });
            (Some(handle), Some(tx))
        } else {
            (None, None)
        };
        let stat_workers = config.threads.saturating_add(1) as usize; // extra slot for bg writer stats
        let worker_stats = Some(Arc::new(PerWorkerStats::new(stat_workers)));
        Ok(Self {
            config,
            virt_size,
            phys_size,
            virt_count,
            phys_count,
            phys_used_count: AtomicU64::new(0),
            alloc_count: AtomicU64::new(1), // PID 0 reserved
            read_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            virt_region,
            region_kind,
            exmap_reason,
            page_state,
            resident_set: ResidentPageSet::new(phys_count),
            batch,
            io,
            io_inflight_count,
            io_inflight_cv,
            evict_cv,
            free_list: Mutex::new(Vec::new()),
            bg_write,
            bg_handle,
            bg_tx,
            bg_stats,
            bg_inflight: Mutex::new(std::collections::HashMap::new()),
            worker_stats,
            worker_id_alloc: AtomicU64::new(0),
        })
    }

    #[cfg(test)]
    pub fn new_with_pages(
        block_path: String,
        virt_pages: u64,
        phys_pages: u64,
        batch: u64,
    ) -> Result<Self> {
        let mut cfg = Config::default();
        cfg.block_path = block_path;
        cfg.batch = batch;
        let virt_size = virt_pages * PAGE_SIZE as u64;
        let phys_size = phys_pages * PAGE_SIZE as u64;
        Self::new_with_sizes(cfg, virt_size, phys_size)
    }

    /// Map PID to a page pointer.
    pub fn to_ptr(&self, pid: u64) -> *mut Page {
        assert!(pid < self.virt_count, "pid {} out of bounds", pid);
        unsafe { self.virt_region.as_ptr().add(pid as usize) }
    }

    /// mark dirty (caller ensures pid validity and pinning).
    pub fn mark_dirty(&self, pid: u64) {
        unsafe {
            (*self.to_ptr(pid)).dirty = true;
        }
    }

    /// Attempt to map a page pointer back to PID.
    pub fn to_pid(&self, ptr: *const Page) -> Option<u64> {
        let base = self.virt_region.as_ptr() as usize;
        let end = base + (self.virt_count as usize * PAGE_SIZE);
        let p = ptr as usize;
        if p < base || p >= end {
            return None;
        }
        let delta = p - base;
        Some((delta / PAGE_SIZE) as u64)
    }

    /// Check whether a pointer lies within the managed virtual region.
    pub fn is_valid_ptr(&self, ptr: *const Page) -> bool {
        self.to_pid(ptr).is_some()
    }

    /// Per-page state accessor.
    #[inline]
    pub fn page_state_ref(&self, pid: u64) -> &PageState {
        &self.page_state[pid as usize]
    }

    /// Reserve a new page ID and mark it dirty.
    pub fn alloc_page(&self) -> Result<u64> {
        // reuse from free list if available
        let pid = if let Some(pid) = self.free_list.lock().unwrap().pop() {
            pid
        } else {
            self.alloc_count.fetch_add(1, Ordering::SeqCst)
        };
        if pid >= self.virt_count {
            return Err("VIRTGB too low for allocation".into());
        }
        self.phys_used_count.fetch_add(1, Ordering::SeqCst);
        self.ensure_free_pages();
        unsafe {
            (*self.to_ptr(pid)).dirty = true;
        }
        // Mark as locked to mimic allocation path; full fix/unfix will refine this.
        let state = self.page_state_ref(pid).load();
        let locked = self.page_state_ref(pid).try_lock_x(state);
        if !locked {
            return Err("failed to lock new page".into());
        }
        // Unlock immediately; alloc_page does not return a fixed pointer.
        self.page_state_ref(pid).unlock_x();
        self.resident_set.insert(pid);
        Ok(pid)
    }

    /// Ensure there is free space; placeholder for future eviction hook.
    pub fn ensure_free_pages(&self) {
        if self.phys_used_count.load(Ordering::Relaxed) >= (self.phys_count as f64 * 0.95) as u64 {
            self.evict();
        }
    }

    /// Fix a page in exclusive (write) mode, handling faults.
    pub fn fix_x(&self, pid: u64) -> *mut Page {
        let ps = self.page_state_ref(pid);
        let mut spins: u32 = 0;
        loop {
            let v = ps.load();
            match PageState::state(v) {
                crate::memory::page_state::state::EVICTED => {
                    if ps.try_lock_x(v) {
                        self.handle_fault(pid);
                        unsafe {
                            (*self.to_ptr(pid)).dirty = true;
                        }
                        return self.to_ptr(pid);
                    }
                }
                crate::memory::page_state::state::MARKED
                | crate::memory::page_state::state::UNLOCKED => {
                    if ps.try_lock_x(v) {
                        unsafe {
                            (*self.to_ptr(pid)).dirty = true;
                        }
                        return self.to_ptr(pid);
                    }
                }
                _ => {}
            }
            spins = spins.wrapping_add(1);
            if spins & 0x3FF == 0 {
                ps.wait_for_unlock(Duration::from_micros(50));
            }
            spin();
        }
    }

    /// Fix a page in shared (read) mode, handling faults.
    pub fn fix_s(&self, pid: u64) -> *mut Page {
        let ps = self.page_state_ref(pid);
        let mut spins: u32 = 0;
        loop {
            let v = ps.load();
            match PageState::state(v) {
                crate::memory::page_state::state::LOCKED => {}
                crate::memory::page_state::state::EVICTED => {
                    if ps.try_lock_x(v) {
                        self.handle_fault(pid);
                        ps.unlock_x();
                    }
                }
                _ => {
                    if ps.try_lock_s(v) {
                        self.stats_inc_read();
                        return self.to_ptr(pid);
                    }
                }
            }
            spins = spins.wrapping_add(1);
            if spins & 0x3FF == 0 {
                ps.wait_for_unlock(Duration::from_micros(50));
            }
            spin();
        }
    }

    pub fn unfix_s(&self, pid: u64) {
        self.page_state_ref(pid).unlock_s();
    }

    pub fn unfix_x(&self, pid: u64) {
        self.page_state_ref(pid).unlock_x();
    }

    /// Handle a page fault (read a page back into memory). IO is stubbed for now.
    pub fn handle_fault(&self, pid: u64) {
        self.phys_used_count.fetch_add(1, Ordering::SeqCst);
        self.ensure_free_pages();
        self.read_page(pid).expect("read_page failed");
        self.resident_set.insert(pid);
        self.read_count.fetch_add(1, Ordering::SeqCst);
        self.stats_inc_fault();
    }

    fn read_page(&self, pid: u64) -> Result<()> {
        self.io.read_page(pid, self.to_ptr(pid))?;
        unsafe {
            (*self.to_ptr(pid)).dirty = false;
        }
        Ok(())
    }

    /// Evict up to `batch` pages using a clock-style second chance policy.
    pub fn evict(&self) {
        if self.bg_write {
            self.wait_for_bg_capacity();
        }
        // Opportunistically reap finished BGWRITE completions to avoid buildup.
        if self.bg_write {
            self.poll_bg_completions();
        }
        let batch = self.batch as usize;
        let mut to_evict: Vec<u64> = Vec::with_capacity(batch);
        let mut to_write: Vec<u64> = Vec::with_capacity(batch);
        let _bg_wait: Vec<(u64, Receiver<FerricResult<()>>)> = Vec::with_capacity(batch);
        let mut scanned: u64 = 0;
        let max_scan = self.resident_set.capacity();

        // Phase 0: find candidates; first mark, then collect marked clean or lock S for dirty
        while to_evict.len() + to_write.len() < batch {
            let before = to_evict.len() + to_write.len();
            let mut marked_progress = false;
            self.resident_set.iterate_clock_batch(self.batch, |pid| {
                if to_evict.len() + to_write.len() >= batch {
                    return;
                }
                let ps = self.page_state_ref(pid);
                let v = ps.load();
                match PageState::state(v) {
                    state::MARKED => {
                        let dirty = unsafe { (*self.to_ptr(pid)).dirty };
                        if dirty {
                            if ps.try_lock_s(v) {
                                to_write.push(pid);
                            }
                        } else {
                            to_evict.push(pid);
                        }
                    }
                    state::UNLOCKED => {
                        if ps.try_mark(v) {
                            marked_progress = true;
                        }
                    }
                    _ => {}
                }
            });
            scanned += self.batch;
            if self.phys_used_count.load(Ordering::Relaxed) == 0 {
                break;
            }
            let after = to_evict.len() + to_write.len();
            if after == before && !marked_progress && scanned >= max_scan {
                break;
            }
        }

        // Phase 1: write dirty pages (still S-locked). If bg_write is enabled, enqueue and track inflight.
        if !to_write.is_empty() {
            if self.bg_write {
                // If inflight is saturated, wait instead of immediate fallback.
                self.wait_for_bg_capacity();
                for &pid in &to_write {
                    self.wait_for_bg_capacity();
                    let ps = self.page_state_ref(pid);
                    let mut buf = unsafe {
                        std::slice::from_raw_parts(self.to_ptr(pid) as *const u8, PAGE_SIZE)
                            .to_vec()
                    };
                    if let Some(tx) = &self.bg_tx {
                        let (done_tx, done_rx) = std::sync::mpsc::channel();
                        // try to enqueue; if full or closed, fall back to sync write
                        loop {
                            match tx.try_send(BgRequest {
                                pid,
                                buf,
                                done: done_tx.clone(),
                            }) {
                                Ok(_) => {
                                    self.bg_stats.enqueued.fetch_add(1, Ordering::Relaxed);
                                    self.bg_inflight.lock().unwrap().insert(pid, done_rx);
                                    break;
                                }
                                Err(TrySendError::Full(err_req)) => {
                                    self.bg_stats.saturated.fetch_add(1, Ordering::Relaxed);
                                    buf = err_req.buf;
                                    self.wait_for_bg_capacity();
                                    continue;
                                }
                                Err(TrySendError::Disconnected(_)) => break,
                            }
                        }
                        if self.bg_inflight.lock().unwrap().contains_key(&pid) {
                            continue;
                        }
                    }
                    // fallback sync if send fails
                    if let Err(e) = self.io.write_page(pid, self.to_ptr(pid)) {
                        Self::classify_io_error(&self.bg_stats, e.as_ref());
                        ps.unlock_s();
                    } else {
                        self.bg_stats.fallback_sync.fetch_add(1, Ordering::Relaxed);
                        unsafe {
                            (*self.to_ptr(pid)).dirty = false;
                        }
                        self.write_count.fetch_add(1, Ordering::SeqCst);
                        self.stats_inc_write();
                        // release the eviction-held shared lock immediately on sync fallback
                        ps.unlock_s();
                        self.try_mark_if_unlocked(pid);
                    }
                }
            } else {
                if let Err(e) = self.io.write_pages(&to_write, self.to_ptr(0)) {
                    // fallback per-page
                    Self::classify_io_error(&self.bg_stats, e.as_ref());
                    for &pid in &to_write {
                        if let Err(e2) = self.io.write_page(pid, self.to_ptr(pid)) {
                            Self::classify_io_error(&self.bg_stats, e2.as_ref());
                            self.page_state_ref(pid).unlock_s();
                        } else {
                            unsafe {
                                (*self.to_ptr(pid)).dirty = false;
                            }
                            self.write_count.fetch_add(1, Ordering::SeqCst);
                            self.stats_inc_write();
                        }
                    }
                } else {
                    // clear dirty flags on success
                    for &pid in &to_write {
                        unsafe {
                            (*self.to_ptr(pid)).dirty = false;
                        }
                        self.write_count.fetch_add(1, Ordering::SeqCst);
                        self.stats_inc_write();
                    }
                }
            }
        }

        // For async BGWRITE we don't block; completions will be reaped and marked for later eviction.
        if self.bg_write {
            // avoid Phase 3 upgrade; background completions will unlock and allow future eviction passes
            to_evict.retain(|&pid| {
                let ps = self.page_state_ref(pid);
                let v = ps.load();
                PageState::state(v) == state::MARKED && ps.try_lock_x(v)
            });
            // Phase 4: free pages (madvise/dontneed) and remove from resident set
            for pid in to_evict {
                let removed = self.resident_set.remove(pid);
                debug_assert!(removed);
                self.page_state_ref(pid).unlock_x_evicted();
                self.phys_used_count.fetch_sub(1, Ordering::SeqCst);
                self.stats_inc_evict();
                unsafe {
                    libc::madvise(
                        self.to_ptr(pid) as *mut libc::c_void,
                        PAGE_SIZE,
                        libc::MADV_DONTNEED,
                    );
                }
            }
            return;
        }

        // Phase 2: lock clean candidates (currently MARKED)
        to_evict.retain(|&pid| {
            let ps = self.page_state_ref(pid);
            let v = ps.load();
            PageState::state(v) == state::MARKED && ps.try_lock_x(v)
        });

        // Phase 3: upgrade dirty candidates (currently S-locked) to X
        for pid in to_write {
            let ps = self.page_state_ref(pid);
            let v = ps.load();
            if PageState::state(v) == 1 && ps.try_lock_x(v) {
                to_evict.push(pid);
            } else {
                ps.unlock_s();
            }
        }

        // Phase 4: free pages (madvise/dontneed) and remove from resident set
        for pid in to_evict {
            let removed = self.resident_set.remove(pid);
            debug_assert!(removed);
            self.page_state_ref(pid).unlock_x_evicted();
            self.phys_used_count.fetch_sub(1, Ordering::SeqCst);
            self.stats_inc_evict();
            unsafe {
                libc::madvise(
                    self.to_ptr(pid) as *mut libc::c_void,
                    PAGE_SIZE,
                    libc::MADV_DONTNEED,
                );
            }
        }
    }

    /// Return a page ID to the free list (caller ensures it's safe).
    pub fn free_page(&self, pid: u64) {
        if pid == 0 {
            return;
        }
        self.free_list.lock().unwrap().push(pid);
    }

    /// Acquire and set a worker id for the current thread (0-based).
    /// Threads beyond configured count will reuse modulo threads for stats.
    pub fn register_worker(&self) -> u16 {
        let id = self.worker_id_alloc.fetch_add(1, Ordering::Relaxed) as u16;
        let normalized = if self.config.threads == 0 {
            id
        } else {
            id % self.config.threads as u16
        };
        set_worker_id(normalized);
        normalized
    }
}

const GB: u64 = 1024 * 1024 * 1024;

#[inline]
fn spin() {
    thread_local! {
        static SPIN_COUNTER: Cell<u32> = Cell::new(0);
    }
    SPIN_COUNTER.with(|c| {
        let v = c.get().wrapping_add(1);
        c.set(v);
        if v & 0x3FF == 0 {
            // Occasionally sleep a little to reduce contention under heavy spin.
            std::thread::sleep(std::time::Duration::from_nanos(50));
        } else if v & 0x7F == 0 {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    });
}

#[derive(Default, Clone, Debug)]
pub struct WorkerStats {
    pub reads: u64,
    pub writes: u64,
    pub faults: u64,
    pub evicts: u64,
}

#[derive(Default, Clone, Debug)]
pub struct StatsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub phys_used: u64,
    pub alloc: u64,
    pub worker: Option<Vec<WorkerStats>>,
    pub bgwrite: Option<BgStatsSnapshot>,
    pub exmap: ExmapSnapshot,
}

pub struct PerWorkerStats {
    reads: Vec<AtomicU64>,
    writes: Vec<AtomicU64>,
    faults: Vec<AtomicU64>,
    evicts: Vec<AtomicU64>,
}

#[derive(Default, Clone, Debug)]
pub struct BgStatsSnapshot {
    pub enqueued: u64,
    pub completed: u64,
    pub saturated: u64,
    pub fallback_sync: u64,
    pub errors: u64,
    pub errors_enospc: u64,
    pub errors_eio: u64,
    pub batches: u64,
    pub max_batch: u64,
    pub inflight: u64,
    pub queue_len: u64,
    pub retries: u64,
    pub wait_park: u64,
    pub io_queues: Option<crate::io::IoStatsSnapshot>,
}

#[derive(Default, Clone, Debug)]
pub struct ExmapSnapshot {
    pub requested: bool,
    pub active: bool,
    pub reason: Option<String>,
}

impl PerWorkerStats {
    fn new(workers: usize) -> Self {
        let make_vec =
            |size: usize| -> Vec<AtomicU64> { (0..size).map(|_| AtomicU64::new(0)).collect() };
        Self {
            reads: make_vec(workers),
            writes: make_vec(workers),
            faults: make_vec(workers),
            evicts: make_vec(workers),
        }
    }

    fn inc(
        reads: &Vec<AtomicU64>,
        writes: &Vec<AtomicU64>,
        faults: &Vec<AtomicU64>,
        evicts: &Vec<AtomicU64>,
        idx: usize,
        kind: StatKind,
    ) {
        if idx >= reads.len() {
            return;
        }
        match kind {
            StatKind::Read => {
                reads[idx].fetch_add(1, Ordering::Relaxed);
            }
            StatKind::Write => {
                writes[idx].fetch_add(1, Ordering::Relaxed);
            }
            StatKind::Fault => {
                faults[idx].fetch_add(1, Ordering::Relaxed);
            }
            StatKind::Evict => {
                evicts[idx].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> Vec<WorkerStats> {
        let mut out = Vec::with_capacity(self.reads.len());
        for i in 0..self.reads.len() {
            out.push(WorkerStats {
                reads: self.reads[i].load(Ordering::Relaxed),
                writes: self.writes[i].load(Ordering::Relaxed),
                faults: self.faults[i].load(Ordering::Relaxed),
                evicts: self.evicts[i].load(Ordering::Relaxed),
            });
        }
        out
    }
}

enum StatKind {
    Read,
    Write,
    Fault,
    Evict,
}

/// Background write request: pid, page copy, and completion tx.
pub struct BgRequest {
    pub pid: u64,
    pub buf: Vec<u8>,
    pub done: Sender<FerricResult<()>>,
}

#[derive(Default, Debug)]
pub struct BgStats {
    pub enqueued: AtomicU64,
    pub completed: AtomicU64,
    pub saturated: AtomicU64,
    pub fallback_sync: AtomicU64,
    pub errors: AtomicU64,
    pub batches: AtomicU64,
    pub max_batch: AtomicU64,
    pub retries: AtomicU64,
    pub errors_enospc: AtomicU64,
    pub errors_eio: AtomicU64,
    pub wait_park: AtomicU64,
}

impl BufferManager {
    /// Eviction-side coordination: if inflight IO or BG queue is saturated, park until space frees.
    fn wait_for_bg_capacity(&self) {
        if !self.bg_write {
            return;
        }
        let limit = self.config.io_depth * self.config.io_workers;
        loop {
            let inflight = self.io_inflight_count.load(Ordering::SeqCst);
            let queue_full = self
                .bg_tx
                .as_ref()
                .map(|tx| tx.len() as u64 >= (self.batch as u64 * 2))
                .unwrap_or(false);
            if inflight < limit && !queue_full {
                return;
            }
            self.bg_stats.wait_park.fetch_add(1, Ordering::Relaxed);
            let (lock, cvar) = (&self.evict_cv.0, &self.evict_cv.1);
            let guard = lock.lock().unwrap();
            let _ = cvar.wait_timeout(guard, std::time::Duration::from_millis(1));
        }
    }

    fn classify_io_error(stats: &Arc<BgStats>, err: &(dyn std::error::Error + 'static)) {
        if let Some(ioe) = err.downcast_ref::<io::Error>() {
            if let Some(code) = ioe.raw_os_error() {
                match code {
                    libc::ENOSPC => {
                        stats.errors_enospc.fetch_add(1, Ordering::Relaxed);
                    }
                    libc::EIO => {
                        stats.errors_eio.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
        }
    }

    /// Background writer thread: receives page copies and writes them out.
    fn bg_worker(
        rx: CbReceiver<BgRequest>,
        io: Arc<dyn PageIo>,
        stats: Arc<BgStats>,
        inflight_count: Arc<AtomicU64>,
        inflight_cv: Arc<(Mutex<()>, std::sync::Condvar)>,
        evict_cv: Arc<(Mutex<()>, std::sync::Condvar)>,
        inflight_limit: u64,
    ) {
        let mut batch = Vec::with_capacity(64);
        loop {
            batch.clear();
            // blocking receive for first item
            let first = match rx.recv() {
                Ok(req) => req,
                Err(_) => break,
            };
            batch.push(first);
            // try to drain additional items without blocking
            while let Ok(req) = rx.try_recv() {
                batch.push(req);
                if batch.len() >= 64 {
                    break;
                }
            }

            // If only one, do single write_buf to avoid batch overhead
            let batch_len = batch.len();
            if batch_len == 1 {
                let req = batch.pop().unwrap();
                let res = io.write_buf(req.pid, &req.buf);
                let _ = req.done.send(res);
                stats.completed.fetch_add(1, Ordering::Relaxed);
                evict_cv.1.notify_all();
                inflight_cv.1.notify_all();
                continue;
            }

            // Batch write using contiguous buffers is not available; write_pages on pids order.
            // We sort by pid to improve sequential IO locality.
            batch.sort_by_key(|r| r.pid);
            // Try batch write_pages when buffers are page sized and contiguous in memory order of pids.
            let first_pid = batch.first().map(|r| r.pid).unwrap_or(0);
            let mut contiguous = true;
            for (idx, req) in batch.iter().enumerate() {
                if req.pid != first_pid + idx as u64 {
                    contiguous = false;
                    break;
                }
            }
            if contiguous {
                // Attempt async batch if supported and inflight under limit.
                let mut bufs: Vec<(u64, Vec<u8>)> = Vec::with_capacity(batch_len);
                for (idx, req) in batch.iter().enumerate() {
                    bufs.push((first_pid + idx as u64, req.buf.clone()));
                }
                if let Some(rx) = io.write_pages_async(bufs) {
                    let needed = batch_len as u64;
                    // Park until inflight budget is available instead of falling back to sync.
                    Self::wait_for_inflight(
                        &inflight_count,
                        &inflight_cv,
                        inflight_limit,
                        needed,
                        &stats,
                    );
                    inflight_count.fetch_add(needed, Ordering::SeqCst);
                    let res = rx
                        .recv()
                        .unwrap_or_else(|_| Err("async channel closed".into()));
                    inflight_count.fetch_sub(needed, Ordering::SeqCst);
                    if let Err(e) = &res {
                        // retry once synchronously for hard errors
                        Self::classify_io_error(&stats, e.as_ref());
                        for req in &batch {
                            let _ = io.write_page(req.pid, req.buf.as_ptr() as *const Page);
                        }
                        stats.retries.fetch_add(1, Ordering::Relaxed);
                    }
                    for req in batch.drain(..) {
                        let mapped = res.as_ref().map(|_| ()).map_err(|e| e.to_string().into());
                        let _ = req.done.send(mapped);
                    }
                    if res.is_err() {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats
                            .completed
                            .fetch_add(batch_len as u64, Ordering::Relaxed);
                    }
                    stats.batches.fetch_add(1, Ordering::Relaxed);
                    stats
                        .max_batch
                        .fetch_max(batch_len as u64, Ordering::Relaxed);
                    // notify any waiters that inflight dropped
                    inflight_cv.1.notify_all();
                    evict_cv.1.notify_all();
                    continue;
                }

                // fallback sync batch
                let mut tmp = Vec::with_capacity(batch_len * PAGE_SIZE);
                for req in &batch {
                    tmp.extend_from_slice(&req.buf);
                }
                let base_ptr = tmp.as_ptr() as *const Page;
                let pid_list: Vec<u64> = (0..batch_len).map(|i| first_pid + i as u64).collect();
                let res = io.write_pages(&pid_list, base_ptr);
                if let Err(e) = &res {
                    Self::classify_io_error(&stats, e.as_ref());
                    // partial retry per-page on failure
                    stats.retries.fetch_add(1, Ordering::Relaxed);
                    for req in batch.drain(..) {
                        let per = io.write_page(req.pid, req.buf.as_ptr() as *const Page);
                        let _ = req
                            .done
                            .send(per.as_ref().map(|_| ()).map_err(|e| e.to_string().into()));
                        if let Err(err) = &per {
                            Self::classify_io_error(&stats, err.as_ref());
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                        } else {
                            stats.completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                } else {
                    for req in batch.drain(..) {
                        let _ = req.done.send(Ok(()));
                        stats.completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else {
                let mut first_err: Option<FerricResult<()>> = None;
                for req in &batch {
                    let res = io.write_buf(req.pid, &req.buf);
                    if let Err(e) = &res {
                        Self::classify_io_error(&stats, e.as_ref());
                        if first_err.is_none() {
                            first_err = Some(Err(e.to_string().into()));
                        }
                    }
                }
                for req in batch.drain(..) {
                    if let Some(Err(ref e)) = first_err {
                        let _ = req.done.send(Err(e.to_string().into()));
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let _ = req.done.send(Ok(()));
                        stats.completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            stats.batches.fetch_add(1, Ordering::Relaxed);
            stats
                .max_batch
                .fetch_max(batch_len as u64, Ordering::Relaxed);
            inflight_cv.1.notify_all();
            evict_cv.1.notify_all();
        }
    }

    fn make_io_for_bg(config: &Config, virt_size: u64) -> Result<Arc<dyn PageIo>> {
        #[cfg(feature = "libaio")]
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&config.block_path)?;
            file.set_len(virt_size)?;
            let affinity = config.io_affinity.as_ref().map(|s| {
                s.split(',')
                    .filter_map(|t| t.trim().parse::<usize>().ok())
                    .collect::<Vec<_>>()
            });
            return LibaioIo::open(
                file,
                config.io_depth as usize,
                config.io_workers as usize,
                affinity,
            );
        }
        #[cfg(not(feature = "libaio"))]
        {
            Ok(SyncFileIo::open_with_len(&config.block_path, virt_size)? as Arc<dyn PageIo>)
        }
    }
    fn stat_index(&self) -> Option<usize> {
        let id = get_worker_id() as usize;
        self.worker_stats.as_ref().and_then(|stats| {
            if id < stats.reads.len() {
                Some(id)
            } else {
                None
            }
        })
    }

    pub(crate) fn stats_inc_read(&self) {
        if let (Some(idx), Some(stats)) = (self.stat_index(), &self.worker_stats) {
            PerWorkerStats::inc(
                &stats.reads,
                &stats.writes,
                &stats.faults,
                &stats.evicts,
                idx,
                StatKind::Read,
            );
        }
    }

    pub(crate) fn stats_inc_write(&self) {
        if let (Some(idx), Some(stats)) = (self.stat_index(), &self.worker_stats) {
            PerWorkerStats::inc(
                &stats.reads,
                &stats.writes,
                &stats.faults,
                &stats.evicts,
                idx,
                StatKind::Write,
            );
        }
    }

    pub(crate) fn stats_inc_fault(&self) {
        if let (Some(idx), Some(stats)) = (self.stat_index(), &self.worker_stats) {
            PerWorkerStats::inc(
                &stats.reads,
                &stats.writes,
                &stats.faults,
                &stats.evicts,
                idx,
                StatKind::Fault,
            );
        }
    }

    pub(crate) fn stats_inc_evict(&self) {
        if let (Some(idx), Some(stats)) = (self.stat_index(), &self.worker_stats) {
            PerWorkerStats::inc(
                &stats.reads,
                &stats.writes,
                &stats.faults,
                &stats.evicts,
                idx,
                StatKind::Evict,
            );
        }
    }

    /// Snapshot per-worker counters (if enabled).
    pub fn worker_stats(&self) -> Option<Vec<WorkerStats>> {
        self.worker_stats.as_ref().map(|s| s.snapshot())
    }

    /// BGWRITE stats helper.
    pub fn bg_stats_snapshot(&self) -> Option<BgStatsSnapshot> {
        if !self.bg_write {
            return None;
        }
        let queued = self.bg_tx.as_ref().map(|tx| tx.len() as u64).unwrap_or(0);
        let io_stats = self.io.queue_stats();
        Some(BgStatsSnapshot {
            enqueued: self.bg_stats.enqueued.load(Ordering::Relaxed),
            completed: self.bg_stats.completed.load(Ordering::Relaxed),
            saturated: self.bg_stats.saturated.load(Ordering::Relaxed),
            fallback_sync: self.bg_stats.fallback_sync.load(Ordering::Relaxed),
            errors: self.bg_stats.errors.load(Ordering::Relaxed),
            errors_enospc: self.bg_stats.errors_enospc.load(Ordering::Relaxed),
            errors_eio: self.bg_stats.errors_eio.load(Ordering::Relaxed),
            batches: self.bg_stats.batches.load(Ordering::Relaxed),
            max_batch: self.bg_stats.max_batch.load(Ordering::Relaxed),
            inflight: self.bg_inflight.lock().unwrap().len() as u64,
            queue_len: queued,
            retries: self.bg_stats.retries.load(Ordering::Relaxed),
            wait_park: self.bg_stats.wait_park.load(Ordering::Relaxed),
            io_queues: io_stats,
        })
    }

    pub fn stats_snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            reads: self.read_count.load(Ordering::Relaxed),
            writes: self.write_count.load(Ordering::Relaxed),
            phys_used: self.phys_used_count.load(Ordering::Relaxed),
            alloc: self.alloc_count.load(Ordering::Relaxed),
            worker: self.worker_stats(),
            bgwrite: self.bg_stats_snapshot(),
            exmap: ExmapSnapshot {
                requested: self.config.use_exmap,
                active: matches!(self.region_kind, RegionKind::Exmap),
                reason: self.exmap_reason.clone(),
            },
        }
    }
}

impl BufferManager {
    /// Non-blocking reap of background completions to reduce queue pressure.
    pub fn poll_bg_completions(&self) {
        let mut inflight = self.bg_inflight.lock().unwrap();
        let keys: Vec<u64> = inflight.keys().copied().collect();
        let mut finished = Vec::new();
        for pid in keys {
            if let Some(rx) = inflight.get(&pid) {
                match rx.try_recv() {
                    Ok(res) => {
                        self.handle_bg_completion(pid, res);
                        finished.push(pid);
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        self.bg_stats.errors.fetch_add(1, Ordering::Relaxed);
                        finished.push(pid);
                    }
                }
            }
        }
        for pid in finished {
            inflight.remove(&pid);
        }
    }

    /// Apply completion bookkeeping for a background write and best-effort unpin for eviction.
    fn handle_bg_completion(&self, pid: u64, res: FerricResult<()>) {
        match res {
            Ok(()) => {
                self.bg_stats.completed.fetch_add(1, Ordering::Relaxed);
                unsafe {
                    (*self.to_ptr(pid)).dirty = false;
                }
                self.write_count.fetch_add(1, Ordering::SeqCst);
                self.stats_inc_write();
            }
            Err(e) => {
                Self::classify_io_error(&self.bg_stats, e.as_ref());
                self.bg_stats.errors.fetch_add(1, Ordering::Relaxed);
                let _ = self.io.write_page(pid, self.to_ptr(pid));
            }
        }
        self.release_bg_shared_lock(pid);
        self.try_mark_if_unlocked(pid);
        // Wake eviction thread to pick up newly free inflight slots.
        self.evict_cv.1.notify_all();
        self.io_inflight_cv.1.notify_all();
    }

    /// Release the S-lock held by eviction for bgwrite, if still present.
    fn release_bg_shared_lock(&self, pid: u64) {
        let ps = self.page_state_ref(pid);
        let v = ps.load();
        let s = PageState::state(v);
        if s > 0 && s < state::LOCKED {
            ps.unlock_s();
        }
    }

    /// Best-effort remark a clean page so a subsequent eviction pass can reclaim it quickly.
    fn try_mark_if_unlocked(&self, pid: u64) {
        let ps = self.page_state_ref(pid);
        let v = ps.load();
        if PageState::state(v) == state::UNLOCKED {
            let _ = ps.try_mark(v);
        }
    }

    /// Park/backoff until inflight allows additional async submissions.
    fn wait_for_inflight(
        count: &Arc<AtomicU64>,
        cv: &Arc<(Mutex<()>, std::sync::Condvar)>,
        limit: u64,
        need: u64,
        stats: &Arc<BgStats>,
    ) {
        loop {
            let current = count.load(Ordering::SeqCst);
            if current + need <= limit {
                return;
            }
            stats.wait_park.fetch_add(1, Ordering::Relaxed);
            // Park on condvar to be woken when inflight drains or on timeout.
            let (lock, cvar) = (&cv.0, &cv.1);
            let guard = lock.lock().unwrap();
            let _ = cvar.wait_timeout(guard, std::time::Duration::from_micros(200));
        }
    }
}
