use crate::buffer_manager::BufferManager;
use crate::memory::page::Page;
use crate::memory::page_state::{state, PageState};
use crate::Result;
use std::marker::PhantomData;
use std::ptr::NonNull;

/// Error used to signal optimistic read restart.
#[derive(Debug)]
pub struct Restart;

impl std::fmt::Display for Restart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "optimistic restart")
    }
}

impl std::error::Error for Restart {}

pub trait PageCast {
    fn as_ptr(&self) -> *mut Page;
}

impl PageCast for Page {
    fn as_ptr(&self) -> *mut Page {
        self as *const Page as *mut Page
    }
}

/// Optimistic guard that tracks version and validates on drop.
pub struct GuardO<'a, T> {
    bm: &'a BufferManager,
    pub pid: u64,
    pub ptr: NonNull<T>,
    pub version: u64,
}

impl<'a, T> GuardO<'a, T> {
    pub fn new(bm: &'a BufferManager, pid: u64) -> Result<Self> {
        let ps = bm.page_state_ref(pid);
        loop {
            let v = ps.load();
            match PageState::state(v) {
                state::MARKED => {
                    let new_v = PageState::with_same_version(v, state::UNLOCKED);
                    let _ = ps.try_mark(new_v); // best-effort clear
                }
                state::LOCKED => {}
                state::EVICTED => {
                    if ps.try_lock_x(v) {
                        bm.handle_fault(pid);
                        bm.unfix_x(pid);
                    }
                }
                _ => {
                    let ptr = unsafe { NonNull::new_unchecked(bm.to_ptr(pid) as *mut T) };
                    bm.stats_inc_read();
                    return Ok(Self {
                        bm,
                        pid,
                        ptr,
                        version: v,
                    });
                }
            }
            spin();
        }
    }

    pub fn check(&self) -> Result<()> {
        let ps = self.bm.page_state_ref(self.pid);
        let cur = ps.load();
        if self.version == cur {
            return Ok(());
        }
        if (cur << 8) == (self.version << 8) {
            let s = PageState::state(cur);
            if s <= state::MAX_SHARED {
                return Ok(());
            }
            if s == state::MARKED {
                let _ = self
                    .bm
                    .page_state_ref(self.pid)
                    .try_mark(PageState::with_same_version(cur, state::UNLOCKED));
                return Ok(());
            }
        }
        Err(Box::new(Restart))
    }

    pub fn ptr(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }
}

impl<'a, T> Drop for GuardO<'a, T> {
    fn drop(&mut self) {
        // Ignore restart error here; caller should check explicitly if needed.
        let _ = self.check();
    }
}

pub struct GuardX<'a, T> {
    bm: &'a BufferManager,
    pub pid: u64,
    pub ptr: NonNull<T>,
}

impl<'a, T> GuardX<'a, T> {
    pub fn new_from_pid(bm: &'a BufferManager, pid: u64) -> Self {
        let ptr = bm.fix_x(pid) as *mut T;
        Self {
            bm,
            pid,
            ptr: NonNull::new(ptr).expect("null page ptr"),
        }
    }

    pub fn new_from_optimistic(guard: GuardO<'a, T>) -> Result<Self> {
        guard.check()?;
        let bm = guard.bm;
        let pid = guard.pid;
        std::mem::forget(guard);
        let ptr = bm.fix_x(pid) as *mut T;
        Ok(Self {
            bm,
            pid,
            ptr: NonNull::new(ptr).expect("null page ptr"),
        })
    }

    pub fn ptr(&mut self) -> &mut T {
        unsafe { self.ptr.as_mut() }
    }
}

impl<'a, T> Drop for GuardX<'a, T> {
    fn drop(&mut self) {
        self.bm.unfix_x(self.pid);
    }
}

pub struct GuardS<'a, T> {
    bm: &'a BufferManager,
    pid: u64,
    ptr: NonNull<T>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> GuardS<'a, T> {
    pub fn new_from_pid(bm: &'a BufferManager, pid: u64) -> Self {
        let ptr = bm.fix_s(pid) as *mut T;
        Self {
            bm,
            pid,
            ptr: NonNull::new(ptr).expect("null page ptr"),
            _marker: PhantomData,
        }
    }

    pub fn new_from_optimistic(guard: GuardO<'a, T>) -> Result<Self> {
        guard.check()?;
        let bm = guard.bm;
        let pid = guard.pid;
        std::mem::forget(guard);
        let ptr = bm.fix_s(pid) as *mut T;
        Ok(Self {
            bm,
            pid,
            ptr: NonNull::new(ptr).expect("null page ptr"),
            _marker: PhantomData,
        })
    }

    pub fn ptr(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }
}

impl<'a, T> Drop for GuardS<'a, T> {
    fn drop(&mut self) {
        self.bm.unfix_s(self.pid);
    }
}

#[inline]
fn spin() {
    thread_local! {
        static SPIN_COUNTER: std::cell::Cell<u32> = std::cell::Cell::new(0);
    }
    SPIN_COUNTER.with(|c| {
        let v = c.get().wrapping_add(1);
        c.set(v);
        if v & 0x3FF == 0 {
            std::thread::sleep(std::time::Duration::from_nanos(50));
        } else if v & 0x7F == 0 {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    });
}
