use crate::memory::page::Page;
use crate::memory::PAGE_SIZE;
use crate::Result;
use libc::{c_void, mmap, munmap, MAP_FAILED, MAP_SHARED, PROT_READ, PROT_WRITE};
use std::ptr::NonNull;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

/// Minimal exmap-backed region shim. On Linux we attempt to mmap via /dev/exmap;
/// on other platforms we return an error so callers can fall back to mmap.
#[cfg(target_os = "linux")]
pub struct ExmapRegion {
    ptr: NonNull<Page>,
    page_count: usize,
    len_bytes: usize,
    _dev: std::fs::File,
}

#[cfg(target_os = "linux")]
impl ExmapRegion {
    pub fn new(page_count: usize) -> Result<Self> {
        if page_count == 0 {
            return Err("page_count must be > 0".into());
        }
        let len_bytes = page_count
            .checked_mul(PAGE_SIZE)
            .ok_or("page_count too large")?;
        // Open the exmap device; caller should ensure the module is loaded.
        let dev = Self::open_device()?;
        let raw = unsafe {
            mmap(
                std::ptr::null_mut(),
                len_bytes,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                dev.as_raw_fd(),
                0,
            )
        };
        if raw == MAP_FAILED {
            return Err("exmap mmap failed".into());
        }
        let ptr = NonNull::new(raw as *mut Page).ok_or("exmap mmap returned null")?;
        Ok(Self {
            ptr,
            page_count,
            len_bytes,
            _dev: dev,
        })
    }

    /// Lightweight probe to see if /dev/exmap is usable.
    pub fn probe() -> Result<()> {
        let _ = Self::open_device()?;
        Ok(())
    }

    fn open_device() -> Result<std::fs::File> {
        let dev = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/exmap")
            .map_err(|e| format!("open /dev/exmap failed: {}", e))?;
        Ok(dev)
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut Page {
        self.ptr.as_ptr()
    }

    pub fn len_pages(&self) -> usize {
        self.page_count
    }
}

#[cfg(target_os = "linux")]
impl Drop for ExmapRegion {
    fn drop(&mut self) {
        unsafe {
            let _ = munmap(self.ptr.as_ptr() as *mut c_void, self.len_bytes);
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub struct ExmapRegion;

#[cfg(not(target_os = "linux"))]
impl ExmapRegion {
    pub fn new(_page_count: usize) -> Result<Self> {
        Err("exmap is only supported on Linux".into())
    }

    pub fn probe() -> Result<()> {
        Err("exmap is only supported on Linux".into())
    }
}
