use crate::memory::page::Page;
use crate::memory::PAGE_SIZE;
use crate::Result;
use libc::{c_void, mmap, munmap, MAP_ANON, MAP_FAILED, MAP_PRIVATE, PROT_READ, PROT_WRITE};
use std::ptr::NonNull;

/// Mmap-backed region used to host page-aligned `Page` objects.
pub struct MmapRegion {
    ptr: NonNull<Page>,
    page_count: usize,
    len_bytes: usize,
}

impl MmapRegion {
    pub fn new(page_count: usize) -> Result<Self> {
        if page_count == 0 {
            return Err("page_count must be > 0".into());
        }
        let len_bytes = page_count
            .checked_mul(PAGE_SIZE)
            .ok_or("page_count too large")?;
        let raw = unsafe {
            mmap(
                std::ptr::null_mut(),
                len_bytes,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANON,
                -1,
                0,
            )
        };
        if raw == MAP_FAILED {
            return Err("mmap failed".into());
        }
        let ptr = NonNull::new(raw as *mut Page).ok_or("mmap returned null")?;
        Ok(Self {
            ptr,
            page_count,
            len_bytes,
        })
    }

    /// Base pointer to the region.
    #[inline]
    pub fn as_ptr(&self) -> *mut Page {
        self.ptr.as_ptr()
    }

    /// Number of pages in the region.
    pub fn len_pages(&self) -> usize {
        self.page_count
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        unsafe {
            let _ = munmap(self.ptr.as_ptr() as *mut c_void, self.len_bytes);
        }
    }
}
