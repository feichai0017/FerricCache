use crate::memory::page::Page;
use crate::memory::{exmap::ExmapRegion, region::MmapRegion};
use crate::Result;

/// Abstraction over the backing virtual memory region (mmap or exmap).
pub enum VirtualRegion {
    Mmap(MmapRegion),
    Exmap(ExmapRegion),
}

impl VirtualRegion {
    pub fn mmap(page_count: usize) -> Result<Self> {
        Ok(Self::Mmap(MmapRegion::new(page_count)?))
    }

    pub fn exmap(page_count: usize) -> Result<Self> {
        Ok(Self::Exmap(ExmapRegion::new(page_count)?))
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut Page {
        match self {
            VirtualRegion::Mmap(r) => r.as_ptr(),
            VirtualRegion::Exmap(r) => r.as_ptr(),
        }
    }

    pub fn len_pages(&self) -> usize {
        match self {
            VirtualRegion::Mmap(r) => r.len_pages(),
            VirtualRegion::Exmap(r) => r.len_pages(),
        }
    }

    pub fn is_exmap(&self) -> bool {
        matches!(self, VirtualRegion::Exmap(_))
    }
}

/// Helper to construct a region honoring the exmap flag with graceful fallback.
pub fn create_region(page_count: usize, use_exmap: bool) -> Result<(VirtualRegion, bool)> {
    if use_exmap {
        if let Ok(region) = VirtualRegion::exmap(page_count) {
            return Ok((region, true));
        }
    }
    Ok((VirtualRegion::mmap(page_count)?, false))
}
