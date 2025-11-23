use super::PAGE_SIZE;

/// A page-sized buffer with a dirty marker.
///
/// We keep the same size/alignment assumptions as the C++ implementation:
/// the struct is 4 KiB large and 4 KiB aligned so that pointer arithmetic on
/// `Page` matches page boundaries.
#[repr(C, align(4096))]
pub struct Page {
    pub dirty: bool,
    /// Padding to reach exactly one page.
    pub _padding: [u8; PAGE_SIZE - 1],
}

impl Page {
    pub const fn new() -> Self {
        Self {
            dirty: false,
            _padding: [0u8; PAGE_SIZE - 1],
        }
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}
