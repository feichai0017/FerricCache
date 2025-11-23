pub mod page_state;
pub mod page;
pub mod region;
pub mod resident_set;
pub use region::MmapRegion;
pub use resident_set::ResidentPageSet;

/// Page size in bytes.
pub const PAGE_SIZE: usize = 4096;
