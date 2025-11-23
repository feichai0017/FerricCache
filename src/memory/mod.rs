pub mod page_state;
pub mod page;
pub mod region;
pub mod resident_set;
pub mod exmap;
pub mod virtual_region;
pub use region::MmapRegion;
pub use resident_set::ResidentPageSet;
pub use virtual_region::VirtualRegion;
pub use virtual_region::create_region;
pub use virtual_region::RegionKind;

/// Page size in bytes.
pub const PAGE_SIZE: usize = 4096;
