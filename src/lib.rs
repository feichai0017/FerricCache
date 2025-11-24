pub mod btree;
pub mod buffer_manager;
pub mod config;
pub mod ffi;
pub mod guard;
pub mod io;
pub mod memory;
pub mod thread_local;

/// Shared result type for fallible operations.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub use buffer_manager::{
    BgStatsSnapshot, BufferManager, ExmapSnapshot, StatsSnapshot, WorkerStats,
};
pub use config::Config;

#[cfg(test)]
mod tests;
