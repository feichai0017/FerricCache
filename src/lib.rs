pub mod config;
pub mod memory;
pub mod buffer_manager;
pub mod guard;
pub mod btree;
pub mod io;
pub mod thread_local;

/// Shared result type for fallible operations.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
mod tests;
