use std::env;

/// Environment-driven configuration mirroring the original vmcache defaults.
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage block device or file path (BLOCK).
    pub block_path: String,
    /// Virtual memory allocation in gigabytes (VIRTGB).
    pub virt_gb: u64,
    /// Physical memory allocation in gigabytes (PHYSGB).
    pub phys_gb: u64,
    /// Use exmap interface if non-zero (EXMAP).
    pub use_exmap: bool,
    /// Eviction batch size in pages (BATCH).
    pub batch: u64,
    /// Benchmark runtime in seconds (RUNFOR).
    pub run_for: u64,
    /// Thread count (THREADS).
    pub threads: u64,
    /// Dataset size interpretation depends on workload (DATASIZE).
    pub data_size: u64,
    /// Run random-read benchmark when true, otherwise TPC-C (RNDREAD).
    pub random_read: bool,
    /// Enable background write thread for eviction (BGWRITE).
    pub bg_write: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            block_path: "/tmp/bm".to_string(),
            virt_gb: 16,
            phys_gb: 4,
            use_exmap: false,
            batch: 64,
            run_for: 30,
            threads: 1,
            data_size: 10,
            random_read: false,
            bg_write: false,
        }
    }
}

impl Config {
    /// Build configuration from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        cfg.block_path = env::var("BLOCK").unwrap_or(cfg.block_path);
        cfg.virt_gb = env_or("VIRTGB", cfg.virt_gb);
        cfg.phys_gb = env_or("PHYSGB", cfg.phys_gb);
        cfg.use_exmap = env_or("EXMAP", cfg.use_exmap as u64) != 0;
        cfg.batch = env_or("BATCH", cfg.batch);
        cfg.run_for = env_or("RUNFOR", cfg.run_for);
        cfg.threads = env_or("THREADS", cfg.threads);
        cfg.data_size = env_or("DATASIZE", cfg.data_size);
        cfg.random_read = env_or("RNDREAD", cfg.random_read as u64) != 0;
        cfg.bg_write = env_or("BGWRITE", cfg.bg_write as u64) != 0;
        cfg
    }
}

fn env_or(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| parse_num(&v))
        .unwrap_or(default)
}

fn parse_num(s: &str) -> Option<u64> {
    // Allow simple floats like "1e6" by using f64 then truncating.
    s.parse::<u64>()
        .ok()
        .or_else(|| s.parse::<f64>().ok().map(|f| f as u64))
}
