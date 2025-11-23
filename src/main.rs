use ferric_cache::buffer_manager::BufferManager;
use ferric_cache::config::Config;
use ferric_cache::Result;

fn main() -> Result<()> {
    let config = Config::from_env();
    println!(
        "vmcache (rust) starting: block={}, virt_gb={}, phys_gb={}, exmap={}, threads={}, datasize={}, workload={}",
        config.block_path,
        config.virt_gb,
        config.phys_gb,
        config.use_exmap,
        config.threads,
        config.data_size,
        if config.random_read { "rndread" } else { "tpcc" }
    );

    // Build a skeleton buffer manager; full IO/eviction will follow.
    let bm = BufferManager::new(config)?;
    // register main thread as worker 0 for stats
    let wid = bm.register_worker();
    println!("registered worker id {}", wid);

    println!("Initialized buffer manager stub. Stats snapshot: {:?}", bm.stats_snapshot());
    Ok(())
}
