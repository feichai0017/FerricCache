use ferric_cache::buffer_manager::BufferManager;
use ferric_cache::btree::tree::BTree;
use ferric_cache::config::Config;
use ferric_cache::thread_local::set_worker_id;
use std::env;
use rand::distributions::{Distribution, Uniform};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

fn main() -> ferric_cache::Result<()> {
    let mut cfg = Config::from_env();
    // Only override datasize when env not provided; otherwise respect user value.
    let datasize_env = env::var("DATASIZE").ok();
    if datasize_env.is_none() && cfg.data_size == Config::default().data_size {
        cfg.data_size = 100_000; // leaner default for quick runs
    }
    if env::var("VIRTGB").is_err() && cfg.virt_gb == Config::default().virt_gb {
        cfg.virt_gb = 1;
    }
    if env::var("PHYSGB").is_err() && cfg.phys_gb == Config::default().phys_gb {
        cfg.phys_gb = 1;
    }
    if cfg.threads == 0 {
        cfg.threads = 1;
    }

    let bm = Arc::new(BufferManager::new(cfg.clone())?);
    let tree = BTree::new(bm.clone())?;

    println!(
        "random-read bench: keys={}, threads={}, run_for={}s, phys_gb={}, virt_gb={}",
        cfg.data_size, cfg.threads, cfg.run_for, cfg.phys_gb, cfg.virt_gb
    );

    // Optional periodic stats printer (env STATS_INTERVAL_SECS).
    let stop = Arc::new(AtomicBool::new(false));
    if let Some(intv) = env::var("STATS_INTERVAL_SECS").ok().and_then(|v| v.parse::<u64>().ok()) {
        let bm_stats = bm.clone();
        let stop_flag = stop.clone();
        thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                let snap = bm_stats.stats_snapshot();
                let total_reads = snap.worker.as_ref().map(|w| w.iter().map(|s| s.reads).sum::<u64>()).unwrap_or(0);
                let total_faults = snap.worker.as_ref().map(|w| w.iter().map(|s| s.faults).sum::<u64>()).unwrap_or(0);
                let total_writes = snap.worker.as_ref().map(|w| w.iter().map(|s| s.writes).sum::<u64>()).unwrap_or(0);
                let total_evicts = snap.worker.as_ref().map(|w| w.iter().map(|s| s.evicts).sum::<u64>()).unwrap_or(0);
                let hit_rate = if total_reads == 0 { 0.0 } else { 1.0 - (total_faults as f64 / total_reads as f64) };
                println!(
                    "[stats] reads={}, faults={}, hit_rate={:.4}, evicts={}, writes={}, phys_used={}",
                    total_reads,
                    total_faults,
                    hit_rate,
                    total_evicts,
                    total_writes,
                    snap.phys_used,
                );
                thread::sleep(Duration::from_secs(intv));
            }
        });
    }

    // Load + run in worker threads.
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(cfg.threads as usize));
    for tid in 0..cfg.threads {
        let tree_cloned = tree.clone();
        let barrier = barrier.clone();
        let cfg_cloned = cfg.clone();
        handles.push(std::thread::spawn(move || -> ferric_cache::Result<(u64, u64)> {
            set_worker_id(tid as u16);
            if tid == 0 {
                // single-threaded load to reduce contention
                for i in 0..cfg_cloned.data_size {
                    let key = i.to_le_bytes();
                    let value = key;
                    tree_cloned.insert(&key, &value)?;
                }
            }
            // simple barrier: wait for loader to finish
            barrier.wait();

            let start = Instant::now();
            let deadline = start + Duration::from_secs(cfg_cloned.run_for);
            let mut rng = StdRng::seed_from_u64(42 + tid);
            let dist = Uniform::from(0..cfg_cloned.data_size);
            let mut ops = 0u64;
            while Instant::now() < deadline {
                let k = dist.sample(&mut rng);
                let key = k.to_le_bytes();
                let ok = tree_cloned.lookup(&key, |_| {});
                assert!(ok, "key should exist");
                ops += 1;
            }
            let elapsed = Instant::now() - start;
            Ok((ops, elapsed.as_micros() as u64))
        }));
    }

    let mut total_ops = 0u64;
    let mut max_time_us = 0u64;
    let mut per_thread: Vec<(u64, u64)> = Vec::with_capacity(handles.len());
    for h in handles {
        let (ops, time_us) = h.join().unwrap()?;
        total_ops += ops;
        max_time_us = max_time_us.max(time_us);
        per_thread.push((ops, time_us));
    }

    let stats = bm.stats_snapshot();
    let elapsed_s = max_time_us as f64 / 1_000_000.0;
    let total_faults = stats
        .worker
        .as_ref()
        .map(|w| w.iter().map(|s| s.faults).sum::<u64>())
        .unwrap_or(0);
    let total_writes = stats
        .worker
        .as_ref()
        .map(|w| w.iter().map(|s| s.writes).sum::<u64>())
        .unwrap_or(0);
    let total_evicts = stats
        .worker
        .as_ref()
        .map(|w| w.iter().map(|s| s.evicts).sum::<u64>())
        .unwrap_or(0);
    let hit_rate = if total_ops == 0 { 0.0 } else { 1.0 - (total_faults as f64 / total_ops as f64) };
    println!(
        "threads={}, ops_total={}, throughput={:.2} ops/s, faults={}, hit_rate={:.4}, evicts={}, writes={}",
        cfg.threads,
        total_ops,
        total_ops as f64 / elapsed_s,
        total_faults,
        hit_rate,
        total_evicts,
        total_writes,
    );
    stop.store(true, Ordering::Relaxed);
    for (i, (ops, time_us)) in per_thread.iter().enumerate() {
        let tput = if *time_us == 0 { 0.0 } else { *ops as f64 / (*time_us as f64 / 1_000_000.0) };
        println!("thread {}: ops={}, throughput={:.2} ops/s", i, ops, tput);
    }
    if let Some(bg) = stats.bgwrite {
        println!(
            "bgwrite: enq={}, done={}, saturated={}, fallback_sync={}, errors={}, batches={}, max_batch={}",
            bg.enqueued, bg.completed, bg.saturated, bg.fallback_sync, bg.errors, bg.batches, bg.max_batch
        );
    }
    Ok(())
}
