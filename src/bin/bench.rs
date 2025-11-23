use ferric_cache::buffer_manager::BufferManager;
use ferric_cache::btree::tree::BTree;
use ferric_cache::config::Config;
use ferric_cache::memory::RegionKind;
use ferric_cache::thread_local::set_worker_id;
use rand::distributions::{Distribution, Uniform, WeightedIndex};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use std::thread;

fn main() -> ferric_cache::Result<()> {
    let mut cfg = Config::from_env();
    // Only override datasize when env not provided; otherwise respect user value.
    let datasize_env = env::var("DATASIZE").ok();
    if datasize_env.is_none() && cfg.data_size == Config::default().data_size {
        cfg.data_size = 10; // warehouses for TPCC by default
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

    let exmap_tag = if bm.region_kind == RegionKind::Exmap { "exmap" } else { "mmap" };
    if cfg.random_read {
        run_random_read(cfg, bm, tree, exmap_tag)?;
    } else {
        run_tpcc(cfg, bm, tree, exmap_tag)?;
    }
    Ok(())
}

fn run_random_read(cfg: Config, bm: Arc<BufferManager>, tree: BTree, exmap_tag: &str) -> ferric_cache::Result<()> {
    println!(
        "random-read bench: keys={}, threads={}, run_for={}s, phys_gb={}, virt_gb={}, region={}, bgwrite={}",
        cfg.data_size, cfg.threads, cfg.run_for, cfg.phys_gb, cfg.virt_gb, exmap_tag, cfg.bg_write
    );

    // Optional periodic stats printer (env STATS_INTERVAL_SECS).
    let stop = Arc::new(AtomicBool::new(false));
    if let Some(intv) = env::var("STATS_INTERVAL_SECS").ok().and_then(|v| v.parse::<u64>().ok()) {
        let bm_stats = bm.clone();
        let stop_flag = stop.clone();
        thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                // reap bgwrite completions even when eviction not running
                bm_stats.poll_bg_completions();
                let snap = bm_stats.stats_snapshot();
                let total_reads = snap.worker.as_ref().map(|w| w.iter().map(|s| s.reads).sum::<u64>()).unwrap_or(0);
                let total_faults = snap.worker.as_ref().map(|w| w.iter().map(|s| s.faults).sum::<u64>()).unwrap_or(0);
                let total_writes = snap.worker.as_ref().map(|w| w.iter().map(|s| s.writes).sum::<u64>()).unwrap_or(0);
                let total_evicts = snap.worker.as_ref().map(|w| w.iter().map(|s| s.evicts).sum::<u64>()).unwrap_or(0);
                let hit_rate = if total_reads == 0 { 0.0 } else { 1.0 - (total_faults as f64 / total_reads as f64) };
                let bg = snap.bgwrite.unwrap_or_default();
                println!(
                    "[stats] reads={}, faults={}, hit_rate={:.4}, evicts={}, writes={}, phys_used={}, bg_queue={}, bg_inflight={}, bg_done={}, bg_enq={}, bg_err={}",
                    total_reads,
                    total_faults,
                    hit_rate,
                    total_evicts,
                    total_writes,
                    snap.phys_used,
                    bg.queue_len,
                    bg.inflight,
                    bg.completed,
                    bg.enqueued,
                    bg.errors,
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
            "bgwrite: enq={}, done={}, saturated={}, fallback_sync={}, errors={}, batches={}, max_batch={}, queue={}, inflight={}",
            bg.enqueued, bg.completed, bg.saturated, bg.fallback_sync, bg.errors, bg.batches, bg.max_batch, bg.queue_len, bg.inflight
        );
    }
    println!(
        "exmap: requested={}, active={}, reason={:?}",
        stats.exmap.requested, stats.exmap.active, stats.exmap.reason
    );
    Ok(())
}

/// TPCC-like mixed workload (simplified) using the B+Tree as backing store.
fn run_tpcc(cfg: Config, bm: Arc<BufferManager>, tree: BTree, exmap_tag: &str) -> ferric_cache::Result<()> {
    let warehouses = cfg.data_size; // interpret datasize as number of warehouses
    let districts_per_wh = 10u64;
    let customers_per_dist = 100u64;
    println!(
        "tpcc bench: wh={}, districts/wh={}, customers/dist={}, threads={}, run_for={}s, phys_gb={}, virt_gb={}, region={}, bgwrite={}",
        warehouses, districts_per_wh, customers_per_dist, cfg.threads, cfg.run_for, cfg.phys_gb, cfg.virt_gb, exmap_tag, cfg.bg_write
    );

    // Preload base tables (customer balances).
    {
        for w in 0..warehouses {
            for d in 0..districts_per_wh {
                for c in 0..customers_per_dist {
                    let key = customer_key(w, d, c);
                    let value = 1_000u64.to_le_bytes();
                    tree.insert(&key, &value)?;
                }
            }
        }
    }

    // Optional periodic stats printer (env STATS_INTERVAL_SECS).
    let stop = Arc::new(AtomicBool::new(false));
    if let Some(intv) = env::var("STATS_INTERVAL_SECS").ok().and_then(|v| v.parse::<u64>().ok()) {
        let bm_stats = bm.clone();
        let stop_flag = stop.clone();
        thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                bm_stats.poll_bg_completions();
                let snap = bm_stats.stats_snapshot();
                let total_reads = snap.worker.as_ref().map(|w| w.iter().map(|s| s.reads).sum::<u64>()).unwrap_or(0);
                let total_faults = snap.worker.as_ref().map(|w| w.iter().map(|s| s.faults).sum::<u64>()).unwrap_or(0);
                let total_writes = snap.worker.as_ref().map(|w| w.iter().map(|s| s.writes).sum::<u64>()).unwrap_or(0);
                let total_evicts = snap.worker.as_ref().map(|w| w.iter().map(|s| s.evicts).sum::<u64>()).unwrap_or(0);
                let hit_rate = if total_reads == 0 { 0.0 } else { 1.0 - (total_faults as f64 / total_reads as f64) };
                let bg = snap.bgwrite.unwrap_or_default();
                println!(
                    "[stats] reads={}, faults={}, hit_rate={:.4}, evicts={}, writes={}, phys_used={}, bg_queue={}, bg_inflight={}, bg_done={}, bg_enq={}, bg_err={}",
                    total_reads,
                    total_faults,
                    hit_rate,
                    total_evicts,
                    total_writes,
                    snap.phys_used,
                    bg.queue_len,
                    bg.inflight,
                    bg.completed,
                    bg.enqueued,
                    bg.errors,
                );
                thread::sleep(Duration::from_secs(intv));
            }
        });
    }

    // Run workload across workers.
    let barrier = Arc::new(Barrier::new(cfg.threads as usize));
    let mut handles = Vec::new();
    for tid in 0..cfg.threads {
        let tree_cloned = tree.clone();
        let barrier = barrier.clone();
        let cfg_cloned = cfg.clone();
        handles.push(thread::spawn(move || -> ferric_cache::Result<(TpccCounters, u64)> {
            set_worker_id(tid as u16);
            barrier.wait();
            let mut rng = StdRng::seed_from_u64(99 + tid);
            let mix = WeightedIndex::new(&[45, 43, 4, 4, 4]).unwrap(); // new_order, payment, order_status, stock_level, delivery
            let warehouses = cfg_cloned.data_size;
            let cust_dist = Uniform::from(0..customers_per_dist);
            let dist_dist = Uniform::from(0..districts_per_wh);
            let wh_dist = Uniform::from(0..warehouses);
            let mut counters = TpccCounters::default();
            let start = Instant::now();
            let deadline = start + Duration::from_secs(cfg_cloned.run_for);
            let mut next_order_id: u64 = 1;
            while Instant::now() < deadline {
                match mix.sample(&mut rng) {
                    0 => {
                        // new order: insert order row
                        let wh = wh_dist.sample(&mut rng);
                        let dist = dist_dist.sample(&mut rng);
                        let key = order_key(wh, dist, next_order_id);
                        let payload = next_order_id.to_le_bytes();
                        let _ = tree_cloned.insert(&key, &payload);
                        next_order_id = next_order_id.wrapping_add(1);
                        counters.new_order += 1;
                    }
                    1 => {
                        // payment: update customer balance (decrement)
                        let wh = wh_dist.sample(&mut rng);
                        let dist = dist_dist.sample(&mut rng);
                        let cust = cust_dist.sample(&mut rng);
                        let key = customer_key(wh, dist, cust);
                        let mut found = false;
                        let _ = tree_cloned.lookup(&key, |p| {
                            let mut val_arr = [0u8; 8];
                            val_arr.copy_from_slice(&p[..8]);
                            let mut balance = u64::from_le_bytes(val_arr);
                            balance = balance.saturating_sub(1);
                            let _ = tree_cloned.insert(&key, &balance.to_le_bytes());
                            found = true;
                        });
                        if !found {
                            // initialize if missing
                            let _ = tree_cloned.insert(&key, &1_000u64.to_le_bytes());
                        }
                        counters.payment += 1;
                    }
                    2 => {
                        // order status: lookup customer
                        let wh = wh_dist.sample(&mut rng);
                        let dist = dist_dist.sample(&mut rng);
                        let cust = cust_dist.sample(&mut rng);
                        let key = customer_key(wh, dist, cust);
                        let _ = tree_cloned.lookup(&key, |_| {});
                        counters.order_status += 1;
                    }
                    3 => {
                        // stock level: scan a small range of orders
                        let wh = wh_dist.sample(&mut rng);
                        let dist = dist_dist.sample(&mut rng);
                        let start_key = order_key(wh, dist, 0);
                        let mut seen = 0;
                        tree_cloned.scan_asc(&start_key, |_k, _p| {
                            seen += 1;
                            seen < 20 // stop after small batch
                        });
                        counters.stock_level += 1;
                    }
                    _ => {
                        // delivery: delete last order if exists
                        if next_order_id > 0 {
                            let wh = wh_dist.sample(&mut rng);
                            let dist = dist_dist.sample(&mut rng);
                            let key = order_key(wh, dist, next_order_id.saturating_sub(1));
                            let _ = tree_cloned.delete(&key);
                        }
                        counters.delivery += 1;
                    }
                }
                counters.ops += 1;
            }
            let elapsed = Instant::now() - start;
            Ok((counters, elapsed.as_micros() as u64))
        }));
    }

    let mut total = TpccCounters::default();
    let mut max_time_us = 0u64;
    let mut per_thread: Vec<(TpccCounters, u64)> = Vec::with_capacity(handles.len());
    for h in handles {
        let (c, time_us) = h.join().unwrap()?;
        max_time_us = max_time_us.max(time_us);
        total.add(&c);
        per_thread.push((c, time_us));
    }

    let stats = bm.stats_snapshot();
    let elapsed_s = max_time_us as f64 / 1_000_000.0;
    println!(
        "tpcc summary: threads={}, ops_total={}, throughput={:.2} ops/s (new_order={}, payment={}, order_status={}, stock_level={}, delivery={})",
        cfg.threads,
        total.ops,
        total.ops as f64 / elapsed_s,
        total.new_order,
        total.payment,
        total.order_status,
        total.stock_level,
        total.delivery,
    );
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
    println!(
        "faults={}, evicts={}, writes={}, phys_used={}, bg_queue={}",
        total_faults,
        total_evicts,
        total_writes,
        stats.phys_used,
        stats.bgwrite.as_ref().map(|b| b.queue_len).unwrap_or(0)
    );
    for (i, (c, time_us)) in per_thread.iter().enumerate() {
        let tput = if *time_us == 0 { 0.0 } else { c.ops as f64 / (*time_us as f64 / 1_000_000.0) };
        println!(
            "thread {}: ops={}, tput={:.2} (new_order={}, payment={}, order_status={}, stock_level={}, delivery={})",
            i, c.ops, tput, c.new_order, c.payment, c.order_status, c.stock_level, c.delivery
        );
    }
    if let Some(bg) = stats.bgwrite {
        println!(
            "bgwrite: enq={}, done={}, saturated={}, fallback_sync={}, errors={}, batches={}, max_batch={}, queue={}, inflight={}",
            bg.enqueued, bg.completed, bg.saturated, bg.fallback_sync, bg.errors, bg.batches, bg.max_batch, bg.queue_len, bg.inflight
        );
    }
    println!(
        "exmap: requested={}, active={}, reason={:?}",
        stats.exmap.requested, stats.exmap.active, stats.exmap.reason
    );
    stop.store(true, Ordering::Relaxed);
    Ok(())
}

#[derive(Default, Clone)]
struct TpccCounters {
    ops: u64,
    new_order: u64,
    payment: u64,
    order_status: u64,
    stock_level: u64,
    delivery: u64,
}

impl TpccCounters {
    fn add(&mut self, other: &TpccCounters) {
        self.ops += other.ops;
        self.new_order += other.new_order;
        self.payment += other.payment;
        self.order_status += other.order_status;
        self.stock_level += other.stock_level;
        self.delivery += other.delivery;
    }
}

#[inline]
fn customer_key(wh: u64, dist: u64, cust: u64) -> [u8; 8] {
    // layout: [table=1|wh|dist|cust]
    let key = (1u64 << 60) | (wh << 40) | (dist << 24) | cust;
    key.to_le_bytes()
}

#[inline]
fn order_key(wh: u64, dist: u64, order_id: u64) -> [u8; 8] {
    // layout: [table=2|wh|dist|order]
    let key = (2u64 << 60) | (wh << 40) | (dist << 24) | (order_id & 0xFFFFFF);
    key.to_le_bytes()
}
