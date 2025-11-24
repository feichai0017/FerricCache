# FerricCache

A Rust reimplementation of [VMCache (SIGMOD’23 Virtual-Memory Assisted Buffer Management)](https://github.com/tuhhosg/vmcache). Same knobs and CSV output as upstream, but with safe Rust, clearer observability, and tests.

## Quickstart
- Prereqs: Rust stable on Linux x86_64. Install `libaio-dev` if you enable the `libaio` feature.
- Build: `cargo build --release` (defaults to mmap + sync IO).
- TPCC example:  
  `RUNFOR=30 THREADS=1 DATASIZE=10 PHYSGB=4 VIRTGB=16 BGWRITE=0 cargo run --release --bin bench`
- Random read:  
  `RNDREAD=1 DATASIZE=1e6 THREADS=8 RUNFOR=30 cargo run --release --bin bench`
- Output matches VMCache CSV: `ts,tx,rmb,wmb,system,threads,datasize,workload,batch` (per-second deltas).

## Configuration (env)
- `BLOCK` `/tmp/bm` — block device/file path
- `VIRTGB` `16` — virtual memory size (GB)
- `PHYSGB` `4` — buffer pool size (GB)
- `EXMAP` `0` — non-zero to request exmap (falls back to mmap if unavailable)
- `BATCH` `64` — eviction batch size (pages)
- `RUNFOR` `30` — benchmark duration (seconds)
- `THREADS` `1` — worker threads
- `DATASIZE` `10` — TPCC warehouses or random-read keys
- `RNDREAD` `0` — non-zero to run random-read instead of TPCC
- `BGWRITE` `0` — enable background write
- `BGW_THREADS` `1` — background write workers
- `IODEPTH` `256` — libaio depth
- `IO_WORKERS` `1` — libaio queues
- `IO_AFFINITY` — optional CPU list (comma-separated), best-effort bind for libaio
- `STATS_INTERVAL_SECS` `1` — bench print interval
- `STATS_CSV` — append stats CSV (includes BGWRITE/EXMAP)

## Architecture
- `config`: environment-driven knobs shared with VMCache.
- `memory`: `Page`, `PageState` (atomic state+version), `ResidentPageSet` (clock), virtual region (mmap; exmap detection/fallback).
- `buffer_manager`: owns region/state/resident set; fix/unfix S/X, fault/alloc, batched eviction, IO submission, per-thread stats, BGWRITE queue + inflight counters, error grading (EAGAIN/ETIMEDOUT/EIO/ENOSPC).
- `io`: sync pread/pwrite; optional libaio multi-queue async with per-queue stats and retries.
- `btree`: VMCache-style node layout (prefix/fence/hints), optimistic/shared/exclusive guards, split/merge, root PID tracking.
- `bench`: TPCC-lite and random-read; VMCache-compatible CSV + optional BGWRITE/IO queue stats; periodic stats thread.
- `tests`: PageState invariants, eviction order/pressure, B+Tree CRUD/merge, BGWRITE config, worker_id stats.

## How it works (pipeline sketch)
1) **Fix/Fault**: `fix_s/fix_x` check `PageState`; if evicted, fault in (pread/exmap alloc), mark dirty on first write, stats per worker.
2) **Eviction**: Clock over resident set in batches → mark unlocked → clean -> X lock -> evict (madvise) or dirty -> S lock -> enqueue BGWRITE (or sync write). BGWRITE tracks inflight, queues, retries, error grades; completions notify condvars to unblock eviction.
3) **IO**: Sync pread/pwrite by default; libaio path batches, binds queue by `worker_id` modulo queues, tracks submit/fail/timeout/retry; errno preserved for grading.
4) **Concurrency**: Guards (O/S/X) with spin/yield and periodic wait/notify; `worker_id` thread-local for IO selection and stats.

## Differences vs VMCache
- Exmap path: only detect/rollback today; ioctl + fault handling not wired yet.
- IO/NUMA: queue choice is worker_id % queues; affinity is best-effort, no dedicated/NUMA-aware queues or priorities yet.
- Locks: wait/notify added, but long-wait starvation handling can be improved.
- Storage: no free-space/WAL/checkpoint (upstream also leaves open).

## Dev tips
- Enable libaio: `cargo build --release --features libaio`
- Run tests: `cargo test`
- Upstream comparison: build `.vmcache_upstream` with `make`; run `THREADS=... ./vmcache` and compare CSV to this bench under the same env.

## License
MIT
