# VMCache Rust Port – Architecture and Plan

Rust reimplementation of VMCache (SIGMOD’23 Virtual-Memory Assisted Buffer Management) with a focus on fidelity to upstream semantics and safe, idiomatic Rust.

## Goals
- Match vmcache (including optional exmap) semantics while keeping unsafe confined and reviewed.
- Preserve core behaviors: page-sized IO, batched eviction, optimistic reads.
- Provide a lean surface: buffer manager + B+tree; binaries for random-read and (future) TPC-C benchmarks.
- Use snake_case naming and clear ownership/synchronization boundaries.

## Current Progress
- Buffer manager: mmap-backed space, per-page state machine, ResidentPageSet clock eviction, mark→batch-write→upgrade→free (sync by default), PID free list, per-thread stats + worker_id, Send/Sync enabled for sharing.
- IO: PageIo abstraction; sync pread/pwrite default; `libaio` feature submits batched writes (caller waits synchronously).
- Guards: GuardO/S/X on PageState versions/states with spin/yield backoff.
- B+tree: fences, prefix compression, hints; insert (split), scan, delete (borrow/merge, root shrink), duplicate overwrite; root PID via Arc<AtomicU64>, Send/Sync.
- Bench: multi-thread random-read microbenchmark (ordered load, parallel lookups, throughput + per-thread stats). Main binary initializes and prints stats.
- Tests: 19 passing unit tests covering page state/eviction/batch write, B+tree CRUD+stress, BGWRITE config stubs, worker_id, per-thread stats. `cargo test --tests --lib` green.

## Gap vs upstream VMCache
- IO/eviction: libaio path is still sync-wait; BGWRITE not truly async (thread stub only, no queued writes yet); no queue saturation/timeout handling.
- exmap: ioctl/interface pages/SEGV restart not implemented; only mmap path exists.
- Concurrency: worker_id not wired into per-thread IO resources; no park/notify; limited backoff tuning.
- B+tree: separator/fence/hint maintenance after borrow/merge can be refined; more high-pressure mixed workloads needed.
- Bench/metrics: TPCC driver and periodic stats output not ported; random-read output is minimal.
- Storage/persistence: only PID free list; no segment space management or WAL/checkpoint (as upstream also leaves TODOs).

## High-Level Architecture (target)
- `config`: parse env (`BLOCK`, `VIRTGB`, `PHYSGB`, `EXMAP`, `BATCH`, `THREADS`, `DATASIZE`, `RUNFOR`, `RNDREAD`, `BGWRITE`).
- `memory`: `Page`, `PageState` (atomic state+version), `ResidentPageSet` (open addressing clock), `MmapRegion`.
- `buffer_manager`: owns region/states/resident set; fix/unfix S/X, alloc/fault, eviction with batching, IO submission, counters, per-worker stats.
- `io`: sync file IO; libaio batch pwrite feature; future exmap path (ioctl + interface pages).
- `btree`: vmcache-like node layout (prefix/fence/hints), optimistic/shared/exclusive guards, splits/merges, metadata page PID 0.
- `bench/bin`: random-read microbenchmark (present); TPC-C driver and richer stats thread planned.

## Prioritized Roadmap
1) IO & eviction parity: real async BGWRITE (bounded queue, inflight tracking, safe fallbacks), harden libaio (async, retry/timeout), eviction stress tests under failure.
2) Concurrency model: wire worker_id into IO/exmap resources, add spin+yield+park backoff, collect per-thread throughput/hit/miss stats.
3) B+tree balancing: refine separator/fence/hint after borrow/merge; add heavier mixed insert/delete/scan tests.
4) exmap path (feature-gated): FFI, interface pages, SEGV restart strategy.
5) Benchmarks & metrics: port TPCC, enrich random-read output, periodic stats thread akin to vmcache reporting.
6) Optional storage/persistence: segment/space management; WAL/checkpoint if DB semantics are desired.

## Open Questions
- How to handle exmap faults safely in Rust (SEGV restart vs. pre-fault/EFAULT handling).
- Keep libaio or add io_uring as an alternative feature?
- Do we extend space management beyond PID free list (upstream leaves this open)?

## Acceptance Targets
- Non-exmap path passes functional tests: alloc/fault/evict, B+tree CRUD, random-read benchmark.
- TPCC run produces stable stats.
- exmap feature builds and runs on systems with the module installed.
- Page state invariants hold under stress (no leaked locks; resident count matches physUsedCount).

## High-Level Architecture (Target Rust Structure)
- `config`: Parse environment variables (`BLOCK`, `VIRTGB`, `PHYSGB`, `EXMAP`, `BATCH`, `THREADS`, `DATASIZE`, `RUNFOR`, `RNDREAD`), with defaults aligned to upstream.
- `memory`:
  - `Page` (4 KiB) wrapper with `dirty` bit.
  - `PageState`: state + version (Locked, Marked, Evicted, shared counts). Atomic 64-bit with helpers to encode/decode state/version and CAS helpers.
  - `ResidentPageSet`: open-addressing hash set with tombstones and a clock pointer for second-chance replacement.
  - `BufferManager`: owns virtual region, page states, resident set, accounting (physUsedCount, allocCount, read/write counters), and batching. Responsible for fix/unfix in S/X, allocation, page faults, eviction, and IO submission.
- `io`:
  - `LibaioInterface`: batch pwrite using `libaio` FFI; synchronous pread path.
  - `exmap`: minimal FFI bindings to `exmap.h` ioctl ABI and interface page layout; enable/disable via config.
- `btree`:
  - Node layout identical to upstream: prefix compression, fence keys, slots + heap, hint array, leaf/inner split/merge, max KV guard.
  - Optimistic latch-free read (`GuardO`), shared (`GuardS`), exclusive (`GuardX`) guards using PageState semantics; restart on version change/marked/evicted.
  - `MetaDataPage` (PID 0) storing root pointers; `BTree` operations (lookup, scanAsc/Desc, insert, remove, updateInPlace) with splitOrdered option.
- `bench`:
  - `tpcc` port of workload schema and logic; `RandomGenerator`, schema types, and transaction executor.
  - Random-read microbenchmark path (build BTree with ordered inserts then random lookups).
- `bin`:
  - `main`: sets worker thread IDs, installs optional SIGSEGV handler in exmap mode (or alternative restart strategy), spawns stat thread, runs chosen workload.

## Key Behavioral Requirements to Preserve
- Page size 4096, PID is index into `virtMem`.
- `allocPage` increments `physUsedCount`, ensures free pages (evicts if >95% of phys), assigns PID, locks X, inserts into resident set, performs exmap alloc or marks dirty.
- `fixX`/`fixS`:
  - If Evicted, fault-in (read) then return.
  - If Marked/Unlocked, attempt to lock; Shared increments count or reuses Marked->S.
  - Yield/spin with `_mm_pause` equivalent.
- Eviction:
  - Clock over resident set in batches.
  - First pass marks Unlocked pages; Marked clean -> evict candidate; Marked dirty -> try S lock, enqueue for write.
  - Batch pwrite dirty pages; then try upgrade dirty pages to X for eviction; clean up: exmap FREE or madvise DONTNEED, remove from resident set, unlock as Evicted, decrement physUsedCount.
- IO:
  - Reads: pread on blockfd or exmap pread (using per-thread interface offset).
  - Writes: libaio batch pwrite; set dirty=false before submit.
- Concurrency:
  - `workerThreadId` thread-local, used for exmap interface and libaio slots.
  - OLC restarts on version mismatch or marked/evicted transitions; destructor checks enforce restart.
  - SIGSEGV handler (exmap path) restarts if fault address is inside virtMem.
- B+tree:
  - Prefix compression and fences; split separator selection with optional truncation.
  - Leaf split ordered fast path for random-read benchmark.
  - Merge of leaves when underfull (partial, inner merge TODO in upstream).

## Rust Mapping Notes
- Atomic semantics: use `AtomicU64` with `SeqCst`/`Acquire`/`Release` mirroring C++ (explicit where needed). Preserve CAS strong/weak loops.
- Thread-locals: `std::thread_local!` for workerThreadId.
- Unsafe boundary: page buffer is raw mapped memory; BTree nodes overlay it via pointers. Encapsulate unsafe blocks and add debug assertions matching upstream.
- FFI: `libc`, `libaio-sys` (or small custom bindings), and hand-written `exmap` bindings (ioctl numbers, structs, interface pages). Keep `no_std`? → No, stay in std for now.
- Signals: Rust cannot unwind across FFI safely; prefer `sigaction` that sets a flag and longjmp-like restart or switch to pre-fault strategy in exmap mode (design decision: provide a guarded read path and handle EFAULT by restart, rather than relying on SEGV unwinding).
- Alignment: ensure `Page`/`PageState` are `#[repr(C, align(4096))]` where appropriate.
- Memory allocation: use `mmap` with MADV_NOHUGEPAGE for non-exmap; `allocHuge` equivalent for pageState/resident set.
- Testing hooks: add invariant checks (page state, resident set uniqueness) behind debug feature.

## Prioritized Roadmap to vmcache Parity / 优先级路线图
1) **IO & Eviction Parity**
   - Hardening libaio path: async batch with reliable error/timeout handling and retry; retain sync fallback.
   - Finish BGWRITE 可选异步 writer：有界队列、inflight 状态跟踪、对齐 mark→write→upgrade→free 流程；保持默认安全同步模式。
   - Add stress tests for batch write failures/retries and eviction correctness under pressure.
2) **Concurrency Model**
   - Wire `worker_thread_id` through IO/exmap + stats; per-thread IO slots.  
   - Replace纯忙等为 “spin + yield/park” 策略，降低 CPU 抢占。
3) **B+tree Balancing & Accuracy**
   - Strengthen separator/fence/hint recompute after borrow/merge; verify负载均衡。  
   - 更多混合随机插删查压力测试，含重复键/分层 split/merge 场景。
4) **Exmap Path**
   - FFI + interface pages + SIGSEGV 恢复/重启策略；feature gate 与 CI stub。  
   - mmap/exmap 代码路径统一的安全抽象与错误报告。
5) **Benchmarks & Stats**
   - 提供 binary：random-read microbenchmark、TPC-C 驱动，输出与 vmcache 类似统计（吞吐、命中、写批次）。  
   - 轻量 metrics 钩子，方便后续 flame/pprof。
6) **Storage Management & Persistence (Optional)**
   - 超出 PID free list 的空间回收/段分配；若需要 DB 级语义，再加 WAL/checkpoint。

## Open Questions / Decisions
- Exact signal/fault handling in Rust for exmap: emulate SEGV restart or pre-fault via try_read + errno check.
- libaio vs io_uring: stick to libaio for fidelity, but consider feature flag for io_uring as replacement.
- Storage free space management is TODO upstream; keep same status or add simple allocator?

## Acceptance Checklist
- Non-exmap path passes functional tests: allocation, fault-in, eviction, BTree CRUD, random-read benchmark.
- TPCC run produces stats output and stable tx progression.
- Exmap feature compiles and basic operations work on systems with module installed.
- Page state invariants hold under stress (no leaked locks, resident set count matches physUsedCount).
