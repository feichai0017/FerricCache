# FerricCache

Rust 版 VMCache（SIGMOD’23 “Virtual-Memory Assisted Buffer Management”）实现，保持与上游 VMCache 语义一致，同时用安全的 Rust 包装、可观测性和可测性。参考上游项目：https://github.com/tuhhosg/vmcache

## 快速开始
- 依赖：Rust stable，`libaio-dev`（开启 `libaio` 功能时），Linux x86_64。
- 构建：`cargo build --release`（默认 mmap + 同步 IO）。
- 运行 TPCC：`RUNFOR=30 THREADS=1 DATASIZE=10 PHYSGB=4 VIRTGB=16 BGWRITE=0 cargo run --release --bin bench`
- 运行随机读：`RNDREAD=1 DATASIZE=1e6 THREADS=8 RUNFOR=30 cargo run --release --bin bench`
- 对齐 VMCache 输出：bench 每秒打印 `ts,tx,rmb,wmb,system,threads,datasize,workload,batch`，便于与上游 CSV 逐行对比。

## 配置（环境变量）
- `BLOCK`：块设备或文件路径，默认 `/tmp/bm`
- `VIRTGB`：虚拟内存大小（GB），默认 16
- `PHYSGB`：物理缓存大小（GB），默认 4
- `EXMAP`：非零则尝试 exmap 区域（当前若不可用会回退 mmap）
- `BATCH`：驱逐批大小（页），默认 64
- `RUNFOR`：基准运行时间（秒），默认 30
- `THREADS`：线程数，默认 1
- `DATASIZE`：TPCC 仓库数或随机读键数，默认 10
- `RNDREAD`：非零运行随机读，否则 TPCC
- `BGWRITE`：启用背景写，默认 0（关闭）
- `BGW_THREADS`：背景写 worker 数，默认 1
- `IODEPTH`：libaio 深度，默认 256
- `IO_WORKERS`：libaio 队列数，默认 1
- `IO_AFFINITY`：可选 CPU 亲和列表（逗号分隔），仅在 libaio 下最佳努力绑定
- `STATS_INTERVAL_SECS`：bench 打印间隔，默认 1 秒
- `STATS_CSV`：若设置则追加 CSV 日志（含 BGWRITE/EXMAP 信息）

## 主要特性
- 缓冲区管理：`PageState` 状态机，Clock 驱逐，批量写出，mmap 区域或 exmap 探测回退。
- IO：同步 pread/pwrite；`libaio` 功能下支持多队列异步提交、队列统计、errno 分级（EAGAIN/ETIMEDOUT/EIO/ENOSPC）和重试/回退。
- 驱逐/写出协同：BGWRITE 队列 + inflight 计数，饱和时驱逐线程 park，完成时 notify；批量/单页写并分类错误。
- 并发：乐观/共享/独占 Guard，自旋+周期性 wait/notify，worker_id 线程局部用于 IO 队列选择。
- B+Tree：vmcache 风格节点布局，插入/扫描/删除，借/合并，根 PID 保持。
- 基准：TPCC 简化版与随机读，输出与 VMCache 对齐的 CSV，附带 BGWRITE/IO 队列统计。
- 测试：覆盖 PageState、驱逐顺序、B+Tree CRUD/压力、BGWRITE 配置、worker_id 统计等。

## 与 VMCache 的差异
- exmap 模式：当前仅检测/回退，未接入 exmap ioctl 与 fault 处理。
- IO/NUMA：队列选择按 worker_id 取模，亲和是最佳努力；尚未做专属队列/NUMA 绑定和优先级。
- 锁与等待：已增加 wait/notify，但深度饥饿处理仍可优化。
- 存储管理：无 free-space/WAL/检查点（上游也留空）。

## 开发提示
- 启用 libaio：`cargo build --release --features libaio`
- 运行测试：`cargo test`
- 对比上游：`.vmcache_upstream/` 可直接 `make && THREADS=... ./vmcache`，与本项目 bench 同配置对比 CSV。

## 许可
MIT
