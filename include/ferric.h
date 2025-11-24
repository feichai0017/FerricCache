#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Enable with cargo feature "capi" (e.g., cargo build --release --features "capi"). */

typedef struct FerricHandle FerricHandle;

typedef struct FerricConfig {
    const char* block_path;   /* default: /tmp/bm */
    uint64_t virt_gb;         /* default: 16 */
    uint64_t phys_gb;         /* default: 4 */
    uint8_t use_exmap;        /* 0=mmap, 1=exmap (best-effort) */
    uint64_t batch;           /* eviction batch size (pages), default 64 */
    uint64_t run_for;         /* benchmark duration (sec), default 30 */
    uint64_t threads;         /* worker threads, default 1 */
    uint64_t data_size;       /* TPCC warehouses or rndread keys */
    uint8_t random_read;      /* 0=TPCC, 1=rndread */
    uint8_t bg_write;         /* enable background write */
    uint64_t bg_write_threads;
    uint64_t io_depth;        /* libaio depth */
    uint64_t io_workers;      /* libaio queues */
    const char* io_affinity;  /* comma-separated CPU list, best effort */
} FerricConfig;

typedef struct FerricStats {
    uint64_t reads;
    uint64_t writes;
    uint64_t phys_used;
    uint64_t alloc;
    /* BGWRITE */
    uint64_t bg_enqueued;
    uint64_t bg_completed;
    uint64_t bg_errors;
    uint64_t bg_errors_enospc;
    uint64_t bg_errors_eio;
    uint64_t bg_queue_len;
    uint64_t bg_inflight;
    uint64_t bg_retries;
    uint64_t bg_wait_park;
    /* IO queues (aggregated) */
    uint64_t io_submit;
    uint64_t io_fail;
    uint64_t io_timeout;
    uint64_t io_retries;
    /* EXMAP flags */
    uint8_t exmap_requested;
    uint8_t exmap_active;
} FerricStats;

/* Initialize; returns NULL on failure. */
FerricHandle* ferric_init(const FerricConfig* cfg);
void ferric_destroy(FerricHandle* h);

/* Thread-local worker id (used for IO queue selection/stats). */
void ferric_set_worker_id(uint16_t id);

/* Buffer manager operations. */
uint64_t ferric_alloc_page(FerricHandle* h);
void* ferric_fix_s(FerricHandle* h, uint64_t pid);
void* ferric_fix_x(FerricHandle* h, uint64_t pid);
void ferric_unfix_s(FerricHandle* h, uint64_t pid);
void ferric_unfix_x(FerricHandle* h, uint64_t pid);
void ferric_mark_dirty(FerricHandle* h, uint64_t pid);
void ferric_evict(FerricHandle* h);
void ferric_poll_bg(FerricHandle* h);

/* Stats snapshot; returns 0 on success. */
int32_t ferric_stats(FerricHandle* h, FerricStats* out);

#ifdef __cplusplus
}
#endif
