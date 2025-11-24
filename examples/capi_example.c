#include "ferric.h"
#include <stdio.h>

int main() {
    FerricConfig cfg = {
        .block_path = "/tmp/bm",
        .virt_gb = 16,
        .phys_gb = 4,
        .use_exmap = 0,
        .batch = 64,
        .run_for = 0,
        .threads = 1,
        .data_size = 0,
        .random_read = 0,
        .bg_write = 0,
        .bg_write_threads = 1,
        .io_depth = 256,
        .io_workers = 1,
        .io_affinity = NULL,
    };

    FerricHandle* h = ferric_init(&cfg);
    if (!h) {
        fprintf(stderr, "ferric_init failed\n");
        return 1;
    }

    ferric_set_worker_id(0);
    uint64_t pid = ferric_alloc_page(h);
    void* p = ferric_fix_x(h, pid);
    if (!p) {
        fprintf(stderr, "fix_x failed\n");
        ferric_destroy(h);
        return 1;
    }
    /* Write some bytes into the page */
    ((char*)p)[0] = 42;
    ferric_mark_dirty(h, pid);
    ferric_unfix_x(h, pid);

    FerricStats st;
    if (ferric_stats(h, &st) == 0) {
        printf("reads=%lu writes=%lu phys_used=%lu\n",
               st.reads, st.writes, st.phys_used);
    }

    ferric_destroy(h);
    return 0;
}
