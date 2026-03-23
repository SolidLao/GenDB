// Q1: Pricing Summary Report — GenDB iter_1
// Optimizations over iter_0:
//   1. Hybrid accumulators: double for sum_qty/sum_base_price/sum_disc — avoids x87
//      spilling for the 3 fields that don't need extra precision.
//      long double only for sum_disc_price and sum_charge — maintains correctness
//      (14.8M-row accumulated sum would lose 1 cent of precision with double).
//   2. OMP thread pool warmup placed right before main_scan to ensure threads are
//      still spinning (not sleeping) when the parallel region starts.
//   3. Three-way zone-map block classification using both zone_min and zone_max:
//      (a) zone_min[b] > threshold  → skip block entirely
//      (b) zone_max[b] <= threshold → fast path: no per-row date check
//      (c) otherwise                → slow path: scalar with per-row date guard
//   4. Static scheduling for better hardware prefetcher effectiveness.
//
// Correctness anchor (validated): charge = disc_price * (1.0 + tax[i])
//
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
// 1998-09-02 days since epoch (1998-12-01 - INTERVAL '90' DAY)
// Validated in iter_0 as producing correct output.
static constexpr int32_t kQ1ShipdateMax = 10471;
static constexpr int      kNumSlots     = 6;   // 3 returnflag × 2 linestatus

// ---------------------------------------------------------------------------
// Per-group accumulator — hybrid precision.
// double for sum_qty, sum_base_price, sum_disc: values scale well with double.
// long double for sum_disc_price, sum_charge: needed to avoid 1-cent error in
//   the 14.8M-row accumulated sum at ~$50K per row (~$5.4×10^11 total).
//
// Layout on x86-64 (long double = 80-bit stored in 16 bytes, 16-byte aligned):
//   offset  0: double sum_qty        (8 bytes)
//   offset  8: double sum_base_price (8 bytes)
//   offset 16: long double sum_disc_price (16 bytes)
//   offset 32: long double sum_charge     (16 bytes)
//   offset 48: double sum_disc       (8 bytes)
//   offset 56: int64_t count         (8 bytes)
//   Total: 64 bytes/slot — exactly 1 cache line
// ---------------------------------------------------------------------------
struct Q1Accum {
    double      sum_qty        = 0.0;
    double      sum_base_price = 0.0;
    long double sum_disc_price = 0.0L;
    long double sum_charge     = 0.0L;
    double      sum_disc       = 0.0;
    int64_t     count          = 0;
};
static_assert(sizeof(Q1Accum) == 64, "Q1Accum must be 64 bytes (1 cache line)");

// Thread-private accumulator array.
// 6 slots × 64 bytes = 384 bytes = 6 cache lines.
// alignas(64) ensures no false sharing with other per-thread data.
struct alignas(64) ThreadAccums {
    Q1Accum slots[kNumSlots];   // 384 bytes
};
static_assert(sizeof(ThreadAccums) == 384, "ThreadAccums must be 384 bytes");

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE_MS("total", ms_total);

    const std::string base = gendb_dir + "/lineitem/";

    // -----------------------------------------------------------------------
    // Declare columns at outer scope so they outlive phase blocks
    // -----------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int8_t>  col_returnflag;
    gendb::MmapColumn<int8_t>  col_linestatus;
    gendb::MmapColumn<double>  col_quantity;
    gendb::MmapColumn<double>  col_extprice;
    gendb::MmapColumn<double>  col_discount;
    gendb::MmapColumn<double>  col_tax;

    void*          zm_ptr     = nullptr;
    size_t         zm_size    = 0;
    int32_t        num_blocks = 0;
    int32_t        block_size = 0;
    const int32_t* zm_min     = nullptr;
    const int32_t* zm_max     = nullptr;

    // -----------------------------------------------------------------------
    // Phase: data_loading — open columns, fire async prefetch, load zone map
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("data_loading");

        col_shipdate.open   (base + "l_shipdate.bin");
        col_returnflag.open (base + "l_returnflag.bin");
        col_linestatus.open (base + "l_linestatus.bin");
        col_quantity.open   (base + "l_quantity.bin");
        col_extprice.open   (base + "l_extendedprice.bin");
        col_discount.open   (base + "l_discount.bin");
        col_tax.open        (base + "l_tax.bin");

        // Fire async prefetch into page cache (HDD: overlap I/O with CPU setup)
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);

        // Read zone map (int32_t layout: [num_blocks][block_size][min[N]][max[N]])
        const std::string zm_path = base + "l_shipdate_zone_map.bin";
        int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) { perror("open zone_map"); return 1; }
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        zm_size = zm_st.st_size;
        zm_ptr  = mmap(nullptr, zm_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_ptr == MAP_FAILED) { perror("mmap zone_map"); return 1; }
        ::close(zm_fd);

        const int32_t* zm_data = static_cast<const int32_t*>(zm_ptr);
        num_blocks = zm_data[0];
        block_size = zm_data[1];
        zm_min     = zm_data + 2;
        zm_max     = zm_data + 2 + num_blocks;
    }

    int64_t nrows    = (int64_t)col_shipdate.count;
    int     nthreads = omp_get_max_threads();

    std::vector<ThreadAccums> thread_accums(nthreads);

    // -----------------------------------------------------------------------
    // OMP warmup: force thread pool creation right before main_scan.
    // Placing it here (not in data_loading) ensures threads are still spinning
    // when the parallel scan region starts, eliminating OS wakeup latency.
    // -----------------------------------------------------------------------
    #pragma omp parallel num_threads(nthreads)
    { /* warmup — force thread creation and keep threads warm */ }

    // -----------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven scan with three-way zone classification
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int      tid   = omp_get_thread_num();
            Q1Accum* local = thread_accums[tid].slots;

            // Static scheduling: ~600 blocks / 64 threads ≈ 9 consecutive blocks/thread.
            // Consecutive block assignment improves hardware prefetcher effectiveness.
            #pragma omp for schedule(static) nowait
            for (int b = 0; b < num_blocks; b++) {

                // (a) Skip: entire block is after threshold
                if (zm_min[b] > kQ1ShipdateMax) continue;

                int64_t row_start = (int64_t)b * block_size;
                int64_t row_end   = row_start + block_size;
                if (row_end > nrows) row_end = nrows;
                int64_t len = row_end - row_start;

                const int32_t* __restrict__ sd  = col_shipdate.data   + row_start;
                const int8_t*  __restrict__ rf  = col_returnflag.data + row_start;
                const int8_t*  __restrict__ ls  = col_linestatus.data + row_start;
                const double*  __restrict__ qty = col_quantity.data   + row_start;
                const double*  __restrict__ ep  = col_extprice.data   + row_start;
                const double*  __restrict__ dis = col_discount.data   + row_start;
                const double*  __restrict__ tax = col_tax.data        + row_start;

                if (zm_max[b] <= kQ1ShipdateMax) {
                    // (b) Fast path: ALL rows in this block pass the date filter.
                    // No per-row date branch. ~588 of 600 blocks take this path.
                    for (int64_t i = 0; i < len; i++) {
                        int     key        = (int)rf[i] * 2 + (int)ls[i];
                        double  ep_i       = ep[i];
                        double  dis_i      = dis[i];
                        double  disc_price = ep_i * (1.0 - dis_i);
                        double  charge     = disc_price * (1.0 + tax[i]);

                        local[key].sum_qty        += qty[i];
                        local[key].sum_base_price += ep_i;
                        local[key].sum_disc_price += disc_price;
                        local[key].sum_charge     += charge;
                        local[key].sum_disc       += dis_i;
                        local[key].count          += 1;
                    }
                } else {
                    // (c) Slow path: boundary block — some rows may exceed threshold.
                    // Per-row date guard required. Scalar loop.
                    for (int64_t i = 0; i < len; i++) {
                        if (sd[i] > kQ1ShipdateMax) continue;

                        int     key        = (int)rf[i] * 2 + (int)ls[i];
                        double  ep_i       = ep[i];
                        double  dis_i      = dis[i];
                        double  disc_price = ep_i * (1.0 - dis_i);
                        double  charge     = disc_price * (1.0 + tax[i]);

                        local[key].sum_qty        += qty[i];
                        local[key].sum_base_price += ep_i;
                        local[key].sum_disc_price += disc_price;
                        local[key].sum_charge     += charge;
                        local[key].sum_disc       += dis_i;
                        local[key].count          += 1;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: build_joins — reduce thread-local accumulators into global result
    // -----------------------------------------------------------------------
    alignas(64) Q1Accum global_accum[kNumSlots] = {};
    {
        GENDB_PHASE("build_joins");
        for (int t = 0; t < nthreads; t++) {
            for (int s = 0; s < kNumSlots; s++) {
                global_accum[s].sum_qty        += thread_accums[t].slots[s].sum_qty;
                global_accum[s].sum_base_price += thread_accums[t].slots[s].sum_base_price;
                global_accum[s].sum_disc_price += thread_accums[t].slots[s].sum_disc_price;
                global_accum[s].sum_charge     += thread_accums[t].slots[s].sum_charge;
                global_accum[s].sum_disc       += thread_accums[t].slots[s].sum_disc;
                global_accum[s].count          += thread_accums[t].slots[s].count;
            }
        }
    }

    munmap(zm_ptr, zm_size);

    // -----------------------------------------------------------------------
    // Phase: output — write CSV
    // -----------------------------------------------------------------------
    // Decode dicts (from ingest.cpp encode functions):
    //   encode_returnflag: A=0, N=1, R=2 (alphabetical order)
    //   encode_linestatus: F=0, O=1 (alphabetical order)
    const char rf_chars[] = {'A', 'N', 'R'};
    const char ls_chars[] = {'F', 'O'};

    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror("fopen output"); return 1; }

        fprintf(out,
            "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
            "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        // Slots are in key order: key = rf*2 + ls, so iteration order is
        // (rf=0,ls=0), (rf=0,ls=1), (rf=1,ls=0), ... = ascending alphabetical.
        // This matches ORDER BY l_returnflag ASC, l_linestatus ASC — no sort needed.
        for (int s = 0; s < kNumSlots; s++) {
            if (global_accum[s].count == 0) continue;
            int    rf_code = s / 2;
            int    ls_code = s % 2;
            double cnt     = (double)global_accum[s].count;
            fprintf(out,
                "%c,%c,%.2f,%.2f,%.2Lf,%.2Lf,%.2f,%.2f,%.2f,%lld\n",
                rf_chars[rf_code],
                ls_chars[ls_code],
                global_accum[s].sum_qty,
                global_accum[s].sum_base_price,
                global_accum[s].sum_disc_price,    // long double -> %.2Lf
                global_accum[s].sum_charge,         // long double -> %.2Lf
                global_accum[s].sum_qty        / cnt,
                global_accum[s].sum_base_price / cnt,
                global_accum[s].sum_disc       / cnt,
                (long long)global_accum[s].count);
        }

        fclose(out);
    }

    return 0;
}
