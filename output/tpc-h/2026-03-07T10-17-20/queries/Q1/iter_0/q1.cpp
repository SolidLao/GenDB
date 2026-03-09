// Q1: Pricing Summary Report — GenDB iter_0
// Strategy: zone-map pruning + parallel morsel-driven scan + direct array[6] aggregation
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
// Python: (date(1998,12,1)-timedelta(90) - date(1970,1,1)).days = 10471
// Note: the guide's JDN arithmetic example contained a calculation error.
static constexpr int32_t kQ1ShipdateMax = 10471;
static constexpr int      kNumSlots      = 6;   // 3 returnflag × 2 linestatus

// ---------------------------------------------------------------------------
// Per-group accumulator
// Use long double to avoid 1-cent rounding errors when summing ~30M × $50K values.
// Each accumulator is 128-byte aligned to avoid false sharing across threads.
// ---------------------------------------------------------------------------
struct Q1Accum {
    long double sum_qty        = 0.0L;
    long double sum_base_price = 0.0L;
    long double sum_disc_price = 0.0L;
    long double sum_charge     = 0.0L;
    long double sum_disc       = 0.0L;
    int64_t     count          = 0;
};

// Thread-private accumulator array — pad to 128 bytes to avoid false sharing
struct alignas(128) ThreadAccums {
    Q1Accum slots[kNumSlots];
    char _pad[128 - (kNumSlots * sizeof(Q1Accum)) % 128];
};

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
    void*   zm_ptr = nullptr;
    size_t  zm_size = 0;
    int32_t num_blocks = 0, block_size = 0;
    const int32_t* zm_min = nullptr;

    // -----------------------------------------------------------------------
    // Phase: data_loading
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

        // Prefetch into page cache (HDD: overlap I/O with CPU setup)
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);

        // Read zone map (int32_t layout: [num_blocks][block_size][min[N]][max[N]])
        const std::string zm_path = base + "l_shipdate_zone_map.bin";
        int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) { perror("open zone_map"); return 1; }
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        zm_size = zm_st.st_size;
        zm_ptr = mmap(nullptr, zm_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_ptr == MAP_FAILED) { perror("mmap zone_map"); return 1; }
        ::close(zm_fd);

        const int32_t* zm_data = static_cast<const int32_t*>(zm_ptr);
        num_blocks = zm_data[0];
        block_size = zm_data[1];
        zm_min     = zm_data + 2;
    }

    int64_t nrows = (int64_t)col_shipdate.count;

    // -----------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven scan
    // -----------------------------------------------------------------------
    int nthreads = omp_get_max_threads();
    std::vector<ThreadAccums> thread_accums(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            Q1Accum* local = thread_accums[tid].slots;

            // Dynamic scheduling: ~600 blocks / 64 threads ≈ 9-10 blocks each
            #pragma omp for schedule(dynamic, 1) nowait
            for (int b = 0; b < num_blocks; b++) {
                // Zone map prune: skip entire block if all dates exceed threshold
                if (zm_min[b] > kQ1ShipdateMax) continue;

                int64_t row_start = (int64_t)b * block_size;
                int64_t row_end   = row_start + block_size;
                if (row_end > nrows) row_end = nrows;
                int64_t len = row_end - row_start;

                const int32_t* sd  = col_shipdate.data   + row_start;
                const int8_t*  rf  = col_returnflag.data + row_start;
                const int8_t*  ls  = col_linestatus.data + row_start;
                const double*  qty = col_quantity.data   + row_start;
                const double*  ep  = col_extprice.data   + row_start;
                const double*  dis = col_discount.data   + row_start;
                const double*  tax = col_tax.data        + row_start;

                for (int64_t i = 0; i < len; i++) {
                    if (sd[i] > kQ1ShipdateMax) continue;

                    int key = (int)rf[i] * 2 + (int)ls[i];
                    Q1Accum& g = local[key];

                    double ep_i        = ep[i];
                    double dis_i       = dis[i];
                    double disc_price  = ep_i * (1.0 - dis_i);
                    double charge      = disc_price * (1.0 + tax[i]);

                    g.sum_qty        += qty[i];
                    g.sum_base_price += ep_i;
                    g.sum_disc_price += disc_price;
                    g.sum_charge     += charge;
                    g.sum_disc       += dis_i;
                    g.count          += 1;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: merge — reduce thread-local arrays (build_joins slot)
    // -----------------------------------------------------------------------
    Q1Accum global[kNumSlots] = {};
    {
        GENDB_PHASE("build_joins");
        for (int t = 0; t < nthreads; t++) {
            for (int s = 0; s < kNumSlots; s++) {
                global[s].sum_qty        += thread_accums[t].slots[s].sum_qty;
                global[s].sum_base_price += thread_accums[t].slots[s].sum_base_price;
                global[s].sum_disc_price += thread_accums[t].slots[s].sum_disc_price;
                global[s].sum_charge     += thread_accums[t].slots[s].sum_charge;
                global[s].sum_disc       += thread_accums[t].slots[s].sum_disc;
                global[s].count          += thread_accums[t].slots[s].count;
            }
        }
    }

    munmap(zm_ptr, zm_size);

    // -----------------------------------------------------------------------
    // Phase: output — write CSV
    // -----------------------------------------------------------------------
    // Decode dicts (from ingest.cpp encode functions)
    const char rf_chars[] = {'A', 'N', 'R'};  // A=0, N=1, R=2
    const char ls_chars[] = {'F', 'O'};         // F=0, O=1

    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror("fopen output"); return 1; }

        fprintf(out,
            "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
            "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        // Slots in key order = ascending (rf=0,1,2) × (ls=0,1)
        // matches ORDER BY l_returnflag ASC, l_linestatus ASC
        for (int s = 0; s < kNumSlots; s++) {
            if (global[s].count == 0) continue;
            int rf_code = s / 2;
            int ls_code = s % 2;
            long double cnt = (long double)global[s].count;
            fprintf(out,
                "%c,%c,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%.2Lf,%lld\n",
                rf_chars[rf_code],
                ls_chars[ls_code],
                global[s].sum_qty,
                global[s].sum_base_price,
                global[s].sum_disc_price,
                global[s].sum_charge,
                global[s].sum_qty        / cnt,
                global[s].sum_base_price / cnt,
                global[s].sum_disc       / cnt,
                (long long)global[s].count);
        }

        fclose(out);
    }

    return 0;
}
