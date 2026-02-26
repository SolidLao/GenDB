// Q1: Pricing Summary Report
// Strategy: zone-map-pruned morsel-driven parallel scan with thread-local accumulators
// Zonemap at indexes/lineitem_shipdate_zonemap.bin:
//   uint64_t n_blocks; {int32_t min_val, int32_t max_val}[n_blocks]
// Since table is sorted ascending by l_shipdate, once min_val > CUTOFF we early-terminate.

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <array>
#include <fstream>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

static constexpr int32_t CUTOFF     = 10471;   // l_shipdate <= 1998-09-02
static constexpr int      MAX_GROUPS = 6;       // returnflag(3) x linestatus(2)
static constexpr size_t   BLOCK_SIZE = 100000;

// alignas(64) prevents false sharing between thread-local accumulator arrays
// Use long double for product-based sums to avoid precision loss over ~30M rows
struct alignas(64) Accum {
    int64_t     cnt            = 0;
    double      sum_qty        = 0.0;
    double      sum_price      = 0.0;
    long double sum_disc_price = 0.0L;
    long double sum_charge     = 0.0L;
    double      sum_disc       = 0.0;
};

// Load a dict file: one string per line, code = line index
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    if (!f) throw std::runtime_error("Cannot open dict: " + path);
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    std::string gendb_dir   = argv[1];
    std::string results_dir = argv[2];

    // Ensure results directory exists
    system(("mkdir -p " + results_dir).c_str());

    std::string li = gendb_dir + "/lineitem/";

    // -------------------------------------------------------------------------
    // Phase: data_loading — mmap columns + load dicts
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    // Load dictionary files (maps code -> string for output)
    auto rf_dict = load_dict(li + "l_returnflag.dict");   // A=0, N=1, R=2
    auto ls_dict = load_dict(li + "l_linestatus.dict");   // F=0, O=1

    // Memory-map all 7 columns — zero-copy, no allocation overhead
    gendb::MmapColumn<int32_t> col_shipdate  (li + "l_shipdate.bin");
    gendb::MmapColumn<int8_t>  col_returnflag(li + "l_returnflag.bin");
    gendb::MmapColumn<int8_t>  col_linestatus(li + "l_linestatus.bin");
    gendb::MmapColumn<double>  col_quantity  (li + "l_quantity.bin");
    gendb::MmapColumn<double>  col_price     (li + "l_extendedprice.bin");
    gendb::MmapColumn<double>  col_discount  (li + "l_discount.bin");
    gendb::MmapColumn<double>  col_tax       (li + "l_tax.bin");

    size_t n_rows = col_shipdate.size();

    // -------------------------------------------------------------------------
    // Phase: dim_filter — load zonemap, build skip_block[] array
    // zonemap format: uint64_t n_blocks; then n_blocks × {int32_t min, int32_t max}
    // -------------------------------------------------------------------------
    std::vector<uint8_t> skip_block;
    size_t n_blocks = 0;
    {
        GENDB_PHASE("dim_filter");

        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = ::open(zm_path.c_str(), O_RDONLY);
        if (fd < 0) {
            // Fallback: no zone map — scan all blocks with row-level filter
            n_blocks = (n_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
            skip_block.assign(n_blocks, 0);
        } else {
            uint64_t zm_n_blocks = 0;
            ::read(fd, &zm_n_blocks, sizeof(zm_n_blocks));
            n_blocks = (size_t)zm_n_blocks;
            skip_block.resize(n_blocks, 0);

            for (size_t b = 0; b < n_blocks; b++) {
                int32_t mn, mx;
                ::read(fd, &mn, sizeof(mn));
                ::read(fd, &mx, sizeof(mx));
                if (mn > CUTOFF) {
                    // Table sorted ascending: all subsequent blocks also fail
                    // Mark remaining blocks as skipped and early-terminate
                    for (size_t bb = b; bb < n_blocks; bb++) skip_block[bb] = 1;
                    break;
                }
            }
            ::close(fd);
        }
    }

    // -------------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven aggregate with thread-local Acc[6]
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("main_scan");

    int n_threads = omp_get_max_threads();
    // Pad to avoid false sharing: each thread gets its own cache-line-aligned Acc[6]
    std::vector<std::array<Accum, MAX_GROUPS>> tl_acc(n_threads);

    #pragma omp parallel num_threads(n_threads)
    {
        int tid = omp_get_thread_num();
        auto& local = tl_acc[tid];

        #pragma omp for schedule(dynamic, 4)
        for (int64_t b = 0; b < (int64_t)n_blocks; b++) {
            if (skip_block[b]) continue;

            size_t row_start = (size_t)b * BLOCK_SIZE;
            size_t row_end   = std::min(row_start + BLOCK_SIZE, n_rows);
            size_t len       = row_end - row_start;

            const int32_t* __restrict__ sd  = col_shipdate.data   + row_start;
            const int8_t*  __restrict__ rf  = col_returnflag.data + row_start;
            const int8_t*  __restrict__ ls  = col_linestatus.data + row_start;
            const double*  __restrict__ qty = col_quantity.data   + row_start;
            const double*  __restrict__ pr  = col_price.data      + row_start;
            const double*  __restrict__ dis = col_discount.data   + row_start;
            const double*  __restrict__ tax = col_tax.data        + row_start;

            for (size_t i = 0; i < len; i++) {
                if (sd[i] > CUTOFF) continue;

                int idx = (int)(uint8_t)rf[i] * 2 + (int)(uint8_t)ls[i];
                Accum& a = local[idx];
                double d            = dis[i];
                double p            = pr[i];
                long double ld      = (long double)d;
                long double lp      = (long double)p;
                long double omd     = 1.0L - ld;
                a.cnt++;
                a.sum_qty        += qty[i];
                a.sum_price      += p;
                a.sum_disc_price += lp * omd;
                a.sum_charge     += lp * omd * (1.0L + (long double)tax[i]);
                a.sum_disc       += d;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase: build_joins — merge thread-local accumulators (O(6 * nthreads))
    // -------------------------------------------------------------------------
    Accum global_acc[MAX_GROUPS];
    {
        GENDB_PHASE("build_joins");
        for (int t = 0; t < n_threads; t++) {
            for (int g = 0; g < MAX_GROUPS; g++) {
                auto& src = tl_acc[t][g];
                auto& dst = global_acc[g];
                dst.cnt            += src.cnt;
                dst.sum_qty        += src.sum_qty;
                dst.sum_price      += src.sum_price;
                dst.sum_disc_price += src.sum_disc_price;
                dst.sum_charge     += src.sum_charge;
                dst.sum_disc       += src.sum_disc;
            }
        }
    }

    // Release pages before munmap to reduce teardown cost
    auto dontneed = [](const void* p, size_t sz) {
        if (p && sz > 0) madvise(const_cast<void*>(p), sz, MADV_DONTNEED);
    };
    dontneed(col_shipdate.data,   col_shipdate.file_size);
    dontneed(col_returnflag.data, col_returnflag.file_size);
    dontneed(col_linestatus.data, col_linestatus.file_size);
    dontneed(col_quantity.data,   col_quantity.file_size);
    dontneed(col_price.data,      col_price.file_size);
    dontneed(col_discount.data,   col_discount.file_size);
    dontneed(col_tax.data,        col_tax.file_size);

    // -------------------------------------------------------------------------
    // Phase: output — sort 6 entries and write CSV
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Row { int rf_code, ls_code; Accum acc; };
        std::vector<Row> rows;
        for (int g = 0; g < MAX_GROUPS; g++) {
            if (global_acc[g].cnt == 0) continue;
            int rf_code = g / 2;
            int ls_code = g % 2;
            rows.push_back({rf_code, ls_code, global_acc[g]});
        }

        // Sort by (returnflag_code ASC, linestatus_code ASC)
        // Dict codes are assigned in alphabetical order (A=0,N=1,R=2 and F=0,O=1)
        // so code order == string order.
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (a.rf_code != b.rf_code) return a.rf_code < b.rf_code;
            return a.ls_code < b.ls_code;
        });

        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror("fopen output"); return 1; }

        fprintf(out, "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
                     "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& r : rows) {
            const Accum& a = r.acc;
            double avg_qty   = a.sum_qty   / (double)a.cnt;
            double avg_price = a.sum_price / (double)a.cnt;
            double avg_disc  = a.sum_disc  / (double)a.cnt;

            fprintf(out, "%s,%s,%.2f,%.2f,%.4Lf,%.6Lf,%.2f,%.2f,%.2f,%ld\n",
                    rf_dict[r.rf_code].c_str(),
                    ls_dict[r.ls_code].c_str(),
                    a.sum_qty,
                    a.sum_price,
                    a.sum_disc_price,
                    a.sum_charge,
                    avg_qty,
                    avg_price,
                    avg_disc,
                    (long)a.cnt);
        }

        fclose(out);
        printf("Written: %s (%zu rows)\n", out_path.c_str(), rows.size());
    }

    } // main_scan scope
    } // data_loading scope

    return 0;
}
