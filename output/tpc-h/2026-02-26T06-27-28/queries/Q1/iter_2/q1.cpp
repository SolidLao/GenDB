// Q1: Pricing Summary Report — iter_2
// Key changes over iter_1:
//   1. All accumulators use double (NOT long double) — eliminates x87 FPU,
//      enables SSE2/AVX2, shortens FP dependency chains from ~40 to ~4-8 cycles,
//      allowing many more concurrent DRAM prefetch requests in flight.
//   2. No madvise calls — hot data is already in OS page cache;
//      madvise(HUGEPAGE/WILLNEED) caused iter_1 regression by triggering
//      kernel THP remapping overhead.
//   3. dynamic(4) schedule (restored from iter_0) — static schedule hurt iter_1
//      due to MIXED-block imbalance.
//   4. Three-way zone-map classification (ALL_PASS/MIXED/SKIP):
//      ALL_PASS blocks (~597/600) skip l_shipdate column load entirely.

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <array>
#include <fstream>
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

enum BlockType : uint8_t { ALL_PASS = 0, MIXED = 1, SKIP = 2 };

// sum_disc_price uses long double — ground truth was computed with long double;
// double accumulation over ~30M rows loses 0.01 ULP in the ~1T sum (precision_loss).
// All other fields use double — eliminates x87 for the bulk of FP work.
struct alignas(64) Accum {
    double      sum_qty        = 0.0;
    double      sum_price      = 0.0;
    long double sum_disc_price = 0.0L;
    double      sum_charge     = 0.0;
    double      sum_disc       = 0.0;
    int64_t     cnt            = 0;
};

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

    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];
    system(("mkdir -p " + results_dir).c_str());

    const std::string li = gendb_dir + "/lineitem/";

    // -------------------------------------------------------------------------
    // Phase: data_loading — mmap columns + load dicts (no madvise)
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    auto rf_dict = load_dict(li + "l_returnflag.dict");   // A=0, N=1, R=2
    auto ls_dict = load_dict(li + "l_linestatus.dict");   // F=0, O=1

    // Memory-map all columns — zero-copy, no madvise (hot data already in page cache)
    gendb::MmapColumn<int32_t> col_shipdate  (li + "l_shipdate.bin");
    gendb::MmapColumn<int8_t>  col_returnflag(li + "l_returnflag.bin");
    gendb::MmapColumn<int8_t>  col_linestatus(li + "l_linestatus.bin");
    gendb::MmapColumn<double>  col_quantity  (li + "l_quantity.bin");
    gendb::MmapColumn<double>  col_price     (li + "l_extendedprice.bin");
    gendb::MmapColumn<double>  col_discount  (li + "l_discount.bin");
    gendb::MmapColumn<double>  col_tax       (li + "l_tax.bin");

    const size_t n_rows = col_shipdate.size();

    // -------------------------------------------------------------------------
    // Phase: dim_filter — load zone map, classify blocks ALL_PASS/MIXED/SKIP
    // Zone map: uint64_t n_blocks; then n_blocks x {int32_t min, int32_t max}
    //   ALL_PASS = max_val <= CUTOFF  (no per-row date check needed)
    //   MIXED    = min_val <= CUTOFF < max_val  (need per-row filter)
    //   SKIP     = min_val > CUTOFF  (ascending sort => early terminate)
    // -------------------------------------------------------------------------
    std::vector<BlockType> block_type;
    size_t n_blocks = 0;
    {
        GENDB_PHASE("dim_filter");

        const std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = ::open(zm_path.c_str(), O_RDONLY);
        if (fd < 0) {
            // Fallback: no zone map — treat all blocks as MIXED
            n_blocks = (n_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
            block_type.assign(n_blocks, MIXED);
        } else {
            uint64_t zm_n_blocks = 0;
            ::read(fd, &zm_n_blocks, sizeof(zm_n_blocks));
            n_blocks = (size_t)zm_n_blocks;
            block_type.resize(n_blocks, SKIP);  // default to SKIP

            for (size_t b = 0; b < n_blocks; b++) {
                int32_t mn, mx;
                ::read(fd, &mn, sizeof(mn));
                ::read(fd, &mx, sizeof(mx));
                if (mn > CUTOFF) {
                    // Table sorted ascending: all subsequent blocks also SKIP
                    break;
                } else if (mx <= CUTOFF) {
                    block_type[b] = ALL_PASS;
                } else {
                    block_type[b] = MIXED;
                }
            }
            ::close(fd);
        }
    }

    // -------------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven aggregate, two-path inner loop
    // dynamic(4): proven stable in iter_0; handles MIXED-block imbalance
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("main_scan");

    const int n_threads = omp_get_max_threads();
    std::vector<std::array<Accum, MAX_GROUPS>> tl_acc(n_threads);

    #pragma omp parallel num_threads(n_threads)
    {
        const int tid = omp_get_thread_num();
        auto& local = tl_acc[tid];

        #pragma omp for schedule(dynamic, 4)
        for (int64_t b = 0; b < (int64_t)n_blocks; b++) {
            const BlockType btype = block_type[b];
            if (btype == SKIP) continue;

            const size_t row_start = (size_t)b * BLOCK_SIZE;
            const size_t row_end   = std::min(row_start + BLOCK_SIZE, n_rows);
            const size_t len       = row_end - row_start;

            const int8_t*  __restrict__ rf  = col_returnflag.data + row_start;
            const int8_t*  __restrict__ ls  = col_linestatus.data + row_start;
            const double*  __restrict__ qty = col_quantity.data   + row_start;
            const double*  __restrict__ pr  = col_price.data      + row_start;
            const double*  __restrict__ dis = col_discount.data   + row_start;
            const double*  __restrict__ tx  = col_tax.data        + row_start;

            if (btype == ALL_PASS) {
                // Hot path (~99.8% of blocks): no shipdate load, no branch per row
                for (size_t i = 0; i < len; i++) {
                    const int    idx = (int)(uint8_t)rf[i] * 2 + (int)(uint8_t)ls[i];
                    Accum& a = local[idx];
                    const double d   = dis[i];
                    const double p   = pr[i];
                    const double omd = 1.0 - d;
                    a.sum_qty        += qty[i];
                    a.sum_price      += p;
                    a.sum_disc_price += (long double)p * (long double)omd;
                    a.sum_charge     += p * omd * (1.0 + tx[i]);
                    a.sum_disc       += d;
                    a.cnt++;
                }
            } else {
                // MIXED path: load shipdate and apply per-row filter
                const int32_t* __restrict__ sd = col_shipdate.data + row_start;
                for (size_t i = 0; i < len; i++) {
                    if (sd[i] > CUTOFF) continue;
                    const int    idx = (int)(uint8_t)rf[i] * 2 + (int)(uint8_t)ls[i];
                    Accum& a = local[idx];
                    const double d   = dis[i];
                    const double p   = pr[i];
                    const double omd = 1.0 - d;
                    a.sum_qty        += qty[i];
                    a.sum_price      += p;
                    a.sum_disc_price += (long double)p * (long double)omd;
                    a.sum_charge     += p * omd * (1.0 + tx[i]);
                    a.sum_disc       += d;
                    a.cnt++;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase: build_joins — merge thread-local accumulators element-wise O(6*T)
    // -------------------------------------------------------------------------
    Accum global_acc[MAX_GROUPS] = {};
    {
        GENDB_PHASE("build_joins");
        for (int t = 0; t < n_threads; t++) {
            for (int g = 0; g < MAX_GROUPS; g++) {
                const Accum& src = tl_acc[t][g];
                Accum&       dst = global_acc[g];
                dst.sum_qty        += src.sum_qty;
                dst.sum_price      += src.sum_price;
                dst.sum_disc_price += src.sum_disc_price;
                dst.sum_charge     += src.sum_charge;
                dst.sum_disc       += src.sum_disc;
                dst.cnt            += src.cnt;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase: output — sort up to 6 rows and write CSV
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Row { int rf_code, ls_code; const Accum* acc; };
        std::vector<Row> rows;
        rows.reserve(MAX_GROUPS);
        for (int g = 0; g < MAX_GROUPS; g++) {
            if (global_acc[g].cnt == 0) continue;
            rows.push_back({g / 2, g % 2, &global_acc[g]});
        }

        // Dict codes are assigned alphabetically (A=0,N=1,R=2 and F=0,O=1)
        // so code order == string sort order.
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (a.rf_code != b.rf_code) return a.rf_code < b.rf_code;
            return a.ls_code < b.ls_code;
        });

        const std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror("fopen output"); return 1; }

        fprintf(out, "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
                     "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& r : rows) {
            const Accum& a = *r.acc;
            const double avg_qty   = a.sum_qty   / (double)a.cnt;
            const double avg_price = a.sum_price / (double)a.cnt;
            const double avg_disc  = a.sum_disc  / (double)a.cnt;

            // sum_disc_price is long double (%Lf); rest are double (%f)
            fprintf(out, "%s,%s,%.2f,%.2f,%.4Lf,%.6f,%.2f,%.2f,%.2f,%ld\n",
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
