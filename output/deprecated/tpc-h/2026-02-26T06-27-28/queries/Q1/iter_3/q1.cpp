// Q1: Pricing Summary Report — iter_3
// Key changes over iter_2:
//   1. Compact integer column versions replace double columns:
//      uint8_t for qty (1..50), disc (0..10), tax (0..8)
//      int32_t centiprice for extendedprice (price×100)
//      → 3.7× DRAM bandwidth reduction: 2040MB → 540MB
//   2. Pure int64_t accumulators in inner loop — eliminates ALL floating-point
//      from the hot path. No x87 FPU, enables integer SIMD.
//      sum_disc_price_i += price_i32 × (100 - disc_u8)
//      sum_charge_i     += price_i32 × (100 - disc_u8) × (100 + tax_u8)
//   3. Long double used ONLY for final 6-row output conversion (not in hot loop).
//   4. Overflow verified: max sum_charge_i ≈ 6.55e18 < INT64_MAX(9.22e18). Safe.

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

// Pure int64_t accumulator — no floating-point in the hot inner loop.
// 6 fields × 8 bytes = 48 bytes; padded to 64-byte cache line (false-share-free).
struct alignas(64) Accum {
    int64_t cnt              = 0;
    int64_t sum_qty_i        = 0;   // sum of uint8_t quantities (exact integer 1..50)
    int64_t sum_disc_i       = 0;   // sum of disc_u8 (disc×100, 0..10)
    int64_t sum_price_i      = 0;   // sum of price_i32 (price×100)
    int64_t sum_disc_price_i = 0;   // sum of price_i32×(100-disc_u8); output ×0.0001
    int64_t sum_charge_i     = 0;   // sum of price_i32×(100-disc_u8)×(100+tax_u8); output ×1e-6
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
    const std::string cv = gendb_dir + "/column_versions/";

    // -------------------------------------------------------------------------
    // Phase: data_loading — mmap columns + load dicts
    // Compact integer versions replace the original double columns for 3.7× less DRAM.
    // Only l_shipdate (int32_t) and l_returnflag/l_linestatus (int8_t) use original files.
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    auto rf_dict = load_dict(li + "l_returnflag.dict");   // A=0, N=1, R=2
    auto ls_dict = load_dict(li + "l_linestatus.dict");   // F=0, O=1

    // Original columns still needed
    gendb::MmapColumn<int32_t> col_shipdate  (li + "l_shipdate.bin");
    gendb::MmapColumn<int8_t>  col_returnflag(li + "l_returnflag.bin");
    gendb::MmapColumn<int8_t>  col_linestatus(li + "l_linestatus.bin");

    // Compact integer column versions — replace original double columns
    // qty: uint8_t, range 1..50 (was double, 8B/row → 1B/row)
    // disc: uint8_t = discount×100, range 0..10 (was 8B/row → 1B/row)
    // tax: uint8_t = tax×100, range 0..8 (was 8B/row → 1B/row)
    // price: int32_t = extendedprice×100, range 90091..10494950 (was 8B/row → 4B/row)
    gendb::MmapColumn<uint8_t> col_qty  (cv + "lineitem.l_quantity.uint8/qty.bin");
    gendb::MmapColumn<int32_t> col_price(cv + "lineitem.l_extendedprice.int32/price.bin");
    gendb::MmapColumn<uint8_t> col_disc (cv + "lineitem.l_discount.uint8/disc.bin");
    gendb::MmapColumn<uint8_t> col_tax  (cv + "lineitem.l_tax.uint8/tax.bin");

    const size_t n_rows = col_shipdate.size();

    // -------------------------------------------------------------------------
    // Phase: dim_filter — zone map → three-way block classification
    // Zone map layout: uint64_t n_blocks; then n_blocks × {int32_t min, int32_t max}
    //   ALL_PASS: max_val <= CUTOFF  → no per-row date check needed
    //   MIXED:    min_val <= CUTOFF < max_val  → need per-row filter
    //   SKIP:     min_val > CUTOFF  → early terminate (table sorted ascending)
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
            block_type.assign(n_blocks, SKIP);  // default to SKIP

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
    // Phase: main_scan — parallel morsel-driven aggregate with integer-only inner loop
    // dynamic(4): stable and handles MIXED-block imbalance (proven in iter_0).
    // ALL_PASS path (~597/600 blocks): pure int64 arithmetic, no date check per row.
    // MIXED path (~3/600 blocks): same arithmetic guarded by date predicate.
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
            const uint8_t* __restrict__ qty = col_qty.data        + row_start;
            const int32_t* __restrict__ pr  = col_price.data      + row_start;
            const uint8_t* __restrict__ dis = col_disc.data       + row_start;
            const uint8_t* __restrict__ tx  = col_tax.data        + row_start;

            if (btype == ALL_PASS) {
                // Hot path (~597/600 blocks): no shipdate load, no branch per row.
                // Pure integer arithmetic — no FP instructions in the inner loop.
                for (size_t i = 0; i < len; i++) {
                    const int k = (int)(uint8_t)rf[i] * 2 + (int)(uint8_t)ls[i];
                    Accum& a = local[k];
                    const int64_t p64 = (int64_t)pr[i];
                    const int64_t omd = (int64_t)(100 - (int32_t)dis[i]);  // (1-disc)×100
                    const int64_t opt = (int64_t)(100 + (int32_t)tx[i]);   // (1+tax)×100
                    a.cnt++;
                    a.sum_qty_i        += (int64_t)qty[i];
                    a.sum_disc_i       += (int64_t)dis[i];
                    a.sum_price_i      += p64;
                    a.sum_disc_price_i += p64 * omd;
                    a.sum_charge_i     += p64 * omd * opt;
                }
            } else {
                // MIXED path (~3/600 blocks): per-row shipdate filter applied.
                const int32_t* __restrict__ sd = col_shipdate.data + row_start;
                for (size_t i = 0; i < len; i++) {
                    if (sd[i] > CUTOFF) continue;
                    const int k = (int)(uint8_t)rf[i] * 2 + (int)(uint8_t)ls[i];
                    Accum& a = local[k];
                    const int64_t p64 = (int64_t)pr[i];
                    const int64_t omd = (int64_t)(100 - (int32_t)dis[i]);
                    const int64_t opt = (int64_t)(100 + (int32_t)tx[i]);
                    a.cnt++;
                    a.sum_qty_i        += (int64_t)qty[i];
                    a.sum_disc_i       += (int64_t)dis[i];
                    a.sum_price_i      += p64;
                    a.sum_disc_price_i += p64 * omd;
                    a.sum_charge_i     += p64 * omd * opt;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase: build_joins — merge thread-local accumulators O(6 × T)
    // -------------------------------------------------------------------------
    Accum global_acc[MAX_GROUPS] = {};
    {
        GENDB_PHASE("build_joins");
        for (int t = 0; t < n_threads; t++) {
            for (int g = 0; g < MAX_GROUPS; g++) {
                const Accum& src = tl_acc[t][g];
                Accum&       dst = global_acc[g];
                dst.cnt              += src.cnt;
                dst.sum_qty_i        += src.sum_qty_i;
                dst.sum_disc_i       += src.sum_disc_i;
                dst.sum_price_i      += src.sum_price_i;
                dst.sum_disc_price_i += src.sum_disc_price_i;
                dst.sum_charge_i     += src.sum_charge_i;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase: output — sort up to 6 rows and write CSV
    // Integer→float conversion only here (6 rows, not hot path).
    // Long double for sum_disc_price and sum_charge to preserve output precision.
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

        // Dict codes are assigned alphabetically (A=0, N=1, R=2; F=0, O=1)
        // so code order == string sort order — no string comparison needed.
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

            // Integer → float output conversion (only 6 rows, not hot path).
            // Long double for disc_price and charge to match ground truth precision.
            const double      sum_qty        = (double)a.sum_qty_i;
            const double      sum_base       = (double)a.sum_price_i * 0.01;
            const long double sum_disc_price = (long double)a.sum_disc_price_i * 0.0001L;
            const long double sum_charge     = (long double)a.sum_charge_i * 1e-6L;
            const double      avg_qty        = (double)a.sum_qty_i  / (double)a.cnt;
            const double      avg_price      = (double)a.sum_price_i * 0.01 / (double)a.cnt;
            const double      avg_disc       = (double)a.sum_disc_i  * 0.01 / (double)a.cnt;

            fprintf(out, "%s,%s,%.2f,%.2f,%.4Lf,%.6Lf,%.2f,%.2f,%.2f,%ld\n",
                    rf_dict[r.rf_code].c_str(),
                    ls_dict[r.ls_code].c_str(),
                    sum_qty,
                    sum_base,
                    sum_disc_price,
                    sum_charge,
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
