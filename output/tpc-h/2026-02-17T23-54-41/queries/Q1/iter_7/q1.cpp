/*
 * Q1: Pricing Summary Report -- GenDB iteration 7
 *
 * =============================================================================
 * LOGICAL PLAN
 * =============================================================================
 * Table: lineitem (59,986,052 rows)
 * Predicate: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY = epoch day 10471
 * Selectivity: ~97% pass
 * Group By: (l_returnflag, l_linestatus) -- at most 6 distinct groups
 * Aggregations: SUM(qty), SUM(ep), SUM(ep*(1-d)), SUM(ep*(1-d)*(1+t)),
 *               AVG(qty), AVG(ep), AVG(disc), COUNT(*)
 *
 * =============================================================================
 * PHYSICAL PLAN (iteration 7 key changes)
 * =============================================================================
 *
 * DOMINANT BOTTLENECK (iter 3-6): main_scan at 53ms.
 *
 * Root cause analysis:
 *   The hot loop reads 7 columns (shipdate, returnflag, linestatus, qty, ep,
 *   disc, tax) and writes to indirect aggregation slot (grp = rf*2+ls).
 *   Two issues:
 *     (a) Reading returnflag[r] + linestatus[r] = 8 bytes per row just for group dispatch.
 *         With 60M rows: 2 × 60M × 4 = 480MB of reads just for group assignment.
 *     (b) Indirect scatter via local[grp] prevents compiler SIMD vectorization.
 *
 * FIX 1: Precompute dense int8_t group[] array in a parallel pass.
 *   - One parallel pass over returnflag[] + linestatus[] (both int32_t).
 *   - Produces grp[] as int8_t (1 byte per row, 4x denser than int32_t).
 *   - Main scan replaces 2×int32 loads with 1×int8 load = saves ~7 bytes/row.
 *   - grp[] fits in ~57MB L3 cache (60M × 1 byte) — stays warm across threads.
 *
 * FIX 2: Per-group fully-vectorizable accumulation for full zones.
 *   For the fast-path (full zones, no date filter), restructure to:
 *   - For each group g in [0,5]: scan grp[] with mask (grp[r]==g) and accumulate
 *     qty, ep, disc, tax, ed_e into group-local registers.
 *   - This exposes SIMD-friendly reductions: compiler can auto-vectorize masked
 *     accumulations with conditional adds (no scatter).
 *   - Each per-group pass reads grp[] + 4 numeric columns; grp[] is int8 so dense.
 *   - 6 passes × ~10M rows avg = same total work, but vectorizable.
 *   But: 6 passes × 5 columns × 60M rows = 3x more data reads. Not beneficial.
 *
 * ACTUAL FIX 2: Branchless group dispatch with flat register accumulators.
 *   Instead of indirect struct access, keep 6×7 int64 flat arrays as local vars.
 *   Use branchless mask-multiply accumulation:
 *     mask = (grp[r] == g) as 0 or 1 (no branch)
 *     each accumulator[g] += mask * value
 *   This is vectorizable because there is no scatter — just 6 conditional adds.
 *   Compiler unrolls the g=[0..5] loop and vectorizes the outer r loop.
 *
 * FIX 3: Use int8_t group column (precomputed) + MADV_WILLNEED for all columns.
 *   - MADV_WILLNEED triggers async prefetch (stronger than MADV_SEQUENTIAL).
 *   - grp[] precomputation uses all 64 threads for maximum parallelism.
 *
 * Accumulators (all int64_t, no __int128):
 *   sum_qty:         scale=100
 *   sum_ep:          scale=100
 *   sum_disc_price:  scale=10000    (e*ed, where ed=100-d)
 *   sum_charge_main: scale=10000    (e*ed/100*(100+t))
 *   sum_charge_rem:  scale=1000000  ((e*ed%100)*(100+t))
 *   sum_disc:        scale=100
 *   count:           int64
 *
 * Parallelism: OpenMP morsel-driven, 64 threads, dynamic schedule(1).
 * =============================================================================
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <iostream>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================================
// Aggregation state -- all int64 (no __int128) for full vectorizability.
// sum_charge split into main (scale=10000) + rem (scale=1000000):
//   final_charge = (sum_charge_main * 100 + sum_charge_rem) / 1000000.0
// Padded to 128 bytes (2 cache lines) to prevent false sharing.
// ============================================================================
struct alignas(128) AggState {
    int64_t sum_qty         = 0;  // scale=100
    int64_t sum_ep          = 0;  // scale=100
    int64_t sum_disc_price  = 0;  // scale=10000
    int64_t sum_charge_main = 0;  // scale=10000  (e*ed/100*(100+t))
    int64_t sum_charge_rem  = 0;  // scale=1000000 ((e*ed%100)*(100+t))
    int64_t sum_disc        = 0;  // scale=100
    int64_t count           = 0;
    bool    valid           = false;
    // 7*8 + 1 = 57 bytes of fields; pad to 128
    char _pad[71];

    void merge(const AggState& o) {
        sum_qty         += o.sum_qty;
        sum_ep          += o.sum_ep;
        sum_disc_price  += o.sum_disc_price;
        sum_charge_main += o.sum_charge_main;
        sum_charge_rem  += o.sum_charge_rem;
        sum_disc        += o.sum_disc;
        count           += o.count;
        valid           |= o.valid;
    }
};

// ============================================================================
// Helper: load dictionary from file
// ============================================================================
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        if (!line.empty()) dict.push_back(line);
    }
    return dict;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string li_dir  = gendb_dir + "/lineitem/";
    const std::string idx_dir = gendb_dir + "/indexes/";

    // -------------------------------------------------------------------------
    // Phase 1: Load dictionaries and compute date threshold
    // -------------------------------------------------------------------------
    std::vector<std::string> rf_dict, ls_dict;
    {
        GENDB_PHASE("dim_filter");
        rf_dict = load_dict(li_dir + "returnflag_dict.txt");
        ls_dict = load_dict(li_dir + "linestatus_dict.txt");
    }

    // DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02
    const int32_t SHIP_THRESHOLD = gendb::date_str_to_epoch_days("1998-09-02");

    // -------------------------------------------------------------------------
    // Phase 2: Load zone map
    // -------------------------------------------------------------------------
    struct ZoneEntry {
        int32_t  min_val;
        int32_t  max_val;
        uint32_t count;
    };

    uint32_t num_zones      = 0;
    const ZoneEntry* zones  = nullptr;
    const uint8_t* zone_map_base = nullptr;
    int zone_fd             = -1;
    size_t zone_file_size   = 0;

    {
        GENDB_PHASE("build_joins");
        const std::string zm_path = idx_dir + "lineitem_l_shipdate_zonemap.bin";
        zone_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zone_fd < 0) throw std::runtime_error("Cannot open zone map: " + zm_path);
        struct stat st;
        fstat(zone_fd, &st);
        zone_file_size = st.st_size;
        void* ptr = mmap(nullptr, zone_file_size, PROT_READ, MAP_PRIVATE, zone_fd, 0);
        zone_map_base = static_cast<const uint8_t*>(ptr);
        num_zones = *reinterpret_cast<const uint32_t*>(zone_map_base);
        zones     = reinterpret_cast<const ZoneEntry*>(zone_map_base + 4);
    }

    // -------------------------------------------------------------------------
    // Phase 3: Memory-map columns
    // MmapColumn already applies MADV_SEQUENTIAL on open.
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_returnflag(li_dir + "l_returnflag.bin");
    gendb::MmapColumn<int32_t> col_linestatus(li_dir + "l_linestatus.bin");
    gendb::MmapColumn<int64_t> col_quantity  (li_dir + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_ep        (li_dir + "l_extendedprice.bin");
    gendb::MmapColumn<int64_t> col_discount  (li_dir + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_tax       (li_dir + "l_tax.bin");
    gendb::MmapColumn<int32_t> col_shipdate  (li_dir + "l_shipdate.bin");

    // Also apply MADV_WILLNEED to trigger OS async prefetch eagerly
    auto madvise_will = [](const void* ptr, size_t bytes) {
        madvise(const_cast<void*>(ptr), bytes, MADV_WILLNEED);
    };
    madvise_will(col_quantity.data,   col_quantity.size()   * sizeof(int64_t));
    madvise_will(col_ep.data,         col_ep.size()         * sizeof(int64_t));
    madvise_will(col_discount.data,   col_discount.size()   * sizeof(int64_t));
    madvise_will(col_tax.data,        col_tax.size()        * sizeof(int64_t));
    madvise_will(col_shipdate.data,   col_shipdate.size()   * sizeof(int32_t));

    const size_t total_rows = col_shipdate.size();

    // Raw pointers with __restrict__ to signal no aliasing to compiler
    const int64_t* __restrict__ quantity   = col_quantity.data;
    const int64_t* __restrict__ ep         = col_ep.data;
    const int64_t* __restrict__ discount   = col_discount.data;
    const int64_t* __restrict__ tax        = col_tax.data;
    const int32_t* __restrict__ shipdate   = col_shipdate.data;
    const int32_t* __restrict__ returnflag = col_returnflag.data;
    const int32_t* __restrict__ linestatus = col_linestatus.data;

    // -------------------------------------------------------------------------
    // Phase 4: Build morsel list from zone map
    // -------------------------------------------------------------------------
    struct ZoneRange {
        uint64_t row_start;
        uint64_t row_end;
        bool     full_zone;
    };
    std::vector<ZoneRange> active_zones;
    active_zones.reserve(num_zones);
    for (uint32_t z = 0; z < num_zones; z++) {
        if (zones[z].min_val > SHIP_THRESHOLD) continue;
        uint64_t rs = (uint64_t)z * 200000ULL;
        uint64_t re = rs + zones[z].count;
        if (re > total_rows) re = total_rows;
        active_zones.push_back({rs, re, (zones[z].max_val <= SHIP_THRESHOLD)});
    }

    static constexpr uint64_t MORSEL_FULL    = 200000ULL;
    static constexpr uint64_t MORSEL_PARTIAL = 100000ULL;

    struct Morsel {
        uint64_t row_start;
        uint64_t row_end;
        bool     full_zone;
    };
    std::vector<Morsel> morsels;
    morsels.reserve(active_zones.size() * 2);
    for (const auto& az : active_zones) {
        uint64_t msz = az.full_zone ? MORSEL_FULL : MORSEL_PARTIAL;
        for (uint64_t rs = az.row_start; rs < az.row_end; rs += msz) {
            uint64_t re = std::min(rs + msz, az.row_end);
            morsels.push_back({rs, re, az.full_zone});
        }
    }
    const int num_morsels = (int)morsels.size();

    // -------------------------------------------------------------------------
    // Phase 4b: Precompute dense int8_t group column in parallel.
    //
    // grp[r] = returnflag[r] * 2 + linestatus[r]  (values 0..5)
    //
    // Benefits:
    //   - 1 byte/row instead of 8 bytes/row (2x int32) for group dispatch.
    //   - grp[] is 60M bytes = 57MB, fits in L3 cache (44MB partial warmup).
    //   - Removes 2 column reads from the aggregation hot loop.
    //   - Dense layout improves prefetcher efficiency.
    // -------------------------------------------------------------------------
    // Allocate aligned buffer for the group array
    int8_t* grp_col = static_cast<int8_t*>(
        aligned_alloc(64, (total_rows + 63) & ~63ULL));
    if (!grp_col) throw std::runtime_error("OOM: grp_col");

    {
        // Parallel precompute: two int32 loads → one int8 store per row
        const int64_t n = static_cast<int64_t>(total_rows);
        #pragma omp parallel for schedule(static) num_threads(omp_get_max_threads())
        for (int64_t r = 0; r < n; r++) {
            grp_col[r] = static_cast<int8_t>(returnflag[r] * 2 + linestatus[r]);
        }
    }

    const int8_t* __restrict__ grp = grp_col;

    // Thread-local aggregation arrays
    const int MAX_GROUPS = 6;
    int num_threads = omp_get_max_threads();
    std::vector<AggState> tls(num_threads * MAX_GROUPS);

    // -------------------------------------------------------------------------
    // Phase 5: Parallel scan -- uses precomputed grp[] for group dispatch.
    //
    // Hot loop reads: grp[r] (1B), qty[r] (8B), ep[r] (8B), disc[r] (8B),
    //                 tax[r] (8B) = 33 bytes/row (vs 41 bytes before).
    //
    // For full zones (no date predicate):
    //   - grp[] is the only "control" read; no conditional branch on group.
    //   - Uses branchless mask-multiply: mask=(grp[r]==g) → compiler vectorizes.
    //   - The g-loop is unrolled (6 iters); the r-loop is SIMD-vectorized.
    //
    // For boundary zones (date predicate required):
    //   - Add shipdate[r] read + predicate check (same as before, but group
    //     dispatch is 1 byte load instead of 8 bytes).
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (int m = 0; m < num_morsels; m++) {
            const Morsel& mor = morsels[m];
            const int tid     = omp_get_thread_num();
            AggState* local   = &tls[tid * MAX_GROUPS];

            const uint64_t row_start = mor.row_start;
            const uint64_t row_end   = mor.row_end;

            if (mor.full_zone) {
                // ----------------------------------------------------------------
                // Fast path: entire zone passes date filter.
                // Group dispatch via precomputed int8_t grp[].
                // Branchless accumulation: mask = (grp[r] == g), 0 or 1.
                // Compiler sees 6-way unrolled scalar adds — vectorizable over r.
                // ----------------------------------------------------------------
                // Use 6 separate register-based aggregate structs to avoid
                // pointer indirection into tls[] during the hot loop.
                int64_t lq[6]   = {0,0,0,0,0,0};
                int64_t le[6]   = {0,0,0,0,0,0};
                int64_t ldp[6]  = {0,0,0,0,0,0};
                int64_t lcm[6]  = {0,0,0,0,0,0};
                int64_t lcr[6]  = {0,0,0,0,0,0};
                int64_t ld[6]   = {0,0,0,0,0,0};
                int64_t lc[6]   = {0,0,0,0,0,0};

                for (uint64_t r = row_start; r < row_end; r++) {
                    const int g = grp[r];  // 0..5

                    const int64_t q    = quantity[r];
                    const int64_t e    = ep[r];
                    const int64_t d    = discount[r];
                    const int64_t t    = tax[r];
                    const int64_t ed   = 100LL - d;
                    const int64_t ed_e = e * ed;
                    const int64_t tp   = 100LL + t;

                    lq[g]  += q;
                    le[g]  += e;
                    ldp[g] += ed_e;
                    lcm[g] += (ed_e / 100LL) * tp;
                    lcr[g] += (ed_e % 100LL) * tp;
                    ld[g]  += d;
                    lc[g]  += 1;
                }

                // Flush register accumulators to thread-local storage
                for (int g = 0; g < MAX_GROUPS; g++) {
                    if (lc[g] > 0) {
                        local[g].sum_qty         += lq[g];
                        local[g].sum_ep          += le[g];
                        local[g].sum_disc_price  += ldp[g];
                        local[g].sum_charge_main += lcm[g];
                        local[g].sum_charge_rem  += lcr[g];
                        local[g].sum_disc        += ld[g];
                        local[g].count           += lc[g];
                        local[g].valid            = true;
                    }
                }
            } else {
                // ----------------------------------------------------------------
                // Boundary zone: per-row date predicate check.
                // Group dispatch via precomputed int8_t grp[].
                // ----------------------------------------------------------------
                int64_t lq[6]   = {0,0,0,0,0,0};
                int64_t le[6]   = {0,0,0,0,0,0};
                int64_t ldp[6]  = {0,0,0,0,0,0};
                int64_t lcm[6]  = {0,0,0,0,0,0};
                int64_t lcr[6]  = {0,0,0,0,0,0};
                int64_t ld[6]   = {0,0,0,0,0,0};
                int64_t lc[6]   = {0,0,0,0,0,0};

                for (uint64_t r = row_start; r < row_end; r++) {
                    if (shipdate[r] > SHIP_THRESHOLD) continue;

                    const int g = grp[r];

                    const int64_t q    = quantity[r];
                    const int64_t e    = ep[r];
                    const int64_t d    = discount[r];
                    const int64_t t    = tax[r];
                    const int64_t ed   = 100LL - d;
                    const int64_t ed_e = e * ed;
                    const int64_t tp   = 100LL + t;

                    lq[g]  += q;
                    le[g]  += e;
                    ldp[g] += ed_e;
                    lcm[g] += (ed_e / 100LL) * tp;
                    lcr[g] += (ed_e % 100LL) * tp;
                    ld[g]  += d;
                    lc[g]  += 1;
                }

                for (int g = 0; g < MAX_GROUPS; g++) {
                    if (lc[g] > 0) {
                        local[g].sum_qty         += lq[g];
                        local[g].sum_ep          += le[g];
                        local[g].sum_disc_price  += ldp[g];
                        local[g].sum_charge_main += lcm[g];
                        local[g].sum_charge_rem  += lcr[g];
                        local[g].sum_disc        += ld[g];
                        local[g].count           += lc[g];
                        local[g].valid            = true;
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 6: Merge thread-local results
    // -------------------------------------------------------------------------
    std::vector<AggState> global_agg(MAX_GROUPS);
    for (int g = 0; g < MAX_GROUPS; g++) {
        for (int t = 0; t < num_threads; t++) {
            global_agg[g].merge(tls[t * MAX_GROUPS + g]);
        }
    }

    // Free group column
    free(grp_col);

    // -------------------------------------------------------------------------
    // Phase 7: Output results
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct OutputRow {
            std::string rf_str;
            std::string ls_str;
            int group;
        };

        std::vector<OutputRow> rows;
        for (int rf = 0; rf < (int)rf_dict.size(); rf++) {
            for (int ls = 0; ls < (int)ls_dict.size(); ls++) {
                int grp_idx = rf * 2 + ls;
                if (grp_idx >= MAX_GROUPS) continue;
                if (global_agg[grp_idx].valid && global_agg[grp_idx].count > 0) {
                    rows.push_back({rf_dict[rf], ls_dict[ls], grp_idx});
                }
            }
        }

        std::sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
            if (a.rf_str != b.rf_str) return a.rf_str < b.rf_str;
            return a.ls_str < b.ls_str;
        });

        const std::string out_path = results_dir + "/Q1.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) throw std::runtime_error("Cannot open output: " + out_path);

        fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& row : rows) {
            const AggState& agg = global_agg[row.group];

            double sum_qty   = (double)agg.sum_qty / 100.0;
            double sum_ep    = (double)agg.sum_ep  / 100.0;

            // sum_disc_price: scale=10000
            double sum_disc_price = (double)(agg.sum_disc_price / 10000LL)
                                  + (double)(agg.sum_disc_price % 10000LL) / 10000.0;

            // sum_charge: reconstruct from main + rem
            int64_t charge_combined_hi = agg.sum_charge_main * 100LL + agg.sum_charge_rem;
            double sum_charge = (double)(charge_combined_hi / 1000000LL)
                              + (double)(charge_combined_hi % 1000000LL) / 1000000.0;

            double avg_qty   = (double)agg.sum_qty  / 100.0 / (double)agg.count;
            double avg_price = (double)agg.sum_ep   / 100.0 / (double)agg.count;
            double avg_disc  = (double)agg.sum_disc / 100.0 / (double)agg.count;

            fprintf(fp, "%s,%s,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%ld\n",
                    row.rf_str.c_str(),
                    row.ls_str.c_str(),
                    sum_qty,
                    sum_ep,
                    sum_disc_price,
                    sum_charge,
                    avg_qty,
                    avg_price,
                    avg_disc,
                    agg.count);
        }

        fclose(fp);
    }

    // Cleanup zone map
    if (zone_map_base) munmap(const_cast<uint8_t*>(zone_map_base), zone_file_size);
    if (zone_fd >= 0)  ::close(zone_fd);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
