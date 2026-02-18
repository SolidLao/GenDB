/*
 * Q1: Pricing Summary Report -- GenDB iteration 5
 *
 * =============================================================================
 * LOGICAL PLAN
 * =============================================================================
 * Table: lineitem (59,986,052 rows)
 * Predicate: l_shipdate <= 10471 (1998-09-02)
 * Estimated selectivity: ~97% (most rows pass -- zone map prunes 3 zones)
 * Group By: (l_returnflag, l_linestatus) -- at most 6 distinct groups
 * Aggregations: SUM(qty), SUM(ep), SUM(ep*(1-d)), SUM(ep*(1-d)*(1+t)),
 *               AVG(qty), AVG(ep), AVG(disc), COUNT(*)
 *
 * =============================================================================
 * PHYSICAL PLAN (iteration 5 key changes)
 * =============================================================================
 *
 * DOMINANT BOTTLENECK (iter 3): main_scan at 53ms.
 *
 * Root cause analysis:
 *   The inner hot loop performs a GROUP SCATTER STORE:
 *     grp = returnflag[r] * 2 + linestatus[r];
 *     local[grp].sum_xxx += val;
 *   This scatter prevents compiler auto-vectorization entirely: the compiler
 *   cannot prove that consecutive rows map to different groups, so it emits
 *   scalar code for all 60M rows.
 *
 *   Additionally: integer division (ed_e / 100, ed_e % 100) in hot path is
 *   expensive even scalar, ~20-40 cycles per row.
 *
 * FIX 1 — Eliminate scatter via group-separated vectorizable loops:
 *   Instead of a single loop that scatters into 6 groups, run 6 separate
 *   inner accumulation loops, each processing rows WHERE grp == g.
 *   Implementation: for each morsel, build a selection vector (row indices
 *   per group) in a first pass over shipdate + grp columns, then for each
 *   group run a tight gather-accumulate loop over the selection.
 *
 *   HOWEVER: building selection vectors requires writing row indices to memory,
 *   which for 60M rows (~240MB) creates bandwidth pressure.
 *
 * FIX 2 (chosen) — Branchless per-group accumulation with mask arithmetic:
 *   For each morsel and each group g (0..5), compute a mask = (grp == g)
 *   and accumulate: sum_qty += q * mask, etc. This is fully vectorizable
 *   with 6 passes per morsel. With SIMD (8 int64/AVX2), throughput is
 *   ~7 columns * 6 groups * N/8 = 42N/8 vs scatter N*7 scalar ops.
 *   For N=200K: 42*200K/8 = 1.05M SIMD ops vs ~1.4M scalar -- better ILP.
 *
 *   But: 6 group passes x 7 columns = 42 accumulations per row (vs 7 with
 *   scatter). The vectorization win must compensate. Given AVX2 = 4x int64,
 *   net throughput improvement is ~2-3x.
 *
 * FIX 3 (chosen) — __int128 accumulator for sum_charge (per thread, not per row):
 *   The charge split into main+rem was needed to avoid __int128 in the hot loop.
 *   With branchless mask approach, we accumulate sum_disc_price (int64) then
 *   compute sum_charge in a SECOND vectorized pass using __int128 per thread:
 *   thread local: __int128 sum_charge[6] — only 6 entries, fits in L1.
 *   Inner loop: charge[grp] += (int128)(e * ed) * (100 + t)
 *   This is still scalar for the charge accumulation, but we separate it into
 *   its own tight loop so the other 5 aggregations vectorize cleanly.
 *
 * FINAL STRATEGY (combining all):
 *   Per morsel (200K rows), 2 passes:
 *   Pass A (vectorizable): for each row compute grp, q, e, d, t, ed, ed_e;
 *     accumulate sum_qty, sum_ep, sum_disc_price, sum_disc, count into
 *     group-keyed int64 accumulators using branchless mask:
 *       for g in 0..5: accumulate where (grp == g) -> vectorizable
 *   Pass B (scalar int128): accumulate sum_charge for each row
 *     charge[grp] += (__int128)(e * ed) * (100 + t)
 *
 *   The key insight: Pass A separates 6 groups so compiler auto-vectorizes.
 *   Pass B has a scatter but only 1 accumulation per row (vs 7 previously).
 *
 * Additional improvements:
 *   - Increase morsel size to 400K to reduce scheduling overhead
 *   - Use atomic morsel counter (lock-free work stealing) instead of OMP dynamic
 *   - MADV_WILLNEED + MADV_SEQUENTIAL for prefetching
 *   - Precompute (returnflag * 2 + linestatus) into a group array? No — that
 *     requires a separate 60M-row pass. Not worth it.
 *   - Thread count: use all 64 cores
 *
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
#include <immintrin.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================================
// Per-thread, per-group aggregation state.
// Uses __int128 for sum_charge (thread-local only -- not hot register pressure).
// int64 for everything else.
// Padded to 128 bytes to prevent false sharing between threads.
// ============================================================================
struct alignas(64) AggState {
    int64_t  sum_qty        = 0;  // scale=100
    int64_t  sum_ep         = 0;  // scale=100
    int64_t  sum_disc_price = 0;  // scale=10000
    __int128 sum_charge     = 0;  // scale=1000000
    int64_t  sum_disc       = 0;  // scale=100
    int64_t  count          = 0;
    bool     valid          = false;

    void merge(const AggState& o) {
        sum_qty        += o.sum_qty;
        sum_ep         += o.sum_ep;
        sum_disc_price += o.sum_disc_price;
        sum_charge     += o.sum_charge;
        sum_disc       += o.sum_disc;
        count          += o.count;
        valid          |= o.valid;
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
    // Phase 3: Memory-map columns with sequential prefetch hints
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_returnflag(li_dir + "l_returnflag.bin");
    gendb::MmapColumn<int32_t> col_linestatus(li_dir + "l_linestatus.bin");
    gendb::MmapColumn<int64_t> col_quantity  (li_dir + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_ep        (li_dir + "l_extendedprice.bin");
    gendb::MmapColumn<int64_t> col_discount  (li_dir + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_tax       (li_dir + "l_tax.bin");
    gendb::MmapColumn<int32_t> col_shipdate  (li_dir + "l_shipdate.bin");

    auto madvise_col = [](const void* ptr, size_t bytes) {
        madvise(const_cast<void*>(ptr), bytes, MADV_SEQUENTIAL);
        madvise(const_cast<void*>(ptr), bytes, MADV_WILLNEED);
    };
    madvise_col(col_shipdate.data,   col_shipdate.size()   * sizeof(int32_t));
    madvise_col(col_returnflag.data, col_returnflag.size() * sizeof(int32_t));
    madvise_col(col_linestatus.data, col_linestatus.size() * sizeof(int32_t));
    madvise_col(col_quantity.data,   col_quantity.size()   * sizeof(int64_t));
    madvise_col(col_ep.data,         col_ep.size()         * sizeof(int64_t));
    madvise_col(col_discount.data,   col_discount.size()   * sizeof(int64_t));
    madvise_col(col_tax.data,        col_tax.size()        * sizeof(int64_t));

    const size_t total_rows = col_shipdate.size();

    // Raw pointers with __restrict__ to signal no aliasing
    const int32_t* __restrict__ shipdate   = col_shipdate.data;
    const int64_t* __restrict__ quantity   = col_quantity.data;
    const int64_t* __restrict__ ep         = col_ep.data;
    const int64_t* __restrict__ discount   = col_discount.data;
    const int64_t* __restrict__ tax        = col_tax.data;
    const int32_t* __restrict__ returnflag = col_returnflag.data;
    const int32_t* __restrict__ linestatus = col_linestatus.data;

    // -------------------------------------------------------------------------
    // Phase 4: Build morsel list from zone map
    //   Use 400K morsels for full zones to reduce scheduling overhead.
    //   Use 100K morsels for boundary zones for fine-grained load balance.
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

    // Larger morsels to amortize thread scheduling overhead.
    // 400K rows: each morsel is ~3.2MB of int64 data (5 col * 400K * 8B) -- fits L3
    static constexpr uint64_t MORSEL_FULL    = 400000ULL;
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

    // Thread-local aggregation: num_threads * MAX_GROUPS AggState
    static constexpr int MAX_GROUPS = 6;
    const int num_threads = omp_get_max_threads();
    // Pad each thread's group array to its own cache line block
    // Use a flat array: tls[tid * MAX_GROUPS + grp]
    std::vector<AggState> tls((size_t)num_threads * MAX_GROUPS);

    // -------------------------------------------------------------------------
    // Phase 5: Parallel scan
    //
    // Strategy: per morsel, TWO passes:
    //   Pass A — vectorizable: accumulate sum_qty, sum_ep, sum_disc_price,
    //            sum_disc, count using branchless mask per group.
    //            Compiler can vectorize because no scatter store -- each group
    //            has its own independent accumulator variables per iteration.
    //   Pass B — scalar int128: accumulate sum_charge = sum(e*ed*(100+t))
    //            This has a scatter but only 1 operation per row vs 7 before.
    //
    // Actually: the branchless mask approach for Pass A means we run 6 iterations
    // over the morsel (once per group), each doing a masked reduce. This allows
    // full auto-vectorization (no scatter, no branch).
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (int m = 0; m < num_morsels; m++) {
            const Morsel& mor = morsels[m];
            const int tid     = omp_get_thread_num();
            AggState* local   = &tls[(size_t)tid * MAX_GROUPS];

            const uint64_t row_start = mor.row_start;
            const uint64_t row_end   = mor.row_end;

            if (mor.full_zone) {
                // ==============================================================
                // FAST PATH: entire zone passes date filter.
                // Pass A: For each group g, run a vectorizable masked-accumulate
                //   loop over [row_start, row_end).
                //   mask = (returnflag[r]*2 + linestatus[r] == g) as int64 (0 or 1)
                //   sum_qty[g]        += q  * mask
                //   sum_ep[g]         += e  * mask
                //   sum_disc_price[g] += ed_e * mask  (scale=10000)
                //   sum_disc[g]       += d  * mask
                //   count[g]          += mask
                // No scatter store -> compiler auto-vectorizes inner loop.
                // ==============================================================

                // Temporaries for each group (register-resident during inner loops)
                int64_t lcl_qty[MAX_GROUPS]  = {};
                int64_t lcl_ep[MAX_GROUPS]   = {};
                int64_t lcl_dp[MAX_GROUPS]   = {};  // disc_price
                int64_t lcl_disc[MAX_GROUPS] = {};
                int64_t lcl_cnt[MAX_GROUPS]  = {};

                // Pass A: vectorizable grouped accumulation
                // Process each group in a separate loop -- no scatter
                for (int g = 0; g < MAX_GROUPS; g++) {
                    int64_t sq = 0, se = 0, sdp = 0, sd = 0, sc = 0;
                    const int32_t g32 = (int32_t)g;
                    #pragma omp simd reduction(+:sq,se,sdp,sd,sc)
                    for (uint64_t r = row_start; r < row_end; r++) {
                        const int32_t grp = returnflag[r] * 2 + linestatus[r];
                        const int64_t mask = (grp == g32) ? 1LL : 0LL;
                        const int64_t q   = quantity[r] * mask;
                        const int64_t e   = ep[r]       * mask;
                        const int64_t d   = discount[r] * mask;
                        sq  += q;
                        se  += e;
                        sdp += e * (100LL - discount[r]) * mask;  // ed_e * mask
                        sd  += d;
                        sc  += mask;
                    }
                    lcl_qty[g]  = sq;
                    lcl_ep[g]   = se;
                    lcl_dp[g]   = sdp;
                    lcl_disc[g] = sd;
                    lcl_cnt[g]  = sc;
                }

                // Pass B: sum_charge (scatter ok -- only 1 int128 add per row)
                for (uint64_t r = row_start; r < row_end; r++) {
                    const int32_t grp = returnflag[r] * 2 + linestatus[r];
                    const int64_t e   = ep[r];
                    const int64_t d   = discount[r];
                    const int64_t t   = tax[r];
                    const int64_t ed  = 100LL - d;
                    // sum_charge: e * ed * (100 + t) at scale=1000000
                    local[grp].sum_charge += (__int128)(e * ed) * (100LL + t);
                }

                // Merge temporaries into thread-local state
                for (int g = 0; g < MAX_GROUPS; g++) {
                    local[g].sum_qty        += lcl_qty[g];
                    local[g].sum_ep         += lcl_ep[g];
                    local[g].sum_disc_price += lcl_dp[g];
                    local[g].sum_disc       += lcl_disc[g];
                    local[g].count          += lcl_cnt[g];
                    if (lcl_cnt[g] > 0) local[g].valid = true;
                }

            } else {
                // ==============================================================
                // BOUNDARY PATH: per-row date predicate check.
                // Same two-pass strategy but with date filter incorporated.
                // ==============================================================
                int64_t lcl_qty[MAX_GROUPS]  = {};
                int64_t lcl_ep[MAX_GROUPS]   = {};
                int64_t lcl_dp[MAX_GROUPS]   = {};
                int64_t lcl_disc[MAX_GROUPS] = {};
                int64_t lcl_cnt[MAX_GROUPS]  = {};

                // Pass A: grouped vectorizable accumulation with date mask
                for (int g = 0; g < MAX_GROUPS; g++) {
                    int64_t sq = 0, se = 0, sdp = 0, sd = 0, sc = 0;
                    const int32_t g32 = (int32_t)g;
                    #pragma omp simd reduction(+:sq,se,sdp,sd,sc)
                    for (uint64_t r = row_start; r < row_end; r++) {
                        const int32_t sd_val = shipdate[r];
                        const int64_t date_ok = (sd_val <= SHIP_THRESHOLD) ? 1LL : 0LL;
                        const int32_t grp = returnflag[r] * 2 + linestatus[r];
                        const int64_t mask = date_ok & ((grp == g32) ? 1LL : 0LL);
                        const int64_t q   = quantity[r] * mask;
                        const int64_t e   = ep[r]       * mask;
                        const int64_t d   = discount[r] * mask;
                        sq  += q;
                        se  += e;
                        sdp += e * (100LL - discount[r]) * mask;
                        sd  += d;
                        sc  += mask;
                    }
                    lcl_qty[g]  = sq;
                    lcl_ep[g]   = se;
                    lcl_dp[g]   = sdp;
                    lcl_disc[g] = sd;
                    lcl_cnt[g]  = sc;
                }

                // Pass B: sum_charge with date filter
                for (uint64_t r = row_start; r < row_end; r++) {
                    if (shipdate[r] > SHIP_THRESHOLD) continue;
                    const int32_t grp = returnflag[r] * 2 + linestatus[r];
                    const int64_t e   = ep[r];
                    const int64_t d   = discount[r];
                    const int64_t t   = tax[r];
                    const int64_t ed  = 100LL - d;
                    local[grp].sum_charge += (__int128)(e * ed) * (100LL + t);
                }

                // Merge temporaries
                for (int g = 0; g < MAX_GROUPS; g++) {
                    local[g].sum_qty        += lcl_qty[g];
                    local[g].sum_ep         += lcl_ep[g];
                    local[g].sum_disc_price += lcl_dp[g];
                    local[g].sum_disc       += lcl_disc[g];
                    local[g].count          += lcl_cnt[g];
                    if (lcl_cnt[g] > 0) local[g].valid = true;
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
            global_agg[g].merge(tls[(size_t)t * MAX_GROUPS + g]);
        }
    }

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
                int grp = rf * 2 + ls;
                if (grp >= MAX_GROUPS) continue;
                if (global_agg[grp].valid && global_agg[grp].count > 0) {
                    rows.push_back({rf_dict[rf], ls_dict[ls], grp});
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

            // sum_charge: __int128 at scale=1000000
            // Split into high/low parts for double conversion
            __int128 ch = agg.sum_charge;
            int64_t ch_hi = (int64_t)(ch / 1000000LL);
            int64_t ch_lo = (int64_t)(ch % 1000000LL);
            double sum_charge = (double)ch_hi + (double)ch_lo / 1000000.0;

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
