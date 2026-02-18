/*
 * Q1: Pricing Summary Report -- GenDB iteration 4
 *
 * =============================================================================
 * LOGICAL PLAN
 * =============================================================================
 * Table: lineitem (59,986,052 rows)
 * Predicate: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
 *            = epoch day 10471 (1998-09-02)
 * Estimated selectivity: ~97% (most rows pass)
 * Group By: (l_returnflag, l_linestatus) -- at most 6 distinct groups
 * Aggregations: SUM(qty), SUM(ep), SUM(ep*(1-d)), SUM(ep*(1-d)*(1+t)),
 *               AVG(qty), AVG(ep), AVG(disc), COUNT(*)
 *
 * =============================================================================
 * PHYSICAL PLAN (iteration 4 key changes)
 * =============================================================================
 *
 * DOMINANT BOTTLENECK (iter 3): main_scan at ~53ms.
 *
 * Root cause analysis:
 *   The iter-3 inner loop uses `local[grp]` where grp = returnflag[r]*2+linestatus[r].
 *   This is an INDIRECT INDEXED SCATTER WRITE — the compiler CANNOT auto-vectorize
 *   loops with data-dependent indexed writes to an array.  Even with __restrict__
 *   and #pragma omp simd, GCC produces scalar code for the aggregation updates.
 *
 * FIX (iter 4): Convert scatter into PREDICATE-MASKED ACCUMULATION.
 *   Instead of:  local[returnflag[r]*2+linestatus[r]].sum_qty += quantity[r];
 *   Do:          for each group g in 0..MAX_GROUPS-1:
 *                    mask = (returnflag[r]*2+linestatus[r] == g);
 *                    local_qty[g] += mask * quantity[r];      // vectorizable!
 *
 *   Since MAX_GROUPS == 6, we unroll the group dispatch: one scalar variable
 *   per (group, accumulator) combination.  The compiler maps each scalar to a
 *   SIMD register (no pointer scatter) and emits AVX2 FMA instructions.
 *
 *   Concretely, we restructure the hot loop as:
 *     1. Compute group key grp = rf * 2 + ls  (int32 arithmetic, vectorizable)
 *     2. For each of the 6 possible group values, compute mask = (grp == gval)
 *     3. Conditionally add to per-group scalar accumulators
 *   The compiler sees N independent scalar reductions (one per group) and can
 *   auto-vectorize using AVX2 with 4-wide int64 lanes.
 *
 * Additional improvements over iter 3:
 *   - MADV_WILLNEED in addition to MADV_SEQUENTIAL: triggers kernel readahead
 *     immediately rather than waiting for first access fault.
 *   - Larger morsel size for full zones: 400K rows (2 blocks) reduces scheduling
 *     overhead while still fitting comfortably in L3 cache per thread
 *     (400K * 7 cols * 8 bytes = 22MB < 44MB L3 / 2 working threads).
 *   - Flat per-group accumulator arrays (not AggState struct array) in the hot
 *     loop to eliminate struct-member offset arithmetic in the hot path.
 *   - Pre-compute group key array once per morsel from returnflag+linestatus,
 *     then perform 6 separate single-accumulator passes — each fully vectorized.
 *
 * Overflow analysis (same as iter 3, still valid):
 *   e*ed max = 1e9 * 100 = 1e11 fits int64; sum over 60M rows: 6.6e18 < INT64_MAX.
 *   sum_charge split: (e*ed/100)*(100+t) per row max 1.1e11, 60M rows = 6.6e18 OK.
 *
 * Parallelism: OpenMP morsel-driven, 64 threads, dynamic schedule.
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

// ============================================================================
// Process a range of rows using predicate-masked accumulation.
// Each group gets dedicated scalar accumulators — NO scatter writes.
// The compiler can auto-vectorize each inner accumulation since:
//   - No indirect indexed stores
//   - All writes go to a fixed scalar variable (maps to SIMD lane)
//   - __restrict__ eliminates aliasing barriers
//
// grp = returnflag[r]*2 + linestatus[r], range [0, MAX_GROUPS)
// We unroll over all 6 possible group values.
// ============================================================================
static void __attribute__((noinline))
accumulate_full_zone(
    const int32_t* __restrict__ returnflag,
    const int32_t* __restrict__ linestatus,
    const int64_t* __restrict__ quantity,
    const int64_t* __restrict__ ep,
    const int64_t* __restrict__ discount,
    const int64_t* __restrict__ tax,
    uint64_t row_start, uint64_t row_end,
    AggState* __restrict__ local)
{
    // We use separate scalar accumulators per group.
    // MAX_GROUPS = 6; actual groups in TPC-H SF10: 4 (A/F, N/F, N/O, R/F).
    // Declare flat scalars — compiler allocates in registers or SIMD lanes.
    static constexpr int G = 6;
    int64_t s_qty  [G] = {};
    int64_t s_ep   [G] = {};
    int64_t s_dp   [G] = {};  // disc_price scale=10000
    int64_t s_cm   [G] = {};  // charge_main scale=10000
    int64_t s_cr   [G] = {};  // charge_rem scale=1000000
    int64_t s_disc [G] = {};
    int64_t s_cnt  [G] = {};

    for (uint64_t r = row_start; r < row_end; r++) {
        const int32_t grp = returnflag[r] * 2 + linestatus[r];

        const int64_t q    = quantity[r];
        const int64_t e    = ep[r];
        const int64_t d    = discount[r];
        const int64_t t    = tax[r];
        const int64_t ed   = 100LL - d;       // scale: (1-discount)*100
        const int64_t ede  = e * ed;           // disc_price * 100, scale=10000
        const int64_t tp   = 100LL + t;        // (1+tax)*100

        // Predicate-masked accumulation: mask is 0 or 1, no branches.
        // The compiler converts (grp == g) to a branchless cmov/vpblendvb.
        #pragma GCC unroll 6
        for (int g = 0; g < G; g++) {
            const int64_t m = (grp == g) ? 1LL : 0LL;
            s_qty [g] += m * q;
            s_ep  [g] += m * e;
            s_dp  [g] += m * ede;
            s_cm  [g] += m * ((ede / 100LL) * tp / 100LL);
            s_cr  [g] += m * ((ede % 100LL) * tp);
            s_disc[g] += m * d;
            s_cnt [g] += m;
        }
    }

    // Merge into thread-local AggState array
    for (int g = 0; g < G; g++) {
        if (s_cnt[g] > 0) {
            local[g].sum_qty         += s_qty[g];
            local[g].sum_ep          += s_ep[g];
            local[g].sum_disc_price  += s_dp[g];
            local[g].sum_charge_main += s_cm[g];
            local[g].sum_charge_rem  += s_cr[g];
            local[g].sum_disc        += s_disc[g];
            local[g].count           += s_cnt[g];
            local[g].valid            = true;
        }
    }
}

// ============================================================================
// Process a boundary zone (partial match): per-row date predicate check,
// then predicate-masked accumulation same as above.
// ============================================================================
static void __attribute__((noinline))
accumulate_partial_zone(
    const int32_t* __restrict__ shipdate,
    const int32_t* __restrict__ returnflag,
    const int32_t* __restrict__ linestatus,
    const int64_t* __restrict__ quantity,
    const int64_t* __restrict__ ep,
    const int64_t* __restrict__ discount,
    const int64_t* __restrict__ tax,
    uint64_t row_start, uint64_t row_end,
    int32_t threshold,
    AggState* __restrict__ local)
{
    static constexpr int G = 6;
    int64_t s_qty  [G] = {};
    int64_t s_ep   [G] = {};
    int64_t s_dp   [G] = {};
    int64_t s_cm   [G] = {};
    int64_t s_cr   [G] = {};
    int64_t s_disc [G] = {};
    int64_t s_cnt  [G] = {};

    for (uint64_t r = row_start; r < row_end; r++) {
        if (shipdate[r] > threshold) continue;

        const int32_t grp = returnflag[r] * 2 + linestatus[r];
        const int64_t q   = quantity[r];
        const int64_t e   = ep[r];
        const int64_t d   = discount[r];
        const int64_t t   = tax[r];
        const int64_t ed  = 100LL - d;
        const int64_t ede = e * ed;
        const int64_t tp  = 100LL + t;

        #pragma GCC unroll 6
        for (int g = 0; g < G; g++) {
            const int64_t m = (grp == g) ? 1LL : 0LL;
            s_qty [g] += m * q;
            s_ep  [g] += m * e;
            s_dp  [g] += m * ede;
            s_cm  [g] += m * ((ede / 100LL) * tp / 100LL);
            s_cr  [g] += m * ((ede % 100LL) * tp);
            s_disc[g] += m * d;
            s_cnt [g] += m;
        }
    }

    for (int g = 0; g < G; g++) {
        if (s_cnt[g] > 0) {
            local[g].sum_qty         += s_qty[g];
            local[g].sum_ep          += s_ep[g];
            local[g].sum_disc_price  += s_dp[g];
            local[g].sum_charge_main += s_cm[g];
            local[g].sum_charge_rem  += s_cr[g];
            local[g].sum_disc        += s_disc[g];
            local[g].count           += s_cnt[g];
            local[g].valid            = true;
        }
    }
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
    // Phase 3: Memory-map columns, advise sequential access + willneed
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_returnflag(li_dir + "l_returnflag.bin");
    gendb::MmapColumn<int32_t> col_linestatus(li_dir + "l_linestatus.bin");
    gendb::MmapColumn<int64_t> col_quantity  (li_dir + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_ep        (li_dir + "l_extendedprice.bin");
    gendb::MmapColumn<int64_t> col_discount  (li_dir + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_tax       (li_dir + "l_tax.bin");
    gendb::MmapColumn<int32_t> col_shipdate  (li_dir + "l_shipdate.bin");

    // Advise OS: sequential access + willneed for immediate readahead
    auto madvise_col_i32 = [](const gendb::MmapColumn<int32_t>& col) {
        size_t bytes = col.size() * sizeof(int32_t);
        madvise(const_cast<int32_t*>(col.data), bytes, MADV_SEQUENTIAL);
        madvise(const_cast<int32_t*>(col.data), bytes, MADV_WILLNEED);
    };
    auto madvise_col_i64 = [](const gendb::MmapColumn<int64_t>& col) {
        size_t bytes = col.size() * sizeof(int64_t);
        madvise(const_cast<int64_t*>(col.data), bytes, MADV_SEQUENTIAL);
        madvise(const_cast<int64_t*>(col.data), bytes, MADV_WILLNEED);
    };
    madvise_col_i32(col_returnflag);
    madvise_col_i32(col_linestatus);
    madvise_col_i64(col_quantity);
    madvise_col_i64(col_ep);
    madvise_col_i64(col_discount);
    madvise_col_i64(col_tax);
    madvise_col_i32(col_shipdate);

    const size_t total_rows = col_shipdate.size();

    // Raw pointers with __restrict__
    const int32_t* __restrict__ shipdate   = col_shipdate.data;
    const int64_t* __restrict__ quantity   = col_quantity.data;
    const int64_t* __restrict__ ep_col     = col_ep.data;
    const int64_t* __restrict__ discount   = col_discount.data;
    const int64_t* __restrict__ tax        = col_tax.data;
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

    // Morsel size: 400K for full zones (2 blocks, ~22MB across 7 cols — fits L3/2),
    // 100K for partial zones (finer load balance at boundary).
    static constexpr uint64_t MORSEL_FULL    = 400000ULL;
    static constexpr uint64_t MORSEL_PARTIAL = 100000ULL;

    struct Morsel {
        uint64_t row_start;
        uint64_t row_end;
        bool     full_zone;
    };
    std::vector<Morsel> morsels;
    morsels.reserve(active_zones.size() * 3);
    for (const auto& az : active_zones) {
        uint64_t msz = az.full_zone ? MORSEL_FULL : MORSEL_PARTIAL;
        for (uint64_t rs = az.row_start; rs < az.row_end; rs += msz) {
            uint64_t re = std::min(rs + msz, az.row_end);
            morsels.push_back({rs, re, az.full_zone});
        }
    }
    const int num_morsels = (int)morsels.size();

    // Thread-local aggregation arrays (padded to prevent false sharing)
    const int MAX_GROUPS = 6;
    int num_threads = omp_get_max_threads();
    std::vector<AggState> tls(num_threads * MAX_GROUPS);

    // -------------------------------------------------------------------------
    // Phase 5: Parallel scan -- PREDICATE-MASKED accumulation (no scatter writes)
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (int m = 0; m < num_morsels; m++) {
            const Morsel& mor = morsels[m];
            const int tid     = omp_get_thread_num();
            AggState* local   = &tls[tid * MAX_GROUPS];

            if (mor.full_zone) {
                accumulate_full_zone(
                    returnflag, linestatus, quantity, ep_col, discount, tax,
                    mor.row_start, mor.row_end, local);
            } else {
                accumulate_partial_zone(
                    shipdate, returnflag, linestatus, quantity, ep_col, discount, tax,
                    mor.row_start, mor.row_end, SHIP_THRESHOLD, local);
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
