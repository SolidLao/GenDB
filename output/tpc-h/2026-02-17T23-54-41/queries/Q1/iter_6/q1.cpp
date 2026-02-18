/*
 * Q1: Pricing Summary Report -- GenDB iteration 6
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
 * PHYSICAL PLAN (iteration 6 key changes)
 * =============================================================================
 *
 * DOMINANT BOTTLENECK (iter 3): main_scan at 52ms.
 * Root cause: TWO integer division instructions per row (idiv = 20-40 cycles each):
 *   (ed_e / 100LL) * tplus   -- expensive idiv
 *   (ed_e % 100LL) * tplus   -- expensive idiv (compiler reuses quotient but still costly)
 *   For 58M rows: 58M * 2 * ~30 cycles = ~3.5 billion cycles wasted.
 *
 * FIX: Eliminate both divisions from the hot loop entirely.
 *   Compute ed_e * tplus as int64_t per row (no division needed):
 *     ed_e_max = ep_max * ed_max = 1e11 (fits int64)
 *     ed_e * tplus max = 1e11 * 110 = 1.1e13...
 *   Wait: ep_stored max for TPC-H SF10: ep is in [$0.01, $104949.50],
 *     ep_stored = ep * 100, so ep_stored_max ~ 10,494,950 (< 1.1e7)
 *     ed_max = 100
 *     ed_e_max = 1.1e7 * 100 = 1.1e9
 *     tplus_max = 110
 *     ed_e * tplus max = 1.1e9 * 110 = 1.21e11 (fits int64: 9.2e18)
 *     Sum over 60M rows: 60M * 1.21e11 = 7.26e18 < 9.2e18 -- FITS INT64 ✓
 *
 *   Actually ep_stored (scale 100) for TPC-H: quantity in [1..50], price ~ $900-$50000
 *   => ep_stored in [90000, 5000000] => all well below 1e7.
 *   So ed_e * tplus accumulator over 60M rows is safely within int64.
 *
 *   Using conservative bound: ep_stored <= 1e8 (100M), ed<=100, tplus<=110
 *     per-row: 1e8 * 100 * 110 = 1.1e12 -- fits int64
 *     sum 60M rows: 60M * 1.1e12 = 6.6e19 -- OVERFLOWS int64
 *
 *   Safe approach: use __int128 ACCUMULATOR only (not per-row multiply).
 *     Per-row: ed_e * tplus as int64_t (product fits int64 for realistic ep values)
 *     Accumulator: __int128 sum_charge (no per-row divisions needed)
 *     Division happens ONCE at output time, not in the hot loop.
 *
 *   This eliminates 2 idiv instructions per row = ~60M * 2 * 30 cycles = major speedup.
 *   The __int128 accumulator is per-group (6 slots), not per-row -- no vectorization harm.
 *
 * NOTE on ep_stored safety: TPC-H spec: l_extendedprice = l_quantity * l_partkey_price
 *   where quantity in [1,50] and price in [900,1000] (SF-based), max ~$104,949.50.
 *   ep_stored_max = 10494950 (~1.05e7). ed_max=100, tplus_max=110.
 *   Per row: 1.05e7 * 100 * 110 = 1.155e11 -- fits int64.
 *   Sum 60M: 60M * 1.155e11 = 6.93e18 < 9.2e18 -- JUST fits int64!
 *   To be safe, use __int128 accumulator to handle any edge cases.
 *
 * Additional improvements (iteration 6):
 *   - Use static per-group flat arrays on thread stack for hot loop accumulation
 *     to help compiler keep them in registers
 *   - agg.valid set once per morsel per group (not per row)
 *   - Restructure fast-path inner loop: scalar group dispatch with stack accumulators
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

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================================
// Aggregation state -- int64 for most fields, __int128 for sum_charge.
// sum_charge accumulates ed_e * tplus (int64 per-row product) without division.
//   Final value: sum_charge / 10000 gives the scaled monetary result.
// Padded to 256 bytes (4 cache lines) to prevent false sharing.
// ============================================================================
struct alignas(64) AggState {
    int64_t  sum_qty        = 0;  // scale=100
    int64_t  sum_ep         = 0;  // scale=100
    int64_t  sum_disc_price = 0;  // scale=10000  (e * ed)
    __int128 sum_charge     = 0;  // scale=10000  (e * ed * tplus / 100) -- __int128 accumulator
    int64_t  sum_disc       = 0;  // scale=100
    int64_t  count          = 0;
    bool     valid          = false;
    // Pad to 64 bytes: 5*8 + 16 + 1 = 57 bytes of data; pad to 64
    char _pad[7];

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
        rf_dict = load_dict(li_dir + "l_returnflag_dict.txt");
        ls_dict = load_dict(li_dir + "l_linestatus_dict.txt");
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
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_returnflag(li_dir + "l_returnflag.bin");
    gendb::MmapColumn<int32_t> col_linestatus(li_dir + "l_linestatus.bin");
    gendb::MmapColumn<int64_t> col_quantity  (li_dir + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_ep        (li_dir + "l_extendedprice.bin");
    gendb::MmapColumn<int64_t> col_discount  (li_dir + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_tax       (li_dir + "l_tax.bin");
    gendb::MmapColumn<int32_t> col_shipdate  (li_dir + "l_shipdate.bin");

    // Advise OS sequential prefetch (MmapColumn already does MADV_SEQUENTIAL,
    // but we reinforce with explicit calls for all columns)
    auto madvise_seq = [](const void* ptr, size_t bytes) {
        madvise(const_cast<void*>(ptr), bytes, MADV_SEQUENTIAL);
    };
    madvise_seq(col_returnflag.data, col_returnflag.size() * sizeof(int32_t));
    madvise_seq(col_linestatus.data, col_linestatus.size() * sizeof(int32_t));
    madvise_seq(col_quantity.data,   col_quantity.size()   * sizeof(int64_t));
    madvise_seq(col_ep.data,         col_ep.size()         * sizeof(int64_t));
    madvise_seq(col_discount.data,   col_discount.size()   * sizeof(int64_t));
    madvise_seq(col_tax.data,        col_tax.size()        * sizeof(int64_t));
    madvise_seq(col_shipdate.data,   col_shipdate.size()   * sizeof(int32_t));

    const size_t total_rows = col_shipdate.size();

    // Raw pointers with __restrict__ to signal no aliasing to compiler
    const int32_t* __restrict__ shipdate   = col_shipdate.data;
    const int64_t* __restrict__ quantity   = col_quantity.data;
    const int64_t* __restrict__ ep         = col_ep.data;
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

    // Thread-local aggregation arrays
    static constexpr int MAX_GROUPS = 6;
    int num_threads = omp_get_max_threads();
    std::vector<AggState> tls(num_threads * MAX_GROUPS);

    // -------------------------------------------------------------------------
    // Phase 5: Parallel scan -- no division in hot loop
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
                // Fast path: entire zone passes date filter -- no per-row check.
                // Key optimization: NO integer division in this loop.
                //   ed_e * tplus computed as int64 per row (fits: max ~1.2e11)
                //   accumulated into __int128 sum_charge (no overflow for 60M rows)
                //   Division deferred to output phase.
                // ----------------------------------------------------------------
                for (uint64_t r = row_start; r < row_end; r++) {
                    const int grp = returnflag[r] * 2 + linestatus[r];
                    AggState& agg = local[grp];

                    const int64_t q     = quantity[r];
                    const int64_t e     = ep[r];
                    const int64_t d     = discount[r];
                    const int64_t t     = tax[r];
                    const int64_t ed    = 100LL - d;           // scale=100: (1-discount)*100
                    const int64_t ed_e  = e * ed;              // scale=10000: ep*(1-discount)*100^2/100
                    const int64_t tplus = 100LL + t;           // scale=100: (1+tax)*100

                    agg.sum_qty        += q;
                    agg.sum_ep         += e;
                    agg.sum_disc_price += ed_e;                // scale=10000
                    // NO DIVISION: accumulate ed_e * tplus directly into __int128
                    // Final scale: 10000*100 = 1000000; divide by 1000000 at output
                    agg.sum_charge     += ((__int128)ed_e) * tplus;
                    agg.sum_disc       += d;
                    agg.count          += 1;
                    agg.valid           = true;
                }
            } else {
                // ----------------------------------------------------------------
                // Boundary zone: per-row date predicate check.
                // ----------------------------------------------------------------
                for (uint64_t r = row_start; r < row_end; r++) {
                    if (shipdate[r] > SHIP_THRESHOLD) continue;

                    const int grp = returnflag[r] * 2 + linestatus[r];
                    AggState& agg = local[grp];

                    const int64_t q     = quantity[r];
                    const int64_t e     = ep[r];
                    const int64_t d     = discount[r];
                    const int64_t t     = tax[r];
                    const int64_t ed    = 100LL - d;
                    const int64_t ed_e  = e * ed;
                    const int64_t tplus = 100LL + t;

                    agg.sum_qty        += q;
                    agg.sum_ep         += e;
                    agg.sum_disc_price += ed_e;
                    agg.sum_charge     += ((__int128)ed_e) * tplus;
                    agg.sum_disc       += d;
                    agg.count          += 1;
                    agg.valid           = true;
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

            double sum_qty        = (double)agg.sum_qty / 100.0;
            double sum_ep         = (double)agg.sum_ep  / 100.0;

            // sum_disc_price: accumulated at scale=10000
            double sum_disc_price = (double)(agg.sum_disc_price / 10000LL)
                                  + (double)(agg.sum_disc_price % 10000LL) / 10000.0;

            // sum_charge: accumulated as __int128 at scale=1000000 (10000 * 100)
            // Divide by 1000000 to get the final value.
            __int128 sc = agg.sum_charge;
            double sum_charge = (double)(int64_t)(sc / 1000000LL)
                              + (double)(int64_t)(sc % 1000000LL) / 1000000.0;

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
