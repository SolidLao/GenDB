/*
 * Q1: Pricing Summary Report — GenDB iteration 2
 *
 * =============================================================================
 * LOGICAL PLAN
 * =============================================================================
 * Table: lineitem (59,986,052 rows)
 * Predicate: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
 *            = epoch day 10471 (1998-09-02)
 * Estimated selectivity: ~97% (most rows pass — data goes up to 1998-12-01)
 * Group By: (l_returnflag, l_linestatus) — at most 6 distinct groups
 *   l_returnflag ∈ {A=0, N=1, R=2}  (dict idx)
 *   l_linestatus ∈ {O=0, F=1}       (dict idx)
 * Aggregations: SUM(qty), SUM(ep), SUM(ep*(1-d)), SUM(ep*(1-d)*(1+t)),
 *               SUM(qty for avg), SUM(ep for avg), SUM(disc), COUNT(*)
 * Output: sorted by (l_returnflag, l_linestatus) — only 6 groups, trivial
 *
 * =============================================================================
 * PHYSICAL PLAN (iteration 2 changes)
 * =============================================================================
 * Scan strategy:
 *   - Zone map pruning on l_shipdate (300 zones, block_size=200K)
 *     skip zones where zone.min > threshold (entire zone eliminated at O(1))
 *   - Morsel-driven parallelism at row level:
 *       - 64 cores available; 300 zones = only 300 work items (too coarse)
 *       - Instead: partition at morsel granularity (MORSEL_SIZE rows per task)
 *         Zone-map filtering first to skip entire zones; within each active zone,
 *         threads process morsels of MORSEL_SIZE rows independently.
 *       - This yields ~600 morsels → much better load balance across 64 cores.
 *   - Full-zone fast path: no per-row shipdate check needed.
 *
 * Aggregation:
 *   - Group key = returnflag_code * 2 + linestatus_code (0..5, flat array)
 *   - Thread-local flat arrays, cache-line padded to avoid false sharing
 *   - All DECIMAL columns stored as int64_t at scale=100
 *   - Accumulators:
 *     sum_qty:        int64 scaled by 100 → output /100
 *     sum_ep:         int64 scaled by 100 → output /100
 *     sum_disc_price: int64, scale=10000 → output /10000
 *                     max per-row: ep_max_stored=17M, (100-d_max)=100 → 1.7B
 *                     sum over 60M rows: 1.7B*60M=1.02e17 < INT64_MAX(9.2e18) ✓
 *     sum_charge:     __int128, scale=1e6 → output /1e6
 *                     max per-row: 1.7B * 110 = 187B; *60M = 1.12e19 > INT64_MAX
 *                     → must use __int128
 *     sum_disc:       int64 scaled by 100 → output /100
 *   - valid flag set once per zone, not per row
 *   - NO intermediate division in hot loop
 *
 * Parallelism:
 *   - OpenMP parallel for over morsels with schedule(dynamic,1)
 *   - AggState padded to 128 bytes (2 cache lines) to eliminate false sharing
 *   - Thread-local aggregation structs, merged after parallel scan
 *
 * Dictionary mappings (loaded at runtime from dict files):
 *   returnflag: line index = dict code
 *   linestatus: line index = dict code
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
// Aggregation state for one (returnflag, linestatus) group
// Scaling:
//   sum_qty        : raw int64, scale=100   → output /100
//   sum_ep         : raw int64, scale=100   → output /100
//   sum_disc_price : int64,     scale=10000 → output /10000
//                    max: ep_stored_max(~17M) * 100 = 1.7B per row
//                    60M rows * 1.7B = 1.02e17 < INT64_MAX(9.2e18) ✓
//   sum_charge     : __int128,  scale=1e6   → output /1000000
//                    max: 1.7B * 110 = 187B per row; 60M * 187B = 1.12e19 → needs __int128
//   sum_disc       : raw int64, scale=100   → output /100
// Padded to 128 bytes (2 cache lines) to prevent false sharing across threads.
// ============================================================================
struct alignas(128) AggState {
    int64_t  sum_qty        = 0;
    int64_t  sum_ep         = 0;
    int64_t  sum_disc_price = 0;  // int64 sufficient — see bounds above
    __int128 sum_charge     = 0;
    int64_t  sum_disc       = 0;
    int64_t  count          = 0;
    bool     valid          = false;
    // Padding to reach 128 bytes:
    // 3*8 + 16 + 8 + 8 + 1 = 65 bytes → pad 63 bytes
    char _pad[63];

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
// Helper: load dictionary from file, return vector of strings indexed by code
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

    const std::string li_dir = gendb_dir + "/lineitem/";
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

    // DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 = epoch day 10471
    const int32_t SHIP_THRESHOLD = gendb::date_str_to_epoch_days("1998-09-02");

    // -------------------------------------------------------------------------
    // Phase 2: Load zone map
    // -------------------------------------------------------------------------
    // Zone map layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
    struct ZoneEntry {
        int32_t  min_val;
        int32_t  max_val;
        uint32_t count;
    };

    uint32_t num_zones = 0;
    const ZoneEntry* zones = nullptr;
    const uint8_t* zone_map_base = nullptr;
    int zone_fd = -1;
    size_t zone_file_size = 0;

    {
        GENDB_PHASE("build_joins");
        const std::string zm_path = idx_dir + "lineitem_l_shipdate_zonemap.bin";
        zone_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zone_fd < 0) {
            throw std::runtime_error("Cannot open zone map: " + zm_path);
        }
        struct stat st;
        fstat(zone_fd, &st);
        zone_file_size = st.st_size;
        void* ptr = mmap(nullptr, zone_file_size, PROT_READ, MAP_PRIVATE, zone_fd, 0);
        zone_map_base = static_cast<const uint8_t*>(ptr);
        num_zones = *reinterpret_cast<const uint32_t*>(zone_map_base);
        zones = reinterpret_cast<const ZoneEntry*>(zone_map_base + 4);
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

    const size_t total_rows = col_shipdate.size();
    const int32_t* shipdate   = col_shipdate.data;
    const int64_t* quantity   = col_quantity.data;
    const int64_t* ep         = col_ep.data;
    const int64_t* discount   = col_discount.data;
    const int64_t* tax        = col_tax.data;
    const int32_t* returnflag = col_returnflag.data;
    const int32_t* linestatus = col_linestatus.data;

    // -------------------------------------------------------------------------
    // Phase 4: Parallel scan with zone map pruning — morsel-driven row-level
    // -------------------------------------------------------------------------
    // Group key = rf_code * 2 + ls_code  (max 6 groups)
    const int MAX_GROUPS = 6;
    int num_threads = omp_get_max_threads();

    // Pre-compute active zone ranges to avoid zone-map check inside parallel loop.
    // Also determine full vs partial zones.
    struct ZoneRange {
        uint64_t row_start;
        uint64_t row_end;
        bool     full_zone;  // true = no per-row date check needed
    };
    std::vector<ZoneRange> active_zones;
    active_zones.reserve(num_zones);
    for (uint32_t z = 0; z < num_zones; z++) {
        if (zones[z].min_val > SHIP_THRESHOLD) continue;  // skip entirely
        uint64_t rs = (uint64_t)z * 200000ULL;
        uint64_t re = rs + zones[z].count;
        if (re > total_rows) re = total_rows;
        active_zones.push_back({rs, re, (zones[z].max_val <= SHIP_THRESHOLD)});
    }

    // Build morsel list: split each active zone into morsels of MORSEL_SIZE rows.
    // This gives ~600+ tasks for 64 cores → excellent load balance.
    static constexpr uint64_t MORSEL_SIZE = 100000ULL;  // 100K rows/morsel

    struct Morsel {
        uint64_t row_start;
        uint64_t row_end;
        bool     full_zone;
    };
    std::vector<Morsel> morsels;
    morsels.reserve(active_zones.size() * 3);
    for (const auto& az : active_zones) {
        for (uint64_t rs = az.row_start; rs < az.row_end; rs += MORSEL_SIZE) {
            uint64_t re = std::min(rs + MORSEL_SIZE, az.row_end);
            morsels.push_back({rs, re, az.full_zone});
        }
    }
    const int num_morsels = (int)morsels.size();

    // Thread-local aggregation: [thread][group] — AggState is 128-byte aligned
    // Use raw allocation to guarantee alignment across the array.
    const int tls_stride = MAX_GROUPS;  // groups per thread
    std::vector<AggState> tls(num_threads * tls_stride);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (int m = 0; m < num_morsels; m++) {
            const Morsel& mor = morsels[m];
            int tid = omp_get_thread_num();
            AggState* local = &tls[tid * tls_stride];

            uint64_t row_start = mor.row_start;
            uint64_t row_end   = mor.row_end;

            if (mor.full_zone) {
                // No per-row date check needed — tight inner loop
                for (uint64_t r = row_start; r < row_end; r++) {
                    int grp = returnflag[r] * 2 + linestatus[r];
                    AggState& agg = local[grp];

                    int64_t q  = quantity[r];
                    int64_t e  = ep[r];
                    int64_t d  = discount[r];
                    int64_t t  = tax[r];
                    int64_t ed = 100 - d;            // (100 - d_stored)

                    agg.sum_qty        += q;
                    agg.sum_ep         += e;
                    agg.sum_disc_price += e * ed;    // int64: within safe range
                    agg.sum_charge     += (__int128)e * ed * (100 + t);
                    agg.sum_disc       += d;
                    agg.count          += 1;
                    agg.valid           = true;
                }
            } else {
                // Partial zone: per-row date check
                for (uint64_t r = row_start; r < row_end; r++) {
                    if (shipdate[r] > SHIP_THRESHOLD) continue;

                    int grp = returnflag[r] * 2 + linestatus[r];
                    AggState& agg = local[grp];

                    int64_t q  = quantity[r];
                    int64_t e  = ep[r];
                    int64_t d  = discount[r];
                    int64_t t  = tax[r];
                    int64_t ed = 100 - d;

                    agg.sum_qty        += q;
                    agg.sum_ep         += e;
                    agg.sum_disc_price += e * ed;    // int64: within safe range
                    agg.sum_charge     += (__int128)e * ed * (100 + t);
                    agg.sum_disc       += d;
                    agg.count          += 1;
                    agg.valid           = true;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 5: Merge thread-local results
    // -------------------------------------------------------------------------
    std::vector<AggState> global_agg(MAX_GROUPS);
    for (int g = 0; g < MAX_GROUPS; g++) {
        for (int t = 0; t < num_threads; t++) {
            global_agg[g].merge(tls[t * tls_stride + g]);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 6: Output results
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Build output rows: (rf_str, ls_str, group_idx)
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

        // Sort by (l_returnflag, l_linestatus)
        std::sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
            if (a.rf_str != b.rf_str) return a.rf_str < b.rf_str;
            return a.ls_str < b.ls_str;
        });

        // Write CSV
        const std::string out_path = results_dir + "/Q1.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) throw std::runtime_error("Cannot open output: " + out_path);

        fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& row : rows) {
            const AggState& agg = global_agg[row.group];

            // sum_qty: stored as int64 at scale=100 → /100
            double sum_qty      = (double)agg.sum_qty / 100.0;
            // sum_ep: stored as int64 at scale=100 → /100
            double sum_ep       = (double)agg.sum_ep  / 100.0;

            // sum_disc_price: int64 = sum(ep_stored * (100 - d_stored))
            //   scale = 100 * 100 = 10000
            //   monetary value = sum_disc_price / 10000
            double sum_disc_price = (double)(agg.sum_disc_price / 10000)
                                  + (double)(agg.sum_disc_price % 10000) / 10000.0;

            // sum_charge: __int128 = sum(ep_stored * (100 - d_stored) * (100 + t_stored))
            //   scale = 100 * 100 * 100 = 1000000
            //   monetary value = sum_charge / 1000000
            double sum_charge_d  = (double)(int64_t)(agg.sum_charge / 1000000)
                                 + (double)(int64_t)(agg.sum_charge % 1000000) / 1000000.0;

            double avg_qty      = (double)agg.sum_qty  / 100.0 / agg.count;
            double avg_price    = (double)agg.sum_ep   / 100.0 / agg.count;
            double avg_disc     = (double)agg.sum_disc / 100.0 / agg.count;

            fprintf(fp, "%s,%s,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%ld\n",
                    row.rf_str.c_str(),
                    row.ls_str.c_str(),
                    sum_qty,
                    sum_ep,
                    sum_disc_price,
                    sum_charge_d,
                    avg_qty,
                    avg_price,
                    avg_disc,
                    agg.count);
        }

        fclose(fp);
    }

    // Clean up zone map
    if (zone_map_base) {
        munmap(const_cast<uint8_t*>(zone_map_base), zone_file_size);
    }
    if (zone_fd >= 0) ::close(zone_fd);
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
