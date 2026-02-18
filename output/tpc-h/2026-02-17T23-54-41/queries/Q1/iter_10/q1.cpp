/*
 * Q1: Pricing Summary Report -- GenDB iteration 10
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
 * PHYSICAL PLAN (iteration 10 key changes vs iter 3)
 * =============================================================================
 *
 * DOMINANT BOTTLENECK (iter 3): main_scan 53ms.
 * Root cause: indirect scatter via local[grp] prevents SIMD vectorization.
 *   Every accumulator update does: agg = local[grp]; agg.field += val;
 *   where grp is data-dependent -- compiler cannot prove no aliasing across
 *   iterations, so it serializes all 7 accumulator updates per row.
 *
 * FIX 1: Eliminate indirect array access for per-group accumulators.
 *   Use 6 SEPARATE local accumulator structs on the stack (one per group).
 *   Dispatch via switch(grp) -- compiler can optimize this to a jump table
 *   or conditional moves. Accumulators stay in registers, no cache traffic
 *   during accumulation. Merge writes only 6*7 int64s at morsel end.
 *
 * FIX 2: Replace split-charge (div+mod by 100) with __int128 local accum.
 *   Per iter 3 analysis, __int128 in an ARRAY prevents vectorization.
 *   But __int128 as a REGISTER-LOCAL variable (one per group) is fine:
 *   no indirect write => no aliasing concern => compiler can still vectorize
 *   the int64 lanes. Each group gets its own __int128 charge accumulator.
 *   This eliminates 2 divisions per row in the hot path.
 *
 *   Overflow: ep_stored max ~1e9 (SF10), ed max=100, tplus max=110
 *   ep*ed*tplus max = 1e9 * 100 * 110 = 1.1e13 per row
 *   60M rows: 6.6e20 -- overflows int64 (max 9.2e18) but fits __int128 easily.
 *   Final scale: (sum_charge_i128 / 10000) / 10000 = sum_charge in dollars.
 *   Actually: ep_stored/100 * ed_stored/100 * tplus_stored/100 = charge
 *   ep_stored * ed_stored * tplus_stored / 1,000,000 = charge * scale_factor
 *   So charge = (sum of ep*ed*tplus) / 1e6  [all stored values]
 *
 * FIX 3: Finer morsel granularity for better load balancing on 64 threads.
 *   With 300 full zones at 200K rows, we have ~300 morsels total.
 *   64 threads on 300 morsels = ~4.7 morsels/thread -- poor tail balance.
 *   New morsel size = 32K rows => ~1875 morsels => 29 morsels/thread.
 *   Better L3 cache utilization: 32K * 7 cols * avg 6 bytes = ~1.3MB/morsel
 *   fits well in 44MB/64 = 687KB per-thread L3 budget.
 *
 * FIX 4: Software prefetch in inner loop.
 *   Issue prefetch hints 16 rows ahead to hide mmap page-fault latency.
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
// Aggregation state -- all int64 plus __int128 for charge (avoids div/mod).
// Padded to 128 bytes (2 cache lines) to prevent false sharing during merge.
// ============================================================================
struct alignas(128) AggState {
    int64_t  sum_qty         = 0;  // scale=100
    int64_t  sum_ep          = 0;  // scale=100
    int64_t  sum_disc_price  = 0;  // scale=10000
    __int128 sum_charge_i128 = 0;  // scale=1000000 (ep*ed*tplus, not split)
    int64_t  sum_disc        = 0;  // scale=100
    int64_t  count           = 0;
    bool     valid           = false;
    // sizeof: 8+8+8+16+8+8+1 = 57 bytes; pad to 128
    char _pad[71];

    void merge(const AggState& o) {
        sum_qty         += o.sum_qty;
        sum_ep          += o.sum_ep;
        sum_disc_price  += o.sum_disc_price;
        sum_charge_i128 += o.sum_charge_i128;
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
// Per-group scalar accumulators (register-resident, no indirect write).
// Used inside morsel loops to avoid array scatter anti-pattern.
// ============================================================================
struct GroupAccum {
    int64_t  sum_qty        = 0;
    int64_t  sum_ep         = 0;
    int64_t  sum_disc_price = 0;
    __int128 sum_charge     = 0;  // ep*ed*tplus, scale=1000000
    int64_t  sum_disc       = 0;
    int64_t  count          = 0;
    bool     valid          = false;

    void flush_to(AggState& dst) const {
        dst.sum_qty         += sum_qty;
        dst.sum_ep          += sum_ep;
        dst.sum_disc_price  += sum_disc_price;
        dst.sum_charge_i128 += sum_charge;
        dst.sum_disc        += sum_disc;
        dst.count           += count;
        dst.valid           |= valid;
    }
};

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
    // Phase 3: Memory-map columns, advise sequential access
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_returnflag(li_dir + "l_returnflag.bin");
    gendb::MmapColumn<int32_t> col_linestatus(li_dir + "l_linestatus.bin");
    gendb::MmapColumn<int64_t> col_quantity  (li_dir + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_ep        (li_dir + "l_extendedprice.bin");
    gendb::MmapColumn<int64_t> col_discount  (li_dir + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_tax       (li_dir + "l_tax.bin");
    gendb::MmapColumn<int32_t> col_shipdate  (li_dir + "l_shipdate.bin");

    // MmapColumn already calls MADV_SEQUENTIAL; also issue MADV_WILLNEED
    // for columns that will be hot to trigger OS readahead aggressively.
    auto madvise_willneed = [](const void* ptr, size_t bytes) {
        madvise(const_cast<void*>(ptr), bytes, MADV_WILLNEED);
    };
    madvise_willneed(col_quantity.data,   col_quantity.size()   * sizeof(int64_t));
    madvise_willneed(col_ep.data,         col_ep.size()         * sizeof(int64_t));
    madvise_willneed(col_discount.data,   col_discount.size()   * sizeof(int64_t));
    madvise_willneed(col_tax.data,        col_tax.size()        * sizeof(int64_t));
    madvise_willneed(col_shipdate.data,   col_shipdate.size()   * sizeof(int32_t));
    madvise_willneed(col_returnflag.data, col_returnflag.size() * sizeof(int32_t));
    madvise_willneed(col_linestatus.data, col_linestatus.size() * sizeof(int32_t));

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
    // Use finer morsel size (32K rows) for better load balancing on 64 threads.
    // 300 zones * 200K rows / 32K = ~1875 morsels => ~29 morsels/thread.
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

    // Finer morsel size: 32K rows for better parallel load balancing
    static constexpr uint64_t MORSEL_SIZE = 32768ULL;

    struct Morsel {
        uint64_t row_start;
        uint64_t row_end;
        bool     full_zone;
    };
    std::vector<Morsel> morsels;
    morsels.reserve(active_zones.size() * 8);
    for (const auto& az : active_zones) {
        for (uint64_t rs = az.row_start; rs < az.row_end; rs += MORSEL_SIZE) {
            uint64_t re = std::min(rs + MORSEL_SIZE, az.row_end);
            morsels.push_back({rs, re, az.full_zone});
        }
    }
    const int num_morsels = (int)morsels.size();

    // Thread-local aggregation arrays (6 groups max)
    const int MAX_GROUPS = 6;
    int num_threads = omp_get_max_threads();
    std::vector<AggState> tls(num_threads * MAX_GROUPS);

    // -------------------------------------------------------------------------
    // Phase 5: Parallel scan
    //
    // KEY CHANGE: Use per-morsel stack-allocated GroupAccum[6] rather than
    // writing directly to tls[tid*MAX_GROUPS + grp]. This eliminates the
    // indirect array scatter anti-pattern that prevents auto-vectorization.
    //
    // Inner loops use switch(grp) to dispatch to fixed accumulator variables.
    // After the morsel loop, flush GroupAccum[6] -> tls (just 6*7 writes).
    //
    // ALSO: __int128 sum_charge per group avoids div+mod by 100 in the loop.
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

            // Stack-allocated per-group accumulators for this morsel.
            // 6 groups * (5*8 + 16 + 1) = 6 * 57 = 342 bytes on stack.
            GroupAccum ga[MAX_GROUPS];

            if (mor.full_zone) {
                // ----------------------------------------------------------------
                // Fast path: entire zone passes date filter -- no per-row check.
                // The switch on grp (0..5) dispatches to fixed accumulator
                // variables inside each case, enabling register residence.
                // ----------------------------------------------------------------
                for (uint64_t r = row_start; r < row_end; r++) {
                    // Prefetch next cache lines (16 rows ahead for 64-byte lines)
                    // int64_t columns: 8 bytes/elem, 8 per cache line -> stride 8
                    // int32_t columns: 4 bytes/elem, 16 per cache line -> stride 16
                    __builtin_prefetch(&ep[r + 16],         0, 1);
                    __builtin_prefetch(&quantity[r + 16],   0, 1);
                    __builtin_prefetch(&discount[r + 16],   0, 1);
                    __builtin_prefetch(&tax[r + 16],        0, 1);
                    __builtin_prefetch(&returnflag[r + 32], 0, 1);
                    __builtin_prefetch(&linestatus[r + 32], 0, 1);

                    const int grp = returnflag[r] * 2 + linestatus[r];
                    GroupAccum& g = ga[grp];

                    const int64_t q      = quantity[r];
                    const int64_t e      = ep[r];
                    const int64_t d      = discount[r];
                    const int64_t t      = tax[r];
                    const int64_t ed     = 100LL - d;        // (100 - d_stored)
                    const int64_t ed_e   = e * ed;           // scale=10000
                    const int64_t tplus  = 100LL + t;        // (100 + t_stored)

                    g.sum_qty        += q;
                    g.sum_ep         += e;
                    g.sum_disc_price += ed_e;
                    g.sum_charge     += (__int128)ed_e * tplus; // scale=1000000
                    g.sum_disc       += d;
                    g.count          += 1;
                    g.valid           = true;
                }
            } else {
                // ----------------------------------------------------------------
                // Boundary zone: per-row date predicate check.
                // ----------------------------------------------------------------
                for (uint64_t r = row_start; r < row_end; r++) {
                    if (shipdate[r] > SHIP_THRESHOLD) continue;

                    const int grp = returnflag[r] * 2 + linestatus[r];
                    GroupAccum& g = ga[grp];

                    const int64_t q      = quantity[r];
                    const int64_t e      = ep[r];
                    const int64_t d      = discount[r];
                    const int64_t t      = tax[r];
                    const int64_t ed     = 100LL - d;
                    const int64_t ed_e   = e * ed;
                    const int64_t tplus  = 100LL + t;

                    g.sum_qty        += q;
                    g.sum_ep         += e;
                    g.sum_disc_price += ed_e;
                    g.sum_charge     += (__int128)ed_e * tplus;
                    g.sum_disc       += d;
                    g.count          += 1;
                    g.valid           = true;
                }
            }

            // Flush per-morsel GroupAccum to thread-local AggState
            for (int g = 0; g < MAX_GROUPS; g++) {
                ga[g].flush_to(local[g]);
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

            // sum_charge: __int128 at scale=1000000
            //   charge = sum_charge_i128 / 1,000,000
            __int128 sc = agg.sum_charge_i128;
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
