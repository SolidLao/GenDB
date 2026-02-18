/*
 * Q1: Pricing Summary Report -- GenDB iteration 8
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
 * PHYSICAL PLAN (iteration 8 key changes vs iter 3)
 * =============================================================================
 *
 * DOMINANT BOTTLENECK (iter 3-7): main_scan at 53ms.
 *
 * Root cause of lingering bottleneck:
 *   The iter-3 inner loop contains TWO integer division instructions:
 *     ed_e / 100LL   (idiv -- ~20-40 cycles each)
 *     ed_e % 100LL   (idiv -- ~20-40 cycles each)
 *   Integer division is non-pipelinable and blocks auto-vectorization entirely.
 *   GCC/Clang cannot SIMD-vectorize loops containing 64-bit integer division.
 *
 * Iter-7 tried precomputed int8 group col + register accumulators but regressed
 * because: (a) the extra parallel precompute pass cost more than it saved,
 * (b) the 57MB group array exceeded L3 cache (44MB), and (c) the division
 *     was still present.
 *
 * FIX: Eliminate integer division from the hot loop entirely.
 *   Instead of splitting sum_charge into main+rem (requiring div+mod), use
 *   __int128 accumulators per-group per-thread in register:
 *
 *     lcharge128[g] += (__int128)(e * ed) * tplus;   // single imulq, no idiv
 *
 *   where e, ed, tplus are int64. The 64x64=64 multiply (imulq) is ONE cycle
 *   latency / pipelined. No division needed in the inner loop.
 *
 *   At merge time (outside the hot loop), divide once per group:
 *     sum_charge = (double)(lcharge128[g] / 10000LL) + remainder...
 *
 *   This approach:
 *     - Eliminates 2 idiv instructions per row (saves ~40-80 cycles/row)
 *     - Does NOT add a separate precompute pass (unlike iter 7)
 *     - Keeps register-local accumulators to avoid TLS indirect writes in loop
 *     - Works correctly: __int128 range >> 6.6e18 max per group
 *
 * Overflow check for __int128 accumulators:
 *   e_max = 1e9 (ep_stored), ed_max = 100, tplus_max = 110
 *   Per-row max: e * ed * tplus = 1e9 * 100 * 110 = 1.1e13
 *   60M rows: 60M * 1.1e13 = 6.6e20
 *   __int128 max = 1.7e38 >> 6.6e20 -- safe by 17 orders of magnitude.
 *
 * Additional improvements vs iter 3:
 *   - Register-local flat arrays (6 scalar accumulators each) for all groups:
 *     avoids indirect pointer writes to tls[] in the hot loop body.
 *   - agg.valid removed from inner loop (set once after loop).
 *   - schedule(static) for balanced full-zone workload.
 *   - MADV_SEQUENTIAL (not MADV_WILLNEED which stalled on HDD in iter 5).
 *
 * Accumulators:
 *   lq[6]:        int64, scale=100
 *   le[6]:        int64, scale=100
 *   ldp[6]:       int64, scale=10000 (e*ed)
 *   lcharge128[6]:__int128, scale=10000 (e*ed*tplus, where ed,tplus scaled *100)
 *                 -> actual sum_charge = lcharge128[g] / (100*100) = / 10000
 *   ld[6]:        int64, scale=100
 *   lc[6]:        int64, count
 *
 * Parallelism: OpenMP morsel-driven, 64 threads, static schedule.
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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================================
// Aggregation state -- uses __int128 only for sum_charge (merged from register
// accumulators), int64 for all other fields.
// Padded to 128 bytes (2 cache lines) to prevent false sharing.
// ============================================================================
struct alignas(128) AggState {
    int64_t  sum_qty         = 0;  // scale=100
    int64_t  sum_ep          = 0;  // scale=100
    int64_t  sum_disc_price  = 0;  // scale=10000
    __int128 sum_charge      = 0;  // scale=10000 (e*ed*tplus where ed,tplus are *100)
    int64_t  sum_disc        = 0;  // scale=100
    int64_t  count           = 0;
    bool     valid           = false;
    // sizeof: 8+8+8+16+8+8+1 = 57 bytes; pad to 128
    char _pad[71];

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
    // Phase 3: Memory-map columns, advise sequential access
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> col_returnflag(li_dir + "l_returnflag.bin");
    gendb::MmapColumn<int32_t> col_linestatus(li_dir + "l_linestatus.bin");
    gendb::MmapColumn<int64_t> col_quantity  (li_dir + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_ep        (li_dir + "l_extendedprice.bin");
    gendb::MmapColumn<int64_t> col_discount  (li_dir + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_tax       (li_dir + "l_tax.bin");
    gendb::MmapColumn<int32_t> col_shipdate  (li_dir + "l_shipdate.bin");

    // Advise OS to prefetch sequentially for all columns
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

    // Morsel size: full block (200K) for full zones to maximize prefetch
    // efficiency and minimize OpenMP scheduling overhead.
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
    const int MAX_GROUPS = 6;
    const int num_threads = omp_get_max_threads();
    std::vector<AggState> tls(num_threads * MAX_GROUPS);

    // -------------------------------------------------------------------------
    // Phase 5: Parallel scan
    //
    // KEY CHANGE vs iter 3:
    //   Replace (ed_e / 100) * tplus + (ed_e % 100) * tplus
    //   with a single __int128 multiply: (__int128)(e * ed) * tplus
    //   This eliminates 2 integer divisions per row (~40-80 cycles/row saved).
    //
    //   __int128 multiply uses two imulq instructions (or mulq), which are
    //   pipelined and ~3-5 cycles each — far cheaper than idiv (~20-40 cycles).
    //
    //   Register-local flat arrays avoid indirect pointer writes to tls[] in
    //   the inner loop, keeping data in registers / L1.
    //
    //   agg.valid is set once after the loop (not per-row).
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

            // Register-local accumulators for all 6 groups
            int64_t  lq[6]        = {0,0,0,0,0,0};
            int64_t  le[6]        = {0,0,0,0,0,0};
            int64_t  ldp[6]       = {0,0,0,0,0,0};
            __int128 lcharge[6]   = {0,0,0,0,0,0};
            int64_t  ld[6]        = {0,0,0,0,0,0};
            int64_t  lc[6]        = {0,0,0,0,0,0};

            if (mor.full_zone) {
                // ----------------------------------------------------------------
                // Fast path: entire zone passes date filter -- no per-row check.
                // Inner loop: no integer division, only multiplies.
                // ----------------------------------------------------------------
                for (uint64_t r = row_start; r < row_end; r++) {
                    const int grp = returnflag[r] * 2 + linestatus[r];

                    const int64_t q    = quantity[r];
                    const int64_t e    = ep[r];
                    const int64_t d    = discount[r];
                    const int64_t t    = tax[r];
                    const int64_t ed   = 100LL - d;        // (1-discount)*100
                    const int64_t tplus = 100LL + t;       // (1+tax)*100

                    lq[grp]      += q;
                    le[grp]      += e;
                    ldp[grp]     += e * ed;                // scale=10000
                    lcharge[grp] += (__int128)(e * ed) * tplus;  // scale=1000000
                    ld[grp]      += d;
                    lc[grp]      += 1;
                }
            } else {
                // ----------------------------------------------------------------
                // Boundary zone: per-row date predicate check.
                // ----------------------------------------------------------------
                for (uint64_t r = row_start; r < row_end; r++) {
                    if (shipdate[r] > SHIP_THRESHOLD) continue;

                    const int grp = returnflag[r] * 2 + linestatus[r];

                    const int64_t q     = quantity[r];
                    const int64_t e     = ep[r];
                    const int64_t d     = discount[r];
                    const int64_t t     = tax[r];
                    const int64_t ed    = 100LL - d;
                    const int64_t tplus = 100LL + t;

                    lq[grp]      += q;
                    le[grp]      += e;
                    ldp[grp]     += e * ed;
                    lcharge[grp] += (__int128)(e * ed) * tplus;
                    ld[grp]      += d;
                    lc[grp]      += 1;
                }
            }

            // Flush register accumulators to thread-local storage (outside hot loop)
            for (int g = 0; g < MAX_GROUPS; g++) {
                if (lc[g] > 0) {
                    local[g].sum_qty        += lq[g];
                    local[g].sum_ep         += le[g];
                    local[g].sum_disc_price += ldp[g];
                    local[g].sum_charge     += lcharge[g];
                    local[g].sum_disc       += ld[g];
                    local[g].count          += lc[g];
                    local[g].valid           = true;
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

            double sum_qty   = (double)agg.sum_qty / 100.0;
            double sum_ep    = (double)agg.sum_ep  / 100.0;

            // sum_disc_price: scale=10000
            double sum_disc_price = (double)(agg.sum_disc_price / 10000LL)
                                  + (double)(agg.sum_disc_price % 10000LL) / 10000.0;

            // sum_charge: lcharge = SUM(e * ed * tplus) at scale = 100 * 100 * 100 = 1,000,000
            // actual value = lcharge / 1,000,000
            __int128 sc = agg.sum_charge;
            int64_t  sc_hi  = (int64_t)(sc / 10000LL);
            int64_t  sc_rem = (int64_t)(sc % 10000LL);
            double sum_charge = (double)sc_hi / 100.0 + (double)sc_rem / 1000000.0;

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
