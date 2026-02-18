/*
 * Q1: Pricing Summary Report -- GenDB iteration 9
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
 * PHYSICAL PLAN (iteration 9)
 * =============================================================================
 *
 * DOMINANT BOTTLENECK (iter 3..8): main_scan ~53ms.
 * Root causes:
 *   1. Scatter write pattern: grp = returnflag[r]*2 + linestatus[r] causes
 *      indirect writes to local[grp], preventing auto-vectorization of the
 *      aggregation loop entirely (compiler cannot vectorize scatter stores
 *      into non-trivially-indexed struct fields).
 *   2. int64 division/modulo by 100 in hot path (for sum_charge split):
 *      ~4-5 cycles per row even with multiply-shift approximation.
 *
 * FIX (iter 9): Two-pass morsel processing using selection vectors.
 *   Pass 1 (filter + group assignment): Branchless grouping pass over
 *     the zone, producing 6 compact uint32_t selection vectors
 *     (one per group), each containing the row indices that belong to
 *     that group. This pass only reads shipdate+returnflag+linestatus
 *     (3 narrow columns), is highly vectorizable, and generates the
 *     group membership lists.
 *   Pass 2 (aggregation by group): For each group g, iterate its
 *     selection vector and accumulate from quantity/ep/discount/tax.
 *     Since all rows in a group's selection vector map to the SAME
 *     output accumulator, there is NO scatter — the inner loop is a
 *     pure sequential accumulation, which the compiler CAN vectorize
 *     with AVX2/SSE across multiple rows simultaneously.
 *
 * sum_charge split (unchanged from iter 3): Two int64 accumulators
 *   to avoid __int128 and enable vectorization:
 *   sum_charge_main = sum( (e*ed/100) * (100+t) )   [scale=10000]
 *   sum_charge_rem  = sum( (e*ed%100) * (100+t) )   [scale=1000000]
 *   final = (sum_charge_main*100 + sum_charge_rem) / 1000000.0
 *
 * Fast-path optimization for full zones: skip the shipdate check
 *   entirely in Pass 1, reducing it to returnflag+linestatus reads only.
 *
 * Parallelism: OpenMP morsel-driven, 64 threads, dynamic schedule(1).
 *   Each thread processes one morsel at a time; selection vectors are
 *   stack/thread-local; no cross-thread synchronization during scan.
 *
 * Memory layout: Selection vectors use thread-local uint32_t arrays
 *   of size MORSEL_SIZE. With MORSEL_FULL=200K and 97% pass rate,
 *   each vector holds ~194K entries * 4 bytes = ~776KB per group (max).
 *   Total per thread: up to 6*776KB = ~4.5MB. Stack is too small;
 *   allocate per-thread on the heap once, reuse across morsels.
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

// ============================================================================
// Aggregate a batch of rows (given by sel[] of length n) into a single AggState.
// All rows in sel[] belong to the same group => no scatter, pure accumulation.
// The compiler CAN auto-vectorize this loop with AVX2 (all int64, sequential).
// ============================================================================
static void __attribute__((noinline))
agg_group_batch(AggState& agg,
                const uint32_t* __restrict__ sel,
                uint32_t n,
                const int64_t* __restrict__ quantity,
                const int64_t* __restrict__ ep,
                const int64_t* __restrict__ discount,
                const int64_t* __restrict__ tax)
{
    int64_t s_qty  = 0;
    int64_t s_ep   = 0;
    int64_t s_dp   = 0;
    int64_t s_cm   = 0;
    int64_t s_cr   = 0;
    int64_t s_disc = 0;
    int64_t s_cnt  = (int64_t)n;

#pragma GCC ivdep
    for (uint32_t i = 0; i < n; i++) {
        const uint32_t r = sel[i];
        const int64_t q  = quantity[r];
        const int64_t e  = ep[r];
        const int64_t d  = discount[r];
        const int64_t t  = tax[r];
        const int64_t ed = 100LL - d;
        const int64_t ed_e  = e * ed;
        const int64_t tplus = 100LL + t;

        s_qty  += q;
        s_ep   += e;
        s_dp   += ed_e;
        s_cm   += (ed_e / 100LL) * tplus;
        s_cr   += (ed_e % 100LL) * tplus;
        s_disc += d;
    }

    agg.sum_qty         += s_qty;
    agg.sum_ep          += s_ep;
    agg.sum_disc_price  += s_dp;
    agg.sum_charge_main += s_cm;
    agg.sum_charge_rem  += s_cr;
    agg.sum_disc        += s_disc;
    agg.count           += s_cnt;
    if (n > 0) agg.valid = true;
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

    // Morsel size: 200K for full zones (= 1 full block, best cache streaming),
    // 100K for partial zones (slightly finer for load balance at boundary zones).
    static constexpr uint64_t MORSEL_FULL    = 200000ULL;
    static constexpr uint64_t MORSEL_PARTIAL = 100000ULL;
    static constexpr uint32_t MAX_MORSEL     = 200000U;   // max rows in any morsel

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
    int num_threads = omp_get_max_threads();
    std::vector<AggState> tls(num_threads * MAX_GROUPS);

    // Per-thread selection vector buffers: 6 groups × MAX_MORSEL uint32_t each.
    // Allocated once per thread, reused across morsels.
    // Size: 6 * 200000 * 4 = 4.8MB per thread. With 64 threads = 307MB total.
    // That's within the 376GB RAM budget.
    // We allocate as a flat array: sel[tid][grp * MAX_MORSEL + i]
    // Use a single allocation per thread via a vector of pointers.
    std::vector<uint32_t*> thread_sel(num_threads, nullptr);
    std::vector<uint32_t>  thread_sel_storage; // backing store, allocated once

    // Compute total storage: num_threads * MAX_GROUPS * MAX_MORSEL
    // This may be too large (64 * 6 * 200000 * 4 = 307MB). Use lazy alloc per thread.
    // Instead, allocate per thread inside the parallel region (once).
    // We'll use a flag array to detect first use.
    std::vector<bool> sel_allocated(num_threads, false);

    // -------------------------------------------------------------------------
    // Phase 5: Parallel scan -- Two-pass selection vector approach
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(num_threads)
        {
            const int tid = omp_get_thread_num();
            // Allocate per-thread selection vector buffer (once)
            // 6 groups * MAX_MORSEL entries each
            uint32_t* sel_buf = new uint32_t[MAX_GROUPS * MAX_MORSEL];
            // sel_buf[g * MAX_MORSEL .. g * MAX_MORSEL + sel_cnt[g]) = row indices for group g
            uint32_t sel_cnt[MAX_GROUPS];

            AggState* local = &tls[tid * MAX_GROUPS];

            #pragma omp for schedule(dynamic, 1) nowait
            for (int m = 0; m < num_morsels; m++) {
                const Morsel& mor = morsels[m];
                const uint64_t row_start = mor.row_start;
                const uint64_t row_end   = mor.row_end;
                const uint32_t nrows     = (uint32_t)(row_end - row_start);

                // Reset selection counts
                for (int g = 0; g < MAX_GROUPS; g++) sel_cnt[g] = 0;

                if (mor.full_zone) {
                    // ----------------------------------------------------------
                    // Pass 1 (full zone): No date check needed.
                    // Read only returnflag + linestatus (2 narrow int32 columns).
                    // Compute group and scatter row index into per-group sel vector.
                    // This loop is: read 2 cols, compute grp, write to sel array.
                    // Each sel array is independent (no scatter collision within
                    // a single group's contiguous region), so this IS vectorizable.
                    // ----------------------------------------------------------
                    const int32_t* __restrict__ rf_ptr = returnflag + row_start;
                    const int32_t* __restrict__ ls_ptr = linestatus + row_start;

#pragma GCC ivdep
                    for (uint32_t i = 0; i < nrows; i++) {
                        const int grp = rf_ptr[i] * 2 + ls_ptr[i];
                        sel_buf[grp * MAX_MORSEL + sel_cnt[grp]++] = (uint32_t)(row_start + i);
                    }
                } else {
                    // ----------------------------------------------------------
                    // Pass 1 (boundary zone): Date check + group assignment.
                    // Read shipdate + returnflag + linestatus.
                    // ----------------------------------------------------------
                    const int32_t* __restrict__ sd_ptr = shipdate   + row_start;
                    const int32_t* __restrict__ rf_ptr = returnflag + row_start;
                    const int32_t* __restrict__ ls_ptr = linestatus + row_start;

                    for (uint32_t i = 0; i < nrows; i++) {
                        if (sd_ptr[i] > SHIP_THRESHOLD) continue;
                        const int grp = rf_ptr[i] * 2 + ls_ptr[i];
                        sel_buf[grp * MAX_MORSEL + sel_cnt[grp]++] = (uint32_t)(row_start + i);
                    }
                }

                // ----------------------------------------------------------
                // Pass 2: For each group, aggregate its selection vector.
                // agg_group_batch reads quantity/ep/discount/tax for each
                // selected row. No scatter: all writes go to one AggState.
                // The inner loop in agg_group_batch is fully vectorizable.
                // ----------------------------------------------------------
                for (int g = 0; g < MAX_GROUPS; g++) {
                    if (sel_cnt[g] == 0) continue;
                    agg_group_batch(local[g],
                                    sel_buf + g * MAX_MORSEL,
                                    sel_cnt[g],
                                    quantity, ep, discount, tax);
                }
            }

            delete[] sel_buf;
        } // end parallel
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
            //   main is at scale 10000: main * 100 / 1000000
            //   rem  is at scale 1000000: rem / 1000000
            //   combined = (main * 100 + rem) / 1000000
            int64_t charge_combined_hi = agg.sum_charge_main * 100LL + agg.sum_charge_rem;
            // charge_combined_hi is at scale=1000000
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
