/*
 * Q6: Forecasting Revenue Change
 *
 * SQL:
 *   SELECT SUM(l_extendedprice * l_discount) AS revenue
 *   FROM lineitem
 *   WHERE l_shipdate >= DATE '1994-01-01'
 *     AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR   -- i.e., < 1995-01-01
 *     AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01       -- [0.05, 0.07]
 *     AND l_quantity < 24;
 *
 * === LOGICAL PLAN ===
 *   Single table: lineitem (59.9M rows)
 *   Predicates (all single-table):
 *     1. l_shipdate >= 8766  (1994-01-01 epoch days)
 *     2. l_shipdate <  9131  (1995-01-01 epoch days)
 *     3. l_discount >= 5     (0.05 * 100, scaled int64)
 *     4. l_discount <= 7     (0.07 * 100, scaled int64)
 *     5. l_quantity < 2400   (24 * 100, scaled int64)
 *   Aggregation: SUM(l_extendedprice * l_discount)
 *   No joins, no GROUP BY.
 *
 * === PHYSICAL PLAN ===
 *   1. Zone map pruning on l_shipdate to skip blocks outside [8766, 9130]
 *      - Index: indexes/lineitem_l_shipdate_zonemap.bin
 *      - Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
 *      - 300 zones, 200,000 rows each
 *   2. Parallel scan of qualifying blocks with OpenMP
 *      - Thread-local int128 accumulator for SUM (avoid overflow: max per row ~10^6 * 100 * 7 = 7*10^8, times 60M rows = 4.2*10^16 fits int64 but barely; use __int128 to be safe)
 *      - Fused filter: shipdate, discount, quantity checked together
 *   3. Global merge of thread-local accumulators
 *   4. Output: revenue = SUM / (100 * 100) with 2 decimal places
 *
 * === DECIMAL ARITHMETIC ===
 *   l_extendedprice: int64, scale 100 (value = actual * 100)
 *   l_discount:      int64, scale 100 (value = actual * 100)
 *   l_extendedprice * l_discount = actual^2 * 10000
 *   Final revenue = SUM(ep * disc) / 10000
 *   Use __int128 accumulator to avoid overflow.
 *
 * === PARALLELISM ===
 *   OpenMP parallel for over qualifying zones (morsel = 200,000 rows = one block)
 *   64 hardware threads available; ~10-20 qualifying zones expected for 1-year window
 *   Fall back to row-level parallel for within qualifying zone ranges.
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/lineitem/";
    const std::string idx_base = gendb_dir + "/../indexes/";

    // === Date thresholds (epoch days) ===
    // 1994-01-01 and 1995-01-01
    const int32_t date_lo = gendb::date_str_to_epoch_days("1994-01-01");
    const int32_t date_hi = gendb::date_str_to_epoch_days("1995-01-01"); // exclusive

    // === Scaled integer thresholds ===
    // discount BETWEEN 0.05 AND 0.07 → [5, 7] (scale 100)
    const int64_t disc_lo = 5;
    const int64_t disc_hi = 7;
    // quantity < 24 → < 2400 (scale 100)
    const int64_t qty_max = 2400; // exclusive

    // === Load columns via mmap ===
    gendb::MmapColumn<int32_t> col_shipdate(base + "l_shipdate.bin");
    gendb::MmapColumn<int64_t> col_discount(base + "l_discount.bin");
    gendb::MmapColumn<int64_t> col_quantity(base + "l_quantity.bin");
    gendb::MmapColumn<int64_t> col_extprice(base + "l_extendedprice.bin");

    const size_t total_rows = col_shipdate.count;

    // === Zone map pruning ===
    // Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
    struct ZoneEntry { int32_t min_val; int32_t max_val; uint32_t count; };

    // Determine which row ranges to scan
    // Each zone covers block_size rows
    // Try to use zone map
    std::vector<std::pair<size_t, size_t>> scan_ranges; // [start, end) row ranges

    {
        GENDB_PHASE("zone_map_prune");
        std::string zm_path = gendb_dir + "/../indexes/lineitem_l_shipdate_zonemap.bin";
        FILE* f = fopen(zm_path.c_str(), "rb");
        if (f) {
            uint32_t num_zones = 0;
            fread(&num_zones, sizeof(uint32_t), 1, f);
            std::vector<ZoneEntry> zones(num_zones);
            fread(zones.data(), sizeof(ZoneEntry), num_zones, f);
            fclose(f);

            size_t row_offset = 0;
            for (uint32_t z = 0; z < num_zones; z++) {
                size_t zone_count = zones[z].count;
                // Skip zone if its [min, max] doesn't overlap [date_lo, date_hi)
                // Zone can be skipped if: zone.max < date_lo OR zone.min >= date_hi
                if (zones[z].max_val >= date_lo && zones[z].min_val < date_hi) {
                    scan_ranges.push_back({row_offset, row_offset + zone_count});
                }
                row_offset += zone_count;
            }
        } else {
            // No zone map: scan everything
            scan_ranges.push_back({0, total_rows});
        }
    }

    // === Parallel scan with fused filter + accumulate ===
    // Use __int128 per thread to avoid overflow
    // ep * disc: max ~1,000,000 * 100 * 10 = 1e9 per row; 60M rows -> 6e16, fits int64
    // but use __int128 for safety

    int num_threads = omp_get_max_threads();
    std::vector<__int128> thread_sums(num_threads, (__int128)0);

    {
        GENDB_PHASE("main_scan");

        // Flatten scan ranges into a contiguous set of zones for parallel dispatch
        // We'll parallelize over individual zones
        size_t num_ranges = scan_ranges.size();

        #pragma omp parallel for schedule(dynamic, 1) num_threads(num_threads)
        for (size_t r = 0; r < num_ranges; r++) {
            int tid = omp_get_thread_num();
            size_t row_start = scan_ranges[r].first;
            size_t row_end   = scan_ranges[r].second;

            __int128 local_sum = 0;

            const int32_t* sd  = col_shipdate.data + row_start;
            const int64_t* disc = col_discount.data + row_start;
            const int64_t* qty  = col_quantity.data + row_start;
            const int64_t* ep   = col_extprice.data + row_start;
            size_t len = row_end - row_start;

            for (size_t i = 0; i < len; i++) {
                int32_t shipdate = sd[i];
                if (shipdate < date_lo || shipdate >= date_hi) continue;
                int64_t d = disc[i];
                if (d < disc_lo || d > disc_hi) continue;
                int64_t q = qty[i];
                if (q >= qty_max) continue;
                // All predicates pass
                local_sum += ((__int128)ep[i]) * d;
            }

            thread_sums[tid] += local_sum;
        }
    }

    // === Merge thread sums ===
    __int128 total_sum = 0;
    for (auto& s : thread_sums) total_sum += s;

    // Divide by 10000 (scale_factor^2 = 100*100) to get actual revenue
    // Output as decimal with 2 decimal places
    // total_sum / 10000 = integer part with 2 decimal fractional digits
    int64_t int_part = (int64_t)(total_sum / 10000);
    int64_t frac_part = (int64_t)(total_sum % 10000);
    // Round to 2 decimal places: frac_part has 4 digits, divide by 100
    int64_t frac2 = frac_part / 100;
    // Handle rounding
    if ((frac_part % 100) >= 50) frac2++;
    if (frac2 >= 100) { int_part++; frac2 -= 100; }

    // === Output results ===
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) throw std::runtime_error("Cannot open output: " + out_path);
        fprintf(out, "revenue\n");
        fprintf(out, "%lld.%02lld\n", (long long)int_part, (long long)frac2);
        fclose(out);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
