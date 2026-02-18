/*
 * Q3: Shipping Priority — TPC-H
 *
 * SQL:
 *   SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *          o_orderdate, o_shippriority
 *   FROM customer, orders, lineitem
 *   WHERE c_mktsegment = 'BUILDING'
 *     AND c_custkey = o_custkey
 *     AND l_orderkey = o_orderkey
 *     AND o_orderdate < DATE '1995-03-15'
 *     AND l_shipdate  > DATE '1995-03-15'
 *   GROUP BY l_orderkey, o_orderdate, o_shippriority
 *   ORDER BY revenue DESC, o_orderdate
 *   LIMIT 10;
 *
 * ============================================================
 * LOGICAL PLAN
 * ============================================================
 * Step 1 — Single-table predicates:
 *   customer  : c_mktsegment = 'BUILDING' → ~300K rows (1/5 of 1.5M)
 *   orders    : o_orderdate < 1995-03-15  → ~54% of 15M → ~8.1M rows
 *   lineitem  : l_shipdate > 1995-03-15   → ~38% of 60M → ~22M rows (zone-map pruning)
 *
 * Step 2 — Join ordering (smallest filtered result first):
 *   1. Scan customer with mktsegment='BUILDING' → build custkey bitset (~300K bits = 37.5 KB)
 *   2. Scan orders with o_orderdate < threshold, semi-join probe custkey bitset
 *      → qualifying orders ~few million → build CompactHashMap<orderkey, {orderdate, shippriority}>
 *   3. Scan lineitem with l_shipdate > threshold (zone-map skip), probe orders hash map
 *      → accumulate SUM(revenue) per orderkey in aggregation hash map
 *
 * Step 3 — No subqueries to decorrelate.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * customer  : sequential scan, filter mktsegment==BUILDING_CODE → flat bitset[1..1500000]
 * orders    : sequential scan, filter o_orderdate < threshold, test bitset → CompactHashMap<int32_t, OrderInfo>
 * lineitem  : zone-map block pruning on l_shipdate, parallel morsel scan with thread-local
 *             CompactHashMap<int32_t, int64_t> aggregating revenue, merge to global map
 * sort/topk : partial_sort with custom comparator (revenue DESC, o_orderdate ASC), k=10
 * output    : write Q3.csv
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   l_extendedprice scale = 100, l_discount scale = 100
 *   revenue = l_extendedprice * (100 - l_discount) / 10000  (scale down once at output)
 *
 * Parallelism:
 *   64 cores available; lineitem scan parallelized with OpenMP morsel-driven
 *   Thread-local agg maps merged sequentially (groups are small relative to cache)
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <omp.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"
#include <iostream>

namespace {

// ============================================================
// Aggregation value for each (l_orderkey) group
// We store: revenue accumulated as int64_t (sum of price*(100-discount))
// and the order metadata from the orders table.
// ============================================================
struct OrderInfo {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct AggEntry {
    int64_t revenue_scaled; // sum of extendedprice*(100-discount), scale=100*100=10000
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled; // divide by 10000 for output
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ============================================================
// Zone map entry actual layout (verified by binary inspection):
// [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count, uint32_t pad] = 24 bytes
// The storage guide says 20 bytes but actual file has 24-byte entries (natural alignment).
// ============================================================
struct ZoneMapEntry {
    int32_t  min_val;    // 4 bytes
    int32_t  max_val;    // 4 bytes
    uint64_t row_start;  // 8 bytes (naturally aligned at offset 8)
    uint32_t row_count;  // 4 bytes
    uint32_t _pad;       // 4 bytes padding
};                       // total: 24 bytes
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");
    // BUILDING is code 0 in c_mktsegment_dict.txt
    const int32_t BUILDING_CODE = 0;

    // ============================================================
    // Phase 1: Load customer, filter mktsegment='BUILDING' → bitset
    // ============================================================
    std::vector<bool> cust_building; // indexed by c_custkey (1-based, so size = 1500001)
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        // Find max custkey for bitset sizing
        int32_t max_ckey = 0;
        for (size_t i = 0; i < n; i++) {
            if (c_custkey[i] > max_ckey) max_ckey = c_custkey[i];
        }
        cust_building.assign(max_ckey + 1, false);

        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = true;
            }
        }
    }

    // ============================================================
    // Phase 2: Load orders, filter o_orderdate < threshold + custkey in BUILDING
    //          Build hash map: orderkey → {orderdate, shippriority}
    // ============================================================
    gendb::CompactHashMap<int32_t, OrderInfo> orders_map(3000000); // ~2-3M qualifying orders
    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size();
        int32_t max_ckey = (int32_t)cust_building.size() - 1;

        for (size_t i = 0; i < n; i++) {
            if (o_orderdate[i] < DATE_THRESHOLD) {
                int32_t ck = o_custkey[i];
                if (ck >= 0 && ck <= max_ckey && cust_building[ck]) {
                    orders_map.insert(o_orderkey[i], {o_orderdate[i], o_shippriority[i]});
                }
            }
        }
    }

    // ============================================================
    // Phase 3: Scan lineitem with zone-map + l_shipdate filter, probe orders_map
    //          Parallel aggregation with thread-local maps, then merge
    // ============================================================
    // We'll use thread-local maps, then merge
    int num_threads = omp_get_max_threads();
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_maps(num_threads,
        gendb::CompactHashMap<int32_t, AggEntry>(200000));

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

        // Load zone map
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone map");
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        const uint8_t* zm_raw = (const uint8_t*)mmap(nullptr, zm_st.st_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_raw == MAP_FAILED) throw std::runtime_error("Cannot mmap zone map");
        ::close(zm_fd);

        uint32_t num_blocks = *(const uint32_t*)zm_raw;
        const ZoneMapEntry* zones = (const ZoneMapEntry*)(zm_raw + sizeof(uint32_t));

        // Build list of qualifying block ranges (l_shipdate > DATE_THRESHOLD → skip blocks where max <= threshold)
        // We collect (row_start, row_end) pairs of blocks to scan
        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);

        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue; // entire block fails predicate
            uint64_t rstart = zones[b].row_start;
            uint64_t rend = rstart + zones[b].row_count;
            scan_ranges.push_back({rstart, rend});
        }

        munmap((void*)zm_raw, zm_st.st_size);

        // Advise random for hash join probing
        // l_shipdate is sequential, others are random-ish
        l_orderkey.advise_sequential();
        l_shipdate.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();

        size_t num_ranges = scan_ranges.size();

        #pragma omp parallel for schedule(dynamic, 4) num_threads(num_threads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            int tid = omp_get_thread_num();
            auto& lmap = local_maps[tid];
            uint64_t rstart = scan_ranges[ri].start;
            uint64_t rend = scan_ranges[ri].end;

            for (uint64_t i = rstart; i < rend; i++) {
                if (l_shipdate[i] <= DATE_THRESHOLD) continue; // per-row check for partial blocks

                int32_t ok = l_orderkey[i];
                OrderInfo* oi = orders_map.find(ok);
                if (!oi) continue;

                // revenue_scaled = extendedprice * (100 - discount)  [scale 100*100=10000]
                int64_t rev = l_extendedprice[i] * (100LL - l_discount[i]);

                AggEntry* ae = lmap.find(ok);
                if (ae) {
                    ae->revenue_scaled += rev;
                } else {
                    lmap.insert(ok, {rev, oi->o_orderdate, oi->o_shippriority});
                }
            }
        }
    }

    // ============================================================
    // Phase 4: Merge thread-local maps into global aggregation
    // ============================================================
    gendb::CompactHashMap<int32_t, AggEntry> global_agg(500000);
    {
        GENDB_PHASE("aggregation_merge");

        for (int t = 0; t < num_threads; t++) {
            for (const auto [key, val] : local_maps[t]) {
                AggEntry* ge = global_agg.find(key);
                if (ge) {
                    ge->revenue_scaled += val.revenue_scaled;
                } else {
                    global_agg.insert(key, val);
                }
            }
        }
    }

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // Use partial_sort for correct top-K extraction
    // ============================================================
    std::vector<ResultRow> all_rows;
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        all_rows.reserve(global_agg.size());
        for (const auto [key, val] : global_agg) {
            all_rows.push_back({key, val.revenue_scaled, val.o_orderdate, val.o_shippriority});
        }

        // Sort: revenue DESC, then o_orderdate ASC
        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled; // DESC
            return a.o_orderdate < b.o_orderdate;           // ASC
        };

        int k = std::min((size_t)10, all_rows.size());
        std::partial_sort(all_rows.begin(), all_rows.begin() + k, all_rows.end(), cmp);
        top10 = std::vector<ResultRow>(all_rows.begin(), all_rows.begin() + k);
    }

    // ============================================================
    // Phase 6: Output CSV
    // ============================================================
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        char date_buf[11];
        for (auto& row : top10) {
            // revenue_scaled = extendedprice(x100) * (100 - discount(x100))
            // actual revenue = revenue_scaled / 10000 (4 decimal places in scale)
            // Output with 4 decimal places to match TPC-H spec
            int64_t whole = row.revenue_scaled / 10000;
            int64_t frac  = row.revenue_scaled % 10000;
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            fprintf(f, "%d,%lld.%04lld,%s,%d\n",
                row.l_orderkey,
                (long long)whole,
                (long long)frac,   // 4 decimal places
                date_buf,
                row.o_shippriority);
        }

        fclose(f);
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
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
