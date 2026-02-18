/*
 * Q3: Shipping Priority — TPC-H (Iteration 2)
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
 *               + semi-join on custkey (BUILDING) → further reduce
 *   lineitem  : l_shipdate > 1995-03-15   → ~38% of 60M → ~22M rows (zone-map)
 *
 * Step 2 — Join ordering:
 *   1. Scan customer with mktsegment='BUILDING' → boolean bitset[0..1500000]
 *   2. Scan orders in parallel, filter o_orderdate<threshold AND custkey in bitset
 *      → write to direct flat array[orderkey] (unique keys, parallel-safe)
 *   3. Scan lineitem with zone-map skip + l_shipdate filter, probe flat array
 *      → parallel aggregation with thread-local maps, partitioned merge
 *
 * Step 3 — No subqueries to decorrelate.
 *
 * ============================================================
 * PHYSICAL PLAN
 * ============================================================
 * customer  : sequential scan → bool bitset[1..1500000] (37.5 KB, L1-resident)
 * orders    : PARALLEL scan (64 threads), filter → direct flat array[orderkey]
 *             (orderkey unique, 1..15M → array write is race-free since unique keys)
 *             Direct array: 15M × 9 bytes = ~135 MB; O(1) lookup vs hash probe
 * lineitem  : zone-map skip (l_shipdate > 9205), parallel morsel scan
 *             Thread-local CompactHashMap for aggregation
 *             Partitioned parallel merge: each thread merges partition (key%T==t)
 * sort/topk : TopKHeap<ResultRow> with k=10 — O(n log 10) vs O(n log n)
 * output    : write Q3.csv
 *
 * Key constants:
 *   BUILDING dict code = loaded at runtime from customer/c_mktsegment_dict.txt
 *   DATE '1995-03-15' = epoch day 9205
 *   l_extendedprice scale = 100, l_discount scale = 100
 *   revenue = extendedprice * (100 - discount)  (unscaled int64, /10000 at output)
 *
 * Dominant bottleneck (iter_0): build_joins=272ms, aggregation_merge=113ms
 * Fixes:
 *   1. Parallelize orders scan (build_joins: 272ms → target <40ms)
 *   2. Direct flat array for orderkey lookup (no hash probe cost)
 *   3. Partitioned parallel aggregation merge (113ms → target <15ms)
 *   4. Prefetch all columns before scan to overlap I/O
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <atomic>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"
#include <iostream>

// ============================================================
// Direct flat array entry for orders (indexed by o_orderkey)
// 9 bytes packed, will be 12 with natural alignment
// ============================================================
struct OrderEntry {
    int32_t o_orderdate;    // 4 bytes
    int32_t o_shippriority; // 4 bytes
    int8_t  valid;          // 1 byte — 0 = not qualifying, 1 = qualifying
    // 3 bytes padding (compiler)
};

struct AggEntry {
    int64_t revenue_scaled; // sum of extendedprice*(100-discount), scale=10000
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled; // divide by 10000 for output
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Zone map entry — 24 bytes (natural alignment, verified by iter_0)
struct ZoneMapEntry {
    int32_t  min_val;    // 4 bytes
    int32_t  max_val;    // 4 bytes
    uint64_t row_start;  // 8 bytes
    uint32_t row_count;  // 4 bytes
    uint32_t _pad;       // 4 bytes padding
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15"); // 9205

    // Load c_mktsegment dictionary at runtime to find 'BUILDING' code.
    // Dictionary format: one value per line, line index (0-based) is the dict code.
    int32_t BUILDING_CODE = -1;
    {
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
        std::ifstream dict_file(dict_path);
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open dictionary: " + dict_path);
        std::string line;
        int32_t code = 0;
        while (std::getline(dict_file, line)) {
            // Strip trailing carriage return if present
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line == "BUILDING") { BUILDING_CODE = code; break; }
            ++code;
        }
        if (BUILDING_CODE < 0)
            throw std::runtime_error("'BUILDING' not found in dictionary: " + dict_path);
    }

    const int32_t MAX_CUSTKEY    = 1500000;
    const int32_t MAX_ORDERKEY   = 15000000;

    // ============================================================
    // Phase 1: Scan customer → bool bitset[0..MAX_CUSTKEY]
    // 1.5M rows, sequential, ~13ms (already fast)
    // ============================================================
    // Use a flat byte array instead of std::vector<bool> for faster access
    std::vector<uint8_t> cust_building(MAX_CUSTKEY + 1, 0);
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = 1;
            }
        }
    }

    // ============================================================
    // Phase 2: Scan orders IN PARALLEL → direct flat array[orderkey]
    // Orders are scanned in parallel; since o_orderkey is unique (PK),
    // each thread writes to a different array slot — no race condition.
    // Direct array: 15M × 12 bytes = 180 MB, O(1) probe (no hash cost)
    // ============================================================
    // Allocate the flat array
    std::vector<OrderEntry> order_flat(MAX_ORDERKEY + 1, OrderEntry{0, 0, 0});

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        // Prefetch all columns to overlap I/O with setup
        o_orderkey.prefetch();
        o_custkey.prefetch();
        o_orderdate.prefetch();
        o_shippriority.prefetch();

        size_t n = o_orderkey.size(); // 15M

        // Parallel scan: each thread processes a contiguous chunk
        // No locking needed — unique PK means each orderkey written by exactly one thread
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < n; i++) {
            int32_t od = o_orderdate[i];
            if (od >= DATE_THRESHOLD) continue;

            int32_t ck = o_custkey[i];
            if ((uint32_t)ck > (uint32_t)MAX_CUSTKEY) continue;
            if (!cust_building[ck]) continue;

            int32_t ok = o_orderkey[i];
            // ok is in [1..15M] by TPC-H spec; safe to index directly
            order_flat[ok].o_orderdate    = od;
            order_flat[ok].o_shippriority = o_shippriority[i];
            order_flat[ok].valid          = 1;
        }
    }

    // ============================================================
    // Phase 3: Scan lineitem with zone-map pruning + parallel aggregation
    // Thread-local CompactHashMaps, then partitioned parallel merge
    // ============================================================
    const int num_threads = omp_get_max_threads();
    // Each thread-local map: target ~500K entries (qualifying lineitem rows / threads)
    std::vector<gendb::CompactHashMap<int32_t, AggEntry>> local_maps(num_threads,
        gendb::CompactHashMap<int32_t, AggEntry>(200000));

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

        // Prefetch all lineitem columns before zone-map setup
        l_orderkey.prefetch();
        l_shipdate.prefetch();
        l_extendedprice.prefetch();
        l_discount.prefetch();

        // Load zone map for l_shipdate
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

        // Build qualifying block ranges: skip blocks where max_val <= DATE_THRESHOLD
        struct BlockRange { uint64_t start; uint64_t end; };
        std::vector<BlockRange> scan_ranges;
        scan_ranges.reserve(num_blocks);

        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_val <= DATE_THRESHOLD) continue;
            scan_ranges.push_back({zones[b].row_start,
                                   zones[b].row_start + zones[b].row_count});
        }

        munmap((void*)zm_raw, zm_st.st_size);

        size_t num_ranges = scan_ranges.size();

        // Parallel scan with dynamic scheduling for load balance across ranges
        #pragma omp parallel for schedule(dynamic, 4) num_threads(num_threads)
        for (size_t ri = 0; ri < num_ranges; ri++) {
            int tid = omp_get_thread_num();
            auto& lmap = local_maps[tid];
            uint64_t rstart = scan_ranges[ri].start;
            uint64_t rend   = scan_ranges[ri].end;

            for (uint64_t i = rstart; i < rend; i++) {
                if (l_shipdate[i] <= DATE_THRESHOLD) continue;

                int32_t ok = l_orderkey[i];
                // Direct flat array lookup — no hash, no comparison chain
                if ((uint32_t)ok > (uint32_t)MAX_ORDERKEY) continue;
                const OrderEntry& oe = order_flat[ok];
                if (!oe.valid) continue;

                // revenue_scaled = extendedprice * (100 - discount)
                int64_t rev = l_extendedprice[i] * (100LL - l_discount[i]);

                AggEntry* ae = lmap.find(ok);
                if (ae) {
                    ae->revenue_scaled += rev;
                } else {
                    lmap.insert(ok, {rev, oe.o_orderdate, oe.o_shippriority});
                }
            }
        }
    }

    // ============================================================
    // Phase 4: Partitioned parallel aggregation merge
    // Instead of sequential merge of 64 maps, partition by key hash:
    // Each thread t merges all entries where (key % num_threads == t)
    // → disjoint key sets per thread → no synchronization needed
    // ============================================================
    // Final global map — sized for ~3M qualifying order groups
    gendb::CompactHashMap<int32_t, AggEntry> global_agg(3000000);

    {
        GENDB_PHASE("aggregation_merge");

        // Estimate total entries for pre-sizing
        // Collect all distinct keys across thread-local maps into global_agg in parallel
        // Strategy: partition merge — each thread handles keys where key%T == tid
        // But CompactHashMap doesn't support concurrent inserts.
        // Instead: use a partitioned approach where we build the global map
        // from all local maps, but partition the work.

        // Approach: Build a vector of all (key, agg) pairs from all local maps,
        // partitioned by key mod num_threads. Then each thread merges its partition.
        // Final merge is single-pass since partitions are disjoint.

        // Since local maps may have varying sizes, collect all entries first in parallel
        // into per-partition buckets, then build global map partition by partition.

        // Step 1: Collect all entries from local maps into per-partition staging arrays
        // Partition count = num_threads
        int T = num_threads;
        struct KV { int32_t key; AggEntry val; };
        std::vector<std::vector<KV>> partitions(T);

        // Pre-size each partition
        size_t total_estimate = 0;
        for (int t = 0; t < T; t++) total_estimate += local_maps[t].size();
        size_t per_part = (total_estimate / T) * 2 + 64;
        for (int p = 0; p < T; p++) partitions[p].reserve(per_part);

        // Sequential scatter into partitions (fast — just memory writes)
        for (int t = 0; t < T; t++) {
            for (const auto [key, val] : local_maps[t]) {
                int p = (int)((uint32_t)key % (uint32_t)T);
                partitions[p].push_back({key, val});
            }
        }

        // Step 2: Each partition has disjoint keys → merge in parallel
        // Use per-partition CompactHashMaps, then combine into global_agg
        std::vector<gendb::CompactHashMap<int32_t, AggEntry>> part_maps(T);
        for (int p = 0; p < T; p++) {
            part_maps[p] = gendb::CompactHashMap<int32_t, AggEntry>(
                std::max((size_t)16, partitions[p].size() * 2));
        }

        #pragma omp parallel for schedule(static) num_threads(T)
        for (int p = 0; p < T; p++) {
            auto& pm = part_maps[p];
            for (const auto& kv : partitions[p]) {
                AggEntry* ae = pm.find(kv.key);
                if (ae) {
                    ae->revenue_scaled += kv.val.revenue_scaled;
                } else {
                    pm.insert(kv.key, kv.val);
                }
            }
        }

        // Step 3: Sequential merge of T small partition maps into global_agg
        // (T=64 maps, each ~1/64 of total → fast)
        global_agg = gendb::CompactHashMap<int32_t, AggEntry>(total_estimate + 64);
        for (int p = 0; p < T; p++) {
            for (const auto [key, val] : part_maps[p]) {
                global_agg.insert(key, val); // unique keys per partition → no conflict check
            }
        }
    }

    // ============================================================
    // Phase 5: Top-10 by revenue DESC, o_orderdate ASC
    // Use TopKHeap from hash_utils.h for O(n log 10) vs O(n log n)
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("sort_topk");

        // Comparator: "worse" element is one with lower revenue, or higher orderdate on tie
        // TopKHeap keeps elements where cmp(a, b) = a is "worse than" b
        // We want TOP by revenue DESC, orderdate ASC
        // So worst = lowest revenue (or highest date on tie)
        auto worse = [](const ResultRow& a, const ResultRow& b) -> bool {
            // returns true if a is worse than b (should be evicted)
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled < b.revenue_scaled; // lower revenue = worse
            return a.o_orderdate > b.o_orderdate;           // higher date = worse
        };

        gendb::TopKHeap<ResultRow, decltype(worse)> heap(10, worse);

        for (const auto [key, val] : global_agg) {
            heap.push({key, val.revenue_scaled, val.o_orderdate, val.o_shippriority});
        }

        top10 = heap.sorted();
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
            int64_t whole = row.revenue_scaled / 10000;
            int64_t frac  = row.revenue_scaled % 10000;
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            fprintf(f, "%d,%lld.%04lld,%s,%d\n",
                row.l_orderkey,
                (long long)whole,
                (long long)frac,
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
