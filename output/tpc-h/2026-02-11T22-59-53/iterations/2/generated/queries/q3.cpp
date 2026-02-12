#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../utils/flat_hash.h"
#include "../index/index.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <unordered_map>
#include <thread>
#include <vector>
#include <algorithm>
#include <queue>
#include <atomic>

// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING'
//   AND c_custkey = o_custkey
//   AND l_orderkey = o_orderkey
//   AND o_orderdate < '1995-03-15'
//   AND l_shipdate > '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10

struct Q3GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const Q3GroupKey& o) const {
        return l_orderkey == o.l_orderkey &&
               o_orderdate == o.o_orderdate &&
               o_shippriority == o.o_shippriority;
    }
};

namespace std {
    template<>
    struct hash<Q3GroupKey> {
        size_t operator()(const Q3GroupKey& k) const {
            return hash<int32_t>()(k.l_orderkey) ^
                   (hash<int32_t>()(k.o_orderdate) << 1) ^
                   (hash<int32_t>()(k.o_shippriority) << 2);
        }
    };
}

struct Q3Result {
    Q3GroupKey key;
    double revenue;

    bool operator<(const Q3Result& o) const {
        // Min-heap: reverse comparison for top-K
        if (revenue != o.revenue)
            return revenue > o.revenue;
        return key.o_orderdate > o.key.o_orderdate;
    }
};

void execute_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load customer columns
    size_t c_size = get_row_count(gendb_dir, "customer");
    size_t sz;
    int32_t* c_custkey = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", sz);
    uint8_t* c_mktsegment = mmap_column<uint8_t>(gendb_dir, "customer", "c_mktsegment", sz);

    // Load customer dictionary to find "BUILDING" code
    auto c_mkt_dict = load_string_column(gendb_dir, "customer", "c_mktsegment.dict");
    uint8_t building_code = 255;
    for (size_t i = 0; i < c_mkt_dict.size(); i++) {
        if (c_mkt_dict[i] == "BUILDING") {
            building_code = static_cast<uint8_t>(i);
            break;
        }
    }

    // JOIN ORDER OPTIMIZATION: Three-way join customer ⋈ orders ⋈ lineitem
    // Optimal order: customer → orders → lineitem (smallest to largest)
    //
    // Rationale:
    // 1. Customer is smallest (150K rows) with highly selective filter (c_mktsegment = 'BUILDING')
    //    - Selectivity: ~20% (1 of 5 market segments) → ~30K rows after filter
    // 2. Orders is medium (1.5M rows) with date filter (o_orderdate < '1995-03-15')
    //    - After joining with filtered customers: ~100K orders
    // 3. Lineitem is largest (60M rows) with date filter (l_shipdate > '1995-03-15')
    //    - Acts as probe side: ~40M rows after filter, probes orders hash table
    //
    // Build/probe decisions:
    // - Build side 1: filtered_customers (~30K rows) → hash set for orders join
    // - Build side 2: filtered_orders (~100K rows) → hash map for lineitem probe
    // - Probe side: lineitem (60M rows) → probes filtered_orders hash map
    //
    // This minimizes intermediate result sizes: 150K → 30K → 100K → final result
    // Alternative orders (e.g., lineitem → orders → customer) would build hash tables
    // on 40M+ rows, causing memory pressure and cache thrashing.

    // STEP 1: Filter customers and build hash set using scan operator
    // Pre-size for ~20% selectivity (1 of 5 market segments)
    gendb::flat_hash::flat_hash_set<> filtered_customers(c_size / 5);
    gendb::operators::sequential_scan(
        c_size,
        // Predicate: filter by market segment
        [&](size_t i) { return c_mktsegment[i] == building_code; },
        // Process: insert customer key (build side for customer-orders join)
        [&](size_t i) { filtered_customers.insert(c_custkey[i]); }
    );

    // Load orders columns
    size_t o_size = get_row_count(gendb_dir, "orders");
    int32_t* o_orderkey = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", sz);
    int32_t* o_custkey = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", sz);
    int32_t* o_orderdate = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", sz);
    int32_t* o_shippriority = mmap_column<int32_t>(gendb_dir, "orders", "o_shippriority", sz);

    int32_t orderdate_threshold = date_utils::date_to_days(1995, 3, 15);

    // ZONE MAP OPTIMIZATION for orders (sorted by o_orderdate)
    // Block size: 32K rows (from storage design)
    const size_t O_BLOCK_SIZE = 32768;
    const size_t o_num_blocks = (o_size + O_BLOCK_SIZE - 1) / O_BLOCK_SIZE;

    struct OrdersZoneEntry {
        int32_t min_date;
        int32_t max_date;
        size_t start_idx;
        size_t end_idx;
        bool can_skip;
    };

    std::vector<OrdersZoneEntry> o_zone_map(o_num_blocks);

    // Build zone map for orders (data sorted by o_orderdate)
    for (size_t b = 0; b < o_num_blocks; b++) {
        size_t start_idx = b * O_BLOCK_SIZE;
        size_t end_idx = std::min(start_idx + O_BLOCK_SIZE, o_size);

        // Sorted data: min is first, max is last
        int32_t min_date = o_orderdate[start_idx];
        int32_t max_date = (end_idx > start_idx + 1) ? o_orderdate[end_idx - 1] : min_date;

        // Skip if block_min >= threshold (all dates >= threshold, we want < threshold)
        bool can_skip = (min_date >= orderdate_threshold);

        o_zone_map[b] = {min_date, max_date, start_idx, end_idx, can_skip};
    }

    // STEP 2: Join customer with orders, filter by date, and build hash map
    // This is the second join in the sequence: customer ⋈ orders
    // Build hash map on filtered orders (smaller side: ~100K rows after filters)
    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };
    // Pre-size for ~100K expected entries (based on workload analysis)
    gendb::flat_hash::flat_hash_map<OrderInfo> filtered_orders(o_size / 15);

    // Zone-map pruned scan of orders
    for (size_t b = 0; b < o_num_blocks; b++) {
        const auto& zone = o_zone_map[b];

        // Skip entire block if zone map indicates no matches
        if (zone.can_skip) continue;

        // Process this block
        for (size_t i = zone.start_idx; i < zone.end_idx; i++) {
            // Filter by orderdate and join with customer
            if (o_orderdate[i] < orderdate_threshold &&
                filtered_customers.count(o_custkey[i])) {
                filtered_orders[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
            }
        }
    }

    // Load lineitem columns
    size_t l_size = get_row_count(gendb_dir, "lineitem");
    int32_t* l_orderkey = mmap_column<int32_t>(gendb_dir, "lineitem", "l_orderkey", sz);
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", sz);
    double* l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", sz);
    double* l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", sz);

    int32_t shipdate_threshold = date_utils::date_to_days(1995, 3, 15);

    // ZONE MAP OPTIMIZATION for lineitem (sorted by l_shipdate)
    // Block size: 64K rows (from storage design)
    const size_t L_BLOCK_SIZE = 65536;
    const size_t l_num_blocks = (l_size + L_BLOCK_SIZE - 1) / L_BLOCK_SIZE;

    struct LineitemZoneEntry {
        int32_t min_date;
        int32_t max_date;
        size_t start_idx;
        size_t end_idx;
        bool can_skip;
    };

    std::vector<LineitemZoneEntry> l_zone_map(l_num_blocks);

    // Build zone map for lineitem (data sorted by l_shipdate)
    for (size_t b = 0; b < l_num_blocks; b++) {
        size_t start_idx = b * L_BLOCK_SIZE;
        size_t end_idx = std::min(start_idx + L_BLOCK_SIZE, l_size);

        // Sorted data: min is first, max is last
        int32_t min_date = l_shipdate[start_idx];
        int32_t max_date = (end_idx > start_idx + 1) ? l_shipdate[end_idx - 1] : min_date;

        // Skip if block_max <= threshold (all dates <= threshold, we want > threshold)
        bool can_skip = (max_date <= shipdate_threshold);

        l_zone_map[b] = {min_date, max_date, start_idx, end_idx, can_skip};
    }

    // STEP 3: Join lineitem with filtered orders (final join: orders ⋈ lineitem)
    // Lineitem is the probe side (largest table: 60M rows, ~40M after date filter)
    // Probe the filtered_orders hash map (build side: ~100K rows)
    //
    // Build/probe decision: CORRECT
    // - filtered_orders (~100K rows) is much smaller than lineitem (60M rows)
    // - Building hash table on smaller side minimizes memory and lookup cost
    // - Lineitem scans and probes: O(60M) probes into O(100K) hash table
    //
    // OPTIMIZATION: Use manual parallel loop with partition-local aggregation
    // to avoid read contention on shared filtered_orders map
    // ZONE MAP PRUNING: Skip blocks that don't match date filter
    const int num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<Q3GroupKey, double>> local_aggs(num_threads);

    std::vector<std::thread> threads;
    std::atomic<size_t> next_block(0);

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local_agg = local_aggs[t];

            // Each thread pulls blocks from the shared counter
            while (true) {
                size_t block_idx = next_block.fetch_add(1, std::memory_order_relaxed);
                if (block_idx >= l_num_blocks) break;

                const auto& zone = l_zone_map[block_idx];

                // Skip entire block if zone map indicates no matches
                if (zone.can_skip) continue;

                // Process this block
                for (size_t i = zone.start_idx; i < zone.end_idx; i++) {
                    // Filter by shipdate first (cheaper than hash lookup)
                    if (l_shipdate[i] <= shipdate_threshold) continue;

                    // Probe filtered_orders (hash join)
                    const auto* order_info_ptr = filtered_orders.find(l_orderkey[i]);
                    if (order_info_ptr == nullptr) continue;

                    // Construct group key and aggregate
                    const auto& order_info = *order_info_ptr;
                    Q3GroupKey key{l_orderkey[i], order_info.orderdate, order_info.shippriority};
                    local_agg[key] += l_extendedprice[i] * (1.0 - l_discount[i]);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge local aggregations into global
    std::unordered_map<Q3GroupKey, double> global_agg;
    for (const auto& local : local_aggs) {
        for (const auto& [key, revenue] : local) {
            global_agg[key] += revenue;
        }
    }

    // Top-K heap
    std::priority_queue<Q3Result> heap;
    for (const auto& [key, revenue] : global_agg) {
        heap.push({key, revenue});
        if (heap.size() > 10) {
            heap.pop();
        }
    }

    // Extract results
    std::vector<Q3Result> results;
    while (!heap.empty()) {
        results.push_back(heap.top());
        heap.pop();
    }

    // Sort in descending order
    std::sort(results.begin(), results.end(), [](const Q3Result& a, const Q3Result& b) {
        if (a.revenue != b.revenue)
            return a.revenue > b.revenue;
        return a.key.o_orderdate < b.key.o_orderdate;
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Q3: " << results.size() << " rows in " << elapsed << "s" << std::endl;

    // Write results if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& r : results) {
            out << r.key.l_orderkey << ","
                << r.revenue << ","
                << date_utils::days_to_date_str(r.key.o_orderdate) << ","
                << r.key.o_shippriority << "\n";
        }
    }
}
