#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include "../operators/sort.h"
#include "../operators/open_hash_table.h"

#include <sys/mman.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>

// SIMD intrinsics for AVX-512
#ifdef __AVX512F__
#include <immintrin.h>
#endif

namespace gendb {
namespace queries {

// Helper to read string column
static std::vector<std::string> read_string_column(const std::string& file_path, size_t expected_rows) {
    FILE* f = fopen(file_path.c_str(), "rb");
    if (!f) throw std::runtime_error("Failed to open string column: " + file_path);

    std::vector<std::string> result;
    result.reserve(expected_rows);

    while (!feof(f)) {
        uint32_t len;
        if (fread(&len, sizeof(uint32_t), 1, f) != 1) break;
        std::string str(len, '\0');
        fread(&str[0], 1, len, f);
        result.push_back(std::move(str));
    }

    fclose(f);
    return result;
}

struct Q3Result {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const Q3Result& other) const {
        if (revenue != other.revenue)
            return revenue < other.revenue; // Min heap for top-K
        return o_orderdate > other.o_orderdate;
    }
};

// Check if AVX-512 is available at runtime
static bool has_avx512() {
#ifdef __AVX512F__
    return true;
#else
    return false;
#endif
}

// SIMD-vectorized date filtering for l_shipdate > cutoff
static inline void filter_shipdate_simd(
    const int32_t* l_shipdate,
    const int32_t* l_orderkey,
    const double* l_extendedprice,
    const double* l_discount,
    size_t start_row,
    size_t end_row,
    int32_t ship_cutoff,
    const operators::OpenHashTable<int32_t, std::pair<int32_t, int32_t>>& orders_ht,
    operators::OpenHashTableCompositeKey<double>& local_agg
) {
#ifdef __AVX512F__
    // AVX-512: process 16 int32_t dates at a time
    const size_t simd_width = 16;
    __m512i cutoff_vec = _mm512_set1_epi32(ship_cutoff);

    size_t i = start_row;
    // Process full SIMD vectors
    for (; i + simd_width <= end_row; i += simd_width) {
        // Load 16 shipdates
        __m512i dates = _mm512_loadu_si512((__m512i*)&l_shipdate[i]);

        // Compare: dates > cutoff
        __mmask16 mask = _mm512_cmpgt_epi32_mask(dates, cutoff_vec);

        if (mask == 0) continue; // No matches in this vector

        // Process matching indices
        for (size_t j = 0; j < simd_width; ++j) {
            if (mask & (1 << j)) {
                size_t idx = i + j;
                const auto* order_data = orders_ht.find(l_orderkey[idx]);
                if (order_data) {
                    double revenue = l_extendedprice[idx] * (1.0 - l_discount[idx]);
                    double* agg_val = local_agg.find_or_insert(
                        l_orderkey[idx], order_data->first, order_data->second);
                    if (agg_val) {
                        *agg_val += revenue;
                    }
                }
            }
        }
    }

    // Handle tail with scalar code
    for (; i < end_row; ++i) {
        if (l_shipdate[i] > ship_cutoff) {
            const auto* order_data = orders_ht.find(l_orderkey[i]);
            if (order_data) {
                double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
                double* agg_val = local_agg.find_or_insert(
                    l_orderkey[i], order_data->first, order_data->second);
                *agg_val += revenue;
            }
        }
    }
#else
    // Scalar fallback
    for (size_t i = start_row; i < end_row; ++i) {
        if (l_shipdate[i] > ship_cutoff) {
            const auto* order_data = orders_ht.find(l_orderkey[i]);
            if (order_data) {
                double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
                double* agg_val = local_agg.find_or_insert(
                    l_orderkey[i], order_data->first, order_data->second);
                *agg_val += revenue;
            }
        }
    }
#endif
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Step 1: Scan and filter customer by c_mktsegment = 'BUILDING' (encoded as 0)
    // Build hash set with filtered customer keys using open-addressing hash table
    size_t c_num_rows = storage::read_row_count(gendb_dir + "/customer_metadata.json");
    int32_t* c_custkey = storage::mmap_column<int32_t>(gendb_dir + "/customer_c_custkey.bin", c_num_rows);
    madvise(c_custkey, c_num_rows * sizeof(int32_t), MADV_WILLNEED);
    madvise(c_custkey, c_num_rows * sizeof(int32_t), MADV_SEQUENTIAL);

    // Load dictionary-encoded c_mktsegment (uint8_t)
    uint8_t* c_mktsegment = storage::mmap_column<uint8_t>(gendb_dir + "/customer_c_mktsegment.bin", c_num_rows);
    madvise(c_mktsegment, c_num_rows * sizeof(uint8_t), MADV_WILLNEED);
    madvise(c_mktsegment, c_num_rows * sizeof(uint8_t), MADV_SEQUENTIAL);

    // Build customer filter hash table (open-addressing)
    // Expected: ~300K customers with BUILDING segment (20% selectivity from 1.5M)
    const uint8_t BUILDING_CODE = 0;
    operators::OpenHashTable<int32_t, bool> customer_filter(300000);

    for (size_t i = 0; i < c_num_rows; ++i) {
        if (c_mktsegment[i] == BUILDING_CODE) {
            customer_filter.insert(c_custkey[i], true);
        }
    }

    munmap(c_custkey, c_num_rows * sizeof(int32_t));
    munmap(c_mktsegment, c_num_rows * sizeof(uint8_t));

    // Step 2: Scan orders and filter by o_orderdate < '1995-03-15' AND customer filter
    // Build hash table: o_orderkey -> (o_orderdate, o_shippriority)
    // Use zone map pruning on orders table (if available)
    size_t o_num_rows = storage::read_row_count(gendb_dir + "/orders_metadata.json");
    int32_t* o_orderkey = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_orderkey.bin", o_num_rows);
    int32_t* o_custkey = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_custkey.bin", o_num_rows);
    int32_t* o_orderdate = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_orderdate.bin", o_num_rows);
    int32_t* o_shippriority = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_shippriority.bin", o_num_rows);

    int32_t date_cutoff = date_utils::date_to_days(1995, 3, 15);

    // Try to load zone map for orders orderdate (if it exists)
    auto orders_zonemap = storage::read_zone_map(gendb_dir + "/orders_o_orderdate_zonemap.json");

    // Identify blocks to scan using zone map
    std::vector<std::pair<size_t, size_t>> orders_ranges;
    if (!orders_zonemap.empty()) {
        // Use zone map pruning: skip blocks where min_val >= date_cutoff
        for (const auto& block : orders_zonemap) {
            if (block.min_val < date_cutoff) {
                orders_ranges.push_back({block.start_row, block.end_row});
            }
        }
        madvise(o_orderkey, o_num_rows * sizeof(int32_t), MADV_RANDOM);
        madvise(o_custkey, o_num_rows * sizeof(int32_t), MADV_RANDOM);
        madvise(o_orderdate, o_num_rows * sizeof(int32_t), MADV_RANDOM);
        madvise(o_shippriority, o_num_rows * sizeof(int32_t), MADV_RANDOM);
    } else {
        // No zone map: scan entire table
        orders_ranges.push_back({0, o_num_rows});
        madvise(o_orderkey, o_num_rows * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise(o_custkey, o_num_rows * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise(o_orderdate, o_num_rows * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise(o_shippriority, o_num_rows * sizeof(int32_t), MADV_SEQUENTIAL);
    }

    // Build orders hash table with open-addressing
    // Expected: ~675K orders after filters (45% of 15M pass date filter, 20% of those pass customer filter)
    operators::OpenHashTable<int32_t, std::pair<int32_t, int32_t>> orders_ht(700000);

    for (const auto& range : orders_ranges) {
        for (size_t i = range.first; i < range.second; ++i) {
            if (o_orderdate[i] < date_cutoff && customer_filter.contains(o_custkey[i])) {
                orders_ht.insert(o_orderkey[i], {o_orderdate[i], o_shippriority[i]});
            }
        }
    }

    munmap(o_orderkey, o_num_rows * sizeof(int32_t));
    munmap(o_custkey, o_num_rows * sizeof(int32_t));
    munmap(o_orderdate, o_num_rows * sizeof(int32_t));
    munmap(o_shippriority, o_num_rows * sizeof(int32_t));

    // Step 3: Scan lineitem and join with orders, filter by l_shipdate > '1995-03-15'
    // Use zone map for block skipping on l_shipdate
    auto shipdate_zonemap = storage::read_zone_map(gendb_dir + "/lineitem_l_shipdate_zonemap.json");

    size_t l_num_rows = storage::read_row_count(gendb_dir + "/lineitem_metadata.json");
    int32_t* l_orderkey = storage::mmap_column<int32_t>(gendb_dir + "/lineitem_l_orderkey.bin", l_num_rows);
    madvise(l_orderkey, l_num_rows * sizeof(int32_t), MADV_RANDOM);
    int32_t* l_shipdate = storage::mmap_column<int32_t>(gendb_dir + "/lineitem_l_shipdate.bin", l_num_rows);
    madvise(l_shipdate, l_num_rows * sizeof(int32_t), MADV_RANDOM);
    double* l_extendedprice = storage::mmap_column<double>(gendb_dir + "/lineitem_l_extendedprice.bin", l_num_rows);
    madvise(l_extendedprice, l_num_rows * sizeof(double), MADV_RANDOM);
    double* l_discount = storage::mmap_column<double>(gendb_dir + "/lineitem_l_discount.bin", l_num_rows);
    madvise(l_discount, l_num_rows * sizeof(double), MADV_RANDOM);

    int32_t ship_cutoff = date_utils::date_to_days(1995, 3, 15);

    // Identify blocks to scan using zone map pruning for l_shipdate > ship_cutoff
    std::vector<storage::ZoneMapBlock> blocks_to_scan;
    for (const auto& block : shipdate_zonemap) {
        // Skip blocks where max_val <= ship_cutoff (all rows fail the filter)
        if (block.max_val > ship_cutoff) {
            blocks_to_scan.push_back(block);
        }
    }

    // Parallel aggregation using thread-local open-addressing hash tables
    // Expected ~100K groups after all joins and filters
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<operators::OpenHashTableCompositeKey<double>> local_aggregates;
    local_aggregates.reserve(num_threads);
    for (unsigned int t = 0; t < num_threads; ++t) {
        local_aggregates.emplace_back(150000);  // Pre-size for 150K groups
    }

    // Distribute blocks across threads for parallel processing
    std::atomic<size_t> block_counter(0);
    std::vector<std::thread> threads;

    bool use_simd = has_avx512();

    for (unsigned int thread_id = 0; thread_id < num_threads; ++thread_id) {
        threads.emplace_back([&, thread_id]() {
            while (true) {
                size_t block_idx = block_counter.fetch_add(1);
                if (block_idx >= blocks_to_scan.size()) break;

                const auto& block = blocks_to_scan[block_idx];

                if (use_simd) {
                    // Use SIMD-accelerated date filtering
                    filter_shipdate_simd(
                        l_shipdate, l_orderkey, l_extendedprice, l_discount,
                        block.start_row, block.end_row, ship_cutoff,
                        orders_ht, local_aggregates[thread_id]
                    );
                } else {
                    // Scalar fallback
                    for (size_t i = block.start_row; i < block.end_row; ++i) {
                        if (l_shipdate[i] > ship_cutoff) {
                            const auto* order_data = orders_ht.find(l_orderkey[i]);
                            if (order_data) {
                                double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
                                double* agg_val = local_aggregates[thread_id].find_or_insert(
                                    l_orderkey[i], order_data->first, order_data->second);
                                if (agg_val) {
                                    *agg_val += revenue;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local aggregates into global hash table
    operators::OpenHashTableCompositeKey<double> aggregates(150000);
    for (auto& local : local_aggregates) {
        for (auto kv : local) {
            double* global_val = aggregates.find_or_insert(kv.k1, kv.k2, kv.k3);
            if (global_val) {
                *global_val += kv.value;
            }
        }
    }

    munmap(l_orderkey, l_num_rows * sizeof(int32_t));
    munmap(l_shipdate, l_num_rows * sizeof(int32_t));
    munmap(l_extendedprice, l_num_rows * sizeof(double));
    munmap(l_discount, l_num_rows * sizeof(double));

    // Step 4: Top-K selection (LIMIT 10)
    // Convert aggregates to result vector
    std::vector<Q3Result> all_results;
    all_results.reserve(aggregates.size());
    for (auto kv : aggregates) {
        all_results.push_back({kv.k1, kv.value, kv.k2, kv.k3});
    }

    // Use top-K operator for efficient selection
    auto results = operators::top_k_selection(
        all_results,
        10,
        [](const Q3Result& a, const Q3Result& b) {
            if (a.revenue != b.revenue)
                return a.revenue < b.revenue;  // Smaller revenue is worse
            return a.o_orderdate > b.o_orderdate;  // Later date is worse
        }
    );

    // Sort final results (descending revenue, ascending date)
    operators::sort_results(results, [](const Q3Result& a, const Q3Result& b) {
        if (a.revenue != b.revenue)
            return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print results
    std::cout << "Q3: " << results.size() << " rows in " << std::fixed << std::setprecision(3)
              << elapsed << "s" << std::endl;

    // Write to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);
        for (const auto& r : results) {
            out << r.l_orderkey << "," << r.revenue << ","
                << date_utils::days_to_date_str(r.o_orderdate) << ","
                << r.o_shippriority << "\n";
        }
    }
}

} // namespace queries
} // namespace gendb
