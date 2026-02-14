// q3.cpp - TPC-H Q3: Shipping Priority Query
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <thread>
#include <mutex>
#include <cstring>
#include <cmath>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iomanip>
#include <chrono>
#include <ctime>

namespace {

// Helper: mmap a binary file
const void* mmapFile(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return nullptr;
    }

    size_out = st.st_size;
    void* ptr = mmap(nullptr, size_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        return nullptr;
    }

    // Advise sequential access
    madvise(ptr, size_out, MADV_SEQUENTIAL);
    return ptr;
}

// Helper: Convert epoch days to YYYY-MM-DD string
// NOTE: Storage has dates offset by +1 day, so we subtract 1 for output
inline std::string epochDaysToString(int32_t days) {
    std::time_t t = static_cast<int64_t>(days - 1) * 86400LL;
    struct tm* tm = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm);
    return std::string(buf);
}

// Zone map metadata structure
struct ZoneMapBlock {
    int32_t min_value;
    int32_t max_value;
    uint64_t start_offset;
    uint64_t end_offset;
};

// Bloom filter for fast membership testing
class BloomFilter {
private:
    std::vector<uint64_t> bits;
    size_t num_bits;

public:
    BloomFilter(size_t expected_elements, double fpr = 0.01) {
        // Calculate optimal bit array size: m = -n * ln(fpr) / (ln2)^2
        double m = -static_cast<double>(expected_elements) * std::log(fpr) / (std::log(2.0) * std::log(2.0));
        num_bits = static_cast<size_t>(m);
        // Round up to multiple of 64 for efficient storage
        num_bits = ((num_bits + 63) / 64) * 64;
        bits.resize(num_bits / 64, 0);
    }

    void insert(int32_t key) {
        uint64_t h1 = std::hash<int32_t>()(key);
        uint64_t h2 = h1 >> 32;

        // Use two hash functions derived from h1 and h2
        size_t pos1 = h1 % num_bits;
        size_t pos2 = h2 % num_bits;

        bits[pos1 / 64] |= (1ULL << (pos1 % 64));
        bits[pos2 / 64] |= (1ULL << (pos2 % 64));
    }

    bool contains(int32_t key) const {
        uint64_t h1 = std::hash<int32_t>()(key);
        uint64_t h2 = h1 >> 32;

        size_t pos1 = h1 % num_bits;
        size_t pos2 = h2 % num_bits;

        return (bits[pos1 / 64] & (1ULL << (pos1 % 64))) &&
               (bits[pos2 / 64] & (1ULL << (pos2 % 64)));
    }
};

// Result structure for aggregation
struct AggregateKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggregateKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

// Hash function for AggregateKey
namespace std {
    template<>
    struct hash<AggregateKey> {
        size_t operator()(const AggregateKey& k) const {
            return std::hash<int32_t>()(k.l_orderkey) ^
                   (std::hash<int32_t>()(k.o_orderdate) << 1) ^
                   (std::hash<int32_t>()(k.o_shippriority) << 2);
        }
    };
}

struct AggregateValue {
    double revenue;
};

// Final result row
struct ResultRow {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const ResultRow& other) const {
        if (revenue != other.revenue) {
            return revenue > other.revenue; // DESC
        }
        return o_orderdate < other.o_orderdate; // ASC
    }
};

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // Date constants (epoch days since 1970-01-01)
    // NOTE: Storage appears to have dates offset by +1 day
    // 1995-03-15 = 9204 days, but storage uses 9205
    const int32_t date_1995_03_15 = 9205;

    // Hardware detection
    const size_t num_threads = std::thread::hardware_concurrency();

    // 1. Load customer table and filter by c_mktsegment = 'BUILDING' (code 0)
    auto t_load_customer = std::chrono::high_resolution_clock::now();

    size_t customer_custkey_size, customer_mktsegment_size;
    const int32_t* c_custkey = (const int32_t*)mmapFile(gendb_dir + "/customer/c_custkey.bin", customer_custkey_size);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(gendb_dir + "/customer/c_mktsegment.bin", customer_mktsegment_size);

    const size_t customer_row_count = customer_custkey_size / sizeof(int32_t);

    // Build hash set of custkeys with mktsegment = 'BUILDING' (code 0)
    std::unordered_set<int32_t> building_custkeys;
    building_custkeys.reserve(customer_row_count / 5); // ~20% selectivity

    for (size_t i = 0; i < customer_row_count; ++i) {
        if (c_mktsegment[i] == 0) { // 'BUILDING' is code 0
            building_custkeys.insert(c_custkey[i]);
        }
    }

    auto t_customer_done = std::chrono::high_resolution_clock::now();
    double customer_ms = std::chrono::duration<double, std::milli>(t_customer_done - t_load_customer).count();
    std::cout << "[TIMING] scan_filter_customer: " << std::fixed << std::setprecision(1) << customer_ms << " ms" << std::endl;

    // 2. Load orders table and filter by o_orderdate < 1995-03-15 AND o_custkey IN building_custkeys
    auto t_load_orders = std::chrono::high_resolution_clock::now();

    size_t orders_orderkey_size, orders_custkey_size, orders_orderdate_size, orders_shippriority_size;
    const int32_t* o_orderkey = (const int32_t*)mmapFile(gendb_dir + "/orders/o_orderkey.bin", orders_orderkey_size);
    const int32_t* o_custkey = (const int32_t*)mmapFile(gendb_dir + "/orders/o_custkey.bin", orders_custkey_size);
    const int32_t* o_orderdate = (const int32_t*)mmapFile(gendb_dir + "/orders/o_orderdate.bin", orders_orderdate_size);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(gendb_dir + "/orders/o_shippriority.bin", orders_shippriority_size);

    const size_t orders_row_count = orders_orderkey_size / sizeof(int32_t);
    const size_t block_size = 100000; // From storage design

    // Load zone map for o_orderdate to enable block skipping
    size_t zonemap_size;
    const void* zonemap_data = mmapFile(gendb_dir + "/orders/o_orderdate.zonemap.idx", zonemap_size);

    // Parse zone map: first 8 bytes = num_blocks, then array of ZoneMapBlock structs
    const uint64_t* num_blocks_ptr = (const uint64_t*)zonemap_data;
    const size_t num_blocks = *num_blocks_ptr;
    const ZoneMapBlock* zone_blocks = (const ZoneMapBlock*)((const char*)zonemap_data + 8);

    // Identify qualifying blocks using zone map
    std::vector<size_t> qualifying_blocks;
    for (size_t b = 0; b < num_blocks; ++b) {
        // Skip block if min_value >= date_1995_03_15 (all rows fail the < predicate)
        if (zone_blocks[b].min_value < date_1995_03_15) {
            qualifying_blocks.push_back(b);
        }
    }

    // Parallel scan of qualifying blocks
    std::vector<std::unordered_map<int32_t, std::pair<int32_t, int32_t>>> local_qualified_orders(num_threads);
    std::vector<std::thread> order_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        order_threads.emplace_back([&, t]() {
            auto& local_map = local_qualified_orders[t];
            local_map.reserve(qualifying_blocks.size() * block_size / num_threads / 2);

            // Process every num_threads-th qualifying block
            for (size_t idx = t; idx < qualifying_blocks.size(); idx += num_threads) {
                size_t block_id = qualifying_blocks[idx];
                size_t start_row = block_id * block_size;
                size_t end_row = std::min(start_row + block_size, orders_row_count);

                for (size_t i = start_row; i < end_row; ++i) {
                    if (o_orderdate[i] < date_1995_03_15 && building_custkeys.count(o_custkey[i]) > 0) {
                        local_map[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
                    }
                }
            }
        });
    }

    for (auto& thread : order_threads) {
        thread.join();
    }

    // Merge local maps into global qualified_orders
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> qualified_orders;
    qualified_orders.reserve(orders_row_count / 10); // Conservative estimate

    for (const auto& local_map : local_qualified_orders) {
        for (const auto& entry : local_map) {
            qualified_orders[entry.first] = entry.second;
        }
    }

    auto t_orders_done = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_done - t_load_orders).count();
    std::cout << "[TIMING] scan_filter_orders: " << std::fixed << std::setprecision(1) << orders_ms << " ms" << std::endl;

    // Build Bloom filter for qualified orders to accelerate lineitem join
    auto t_bloom_start = std::chrono::high_resolution_clock::now();
    BloomFilter order_bloom(qualified_orders.size(), 0.01);
    for (const auto& entry : qualified_orders) {
        order_bloom.insert(entry.first);
    }
    auto t_bloom_done = std::chrono::high_resolution_clock::now();
    double bloom_ms = std::chrono::duration<double, std::milli>(t_bloom_done - t_bloom_start).count();
    std::cout << "[TIMING] bloom_filter_build: " << std::fixed << std::setprecision(1) << bloom_ms << " ms" << std::endl;

    // 3. Scan lineitem, join with qualified_orders, filter by l_shipdate > 1995-03-15, aggregate
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();

    size_t lineitem_orderkey_size, lineitem_extendedprice_size, lineitem_discount_size, lineitem_shipdate_size;
    const int32_t* l_orderkey = (const int32_t*)mmapFile(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_orderkey_size);
    const double* l_extendedprice = (const double*)mmapFile(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_extendedprice_size);
    const double* l_discount = (const double*)mmapFile(gendb_dir + "/lineitem/l_discount.bin", lineitem_discount_size);
    const int32_t* l_shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_shipdate_size);

    const size_t lineitem_row_count = lineitem_orderkey_size / sizeof(int32_t);

    // Parallel aggregation
    const size_t chunk_size = (lineitem_row_count + num_threads - 1) / num_threads;

    std::vector<std::unordered_map<AggregateKey, AggregateValue>> local_maps(num_threads);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, lineitem_row_count);

            auto& local_map = local_maps[t];
            local_map.reserve(10000); // Reserve space for local aggregation

            for (size_t i = start; i < end; ++i) {
                // Filter: l_shipdate > 1995-03-15
                if (l_shipdate[i] <= date_1995_03_15) {
                    continue;
                }

                // Fast Bloom filter check before hash table lookup
                if (!order_bloom.contains(l_orderkey[i])) {
                    continue;
                }

                // Join: check if l_orderkey exists in qualified_orders
                auto it = qualified_orders.find(l_orderkey[i]);
                if (it == qualified_orders.end()) {
                    continue;
                }

                // Aggregate
                AggregateKey key{l_orderkey[i], it->second.first, it->second.second};
                double revenue_contribution = l_extendedprice[i] * (1.0 - l_discount[i]);

                auto agg_it = local_map.find(key);
                if (agg_it == local_map.end()) {
                    local_map[key] = {revenue_contribution};
                } else {
                    agg_it->second.revenue += revenue_contribution;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto t_join_agg_done = std::chrono::high_resolution_clock::now();
    double join_agg_ms = std::chrono::duration<double, std::milli>(t_join_agg_done - t_lineitem_start).count();
    std::cout << "[TIMING] join_aggregation: " << std::fixed << std::setprecision(1) << join_agg_ms << " ms" << std::endl;

    // 4. Merge local aggregates
    auto t_merge_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<AggregateKey, AggregateValue> global_map;
    for (const auto& local_map : local_maps) {
        for (const auto& entry : local_map) {
            auto it = global_map.find(entry.first);
            if (it == global_map.end()) {
                global_map[entry.first] = entry.second;
            } else {
                it->second.revenue += entry.second.revenue;
            }
        }
    }

    auto t_merge_done = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_done - t_merge_start).count();
    std::cout << "[TIMING] merge: " << std::fixed << std::setprecision(1) << merge_ms << " ms" << std::endl;

    // 5. Convert to vector and sort
    auto t_sort_start = std::chrono::high_resolution_clock::now();

    std::vector<ResultRow> results;
    results.reserve(global_map.size());

    for (const auto& entry : global_map) {
        results.push_back({
            entry.first.l_orderkey,
            entry.second.revenue,
            entry.first.o_orderdate,
            entry.first.o_shippriority
        });
    }

    // Partial sort for top 10 (optimization)
    if (results.size() > 10) {
        std::partial_sort(results.begin(), results.begin() + 10, results.end());
        results.resize(10);
    } else {
        std::sort(results.begin(), results.end());
    }

    auto t_sort_done = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_done - t_sort_start).count();
    std::cout << "[TIMING] sort: " << std::fixed << std::setprecision(1) << sort_ms << " ms" << std::endl;

    // 6. Write results
    if (!results_dir.empty()) {
        auto t_output_start = std::chrono::high_resolution_clock::now();

        std::string output_path = results_dir + "/q3_result.csv";
        std::ofstream out(output_path);
        out << std::fixed << std::setprecision(2);

        // Write header
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

        for (const auto& row : results) {
            out << row.l_orderkey << ","
                << row.revenue << ","
                << epochDaysToString(row.o_orderdate) << ","
                << row.o_shippriority << "\n";
        }

        out.close();

        auto t_output_done = std::chrono::high_resolution_clock::now();
        double output_ms = std::chrono::duration<double, std::milli>(t_output_done - t_output_start).count();
        std::cout << "[TIMING] output: " << std::fixed << std::setprecision(1) << output_ms << " ms" << std::endl;
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "[TIMING] total: " << std::fixed << std::setprecision(1) << total_ms << " ms" << std::endl;
    std::cout << "Query returned " << results.size() << " rows" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q3(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
