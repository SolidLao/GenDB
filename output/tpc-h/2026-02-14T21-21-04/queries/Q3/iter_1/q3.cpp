// Q3: Shipping Priority
// Self-contained C++ implementation with basic optimizations
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>

// Helper: mmap a binary column file
void* mmapFile(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    size = sb.st_size;
    void* addr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        return nullptr;
    }
    madvise(addr, size, MADV_SEQUENTIAL);
    return addr;
}

// Helper: Convert epoch days to YYYY-MM-DD string
std::string epochDaysToString(int32_t days) {
    std::time_t t = static_cast<int64_t>(days) * 86400LL;
    struct tm tm_buf;
    gmtime_r(&t, &tm_buf);
    char buf[11];
    strftime(buf, sizeof(buf), "%Y-%m-%d", &tm_buf);
    return std::string(buf);
}

// Aggregate key for GROUP BY (l_orderkey, o_orderdate, o_shippriority)
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

// Standalone hash struct for aggregate keys
struct AggregateKeyHash {
    size_t operator()(const AggregateKey& k) const {
        size_t h = std::hash<int32_t>()(k.l_orderkey);
        h ^= std::hash<int32_t>()(k.o_orderdate) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.o_shippriority) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

struct AggregateKeyEqual {
    bool operator()(const AggregateKey& a, const AggregateKey& b) const {
        return a.l_orderkey == b.l_orderkey &&
               a.o_orderdate == b.o_orderdate &&
               a.o_shippriority == b.o_shippriority;
    }
};

// Result structure for output
struct Result {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    const int32_t date_cutoff = 9204; // 1995-03-15 in epoch days
    const uint8_t mktsegment_building = 0; // "BUILDING" in dictionary

    // 1. Load customer columns
    auto t_start = std::chrono::high_resolution_clock::now();
    size_t c_custkey_size, c_mktsegment_size;
    const int32_t* c_custkey = (const int32_t*)mmapFile(gendb_dir + "/customer_c_custkey.bin", c_custkey_size);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(gendb_dir + "/customer_c_mktsegment.bin", c_mktsegment_size);
    size_t customer_rows = c_custkey_size / sizeof(int32_t);

    // 2. Load orders columns
    size_t o_orderkey_size, o_custkey_size, o_orderdate_size, o_shippriority_size;
    const int32_t* o_orderkey = (const int32_t*)mmapFile(gendb_dir + "/orders_o_orderkey.bin", o_orderkey_size);
    const int32_t* o_custkey = (const int32_t*)mmapFile(gendb_dir + "/orders_o_custkey.bin", o_custkey_size);
    const int32_t* o_orderdate = (const int32_t*)mmapFile(gendb_dir + "/orders_o_orderdate.bin", o_orderdate_size);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(gendb_dir + "/orders_o_shippriority.bin", o_shippriority_size);
    size_t orders_rows = o_orderkey_size / sizeof(int32_t);

    // 3. Load lineitem columns
    size_t l_orderkey_size, l_extendedprice_size, l_discount_size, l_shipdate_size;
    const int32_t* l_orderkey = (const int32_t*)mmapFile(gendb_dir + "/lineitem_l_orderkey.bin", l_orderkey_size);
    const int64_t* l_extendedprice = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_extendedprice.bin", l_extendedprice_size);
    const int64_t* l_discount = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_discount.bin", l_discount_size);
    const int32_t* l_shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem_l_shipdate.bin", l_shipdate_size);
    size_t lineitem_rows = l_orderkey_size / sizeof(int32_t);

    auto t_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] load_data: " << std::fixed << std::setprecision(1) << load_ms << " ms" << std::endl;

    // 4. Build hash table: customer (c_custkey) WHERE c_mktsegment = 'BUILDING'
    t_start = std::chrono::high_resolution_clock::now();
    std::unordered_map<int32_t, bool> customer_ht;
    customer_ht.reserve(customer_rows / 5); // ~20% selectivity
    for (size_t i = 0; i < customer_rows; ++i) {
        if (c_mktsegment[i] == mktsegment_building) {
            customer_ht[c_custkey[i]] = true;
        }
    }
    t_end = std::chrono::high_resolution_clock::now();
    double customer_filter_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] customer_filter: " << std::fixed << std::setprecision(1) << customer_filter_ms << " ms" << std::endl;

    // 5. Build hash table: orders (o_orderkey -> (o_orderdate, o_shippriority))
    //    WHERE o_custkey IN customer_ht AND o_orderdate < date_cutoff
    //    OPTIMIZED: Parallel morsel-driven scan with zone map pruning
    t_start = std::chrono::high_resolution_clock::now();
    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    const size_t num_threads = std::thread::hardware_concurrency();

    // Load zone map for orders.o_orderdate (block_size = 100000)
    std::string zonemap_path = gendb_dir + "/orders_o_orderdate.zonemap";
    std::vector<std::pair<int32_t, int32_t>> zone_map; // (min, max) per block
    std::ifstream zm_file(zonemap_path, std::ios::binary);
    if (zm_file.is_open()) {
        size_t num_blocks = (orders_rows + 99999) / 100000;
        zone_map.resize(num_blocks);
        zm_file.read((char*)zone_map.data(), num_blocks * sizeof(std::pair<int32_t, int32_t>));
        zm_file.close();
    } else {
        // Generate zone map if not present
        size_t num_blocks = (orders_rows + 99999) / 100000;
        zone_map.resize(num_blocks);
        for (size_t b = 0; b < num_blocks; ++b) {
            size_t start = b * 100000;
            size_t end = std::min(start + 100000, orders_rows);
            int32_t min_date = o_orderdate[start];
            int32_t max_date = o_orderdate[start];
            for (size_t i = start + 1; i < end; ++i) {
                if (o_orderdate[i] < min_date) min_date = o_orderdate[i];
                if (o_orderdate[i] > max_date) max_date = o_orderdate[i];
            }
            zone_map[b] = {min_date, max_date};
        }
    }

    // Parallel morsel-driven scan of orders
    const size_t orders_morsel_size = 100000;
    const size_t num_orders_morsels = (orders_rows + orders_morsel_size - 1) / orders_morsel_size;
    std::vector<std::unordered_map<int32_t, OrderInfo>> thread_orders_maps(num_threads);
    for (auto& m : thread_orders_maps) {
        m.reserve(orders_rows / num_threads / 2); // ~50% selectivity per thread
    }

    std::vector<std::thread> orders_threads;
    for (size_t t = 0; t < num_threads; ++t) {
        orders_threads.emplace_back([&, t]() {
            auto& local_map = thread_orders_maps[t];
            for (size_t m = t; m < num_orders_morsels; m += num_threads) {
                // Zone map pruning: skip block if max_date < date_cutoff (all rows fail)
                if (m < zone_map.size() && zone_map[m].second < date_cutoff) {
                    continue; // Skip this entire block
                }

                size_t start = m * orders_morsel_size;
                size_t end = std::min(start + orders_morsel_size, orders_rows);
                for (size_t i = start; i < end; ++i) {
                    if (o_orderdate[i] < date_cutoff && customer_ht.count(o_custkey[i])) {
                        local_map[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
                    }
                }
            }
        });
    }
    for (auto& th : orders_threads) {
        th.join();
    }

    // Merge thread-local orders hash tables
    std::unordered_map<int32_t, OrderInfo> orders_ht;
    orders_ht.reserve(orders_rows / 2);
    for (const auto& thread_map : thread_orders_maps) {
        for (const auto& [orderkey, info] : thread_map) {
            orders_ht[orderkey] = info;
        }
    }

    t_end = std::chrono::high_resolution_clock::now();
    double orders_filter_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] orders_filter_join: " << std::fixed << std::setprecision(1) << orders_filter_ms << " ms" << std::endl;

    // 6. Probe lineitem and aggregate
    //    WHERE l_shipdate > date_cutoff AND l_orderkey IN orders_ht
    //    GROUP BY (l_orderkey, o_orderdate, o_shippriority)
    //    Aggregate: SUM(l_extendedprice * (1 - l_discount))
    t_start = std::chrono::high_resolution_clock::now();
    const size_t morsel_size = 100000;
    const size_t num_morsels = (lineitem_rows + morsel_size - 1) / morsel_size;

    std::vector<std::unordered_map<AggregateKey, int64_t, AggregateKeyHash, AggregateKeyEqual>> thread_maps(num_threads);
    for (auto& m : thread_maps) {
        m.reserve(50000); // Estimated groups per thread
    }

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& local_map = thread_maps[t];
            for (size_t m = t; m < num_morsels; m += num_threads) {
                size_t start = m * morsel_size;
                size_t end = std::min(start + morsel_size, lineitem_rows);
                for (size_t i = start; i < end; ++i) {
                    if (l_shipdate[i] > date_cutoff) {
                        auto it = orders_ht.find(l_orderkey[i]);
                        if (it != orders_ht.end()) {
                            // Compute revenue in scaled integer form
                            // revenue = l_extendedprice * (1 - l_discount)
                            // Both are scaled by 100, result is scaled by 10000
                            int64_t price = l_extendedprice[i];
                            int64_t discount = l_discount[i];
                            // Keep full precision: scaled by 10000
                            int64_t revenue_scaled = price * (100 - discount);

                            AggregateKey key = {l_orderkey[i], it->second.o_orderdate, it->second.o_shippriority};
                            local_map[key] += revenue_scaled;
                        }
                    }
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    t_end = std::chrono::high_resolution_clock::now();
    double scan_agg_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] lineitem_scan_aggregation: " << std::fixed << std::setprecision(1) << scan_agg_ms << " ms" << std::endl;

    // 7. Merge thread-local aggregation maps
    t_start = std::chrono::high_resolution_clock::now();
    std::unordered_map<AggregateKey, int64_t, AggregateKeyHash, AggregateKeyEqual> final_agg;
    final_agg.reserve(1500000); // Estimated total groups
    for (const auto& thread_map : thread_maps) {
        for (const auto& [key, revenue_scaled] : thread_map) {
            final_agg[key] += revenue_scaled;
        }
    }
    t_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] aggregation_merge: " << std::fixed << std::setprecision(1) << merge_ms << " ms" << std::endl;

    // 8. Convert to vector and sort (ORDER BY revenue DESC, o_orderdate ASC)
    t_start = std::chrono::high_resolution_clock::now();
    std::vector<Result> results;
    results.reserve(final_agg.size());
    for (const auto& [key, revenue_scaled] : final_agg) {
        // Convert to double: revenue_scaled is scaled by 10000
        double revenue = (double)revenue_scaled / 10000.0;
        results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
    }

    // Use partial_sort for LIMIT 10: O(n log K) instead of O(n log n)
    size_t topk = std::min((size_t)10, results.size());
    std::partial_sort(results.begin(), results.begin() + topk, results.end(), [](const Result& a, const Result& b) {
        if (a.revenue != b.revenue) {
            return a.revenue > b.revenue; // DESC
        }
        return a.o_orderdate < b.o_orderdate; // ASC
    });
    t_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] sort: " << std::fixed << std::setprecision(1) << sort_ms << " ms" << std::endl;

    // 9. Output top 10 results (LIMIT 10)
    if (!results_dir.empty()) {
        t_start = std::chrono::high_resolution_clock::now();
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(4);
        size_t limit = std::min((size_t)10, results.size());
        for (size_t i = 0; i < limit; ++i) {
            out << results[i].l_orderkey << ","
                << results[i].revenue << ","
                << epochDaysToString(results[i].o_orderdate) << ","
                << results[i].o_shippriority << "\n";
        }
        out.close();
        t_end = std::chrono::high_resolution_clock::now();
        double output_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
        std::cout << "[TIMING] output: " << std::fixed << std::setprecision(1) << output_ms << " ms" << std::endl;
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "[TIMING] total: " << std::fixed << std::setprecision(1) << total_ms << " ms" << std::endl;
    std::cout << "Query returned " << std::min((size_t)10, results.size()) << " rows" << std::endl;
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
