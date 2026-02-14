#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <ctime>
#include <cmath>
#include <algorithm>
#include <iomanip>
#include <chrono>

// Helper function: convert epoch days to YYYY-MM-DD format
inline std::string epochDaysToString(int32_t days) {
    const int64_t SECONDS_PER_DAY = 86400;
    std::time_t t = days * SECONDS_PER_DAY;
    struct tm* tm_info = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return std::string(buf);
}

// Helper to mmap a file
inline const void* mmapFile(const std::string& filename, size_t& filesize) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filename << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }

    filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << filename << std::endl;
        return nullptr;
    }

    madvise(ptr, filesize, MADV_SEQUENTIAL);
    return ptr;
}

struct AggResult {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const AggResult& other) const {
        if (std::abs(revenue - other.revenue) > 1e-6) {
            return revenue > other.revenue;  // DESC
        }
        return o_orderdate < other.o_orderdate;  // ASC
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    uint8_t building_code = 4;
    int32_t orderdate_cutoff = 9204;  // 1995-03-15
    int32_t shipdate_cutoff = 9204;   // 1995-03-15

    // mmap columns
    size_t customer_size = 0, orders_size = 0, lineitem_size = 0;
    size_t o_orderdate_idx_size = 0, l_shipdate_idx_size = 0;

    const int32_t* c_custkey = (const int32_t*)mmapFile(
        gendb_dir + "/customer.c_custkey.col", customer_size);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(
        gendb_dir + "/customer.c_mktsegment.col", customer_size);

    const int32_t* o_orderkey = (const int32_t*)mmapFile(
        gendb_dir + "/orders.o_orderkey.col", orders_size);
    const int32_t* o_custkey = (const int32_t*)mmapFile(
        gendb_dir + "/orders.o_custkey.col", orders_size);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(
        gendb_dir + "/orders.o_shippriority.col", orders_size);

    // Use sorted index to reconstruct dates
    const int32_t* o_orderdate_idx = (const int32_t*)mmapFile(
        gendb_dir + "/orders.o_orderdate.sorted_idx", o_orderdate_idx_size);
    const int32_t* l_shipdate_idx = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem.l_shipdate.sorted_idx", l_shipdate_idx_size);

    const int32_t* l_orderkey = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem.l_orderkey.col", lineitem_size);
    const double* l_extendedprice = (const double*)mmapFile(
        gendb_dir + "/lineitem.l_extendedprice.col", lineitem_size);
    const double* l_discount = (const double*)mmapFile(
        gendb_dir + "/lineitem.l_discount.col", lineitem_size);

    size_t num_customers = customer_size / sizeof(int32_t);
    size_t num_orders = orders_size / sizeof(int32_t);
    size_t num_lineitems = lineitem_size / sizeof(int32_t);

    // Build maps from row index to date from sorted indexes
    std::unordered_map<int32_t, int32_t> o_orderdate_map;
    std::unordered_map<int32_t, int32_t> l_shipdate_map;

    size_t num_o_idx_entries = o_orderdate_idx_size / (2 * sizeof(int32_t));
    for (size_t i = 0; i < num_o_idx_entries; ++i) {
        int32_t date_val = o_orderdate_idx[2 * i];
        int32_t row_id = o_orderdate_idx[2 * i + 1];
        o_orderdate_map[row_id] = date_val;
    }

    size_t num_l_idx_entries = l_shipdate_idx_size / (2 * sizeof(int32_t));
    for (size_t i = 0; i < num_l_idx_entries; ++i) {
        int32_t date_val = l_shipdate_idx[2 * i];
        int32_t row_id = l_shipdate_idx[2 * i + 1];
        l_shipdate_map[row_id] = date_val;
    }

    // Filter customer on c_mktsegment='BUILDING'
    std::unordered_map<int32_t, bool> customer_hash;
    for (size_t i = 0; i < num_customers; ++i) {
        if (c_mktsegment[i] == building_code) {
            customer_hash[c_custkey[i]] = true;
        }
    }

    // Filter orders and build hash table
    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };
    std::unordered_map<int32_t, OrderInfo> orders_hash;

    for (size_t i = 0; i < num_orders; ++i) {
        if (customer_hash.count(o_custkey[i]) > 0) {
            int32_t orderdate = o_orderdate_map.count(i) > 0 ? o_orderdate_map[i] : 0;
            if (orderdate < orderdate_cutoff) {
                orders_hash[o_orderkey[i]] = {orderdate, o_shippriority[i]};
            }
        }
    }

    // Aggregation key
    struct HashKey {
        int32_t orderkey;
        int32_t order_date;
        int32_t shippriority;

        bool operator==(const HashKey& other) const {
            return orderkey == other.orderkey &&
                   order_date == other.order_date &&
                   shippriority == other.shippriority;
        }
    };

    struct HashKeyHash {
        size_t operator()(const HashKey& k) const {
            return std::hash<int64_t>()(
                ((int64_t)k.orderkey << 32) | (k.shippriority & 0xFFFFFFFF)
            ) ^ std::hash<int32_t>()(k.order_date);
        }
    };

    std::unordered_map<HashKey, double, HashKeyHash> aggregation;
    std::mutex agg_mutex;

    // Morsel-driven parallel scan
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::unordered_map<HashKey, double, HashKeyHash> local_agg;

            for (size_t i = t * morsel_size; i < num_lineitems; i += num_threads * morsel_size) {
                size_t end = std::min(i + morsel_size, num_lineitems);

                for (size_t j = i; j < end; ++j) {
                    int32_t orderkey = l_orderkey[j];
                    int32_t shipdate = l_shipdate_map.count(j) > 0 ? l_shipdate_map[j] : 0;

                    auto it = orders_hash.find(orderkey);
                    if (it != orders_hash.end() && shipdate > shipdate_cutoff) {
                        double revenue = l_extendedprice[j] * (1.0 - l_discount[j]);
                        HashKey key = {orderkey, it->second.orderdate, it->second.shippriority};
                        local_agg[key] += revenue;
                    }
                }
            }

            {
                std::lock_guard<std::mutex> lock(agg_mutex);
                for (auto& entry : local_agg) {
                    aggregation[entry.first] += entry.second;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Convert to result vector and sort
    std::vector<AggResult> results;
    results.reserve(aggregation.size());
    for (auto& entry : aggregation) {
        results.push_back({
            entry.first.orderkey,
            entry.second,
            entry.first.order_date,
            entry.first.shippriority
        });
    }

    std::sort(results.begin(), results.end());

    // Limit to 10
    if (results.size() > 10) {
        results.resize(10);
    }

    // Write results
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q3.csv");
        outfile << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

        for (const auto& result : results) {
            outfile << result.l_orderkey << ","
                    << std::fixed << std::setprecision(4) << result.revenue << ","
                    << epochDaysToString(result.o_orderdate) << ","
                    << result.o_shippriority << "\n";
        }
        outfile.close();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    std::cout << "Query returned " << results.size() << " rows\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << duration_ms << " ms\n";
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
