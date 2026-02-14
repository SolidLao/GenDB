#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>
#include <algorithm>
#include <iomanip>
#include <chrono>
#include <cstring>

inline std::string epochDaysToString(int32_t days) {
    std::time_t t = days * 86400;
    struct tm* tm_info = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return std::string(buf);
}

inline const void* mmapFile(const std::string& filename, size_t& filesize) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) return nullptr;
    struct stat sb;
    if (fstat(fd, &sb) < 0) { close(fd); return nullptr; }
    filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return nullptr;
    return ptr;
}

// Simple compact hash table for integers (robin_hood style)
template <typename K, typename V>
class CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint16_t distance;  // FIXED: Changed from uint8_t to uint16_t to prevent overflow on probe distances > 255
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t count = 0;
    static constexpr float load_factor = 0.75f;

    size_t hash(K key) const {
        // MurmurHash-like simple hash
        uint64_t h = key;
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        return h;
    }

    void resize(size_t new_capacity) {
        std::vector<Entry> old_table = std::move(table);
        table.resize(new_capacity);
        count = 0;
        for (auto& e : old_table) {
            if (e.occupied) {
                insert(e.key, e.value);
            }
        }
    }

public:
    CompactHashTable(size_t initial_capacity = 1024) {
        table.resize(initial_capacity);
    }

    void insert(K key, V value) {
        if (count >= size_t(table.size() * load_factor)) {
            resize(table.size() * 2);
        }

        size_t idx = hash(key) & (table.size() - 1);
        uint16_t distance = 0;  // FIXED: Changed from uint8_t to uint16_t

        while (true) {
            if (!table[idx].occupied) {
                table[idx].key = key;
                table[idx].value = value;
                table[idx].distance = distance;
                table[idx].occupied = true;
                count++;
                return;
            }

            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }

            if (distance > table[idx].distance) {
                std::swap(table[idx].key, key);
                std::swap(table[idx].value, value);
                std::swap(table[idx].distance, distance);
            }

            idx = (idx + 1) & (table.size() - 1);
            distance++;
        }
    }

    V* find(K key) {
        size_t idx = hash(key) & (table.size() - 1);
        uint16_t distance = 0;  // FIXED: Changed from uint8_t to uint16_t

        while (true) {
            if (!table[idx].occupied) return nullptr;
            if (table[idx].key == key) return &table[idx].value;
            if (distance > table[idx].distance) return nullptr;

            idx = (idx + 1) & (table.size() - 1);
            distance++;
        }
    }

    typename std::vector<Entry>::iterator begin() { return table.begin(); }
    typename std::vector<Entry>::iterator end() { return table.end(); }
    typename std::vector<Entry>::const_iterator begin() const { return table.begin(); }
    typename std::vector<Entry>::const_iterator end() const { return table.end(); }

    size_t size() const { return count; }
};

// Lightweight hash table for boolean existence checks
template <typename K>
class CompactBoolHashTable {
    struct Entry {
        K key;
        uint16_t distance = 0;  // FIXED: Changed from uint8_t to uint16_t to prevent overflow on probe distances > 255
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t num_entries = 0;
    static constexpr float load_factor = 0.75f;

    size_t hash(K key) const {
        uint64_t h = key;
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        return h;
    }

public:
    CompactBoolHashTable(size_t initial_capacity = 1024) {
        table.resize(initial_capacity);
    }

    void insert(K key) {
        if (num_entries >= size_t(table.size() * load_factor)) {
            std::vector<Entry> old_table = std::move(table);
            table.resize(old_table.size() * 2);
            num_entries = 0;
            for (auto& e : old_table) {
                if (e.occupied) insert(e.key);
            }
            return;
        }

        size_t idx = hash(key) & (table.size() - 1);
        uint16_t distance = 0;  // FIXED: Changed from uint8_t to uint16_t

        while (true) {
            if (!table[idx].occupied) {
                table[idx].key = key;
                table[idx].distance = distance;
                table[idx].occupied = true;
                num_entries++;
                return;
            }

            if (table[idx].key == key) return;  // Already present

            if (distance > table[idx].distance) {
                std::swap(table[idx].key, key);
                std::swap(table[idx].distance, distance);
            }

            idx = (idx + 1) & (table.size() - 1);
            distance++;
        }
    }

    bool contains(K key) const {
        size_t idx = hash(key) & (table.size() - 1);
        uint16_t distance = 0;  // FIXED: Changed from uint8_t to uint16_t

        while (true) {
            if (!table[idx].occupied) return false;
            if (table[idx].key == key) return true;
            if (distance > table[idx].distance) return false;

            idx = (idx + 1) & (table.size() - 1);
            distance++;
        }
    }

    size_t size() const { return num_entries; }
};

struct AggResult {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const AggResult& other) const {
        if (std::abs(revenue - other.revenue) > 1e-6) return revenue > other.revenue;
        return o_orderdate < other.o_orderdate;
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    uint8_t building_code = 4;
    int32_t orderdate_cutoff = 9204;
    int32_t shipdate_cutoff = 9204;

    size_t customer_size = 0, orders_size = 0, lineitem_size = 0;

    const int32_t* c_custkey = (const int32_t*)mmapFile(gendb_dir + "/customer.c_custkey.col", customer_size);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(gendb_dir + "/customer.c_mktsegment.col", customer_size);
    const int32_t* o_orderkey = (const int32_t*)mmapFile(gendb_dir + "/orders.o_orderkey.col", orders_size);
    const int32_t* o_custkey = (const int32_t*)mmapFile(gendb_dir + "/orders.o_custkey.col", orders_size);
    const int32_t* o_orderdate_raw = (const int32_t*)mmapFile(gendb_dir + "/orders.o_orderdate.col", orders_size);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(gendb_dir + "/orders.o_shippriority.col", orders_size);
    const int32_t* l_orderkey = (const int32_t*)mmapFile(gendb_dir + "/lineitem.l_orderkey.col", lineitem_size);
    const double* l_extendedprice = (const double*)mmapFile(gendb_dir + "/lineitem.l_extendedprice.col", lineitem_size);
    const double* l_discount = (const double*)mmapFile(gendb_dir + "/lineitem.l_discount.col", lineitem_size);
    const int32_t* l_shipdate_raw = (const int32_t*)mmapFile(gendb_dir + "/lineitem.l_shipdate.col", lineitem_size);

    size_t num_customers = customer_size / sizeof(int32_t);
    size_t num_orders = orders_size / sizeof(int32_t);
    size_t num_lineitems = lineitem_size / sizeof(int32_t);

    // STEP 1: Decode delta-encoded o_orderdate
    std::vector<int32_t> o_orderdate_decoded(num_orders);
    if (num_orders > 0) {
        o_orderdate_decoded[0] = o_orderdate_raw[0];
        for (size_t i = 1; i < num_orders; ++i) {
            o_orderdate_decoded[i] = o_orderdate_decoded[i - 1] + o_orderdate_raw[i];
        }
    }

    // STEP 2: Decode delta-encoded l_shipdate
    std::vector<int32_t> l_shipdate_decoded(num_lineitems);
    if (num_lineitems > 0) {
        l_shipdate_decoded[0] = l_shipdate_raw[0];
        for (size_t i = 1; i < num_lineitems; ++i) {
            l_shipdate_decoded[i] = l_shipdate_decoded[i - 1] + l_shipdate_raw[i];
        }
    }

    // STEP 3: Build customer hash with compact hash table
    CompactBoolHashTable<int32_t> customer_hash(300000);  // ~300K customers with segment=BUILDING
    for (size_t i = 0; i < num_customers; ++i) {
        if (c_mktsegment[i] == building_code) {
            customer_hash.insert(c_custkey[i]);
        }
    }

    // STEP 4: Build orders hash with compact hash table
    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };
    CompactHashTable<int32_t, OrderInfo> orders_hash(1500000);  // ~1.5M orders
    for (size_t i = 0; i < num_orders; ++i) {
        if (customer_hash.contains(o_custkey[i]) && o_orderdate_decoded[i] < orderdate_cutoff) {
            orders_hash.insert(o_orderkey[i], {o_orderdate_decoded[i], o_shippriority[i]});
        }
    }

    // STEP 5: Morsel-driven parallel processing of lineitem
    struct AggKey {
        int32_t orderkey;
        int32_t order_date;
        int32_t shippriority;

        bool operator==(const AggKey& other) const {
            return orderkey == other.orderkey && order_date == other.order_date && shippriority == other.shippriority;
        }
    };

    struct AggKeyHash {
        size_t operator()(const AggKey& k) const {
            uint64_t h = ((uint64_t)k.orderkey << 32) | (k.shippriority & 0xFFFFFFFFULL);
            h ^= h >> 33;
            h *= 0xff51afd7ed558ccdULL;
            h ^= k.order_date;
            h ^= h >> 33;
            return h;
        }
    };

    // Thread-local aggregation tables
    int num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<AggKey, double, AggKeyHash>> thread_aggregations(num_threads);

    // Morsel-driven work distribution
    std::atomic<size_t> morsel_counter(0);
    static constexpr size_t morsel_size = 100000;  // 100K rows per morsel

    auto worker_thread = [&](int thread_id) {
        std::unordered_map<AggKey, double, AggKeyHash>& local_agg = thread_aggregations[thread_id];

        while (true) {
            size_t start_idx = morsel_counter.fetch_add(morsel_size);
            if (start_idx >= num_lineitems) break;

            size_t end_idx = std::min(start_idx + morsel_size, num_lineitems);

            for (size_t j = start_idx; j < end_idx; ++j) {
                int32_t orderkey = l_orderkey[j];
                int32_t shipdate = l_shipdate_decoded[j];

                // Only proceed if shipdate > cutoff (decode successful!)
                if (shipdate > shipdate_cutoff) {
                    OrderInfo* order_info = orders_hash.find(orderkey);
                    if (order_info != nullptr) {
                        double revenue = l_extendedprice[j] * (1.0 - l_discount[j]);
                        AggKey key = {orderkey, order_info->orderdate, order_info->shippriority};
                        local_agg[key] += revenue;
                    }
                }
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker_thread, i);
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // STEP 6: Merge thread-local aggregations
    std::unordered_map<AggKey, double, AggKeyHash> aggregation = std::move(thread_aggregations[0]);
    for (int i = 1; i < num_threads; ++i) {
        for (auto& entry : thread_aggregations[i]) {
            aggregation[entry.first] += entry.second;
        }
    }

    // STEP 7: Convert to results and sort
    std::vector<AggResult> results;
    for (auto& entry : aggregation) {
        results.push_back({entry.first.orderkey, entry.second, entry.first.order_date, entry.first.shippriority});
    }

    std::sort(results.begin(), results.end());

    if (results.size() > 10) results.resize(10);

    // STEP 8: Output results
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
