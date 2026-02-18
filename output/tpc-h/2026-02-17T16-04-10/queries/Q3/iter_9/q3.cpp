/*
QUERY PLAN for Q3: Shipping Priority (Iteration 9)
===================================================

SQL:
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND
      l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND
      l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate LIMIT 10

LOGICAL PLAN:
1. Filter customer: c_mktsegment = 'BUILDING' → ~300K rows
2. Filter orders: o_orderdate < 1995-03-15 (date = 9204 days)
3. Filter lineitem: l_shipdate > 1995-03-15 (date = 9204 days)
4. Join customer → orders on c_custkey = o_custkey
5. Join result → lineitem on o_orderkey = l_orderkey
6. GROUP BY (l_orderkey, o_orderdate, o_shippriority) with SUM(revenue)
7. ORDER BY revenue DESC, o_orderdate ASC LIMIT 10

PHYSICAL PLAN (ITER 9 IMPROVEMENTS - ZONE MAP + OPEN-ADDRESSING):
1. Load and filter customer on c_mktsegment = 'BUILDING' → compact hash table
2. PARALLEL: Filter and join orders with customer using compact hash table lookup
3. Build order_lookup as compact hash table from filtered orders
4. Load lineitem zone maps for l_shipdate block pruning (skip blocks where max <= 9204)
5. PARALLEL: Scan lineitem with zone map pruning and open-addressing aggregation:
   - Skip blocks using zone maps (coarse filter)
   - Filter l_shipdate > 9204 within remaining blocks (fine filter)
   - Probe order_lookup for matching orders
   - Thread-local open-addressing aggregation (2-5x faster than std::unordered_map)
6. Merge thread-local aggregations into global open-addressing table
7. Partial sort (std::partial_sort) for TOP 10 instead of full sort
8. Output to CSV

KEY OPTIMIZATIONS (Iter 9):
- Zone map pruning on l_shipdate: Skip entire blocks where max < DATE_1995_03_15
- Replace std::unordered_map with open-addressing hash table (robin hood)
- Use std::partial_sort for Top-K (O(n log K) vs O(n log n))
- Reduce lineitem_filter_join_agg from 67ms to ~40-45ms target
- Target: 3.7x gap → 2.0-2.5x (approaching Umbra)
*/

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <atomic>

// ============================================================================
// Helper structures
// ============================================================================

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

// Hash function for AggregateKey (used in open-addressing hash table)
struct AggregateKeyHashFunc {
    uint64_t operator()(const AggregateKey& k) const {
        // Combine all three fields into a 64-bit hash using FNV-like mixing
        uint64_t h = ((uint64_t)k.l_orderkey * 0x9E3779B97F4A7C15ULL) ^
                     ((uint64_t)k.o_orderdate * 0xBF58476D1CE4E5B9ULL) ^
                     ((uint64_t)k.o_shippriority * 0x94D049BB133111EBULL);
        return h ^ (h >> 32);
    }
};

struct AggregateValue {
    int64_t revenue_sum;
};

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Open-addressing hash table with robin hood hashing (faster than std::unordered_map)
// Used for aggregation with high cardinality (~5M distinct groups)
template<typename K, typename V, typename HashFunc>
struct OpenAddressingHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count;
    static constexpr float load_factor = 0.75f;
    HashFunc hash_func;

    OpenAddressingHashTable() : mask(0), count(0), hash_func() {}

    void reserve(size_t expected) {
        size_t cap = 1;
        // Aim for load factor of 0.75
        size_t desired = (size_t)(expected / load_factor);
        while (cap < desired) cap <<= 1;

        table.clear();
        table.resize(cap, {K(), V(), 0, false});
        mask = cap - 1;
        count = 0;
    }

    void insert(K key, const V& value) {
        if (count >= (size_t)(table.size() * load_factor)) {
            // Rehash if load factor exceeded
            auto old_table = std::move(table);
            table.clear();
            size_t cap = old_table.size() * 2;
            table.resize(cap, {K(), V(), 0, false});
            mask = cap - 1;
            count = 0;

            for (const auto& entry : old_table) {
                if (entry.occupied) {
                    insert_internal(entry.key, entry.value);
                }
            }
            insert_internal(key, value);
            return;
        }
        insert_internal(key, value);
    }

    void insert_internal(K key, const V& value) {
        uint64_t h = hash_func(key);
        size_t pos = (h >> 32) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    V* find(K key) {
        uint64_t h = hash_func(key);
        size_t pos = (h >> 32) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    size_t size() const { return count; }
};


// Compact hash table for order lookup (orderkey -> (orderdate, shippriority))
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashTable() : mask(0), count(0) {}

    void reserve(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.clear();
        table.resize(cap, {0, V(), 0, false});
        mask = cap - 1;
        count = 0;
    }

    static inline uint64_t hash_key(K key) {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, const V& value) {
        size_t pos = (hash_key(key) >> 32) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    const V* find(K key) const {
        size_t pos = (hash_key(key) >> 32) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    size_t size() const { return count; }
};

// ============================================================================
// Helper functions
// ============================================================================

// Load binary file via mmap
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open file: " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < 0) {
        close(fd);
        return nullptr;
    }

    size = (size_t)file_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        return nullptr;
    }

    return ptr;
}

// Load dictionary (format: "code=value\n")
std::unordered_map<std::string, int32_t> load_dict(const std::string& path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(path);
    if (!f.is_open()) {
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[value] = code;
        }
    }
    return dict;
}

// Convert epoch days (int32_t) to YYYY-MM-DD
std::string epoch_days_to_date(int32_t days) {
    // Leap year check
    auto is_leap = [](int y) { return (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0); };

    int year = 1970;
    int day = days + 1;  // 0-indexed to 1-indexed

    // Advance by years
    while (true) {
        int days_in_year = is_leap(year) ? 366 : 365;
        if (day <= days_in_year) break;
        day -= days_in_year;
        year++;
    }

    // Days in each month (non-leap)
    const int daysInMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int month = 1;
    int feb_days = is_leap(year) ? 29 : 28;

    while (true) {
        int days_in_month = (month == 2) ? feb_days : daysInMonth[month];
        if (day <= days_in_month) break;
        day -= days_in_month;
        month++;
    }

    char buf[20];
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wformat-truncation"
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    #pragma GCC diagnostic pop
    return std::string(buf);
}

// Load zone maps from file
std::vector<std::pair<int32_t, int32_t>> load_zone_maps(const std::string& path) {
    std::vector<std::pair<int32_t, int32_t>> zone_maps;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return zone_maps;

    uint32_t num_blocks;
    if (read(fd, &num_blocks, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        return zone_maps;
    }

    zone_maps.reserve(num_blocks);
    for (uint32_t i = 0; i < num_blocks; i++) {
        int32_t min_val, max_val;
        if (read(fd, &min_val, sizeof(int32_t)) != sizeof(int32_t) ||
            read(fd, &max_val, sizeof(int32_t)) != sizeof(int32_t)) {
            break;
        }
        zone_maps.push_back({min_val, max_val});
    }
    close(fd);
    return zone_maps;
}

// ============================================================================
// Q3 Implementation
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Date constants (epoch days)
    const int32_t DATE_1995_03_15 = 9204;

    // Load customer data
    size_t customer_size = 0;
    int32_t* c_custkey = (int32_t*)mmap_file(gendb_dir + "/customer/c_custkey.bin", customer_size);
    int32_t num_customer = customer_size / sizeof(int32_t);

    size_t c_mktseg_size = 0;
    int32_t* c_mktsegment = (int32_t*)mmap_file(gendb_dir + "/customer/c_mktsegment.bin", c_mktseg_size);

    auto c_dict = load_dict(gendb_dir + "/customer/c_mktsegment_dict.txt");
    int32_t building_code = -1;
    for (const auto& p : c_dict) {
        if (p.first == "BUILDING") {
            building_code = p.second;
            break;
        }
    }

    if (building_code == -1) {
        std::cerr << "BUILDING code not found" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Filter customer
    std::vector<int32_t> filtered_custkeys;
    for (int32_t i = 0; i < num_customer; i++) {
        if (c_mktsegment[i] == building_code) {
            filtered_custkeys.push_back(c_custkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] customer_filter: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Build customer lookup with compact hash table (faster than std::unordered_map)
    typedef std::pair<int32_t, int32_t> DummyVal;  // unused, just for type safety
    CompactHashTable<int32_t, DummyVal> customer_lookup;
    customer_lookup.reserve(filtered_custkeys.size());
    for (int32_t key : filtered_custkeys) {
        customer_lookup.insert(key, {0, 0});
    }

    // Load orders
    size_t orders_size = 0;
    int32_t* o_custkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_custkey.bin", orders_size);
    int32_t num_orders = orders_size / sizeof(int32_t);

    size_t order_date_size = 0;
    int32_t* o_orderdate = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderdate.bin", order_date_size);

    size_t order_key_size = 0;
    int32_t* o_orderkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", order_key_size);

    size_t order_shipprio_size = 0;
    int32_t* o_shippriority = (int32_t*)mmap_file(gendb_dir + "/orders/o_shippriority.bin", order_shipprio_size);

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // PARALLEL: Filter and join orders with customer using compact hash table lookup
    struct OrderData {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };

    // Thread-safe vector collection for filtered orders
    int num_threads_orders = omp_get_max_threads();
    std::vector<std::vector<OrderData>> thread_filtered_orders(num_threads_orders);

    #pragma omp parallel num_threads(num_threads_orders)
    {
        int tid = omp_get_thread_num();
        auto& local_orders = thread_filtered_orders[tid];
        local_orders.reserve(100000);  // pre-allocate to reduce reallocations

        // Each thread processes chunk of orders table
        #pragma omp for schedule(static)
        for (int32_t i = 0; i < num_orders; i++) {
            if (o_orderdate[i] < DATE_1995_03_15) {
                // Compact hash table lookup is thread-safe for read operations
                if (customer_lookup.find(o_custkey[i]) != nullptr) {
                    OrderData od;
                    od.orderkey = o_orderkey[i];
                    od.orderdate = o_orderdate[i];
                    od.shippriority = o_shippriority[i];
                    local_orders.push_back(od);
                }
            }
        }
    }

    // Merge thread-local orders into global vector
    std::vector<OrderData> filtered_orders;
    for (int t = 0; t < num_threads_orders; t++) {
        filtered_orders.insert(filtered_orders.end(),
                               thread_filtered_orders[t].begin(),
                               thread_filtered_orders[t].end());
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] orders_filter_join: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Build order lookup with compact hash table
    typedef std::pair<int32_t, int32_t> OrderInfo;  // (orderdate, shippriority)
    CompactHashTable<int32_t, OrderInfo> order_lookup;
    order_lookup.reserve(filtered_orders.size());
    for (const auto& od : filtered_orders) {
        order_lookup.insert(od.orderkey, {od.orderdate, od.shippriority});
    }

    // Load lineitem
    size_t lineitem_size = 0;
    int32_t* l_orderkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_size);
    int32_t num_lineitem = lineitem_size / sizeof(int32_t);

    size_t shipdate_size = 0;
    int32_t* l_shipdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size);

    size_t extprice_size = 0;
    int64_t* l_extendedprice = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", extprice_size);

    size_t discount_size = 0;
    int64_t* l_discount = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin", discount_size);

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Load zone maps for lineitem l_shipdate to prune blocks
    auto zone_maps = load_zone_maps(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

    // Build block skip list: zone_maps[block_id] = (min_val, max_val)
    // Skip block if max_val <= DATE_1995_03_15
    std::vector<bool> skip_block(zone_maps.size(), false);
    for (size_t z = 0; z < zone_maps.size(); z++) {
        if (zone_maps[z].second <= DATE_1995_03_15) {
            skip_block[z] = true;
        }
    }

    // PARALLEL: Filter, join, and aggregate with thread-local open-addressing aggregation
    // Use open-addressing hash table instead of std::unordered_map (2-5x faster)
    int num_threads = omp_get_max_threads();
    std::vector<OpenAddressingHashTable<AggregateKey, AggregateValue, AggregateKeyHashFunc>> thread_local_aggs(num_threads);

    // Pre-allocate each thread's aggregation table (estimated 5M groups / num_threads per thread)
    for (int t = 0; t < num_threads; t++) {
        thread_local_aggs[t].reserve(60000000 / num_threads);
    }

    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();
        auto& local_agg = thread_local_aggs[tid];

        // Each thread processes chunk of lineitem table
        #pragma omp for schedule(static)
        for (int32_t i = 0; i < num_lineitem; i++) {
            // Zone map coarse filter: compute block index
            // Block size: 100,000 rows per block (from Storage Guide)
            const uint32_t BLOCK_SIZE = 100000;
            uint32_t block_id = i / BLOCK_SIZE;

            // Skip this row if its block is pruned
            if (block_id < skip_block.size() && skip_block[block_id]) {
                continue;
            }

            // Fine filter: check actual shipdate
            if (l_shipdate[i] > DATE_1995_03_15) {
                // Use read-only access to order_lookup (thread-safe read from compact hash table)
                const OrderInfo* order_info = order_lookup.find(l_orderkey[i]);
                if (order_info != nullptr) {
                    // Compute revenue: l_extendedprice * (1 - l_discount)
                    // l_extendedprice is int64_t scaled by 100 (e.g., 3307894 means 33078.94)
                    // l_discount is int64_t scaled by 100 (e.g., 4 means 0.04)
                    // Revenue = (extprice/100) * (1 - discount/100)
                    //         = (extprice/100) * ((100 - discount)/100)
                    //         = extprice * (100 - discount) / 10000
                    // We store as int64_t with no additional scaling to preserve precision
                    int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);

                    AggregateKey key;
                    key.l_orderkey = l_orderkey[i];
                    key.o_orderdate = order_info->first;
                    key.o_shippriority = order_info->second;

                    AggregateValue* found = local_agg.find(key);
                    if (found) {
                        found->revenue_sum += revenue;
                    } else {
                        local_agg.insert(key, {revenue});
                    }
                }
            }
        }
    }

    // Merge thread-local aggregations into global open-addressing table
    // Estimate final cardinality (~5M groups)
    OpenAddressingHashTable<AggregateKey, AggregateValue, AggregateKeyHashFunc> agg_table;
    agg_table.reserve(5000000);

    for (int t = 0; t < num_threads; t++) {
        for (size_t i = 0; i < thread_local_aggs[t].table.size(); i++) {
            const auto& entry = thread_local_aggs[t].table[i];
            if (entry.occupied) {
                auto* existing = agg_table.find(entry.key);
                if (existing) {
                    existing->revenue_sum += entry.value.revenue_sum;
                } else {
                    agg_table.insert(entry.key, entry.value);
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] lineitem_filter_join_agg: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Convert aggregation table to result vector
    std::vector<ResultRow> results;
    results.reserve(agg_table.size());

    for (size_t i = 0; i < agg_table.table.size(); i++) {
        const auto& entry = agg_table.table[i];
        if (entry.occupied) {
            ResultRow row;
            row.l_orderkey = entry.key.l_orderkey;
            row.o_orderdate = entry.key.o_orderdate;
            row.o_shippriority = entry.key.o_shippriority;
            row.revenue = entry.value.revenue_sum;
            results.push_back(row);
        }
    }

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Partial sort for Top 10 (O(n log K) instead of O(n log n))
    // Only sort if we have more than 10 results
    size_t result_limit = std::min((size_t)10, results.size());
    if (results.size() > result_limit) {
        std::partial_sort(results.begin(), results.begin() + result_limit, results.end(),
                         [](const ResultRow& a, const ResultRow& b) {
                             if (a.revenue != b.revenue) {
                                 return a.revenue > b.revenue;
                             }
                             return a.o_orderdate < b.o_orderdate;
                         });
        results.resize(result_limit);
    } else {
        // Still need to sort the small result set
        std::sort(results.begin(), results.end(),
                 [](const ResultRow& a, const ResultRow& b) {
                     if (a.revenue != b.revenue) {
                         return a.revenue > b.revenue;
                     }
                     return a.o_orderdate < b.o_orderdate;
                 });
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] sort: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Write results to CSV
    std::ofstream out(results_dir + "/Q3.csv");
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (const auto& row : results) {
        // Revenue is extprice * (100 - discount), divide by 10000 to get actual value
        double revenue_decimal = (double)row.revenue / 10000.0;
        std::string date_str = epoch_days_to_date(row.o_orderdate);
        out << std::fixed << row.l_orderkey << ","
            << std::setprecision(4) << revenue_decimal << ","
            << date_str << ","
            << row.o_shippriority << "\n";
    }
    out.close();

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] output: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] total: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count());
    #endif

    // Cleanup
    if (c_custkey) munmap(c_custkey, customer_size);
    if (c_mktsegment) munmap(c_mktsegment, c_mktseg_size);
    if (o_custkey) munmap(o_custkey, orders_size);
    if (o_orderkey) munmap(o_orderkey, order_key_size);
    if (o_orderdate) munmap(o_orderdate, order_date_size);
    if (o_shippriority) munmap(o_shippriority, order_shipprio_size);
    if (l_orderkey) munmap(l_orderkey, lineitem_size);
    if (l_shipdate) munmap(l_shipdate, shipdate_size);
    if (l_extendedprice) munmap(l_extendedprice, extprice_size);
    if (l_discount) munmap(l_discount, discount_size);
}

// ============================================================================
// Main
// ============================================================================

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
