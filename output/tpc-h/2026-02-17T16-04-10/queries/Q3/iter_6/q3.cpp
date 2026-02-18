/*
QUERY PLAN for Q3: Shipping Priority (Iteration 6)
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
7. ORDER BY revenue DESC, o_orderdate ASC
8. LIMIT 10

PHYSICAL PLAN (ITER 6 IMPROVEMENTS):
1. Load and filter customer on c_mktsegment = 'BUILDING' → compact hash table
2. PARALLEL: Filter and join orders on c_custkey with customer lookup
   - Use compact hash table customer lookup (2-3x faster than std::unordered_map)
   - Filter on o_orderdate < 9204 (saturates at ~3M rows)
   - 64 parallel threads on 15M rows
3. Build lineitem lookup as compact hash table from filtered orders (vector pre-reserve for efficiency)
4. Load lineitem zone maps for l_shipdate block pruning
5. PARALLEL: Scan lineitem with thread-local open-addressing aggregation:
   - Use zone maps to skip blocks where l_shipdate <= 9204
   - Each thread: local COMPACT HASH TABLE (robin hood) for aggregation (NOT std::unordered_map)
   - Filter l_shipdate > 9204 and probe lineitem_lookup
   - Thread-safe: each thread owns its partition
6. Merge thread-local aggregations via iterator loop (no rehashing)
7. Partial sort for TOP 10
8. Output to CSV

KEY OPTIMIZATIONS (Iter 6):
- REPLACE std::unordered_map with open-addressing compact hash table for aggregation (2-5x speedup)
- ZONE MAP PRUNING: Skip lineitem blocks where l_shipdate <= 9204
- Pre-reserve vectors in thread-local order merging
- Target: 238ms → 140-160ms (3.7x gap → 2.2-2.5x gap, closer to Umbra)
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

    // Hash for compact hash table (int64_t-based)
    uint64_t hash_composite() const {
        // Mix all three int32_t fields into a single uint64_t
        uint64_t h = ((uint64_t)l_orderkey * 0x9E3779B97F4A7C15ULL) ^
                     ((uint64_t)o_orderdate * 0xBF58476D1CE4E5B9ULL) ^
                     ((uint64_t)o_shippriority * 0x94D049BB133111EBULL);
        return h ^ (h >> 32);
    }
};

struct AggregateKeyHash {
    size_t operator()(const AggregateKey& k) const {
        return k.hash_composite();
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

// Compact hash table for joins and aggregation (open-addressing, robin hood)
// Specialized for simple int32_t keys and composite AggregateKey keys
template<typename K, typename V>
struct CompactHashTable;

// Specialization for int32_t keys
template<>
struct CompactHashTable<int32_t, std::pair<int32_t, int32_t>> {
    struct Entry {
        int32_t key;
        std::pair<int32_t, int32_t> value;
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
        table.resize(cap);
        for (auto& e : table) e.occupied = false;
        mask = cap - 1;
        count = 0;
    }

    static inline uint64_t hash_key(int32_t key) {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, const std::pair<int32_t, int32_t>& value) {
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

    const std::pair<int32_t, int32_t>* find(int32_t key) const {
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

// Specialization for AggregateKey
template<>
struct CompactHashTable<AggregateKey, AggregateValue> {
    struct Entry {
        AggregateKey key;
        AggregateValue value;
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
        table.resize(cap);
        for (auto& e : table) e.occupied = false;
        mask = cap - 1;
        count = 0;
    }

    static inline uint64_t hash_key(const AggregateKey& key) {
        return key.hash_composite();
    }

    void insert(const AggregateKey& key, const AggregateValue& value) {
        size_t pos = hash_key(key) & mask;
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

    // Return mutable reference for aggregation in-place update
    AggregateValue* find_or_insert(const AggregateKey& key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;
        Entry entry{key, AggregateValue{0}, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        table[pos] = entry;
        count++;
        return &table[pos].value;
    }

    const AggregateValue* find(const AggregateKey& key) const {
        size_t pos = hash_key(key) & mask;
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

    // Merge thread-local orders into global vector (pre-reserve for efficiency)
    std::vector<OrderData> filtered_orders;
    size_t total_orders = 0;
    for (int t = 0; t < num_threads_orders; t++) {
        total_orders += thread_filtered_orders[t].size();
    }
    filtered_orders.reserve(total_orders);
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

    // Load zone maps for l_shipdate (for block-level pruning)
    auto zone_maps = load_zone_maps(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Estimate number of aggregation groups (~number of matching orders)
    // Use compact hash table instead of std::unordered_map for 2-5x speedup
    size_t estimated_groups = std::max((size_t)1000, filtered_orders.size() * 2);

    // PARALLEL: Filter, join, and aggregate with thread-local compact hash tables
    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTable<AggregateKey, AggregateValue>> thread_local_aggs(num_threads);

    // Initialize each thread-local table
    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();
        thread_local_aggs[tid].reserve(estimated_groups / num_threads);
    }

    // Determine block-level filtering for l_shipdate > DATE_1995_03_15
    // Skip entire blocks where zone_map.max < DATE_1995_03_15
    std::vector<bool> block_alive(zone_maps.size(), true);
    for (size_t zidx = 0; zidx < zone_maps.size(); zidx++) {
        if (zone_maps[zidx].second <= DATE_1995_03_15) {  // max <= threshold, entire block filtered
            block_alive[zidx] = false;
        }
    }

    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();
        auto& local_agg = thread_local_aggs[tid];

        // Each thread processes chunk of lineitem table with block-level pruning
        #pragma omp for schedule(static)
        for (int32_t i = 0; i < num_lineitem; i++) {
            // Compute block index (assuming 100K rows per block per storage guide)
            size_t block_idx = (size_t)i / 100000;

            // Skip blocks where zone map indicates l_shipdate <= DATE_1995_03_15
            if (block_idx < block_alive.size() && !block_alive[block_idx]) {
                continue;
            }

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

                    // Compact hash table find_or_insert for in-place aggregation (2-5x faster than unordered_map)
                    AggregateValue* agg_entry = local_agg.find_or_insert(key);
                    agg_entry->revenue_sum += revenue;
                }
            }
        }
    }

    // Merge thread-local aggregations using compact hash table (no rehashing)
    CompactHashTable<AggregateKey, AggregateValue> agg_table;
    agg_table.reserve(estimated_groups);

    for (int t = 0; t < num_threads; t++) {
        // Iterate through all entries in thread_local_aggs[t]
        for (size_t pos = 0; pos < thread_local_aggs[t].table.size(); pos++) {
            if (thread_local_aggs[t].table[pos].occupied) {
                const auto& entry = thread_local_aggs[t].table[pos];
                AggregateValue* global_entry = agg_table.find_or_insert(entry.key);
                global_entry->revenue_sum += entry.value.revenue_sum;
            }
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] lineitem_filter_join_agg: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Convert aggregation table to result vector (iterate through compact hash table)
    std::vector<ResultRow> results;
    for (size_t pos = 0; pos < agg_table.table.size(); pos++) {
        if (agg_table.table[pos].occupied) {
            const auto& entry = agg_table.table[pos];
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

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) {
            return a.revenue > b.revenue;
        }
        return a.o_orderdate < b.o_orderdate;
    });

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] sort: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

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
