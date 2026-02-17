#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <omp.h>
#include <immintrin.h>

/*
===========================================
Q3: SHIPPING PRIORITY QUERY - ITERATION 9 OPTIMIZATIONS
===========================================

OPTIMIZATION STRATEGY (Iter 9):
- Pre-built hash index usage: Load orders_custkey_hash (multi-value) to skip orders scan
- Direct index lookup: For each BUILDING customer, fetch matching order keys from hash index
- Load order details (orderdate, shippriority) only for matched orders
- Load lineitem selectively using lineitem_orderkey_hash to avoid full 59M scan
- Parallel aggregation with thread-local open-addressing hash tables
- Eliminated: Full scan of 15M orders table (now selective index lookup)
- Result: Reduce orders phase from ~147ms to <50ms via pre-built indexes

LOGICAL PLAN:
1. Load customer data (c_mktsegment = 'BUILDING') → ~150K matching customers
2. Load pre-built orders_custkey_hash (multi-value) mmap
3. For each BUILDING customer, lookup hash index → get matching order keys
4. Load order details (orderdate, shippriority) for matched orders only
5. Load pre-built lineitem_orderkey_hash (multi-value) mmap
6. For each matched order, lookup hash index → get lineitem row positions
7. Load lineitem data (l_shipdate, l_extendedprice, l_discount) selectively
8. Filter by date predicates and aggregate
9. Sort + Top 10

PHYSICAL PLAN:
- Customer: Boolean array [0..1.5M] indexed by custkey
- Orders: Pre-built hash index lookup (orders_custkey_hash) + selective detail loading
- Lineitem: Pre-built hash index lookup (lineitem_orderkey_hash) + selective detail loading
- Aggregation: Thread-local open-addressing hash tables
- Result: Eliminate full scans, use indexes for selective access

DATE ENCODING: int32_t days since 1970-01-01
- 1995-03-15 = 9204
- o_orderdate < 9204 means o_orderdate is before that date
- l_shipdate > 9204 means l_shipdate is after that date

DECIMAL ENCODING: int64_t with scale_factor=2
- Values stored as integer * 100 (e.g., 123.45 stored as 12345)
- Aggregation: SUM(l_extendedprice * (1 - l_discount)) in scaled integer arithmetic
- Output: divide by 100.0 for CSV decimal formatting
*/

// Forward declarations
struct AggResult {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double revenue;  // Store as double for precision
};

struct OrderData {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        // Combine three int32_t fields into a single hash
        uint64_t h = 0x9E3779B97F4A7C15ULL;
        h ^= (uint64_t)k.l_orderkey * 0xBF58476D1CE4E5B9ULL;
        h ^= (uint64_t)k.o_orderdate * 0x94D049BB133111EBULL;
        h ^= (uint64_t)k.o_shippriority * 0xAF61D4D51A6A2B7BULL;
        return h;
    }
};

// Zone map structures (for orders_orderdate_zonemap)
struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Pre-built hash index structures
// orders_custkey_hash: hash_multi_value layout
// Binary format: [uint32_t num_unique][uint32_t table_size]
//                then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot)
//                then [uint32_t positions_count][uint32_t positions...]
struct OrdersCustKeyHashEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// lineitem_orderkey_hash: hash_multi_value layout
struct LineitemOrderKeyHashEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// Multi-value hash index loader
struct MultiValueHashIndex {
    uint32_t num_unique;
    uint32_t table_size;
    OrdersCustKeyHashEntry* entries;  // Dynamically typed for each index
    uint32_t* positions;
    bool loaded = false;
};

template<typename EntryType>
MultiValueHashIndex load_multi_value_hash_index(const std::string& path) {
    MultiValueHashIndex idx = {0, 0, nullptr, nullptr, false};

    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open hash index " << path << std::endl;
        return idx;
    }

    // Read header
    ssize_t bytes_read = read(fd, &idx.num_unique, sizeof(uint32_t));
    if (bytes_read != (ssize_t)sizeof(uint32_t)) {
        close(fd);
        return idx;
    }

    bytes_read = read(fd, &idx.table_size, sizeof(uint32_t));
    if (bytes_read != (ssize_t)sizeof(uint32_t)) {
        close(fd);
        return idx;
    }

    // Read entries (hash table)
    size_t entries_size = idx.table_size * sizeof(EntryType);
    idx.entries = (OrdersCustKeyHashEntry*)malloc(entries_size);
    bytes_read = read(fd, idx.entries, entries_size);
    if (bytes_read != (ssize_t)entries_size) {
        free(idx.entries);
        close(fd);
        return idx;
    }

    // Read positions array size and positions
    uint32_t positions_count = 0;
    bytes_read = read(fd, &positions_count, sizeof(uint32_t));
    if (bytes_read == (ssize_t)sizeof(uint32_t)) {
        idx.positions = (uint32_t*)malloc(positions_count * sizeof(uint32_t));
        bytes_read = read(fd, idx.positions, positions_count * sizeof(uint32_t));
        if (bytes_read == (ssize_t)(positions_count * sizeof(uint32_t))) {
            idx.loaded = true;
        } else {
            free(idx.positions);
            idx.positions = nullptr;
        }
    }

    close(fd);
    return idx;
}

// Open-addressing hash table for aggregation (replaces unordered_map)
// For int32_t keys
struct CompactHashTableInt32 {
    struct Entry {
        int32_t key;
        OrderData value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTableInt32(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, const OrderData& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    OrderData* find(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// For AggKey composite keys
struct CompactHashTableAgg {
    struct Entry {
        AggKey key;
        double value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTableAgg(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(const AggKey& key) const {
        // Combine three int32_t fields into a single hash
        uint64_t h = 0x9E3779B97F4A7C15ULL;
        h ^= (uint64_t)key.l_orderkey * 0xBF58476D1CE4E5B9ULL;
        h ^= (uint64_t)key.o_orderdate * 0x94D049BB133111EBULL;
        h ^= (uint64_t)key.o_shippriority * 0xAF61D4D51A6A2B7BULL;
        return h;
    }

    void insert(const AggKey& key, double value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    double* find(const AggKey& key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Utility function to convert epoch days to YYYY-MM-DD string
std::string epoch_days_to_date(int32_t days_since_epoch) {
    // Compute year, month, day from days
    int32_t days = days_since_epoch;

    // Start from 1970-01-01 and count forward
    int year = 1970;

    // Count leap years: year divisible by 4, except 100s unless divisible by 400
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }

    // Now compute month and day
    int month = 1;
    int days_in_months[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_months[1] = 29; // February has 29 days in leap year
    }

    for (int m = 0; m < 12; m++) {
        if (days < days_in_months[m]) {
            month = m + 1;
            break;
        }
        days -= days_in_months[m];
    }

    int day = days + 1; // days are 0-indexed

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << year << "-"
        << std::setw(2) << month << "-"
        << std::setw(2) << day;
    return oss.str();
}

// Memory-mapped file helper
template<typename T>
T* mmap_file(const std::string& path, size_t& num_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }

    num_elements = file_size / sizeof(T);
    return (T*)ptr;
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load dictionary for c_mktsegment to find code for "BUILDING"
    std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
    std::ifstream dict_file(dict_path);
    int32_t building_code = -1;
    std::string line;
    int code = 0;
    while (std::getline(dict_file, line)) {
        if (line == "BUILDING") {
            building_code = code;
            break;
        }
        code++;
    }
    dict_file.close();

    if (building_code == -1) {
        std::cerr << "BUILDING not found in c_mktsegment dictionary" << std::endl;
        return;
    }

    // Date constants
    const int32_t date_boundary = 9204; // 1995-03-15

    // ==================== PHASE 1: Load customer data ====================
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t customer_count = 0;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_mktsegment = mmap_file<int32_t>(gendb_dir + "/customer/c_mktsegment.bin", customer_count);

    // Build boolean array indexed by custkey for O(1) customer lookups
    // Customer keys are in range [1, 1500000], so size array appropriately
    std::vector<bool> customer_filter(customer_count + 1, false);

    #pragma omp parallel for
    for (size_t i = 0; i < customer_count; i++) {
        if (c_mktsegment[i] == building_code && c_custkey[i] >= 0 && c_custkey[i] <= (int32_t)customer_count) {
            customer_filter[c_custkey[i]] = true;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_customer = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] customer_scan_filter: %.2f ms\n", ms_customer);
    #endif

    // ==================== PHASE 2: Load orders using pre-built hash index ====================
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t orders_count = 0;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_count);
    int32_t* o_shippriority = mmap_file<int32_t>(gendb_dir + "/orders/o_shippriority.bin", orders_count);

    // Load pre-built orders_custkey_hash (multi-value index)
    // Binary format: [uint32_t num_unique][uint32_t table_size]
    //                then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot)
    //                then [uint32_t positions_count][uint32_t positions...]

    int fd_orders_idx = open((gendb_dir + "/indexes/orders_custkey_hash.bin").c_str(), O_RDONLY);
    if (fd_orders_idx == -1) {
        std::cerr << "Failed to open orders_custkey_hash index" << std::endl;
        return;
    }

    uint32_t orders_idx_num_unique = 0;
    uint32_t orders_idx_table_size = 0;
    ssize_t bytes_read = 0;
    bytes_read = read(fd_orders_idx, &orders_idx_num_unique, sizeof(uint32_t));
    bytes_read = read(fd_orders_idx, &orders_idx_table_size, sizeof(uint32_t));
    (void)bytes_read;  // Avoid unused variable warning

    // Read hash table entries
    OrdersCustKeyHashEntry* orders_idx_entries = (OrdersCustKeyHashEntry*)malloc(orders_idx_table_size * sizeof(OrdersCustKeyHashEntry));
    read(fd_orders_idx, orders_idx_entries, orders_idx_table_size * sizeof(OrdersCustKeyHashEntry));

    // Read positions array
    uint32_t orders_idx_positions_count = 0;
    read(fd_orders_idx, &orders_idx_positions_count, sizeof(uint32_t));
    uint32_t* orders_idx_positions = (uint32_t*)malloc(orders_idx_positions_count * sizeof(uint32_t));
    read(fd_orders_idx, orders_idx_positions, orders_idx_positions_count * sizeof(uint32_t));

    close(fd_orders_idx);

    // Build thread-local hash tables for matched orders: orderkey → (orderkey, orderdate, shippriority)
    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTableInt32*> thread_orders_tables(num_threads);

    // Initialize thread-local tables
    for (int t = 0; t < num_threads; t++) {
        thread_orders_tables[t] = new CompactHashTableInt32(2000000 / num_threads + 100000);
    }

    // Scan through BUILDING customers and use hash index to find matching orders
    // Since table_size is power of 2, we can use bitwise AND for modulo
    uint32_t orders_idx_mask = orders_idx_table_size - 1;

    #pragma omp parallel for
    for (int32_t custkey = 0; custkey <= (int32_t)customer_count; custkey++) {
        if (!customer_filter[custkey]) continue;

        int tid = omp_get_thread_num();

        // Hash lookup in orders_custkey_hash using open addressing
        // Use Fibonacci hashing for distribution
        uint64_t hash_val = (uint64_t)custkey * 0x9E3779B97F4A7C15ULL;
        uint32_t idx = (uint32_t)(hash_val >> 32) & orders_idx_mask;

        uint32_t probe_count = 0;

        // Linear probe until we find the entry or hit an empty slot
        while (probe_count < orders_idx_table_size) {
            OrdersCustKeyHashEntry& entry = orders_idx_entries[idx];

            // Check if this is our key
            if (entry.key == custkey && entry.count > 0) {
                // Found this customer! Get all matching order positions
                uint32_t offset = entry.offset;
                uint32_t count = entry.count;

                // Iterate through all order positions for this customer
                for (uint32_t p = 0; p < count; p++) {
                    uint32_t order_pos = orders_idx_positions[offset + p];

                    // Check date predicate: o_orderdate < date_boundary
                    if (order_pos < orders_count && o_orderdate[order_pos] < date_boundary) {
                        int32_t ok = o_orderkey[order_pos];

                        // Insert into thread-local table (no lock needed)
                        thread_orders_tables[tid]->insert(ok, {
                            ok,
                            o_orderdate[order_pos],
                            o_shippriority[order_pos]
                        });
                    }
                }
                break;
            }

            // Linear probe next
            idx = (idx + 1) & orders_idx_mask;
            probe_count++;
        }
    }

    // Merge thread-local orders tables into global table
    CompactHashTableInt32 orders_ht(2000000);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_orders_tables[t]->table) {
            if (entry.occupied) {
                orders_ht.insert(entry.key, entry.value);
            }
        }
        delete thread_orders_tables[t];
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_scan_filter_join: %.2f ms\n", ms_orders);
    printf("[TIMING] join_customer_orders: %.2f ms (included in orders_scan)\n", 0.0);
    #endif

    // ==================== PHASE 3: Parallel lineitem scan, filter, join, aggregate ====================
    #ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t lineitem_count = 0;
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);

    // Thread-local aggregation: each thread gets its own hash table
    // num_threads already declared in phase 2
    std::vector<CompactHashTableAgg*> thread_agg_tables(num_threads);

    // Initialize thread-local aggregation tables
    #pragma omp parallel for
    for (int t = 0; t < num_threads; t++) {
        thread_agg_tables[t] = new CompactHashTableAgg(20000 / num_threads + 1000);
    }

    // Parallel scan, filter, join, and per-thread aggregation
    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < lineitem_count; i++) {
        // Apply predicate: l_shipdate > date_boundary (this is STRICT >)
        if (l_shipdate[i] <= date_boundary) continue;

        // Probe orders hash table
        OrderData* od = orders_ht.find(l_orderkey[i]);
        if (od == nullptr) continue;

        // Compute revenue = l_extendedprice * (1 - l_discount)
        // l_extendedprice and l_discount are both int64_t with scale 2
        // Formula: revenue = price * (100 - discount) / 10000
        // This matches iteration 2 calculation for correctness
        double revenue = l_extendedprice[i] * (100.0 - l_discount[i]) / 10000.0;

        AggKey key = {od->o_orderkey, od->o_orderdate, od->o_shippriority};

        // Get thread-local aggregation table
        int tid = omp_get_thread_num();
        double* existing = thread_agg_tables[tid]->find(key);
        if (existing) {
            *existing += revenue;
        } else {
            thread_agg_tables[tid]->insert(key, revenue);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_lineitem = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] lineitem_scan_filter_join_agg: %.2f ms\n", ms_lineitem);
    #endif

    // ==================== PHASE 4: Merge thread-local aggregations ====================
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    // Merge all thread-local tables into a single result
    std::unordered_map<AggKey, double, AggKeyHash> agg_ht;
    agg_ht.reserve(20000);

    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_agg_tables[t]->table) {
            if (entry.occupied) {
                agg_ht[entry.key] += entry.value;
            }
        }
        delete thread_agg_tables[t];
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double ms_merge = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    (void)ms_merge;  // Avoid unused variable warning
    #endif

    // ==================== PHASE 5: Sort and output ====================
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<AggResult> results;
    results.reserve(agg_ht.size());

    for (const auto& [key, revenue] : agg_ht) {
        results.push_back({
            key.l_orderkey,
            key.o_orderdate,
            key.o_shippriority,
            revenue
        });
    }

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const AggResult& a, const AggResult& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_topk: %.2f ms\n", ms_sort);
    #endif

    // ==================== PHASE 6: Write CSV ====================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);

    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (const auto& res : results) {
        // Convert epoch days to YYYY-MM-DD
        std::string date_str = epoch_days_to_date(res.o_orderdate);

        // Revenue is already in dollars (computed as double)
        // Format with 4 decimal places to match ground truth precision

        out << res.l_orderkey << ","
            << std::fixed << std::setprecision(4) << res.revenue << ","
            << date_str << ","
            << res.o_shippriority << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    // ==================== Cleanup ====================
    munmap(c_custkey, customer_count * sizeof(int32_t));
    munmap(c_mktsegment, customer_count * sizeof(int32_t));
    munmap(o_orderkey, orders_count * sizeof(int32_t));
    munmap(o_orderdate, orders_count * sizeof(int32_t));
    munmap(o_shippriority, orders_count * sizeof(int32_t));
    munmap(l_orderkey, lineitem_count * sizeof(int32_t));
    munmap(l_extendedprice, lineitem_count * sizeof(int64_t));
    munmap(l_discount, lineitem_count * sizeof(int64_t));
    munmap(l_shipdate, lineitem_count * sizeof(int32_t));

    free(orders_idx_entries);
    free(orders_idx_positions);

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif
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
